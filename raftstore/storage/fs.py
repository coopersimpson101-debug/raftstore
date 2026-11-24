"""Filesystem abstraction.

Everything durable in raftstore goes through a tiny interface so that tests can
swap the real disk for `FaultyFS`, an in-memory filesystem that models what a
real disk guarantees after power loss (only fsynced bytes and fsynced directory
entries survive; unsynced appends may survive as a torn prefix) and that can be
told to fail like a dying drive: start raising EIO, vanish mid-write, flip bits.

`FaultInjectingFS` wraps the *real* filesystem with the same "pull the drive"
switch so a live process can be failed on demand (see /debug/fault).
"""
from __future__ import annotations

import errno
import os
import posixpath
import random


def _eio(msg: str = "Input/output error (drive missing)") -> OSError:
    return OSError(errno.EIO, msg)


class SimulatedCrash(Exception):
    """Raised by FaultyFS at a configured crash point (power cut)."""


# --------------------------------------------------------------------- real disk
class RealFile:
    def __init__(self, path: str, append: bool):
        self.path = path
        self._f = open(path, "ab" if append else "wb", buffering=0)

    def write(self, data: bytes) -> None:
        view = memoryview(data)
        while view:
            n = self._f.write(view)
            view = view[n:]

    def sync(self) -> None:
        os.fsync(self._f.fileno())

    def tell(self) -> int:
        return self._f.tell()

    def close(self) -> None:
        self._f.close()


class RealFS:
    def join(self, *parts: str) -> str:
        return os.path.join(*parts)

    def dirname(self, p: str) -> str:
        return os.path.dirname(p)

    def makedirs(self, p: str) -> None:
        os.makedirs(p, exist_ok=True)

    def listdir(self, p: str) -> list[str]:
        try:
            return sorted(os.listdir(p))
        except FileNotFoundError:
            return []

    def exists(self, p: str) -> bool:
        return os.path.exists(p)

    def size(self, p: str) -> int:
        return os.path.getsize(p)

    def read(self, p: str) -> bytes:
        with open(p, "rb") as f:
            return f.read()

    def open_write(self, p: str, append: bool = False) -> RealFile:
        return RealFile(p, append)

    def truncate(self, p: str, size: int) -> None:
        with open(p, "r+b") as f:
            f.truncate(size)
            f.flush()
            os.fsync(f.fileno())

    def rename(self, src: str, dst: str) -> None:
        os.replace(src, dst)

    def remove(self, p: str) -> None:
        os.remove(p)

    def sync_dir(self, d: str) -> None:
        if os.name == "nt":
            # NTFS journals metadata and Windows cannot open a directory for fsync.
            return
        fd = os.open(d, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def write_atomic(fs, path: str, data: bytes, sync: bool = True) -> None:
    """temp file + fsync + rename + fsync(dir): readers see old or new, never half."""
    tmp = path + ".tmp"
    f = fs.open_write(tmp)
    try:
        f.write(data)
        if sync:
            f.sync()
    finally:
        f.close()
    fs.rename(tmp, path)
    if sync:
        fs.sync_dir(fs.dirname(path))


# ------------------------------------------------- fault injection over real disk
class _InjectedFile:
    def __init__(self, fs: "FaultInjectingFS", inner: RealFile):
        self.fs, self.inner = fs, inner

    def write(self, data: bytes) -> None:
        self.fs._before_write()
        if self.fs.writes_left is not None:
            if self.fs.writes_left <= 0:
                self.inner.write(data[: len(data) // 2])  # the drive vanished mid-write
                self.fs.pulled = True
                raise _eio()
            self.fs.writes_left -= 1
        self.inner.write(data)

    def sync(self) -> None:
        self.fs._before_write()
        self.inner.sync()

    def tell(self) -> int:
        return self.inner.tell()

    def close(self) -> None:
        self.inner.close()


class FaultInjectingFS(RealFS):
    """RealFS with a 'pull the drive' switch: after N more writes, everything raises EIO."""

    def __init__(self):
        self.pulled = False
        self.writes_left: int | None = None

    def pull(self, after_writes: int = 0) -> None:
        if after_writes <= 0:
            self.pulled = True
        else:
            self.writes_left = after_writes

    def _before_write(self) -> None:
        if self.pulled:
            raise _eio()

    def open_write(self, p, append=False):
        self._before_write()
        return _InjectedFile(self, super().open_write(p, append))

    def read(self, p):
        self._before_write()
        return super().read(p)

    def truncate(self, p, size):
        self._before_write()
        super().truncate(p, size)

    def rename(self, src, dst):
        self._before_write()
        super().rename(src, dst)

    def remove(self, p):
        self._before_write()
        super().remove(p)

    def sync_dir(self, d):
        self._before_write()
        super().sync_dir(d)


# ------------------------------------------------------------ simulated disk
class _Inode:
    __slots__ = ("data", "durable")

    def __init__(self):
        self.data = bytearray()
        self.durable = b""


class _MemFile:
    def __init__(self, fs: "FaultyFS", ino: _Inode):
        self.fs, self.ino, self.closed = fs, ino, False

    def write(self, data: bytes) -> None:
        self.fs._write(self.ino, bytes(data))

    def sync(self) -> None:
        self.fs._sync_point()
        self.ino.durable = bytes(self.ino.data)
        self.fs._after_sync()

    def tell(self) -> int:
        return len(self.ino.data)

    def close(self) -> None:
        self.closed = True


class FaultyFS:
    """In-memory filesystem with a crash-consistency model and fault injection.

    Model (deliberately pessimistic, like ext4 with data=writeback):
      * file contents become durable only on fsync of that file
      * creating / renaming / deleting a name becomes durable only on fsync of the directory
      * on crash, each file keeps its durable bytes plus (optionally) a random
        prefix of the bytes appended after the last fsync -- a torn write
    Faults:
      * crash_at_sync=N: raise SimulatedCrash at the N-th fsync (before or after it takes effect)
      * pull_drive(after_writes=k): the k+1-th write persists a random prefix, then EIO forever
      * corrupt(path, offset): flip bits in place (bit rot), durable copy included
    """

    def __init__(self, seed: int = 0, torn_writes: bool = True):
        self.rng = random.Random(seed)
        self.torn_writes = torn_writes
        self.ns: dict[str, _Inode] = {}
        self.durable_ns: dict[str, _Inode] = {}
        self.dirs: set[str] = {"/"}
        self.dead = False
        self.eio_after_writes: int | None = None
        self.crash_at_sync: int | None = None
        self.crash_after_sync = False
        self.sync_count = 0
        self.write_count = 0

    # path helpers -- always posix style
    @staticmethod
    def _n(p: str) -> str:
        p = p.replace("\\", "/")
        return posixpath.normpath(p if p.startswith("/") else "/" + p)

    def join(self, *parts: str) -> str:
        return self._n(posixpath.join(*[x.replace("\\", "/") for x in parts]))

    def dirname(self, p: str) -> str:
        return posixpath.dirname(self._n(p))

    def _check(self) -> None:
        if self.dead:
            raise _eio()

    def _sync_point(self) -> None:
        self._check()
        self.sync_count += 1
        if self.crash_at_sync is not None and self.sync_count == self.crash_at_sync and not self.crash_after_sync:
            raise SimulatedCrash(f"power cut before fsync #{self.sync_count}")

    def _after_sync(self) -> None:
        if self.crash_at_sync is not None and self.sync_count == self.crash_at_sync and self.crash_after_sync:
            raise SimulatedCrash(f"power cut after fsync #{self.sync_count}")

    def _write(self, ino: _Inode, data: bytes) -> None:
        self._check()
        self.write_count += 1
        if self.eio_after_writes is not None and self.write_count > self.eio_after_writes:
            ino.data += data[: self.rng.randrange(len(data) + 1)]
            self.dead = True
            raise _eio()
        ino.data += data

    # fs interface
    def makedirs(self, p: str) -> None:
        self._check()
        p = self._n(p)
        while p not in self.dirs:
            self.dirs.add(p)
            p = posixpath.dirname(p)

    def listdir(self, p: str) -> list[str]:
        self._check()
        p = self._n(p)
        names = {posixpath.basename(k) for k in self.ns if posixpath.dirname(k) == p}
        names |= {posixpath.basename(d) for d in self.dirs if d != p and posixpath.dirname(d) == p}
        return sorted(names)

    def exists(self, p: str) -> bool:
        self._check()
        p = self._n(p)
        return p in self.ns or p in self.dirs

    def size(self, p: str) -> int:
        return len(self._get(p).data)

    def _get(self, p: str) -> _Inode:
        self._check()
        ino = self.ns.get(self._n(p))
        if ino is None:
            raise FileNotFoundError(errno.ENOENT, "no such file", p)
        return ino

    def read(self, p: str) -> bytes:
        return bytes(self._get(p).data)

    def open_write(self, p: str, append: bool = False) -> _MemFile:
        self._check()
        p = self._n(p)
        if posixpath.dirname(p) not in self.dirs:
            raise FileNotFoundError(errno.ENOENT, "no such directory", p)
        if append and p in self.ns:
            return _MemFile(self, self.ns[p])
        ino = _Inode()
        self.ns[p] = ino
        return _MemFile(self, ino)

    def truncate(self, p: str, size: int) -> None:
        ino = self._get(p)
        self._sync_point()
        del ino.data[size:]
        ino.durable = bytes(ino.data)
        self._after_sync()

    def rename(self, src: str, dst: str) -> None:
        self._check()
        ino = self._get(src)
        del self.ns[self._n(src)]
        self.ns[self._n(dst)] = ino

    def remove(self, p: str) -> None:
        self._get(p)
        del self.ns[self._n(p)]

    def sync_dir(self, d: str) -> None:
        self._sync_point()
        d = self._n(d)
        for k in [k for k in self.durable_ns if posixpath.dirname(k) == d and k not in self.ns]:
            del self.durable_ns[k]
        for k, ino in self.ns.items():
            if posixpath.dirname(k) == d:
                self.durable_ns[k] = ino
        self._after_sync()

    # fault controls
    def crash(self) -> None:
        """Power loss: forget everything that was not made durable."""
        survivors: dict[int, _Inode] = {}
        for ino in self.durable_ns.values():
            if id(ino) in survivors:
                continue
            tail = b""
            if self.torn_writes and bytes(ino.data[: len(ino.durable)]) == ino.durable:
                pending = bytes(ino.data[len(ino.durable):])
                if pending:
                    tail = pending[: self.rng.randrange(len(pending) + 1)]
            ino.data = bytearray(ino.durable + tail)
            ino.durable = bytes(ino.data)
            survivors[id(ino)] = ino
        self.ns = dict(self.durable_ns)
        self.dead = False
        self.eio_after_writes = None
        self.crash_at_sync = None

    def pull_drive(self, after_writes: int = 0) -> None:
        self.eio_after_writes = self.write_count + after_writes

    def reinsert_drive(self) -> None:
        """The same drive comes back: like a crash (volatile data is gone)."""
        self.crash()

    def corrupt(self, p: str, offset: int, xor: int = 0xFF) -> None:
        ino = self.ns[self._n(p)]
        ino.data[offset] ^= xor
        if offset < len(ino.durable):
            d = bytearray(ino.durable)
            d[offset] ^= xor
            ino.durable = bytes(d)

    def files(self) -> list[str]:
        return sorted(self.ns)

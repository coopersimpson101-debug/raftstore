"""Content-addressed chunk store: chunks/<aa>/<sha256 hex>.

A chunk's name *is* its SHA-256, so every read is verifiable and any healthy
replica can repair any other.  Writes are atomic (temp + fsync + rename).
"""
from __future__ import annotations

import hashlib

from .fs import write_atomic


class ChunkMissing(Exception):
    pass


class ChunkCorrupt(Exception):
    pass


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ChunkStore:
    def __init__(self, fs, root: str, sync: bool = True):
        self.fs, self.root, self.sync = fs, root, sync
        fs.makedirs(root)

    def _dir(self, h: str) -> str:
        return self.fs.join(self.root, h[:2])

    def path(self, h: str) -> str:
        return self.fs.join(self._dir(h), h)

    @staticmethod
    def _valid_name(h: str) -> bool:
        return len(h) == 64 and all(c in "0123456789abcdef" for c in h)

    def has(self, h: str) -> bool:
        return self.fs.exists(self.path(h))

    def put(self, h: str, data: bytes) -> bool:
        """Store a chunk; returns False if an intact copy was already present."""
        if not self._valid_name(h) or sha256_hex(data) != h:
            raise ValueError("chunk data does not match its hash")
        if self.verify(h) == "ok":
            return False
        self.fs.makedirs(self._dir(h))
        write_atomic(self.fs, self.path(h), data, sync=self.sync)
        return True

    def get(self, h: str) -> bytes:
        try:
            data = self.fs.read(self.path(h))
        except FileNotFoundError:
            raise ChunkMissing(h) from None
        if sha256_hex(data) != h:
            raise ChunkCorrupt(h)
        return data

    def verify(self, h: str) -> str:
        """'ok' | 'missing' | 'corrupt'"""
        try:
            self.get(h)
            return "ok"
        except ChunkMissing:
            return "missing"
        except ChunkCorrupt:
            return "corrupt"

    def quarantine(self, h: str) -> None:
        """Move a corrupt copy aside (kept for forensics, never served)."""
        qdir = self.fs.join(self.root, "quarantine")
        self.fs.makedirs(qdir)
        n = 0
        while self.fs.exists(self.fs.join(qdir, f"{h}.{n}")):
            n += 1
        self.fs.rename(self.path(h), self.fs.join(qdir, f"{h}.{n}"))
        self.fs.sync_dir(self._dir(h))

    def delete(self, h: str) -> None:
        if self.has(h):
            self.fs.remove(self.path(h))

    def list(self) -> list[str]:
        out = []
        for d in self.fs.listdir(self.root):
            if len(d) != 2:
                continue
            for n in self.fs.listdir(self.fs.join(self.root, d)):
                if self._valid_name(n):
                    out.append(n)
        return sorted(out)

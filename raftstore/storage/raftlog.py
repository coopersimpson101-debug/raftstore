"""Durable Raft state: hard state + log (in the WAL) + snapshots (atomic files)."""
from __future__ import annotations

import json
import struct
import zlib

from ..raft.types import Entry, HardState, Snapshot
from .fs import write_atomic
from .wal import REC_ENTRY, REC_HARDSTATE, REC_RESET, WAL, CorruptLogError, encode_record

_HS = struct.Struct("<QQ")
_ENT = struct.Struct("<QQB")
_RESET = struct.Struct("<QQ")
SNAP_MAGIC = b"RSNAP1\n"


def hs_record(hs: HardState) -> bytes:
    return encode_record(REC_HARDSTATE, _HS.pack(hs.term, hs.commit) + (hs.vote or "").encode())


def entry_record(e: Entry) -> bytes:
    return encode_record(REC_ENTRY, _ENT.pack(e.term, e.index, e.kind) + e.data)


def reset_record(index: int, term: int) -> bytes:
    return encode_record(REC_RESET, _RESET.pack(index, term))


class SnapshotStore:
    """Snapshots as `<index>-<term>.snap` files, each written atomically."""

    def __init__(self, fs, dirpath: str, sync: bool = True):
        self.fs, self.dir, self.sync = fs, dirpath, sync

    def _name(self, s: Snapshot) -> str:
        return f"{s.index:020d}-{s.term:020d}.snap"

    def save(self, s: Snapshot) -> None:
        self.fs.makedirs(self.dir)
        meta = json.dumps({"index": s.index, "term": s.term, "config": s.config}, sort_keys=True).encode()
        body = struct.pack("<I", len(meta)) + meta + s.data
        blob = SNAP_MAGIC + struct.pack("<I", zlib.crc32(body)) + body
        write_atomic(self.fs, self.fs.join(self.dir, self._name(s)), blob, sync=self.sync)

    def _decode(self, blob: bytes) -> Snapshot | None:
        m = len(SNAP_MAGIC)
        if blob[:m] != SNAP_MAGIC or len(blob) < m + 8:
            return None
        (crc,) = struct.unpack_from("<I", blob, m)
        body = blob[m + 4:]
        if zlib.crc32(body) != crc:
            return None
        (ml,) = struct.unpack_from("<I", body, 0)
        meta = json.loads(body[4: 4 + ml])
        return Snapshot(meta["index"], meta["term"], meta["config"], bytes(body[4 + ml:]))

    def names(self) -> list[str]:
        return sorted((n for n in self.fs.listdir(self.dir) if n.endswith(".snap")), reverse=True)

    def load_latest(self) -> tuple[Snapshot | None, list[str]]:
        """Newest snapshot that passes its CRC, plus names of corrupt ones skipped."""
        bad = []
        for n in self.names():
            s = self._decode(self.fs.read(self.fs.join(self.dir, n)))
            if s is not None:
                return s, bad
            bad.append(n)
        return None, bad

    def prune(self, keep: int = 2) -> None:
        for n in self.fs.listdir(self.dir):
            if n.endswith(".tmp"):
                self.fs.remove(self.fs.join(self.dir, n))
        for n in self.names()[keep:]:
            self.fs.remove(self.fs.join(self.dir, n))


class DiskStorage:
    """Raft storage on a (possibly fault-injecting) filesystem.

    Write ordering inside one save():  RESET(snapshot)  ->  ENTRIES  ->  HARDSTATE,
    then a single fsync.  Nothing in the batch has been acknowledged to anyone
    until the fsync returns, and every prefix of the batch is a safe state:
    in particular a surviving commit index always has its entries before it.
    (The opposite order was caught by the disk-mode simulation: a torn tail kept
    HARDSTATE(commit=109) but lost the entries that overwrote a stale suffix, so
    the node applied a never-committed entry after restart.)
    """

    def __init__(self, fs, dirpath: str, sync: bool = True):
        self.fs = fs
        self.dir = dirpath
        self.sync_enabled = sync
        self.wal = WAL(fs, fs.join(dirpath, "wal"))
        self.snaps = SnapshotStore(fs, fs.join(dirpath, "snap"), sync=sync)
        self.hs = HardState()
        self.snapshot: Snapshot | None = None
        self.entries: list[Entry] = []
        self.recovery_notes: list[str] = []

    def load(self) -> tuple[HardState, Snapshot | None, list[Entry]]:
        self.fs.makedirs(self.dir)
        snap, bad = self.snaps.load_latest()
        for b in bad:
            self.recovery_notes.append(f"skipped corrupt snapshot {b}")
        records = self.wal.open()
        if self.wal.truncated_bytes:
            self.recovery_notes.append(f"truncated {self.wal.truncated_bytes} torn bytes from WAL tail")
        hs = HardState()
        base_index, base_term = 0, 0
        ents: list[Entry] = []
        for typ, p in records:
            if typ == REC_HARDSTATE:
                term, commit = _HS.unpack_from(p)
                vote = p[_HS.size:].decode() or None
                hs = HardState(term, vote, commit)
            elif typ == REC_RESET:
                base_index, base_term = _RESET.unpack(p)
                ents = []
            elif typ == REC_ENTRY:
                term, index, kind = _ENT.unpack_from(p)
                e = Entry(term, index, kind, bytes(p[_ENT.size:]))
                first = base_index + 1
                if index < first or index > first + len(ents):
                    raise CorruptLogError(f"WAL entry {index} does not follow log ending at {first + len(ents) - 1}")
                del ents[index - first:]
                ents.append(e)
        if base_index > 0 and (snap is None or snap.index < base_index):
            raise CorruptLogError(f"WAL starts after snapshot {base_index} but no valid snapshot covers it")
        if snap is not None and snap.index > base_index:
            # crash between writing a snapshot and recording it in the WAL
            off = snap.index - base_index - 1
            if 0 <= off < len(ents) and ents[off].term == snap.term:
                ents = ents[off + 1:]
            else:
                ents = []
            # Make the WAL agree with the snapshot before anything is appended to it,
            # otherwise new entries would land after a gap.
            self.hs = hs
            recs = [reset_record(snap.index, snap.term), hs_record(hs)]
            recs.extend(entry_record(e) for e in ents)
            self.wal.rewrite(recs)
            self.recovery_notes.append(f"re-based WAL onto snapshot {snap.index}")
        elif snap is not None and snap.index == base_index and snap.term != base_term:
            raise CorruptLogError("snapshot term does not match WAL base")
        self.hs, self.snapshot, self.entries = hs, snap, ents
        return hs, snap, list(ents)

    def save(self, hs: HardState | None, entries: list[Entry], snapshot: Snapshot | None, sync: bool = True) -> None:
        recs = []
        if snapshot is not None:
            self.snaps.save(snapshot)
            recs.append(reset_record(snapshot.index, snapshot.term))
            self.snapshot = snapshot
            self.entries = []
        if entries:
            base = self.snapshot.index if self.snapshot else 0
            first = entries[0].index
            del self.entries[first - base - 1:]
            self.entries.extend(entries)
            recs.extend(entry_record(e) for e in entries)
        if hs is not None:
            # HardState goes *last*: if a torn tail preserves the commit index, it also
            # preserves every entry written before it in this batch.
            recs.append(hs_record(hs))
            self.hs = hs
        if recs:
            self.wal.append(recs)
        if sync and self.sync_enabled and recs:
            self.wal.sync()

    def compact(self, snapshot: Snapshot) -> None:
        """Persist a locally-taken snapshot, then rewrite the WAL without the covered prefix."""
        self.snaps.save(snapshot)
        keep = [e for e in self.entries if e.index > snapshot.index]
        recs = [reset_record(snapshot.index, snapshot.term), hs_record(self.hs)]
        recs.extend(entry_record(e) for e in keep)
        self.wal.rewrite(recs)
        self.snapshot = snapshot
        self.entries = keep
        self.snaps.prune(keep=2)

    def wal_bytes(self) -> int:
        return self.wal.size

    def close(self) -> None:
        self.wal.close()


class MemoryStorage:
    """Atomic, always-durable storage used by the pure-protocol simulations."""

    def __init__(self):
        self.hs = HardState()
        self.snapshot: Snapshot | None = None
        self.entries: list[Entry] = []

    def load(self):
        return self.hs, self.snapshot, list(self.entries)

    def save(self, hs, entries, snapshot, sync=True):
        if snapshot is not None:
            self.snapshot = snapshot
            self.entries = []
        if hs is not None:
            self.hs = hs
        if entries:
            base = self.snapshot.index if self.snapshot else 0
            del self.entries[entries[0].index - base - 1:]
            self.entries.extend(entries)

    def compact(self, snapshot):
        self.entries = [e for e in self.entries if e.index > snapshot.index]
        self.snapshot = snapshot

    def wal_bytes(self):
        return sum(len(e.data) + 32 for e in self.entries)

    def close(self):
        pass

"""Write-ahead log.

Record format (little endian)::

    +-----------+-----------+--------+-------------------+
    | len  u32  | crc32 u32 | type u8| payload (len B)   |
    +-----------+-----------+--------+-------------------+

crc32 covers the type byte and the payload.  Records are appended to the
newest segment file `<seq>.wal`.  Log compaction writes a brand new segment
(temp file + fsync + rename + fsync dir) that fully supersedes the older ones,
which are then deleted.  Recovery therefore only ever reads the newest segment.

Torn writes: an invalid record at the tail of the segment (short header, short
payload, bad CRC, zero fill) with *no valid record after it* is the remnant of
an un-acknowledged write and is truncated away.  An invalid record followed by a
valid one is genuine corruption of acknowledged data: recovery refuses to start
(fail-stop) instead of silently dropping entries the node may have voted on.
"""
from __future__ import annotations

import struct
import zlib

HDR = struct.Struct("<IIB")
MAX_RECORD = 256 << 20

REC_HARDSTATE = 1
REC_ENTRY = 2
REC_RESET = 3


class CorruptLogError(Exception):
    pass


def encode_record(typ: int, payload: bytes) -> bytes:
    crc = zlib.crc32(payload, zlib.crc32(bytes((typ,))))
    return HDR.pack(len(payload), crc, typ) + payload


def _parse_at(buf: bytes, off: int):
    if len(buf) - off < HDR.size:
        return None
    ln, crc, typ = HDR.unpack_from(buf, off)
    if typ not in (REC_HARDSTATE, REC_ENTRY, REC_RESET) or ln > MAX_RECORD:
        return None
    end = off + HDR.size + ln
    if end > len(buf):
        return None
    payload = bytes(buf[off + HDR.size: end])
    if zlib.crc32(payload, zlib.crc32(bytes((typ,)))) != crc:
        return None
    return typ, payload, end


def scan(buf: bytes) -> tuple[list[tuple[int, bytes]], int, bool]:
    """Returns (records, valid_length, corrupt_after_tail)."""
    recs, off = [], 0
    while off < len(buf):
        r = _parse_at(buf, off)
        if r is None:
            corrupt = any(_parse_at(buf, p) is not None for p in range(off + 1, len(buf) - HDR.size + 1))
            return recs, off, corrupt
        typ, payload, off = r[0], r[1], r[2]
        recs.append((typ, payload))
    return recs, off, False


class WAL:
    def __init__(self, fs, dirpath: str):
        self.fs = fs
        self.dir = dirpath
        self.seq = 0
        self.f = None
        self.size = 0
        self.truncated_bytes = 0  # how many torn bytes the last recovery discarded

    def _path(self, seq: int) -> str:
        return self.fs.join(self.dir, f"{seq:016d}.wal")

    def open(self) -> list[tuple[int, bytes]]:
        fs = self.fs
        fs.makedirs(self.dir)
        names = fs.listdir(self.dir)
        for n in names:
            if n.endswith(".tmp"):
                fs.remove(fs.join(self.dir, n))
        segs = sorted(int(n[:-4]) for n in names if n.endswith(".wal") and n[:-4].isdigit())
        if not segs:
            self.seq = 0
            f = fs.open_write(self._path(0))
            f.sync()
            f.close()
            fs.sync_dir(self.dir)
            segs = [0]
        self.seq = segs[-1]
        path = self._path(self.seq)
        buf = fs.read(path)
        recs, valid, corrupt = scan(buf)
        if valid < len(buf):
            if corrupt:
                raise CorruptLogError(
                    f"{path}: invalid record at offset {valid} followed by valid records; "
                    "refusing to start (acknowledged data may be damaged -- replace this node)")
            self.truncated_bytes = len(buf) - valid
            fs.truncate(path, valid)
        for old in segs[:-1]:
            fs.remove(self._path(old))
        if len(segs) > 1:
            fs.sync_dir(self.dir)
        self.f = fs.open_write(path, append=True)
        self.size = valid
        return recs

    def append(self, records: list[bytes]) -> None:
        blob = b"".join(records)
        self.f.write(blob)
        self.size += len(blob)

    def sync(self) -> None:
        self.f.sync()

    def rewrite(self, records: list[bytes]) -> None:
        """Atomically replace the whole log with `records` (used by compaction)."""
        fs = self.fs
        new_seq = self.seq + 1
        final = self._path(new_seq)
        tmp = final + ".tmp"
        blob = b"".join(records)
        f = fs.open_write(tmp)
        f.write(blob)
        f.sync()
        f.close()
        fs.rename(tmp, final)
        fs.sync_dir(self.dir)
        old = self.seq
        self.f.close()
        self.seq = new_seq
        self.f = fs.open_write(final, append=True)
        self.size = len(blob)
        fs.remove(self._path(old))
        fs.sync_dir(self.dir)

    def close(self) -> None:
        if self.f is not None:
            self.f.close()
            self.f = None

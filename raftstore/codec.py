"""Wire codec for Raft messages (hand-written, binary-safe, no third-party deps).

Frame on the TCP stream::

    u32 body_len | u32 crc32(body) | body

Body::

    u32 header_len | header (compact JSON) | entry payloads ... | snapshot payload

The JSON header carries all scalar fields plus, for each entry,
[term, index, kind, payload_len]; raw payload bytes follow back to back, so
multi-megabyte chunk entries are never base64-inflated.
"""
from __future__ import annotations

import json
import struct
import zlib

from .raft.types import ALL_MSG_TYPES, Entry, Message, Snapshot

MAX_FRAME = 256 << 20
_U32 = struct.Struct("<I")
_FRAME = struct.Struct("<II")


class CodecError(Exception):
    pass


def encode_message(m: Message) -> bytes:
    h = {"t": m.type, "s": m.src, "d": m.dst, "T": m.term}
    for k, v in (("i", m.index), ("lt", m.log_term), ("c", m.commit), ("hi", m.hint_index),
                 ("ht", m.hint_term), ("x", m.ctx)):
        if v:
            h[k] = v
    if m.reject:
        h["r"] = 1
    if m.transfer:
        h["tr"] = 1
    blobs = []
    if m.entries:
        h["e"] = [[e.term, e.index, e.kind, len(e.data)] for e in m.entries]
        blobs.extend(e.data for e in m.entries)
    if m.snapshot is not None:
        s = m.snapshot
        h["sn"] = [s.index, s.term, s.config, len(s.data)]
        blobs.append(s.data)
    hj = json.dumps(h, separators=(",", ":")).encode()
    return _U32.pack(len(hj)) + hj + b"".join(blobs)


def decode_message(body: bytes) -> Message:
    try:
        (hl,) = _U32.unpack_from(body, 0)
        h = json.loads(body[4: 4 + hl])
        off = 4 + hl
        ents = []
        for term, index, kind, ln in h.get("e", ()):
            ents.append(Entry(term, index, kind, bytes(body[off: off + ln])))
            off += ln
        snap = None
        if "sn" in h:
            idx, term, cfg, ln = h["sn"]
            snap = Snapshot(idx, term, cfg, bytes(body[off: off + ln]))
            off += ln
        if off != len(body) or h["t"] not in ALL_MSG_TYPES:
            raise CodecError("malformed message body")
        return Message(h["t"], h["s"], h["d"], h["T"], index=h.get("i", 0), log_term=h.get("lt", 0),
                       entries=tuple(ents), commit=h.get("c", 0), reject=bool(h.get("r")),
                       hint_index=h.get("hi", 0), hint_term=h.get("ht", 0), ctx=h.get("x", 0),
                       snapshot=snap, transfer=bool(h.get("tr")))
    except CodecError:
        raise
    except Exception as e:  # noqa: BLE001 - any parse failure is a codec error
        raise CodecError(f"cannot decode message: {e}") from e


def frame(body: bytes) -> bytes:
    return _FRAME.pack(len(body), zlib.crc32(body)) + body


async def read_frame(reader) -> bytes:
    hdr = await reader.readexactly(_FRAME.size)
    ln, crc = _FRAME.unpack(hdr)
    if ln > MAX_FRAME:
        raise CodecError(f"frame too large ({ln} bytes)")
    body = await reader.readexactly(ln)
    if zlib.crc32(body) != crc:
        raise CodecError("frame checksum mismatch")
    return body

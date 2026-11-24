"""The replicated key -> blob state machine.

Commands carried in Raft log entries (ENTRY_NORMAL data):

    b"C" + sha256(32 raw bytes) + chunk bytes     store a content-addressed chunk
    b"J" + json                                   {"op": "put", "key", "chunks", "size", "sha256"}
                                                  {"op": "del", "key"}
                                                  {"op": "gc", "grace": N}

A file upload is a sequence of chunk commands followed by one `put` manifest,
so a reader can never observe a half-written file.  Chunks themselves live in
the ChunkStore (outside the snapshot); the snapshot only holds manifests.  A
node that installs a snapshot therefore knows exactly which chunks it must
re-replicate from peers (`missing_chunks`).
"""
from __future__ import annotations

import hashlib
import json

from .storage.chunks import ChunkStore, sha256_hex


def encode_chunk_cmd(data: bytes) -> tuple[str, bytes]:
    d = hashlib.sha256(data).digest()
    return d.hex(), b"C" + d + data


def encode_json_cmd(obj: dict) -> bytes:
    return b"J" + json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()


class BlobStateMachine:
    def __init__(self, chunks: ChunkStore):
        self.chunks = chunks
        self.kv: dict[str, dict] = {}
        self.chunk_seen: dict[str, int] = {}  # chunk -> last log index that wrote/referenced it
        self.applied = 0
        self.bad_commands = 0
        # chunks referenced by a manifest but absent locally (e.g. their chunk entries were
        # compacted into a snapshot before this node joined); the node repairs them from peers
        self.pending_repair: set[str] = set()

    def apply(self, index: int, data: bytes):
        self.applied = index
        if not data:
            return None
        tag = data[:1]
        if tag == b"C":
            h = data[1:33].hex()
            payload = data[33:]
            if sha256_hex(payload) != h:
                self.bad_commands += 1  # identical on every replica, so still deterministic
                return {"error": "chunk hash mismatch"}
            if self.chunks.verify(h) != "ok":
                self.chunks.put(h, payload)
            self.chunk_seen[h] = index
            return {"chunk": h}
        if tag != b"J":
            self.bad_commands += 1
            return {"error": "unknown command"}
        cmd = json.loads(data[1:])
        op = cmd.get("op")
        if op == "put":
            self.kv[cmd["key"]] = {"chunks": cmd["chunks"], "size": cmd["size"],
                                   "sha256": cmd["sha256"], "index": index}
            for h in cmd["chunks"]:
                self.chunk_seen[h] = index
                if not self.chunks.has(h):
                    self.pending_repair.add(h)
            return {"ok": True, "index": index}
        if op == "del":
            return {"deleted": self.kv.pop(cmd["key"], None) is not None, "index": index}
        if op == "gc":
            refs = self.referenced_chunks()
            grace = int(cmd.get("grace", 1000))
            victims = [h for h, last in self.chunk_seen.items() if h not in refs and last < index - grace]
            for h in victims:
                del self.chunk_seen[h]
                self.chunks.delete(h)
            return {"removed": len(victims)}
        self.bad_commands += 1
        return {"error": f"unknown op {op!r}"}

    def get_manifest(self, key: str) -> dict | None:
        return self.kv.get(key)

    def keys(self, prefix: str = "") -> list[str]:
        return sorted(k for k in self.kv if k.startswith(prefix))

    def referenced_chunks(self) -> set[str]:
        out: set[str] = set()
        for m in self.kv.values():
            out.update(m["chunks"])
        return out

    def missing_chunks(self) -> list[str]:
        return sorted(h for h in self.referenced_chunks() if self.chunks.verify(h) != "ok")

    def snapshot_bytes(self) -> bytes:
        return json.dumps({"kv": self.kv, "chunk_seen": self.chunk_seen, "applied": self.applied},
                          sort_keys=True, separators=(",", ":")).encode()

    def restore(self, data: bytes) -> None:
        obj = json.loads(data) if data else {}
        self.kv = obj.get("kv", {})
        self.chunk_seen = obj.get("chunk_seen", {})
        self.applied = obj.get("applied", 0)

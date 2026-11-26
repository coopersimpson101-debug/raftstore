"""Opt-in structured execution traces of the Raft core (JSON lines).

Every public entry point of `Raft` (construction, `tick`, `step`, `propose`,
`propose_conf_change`, `read_index`, `transfer_leadership`, `compact`, `ready`)
emits one event when a tracer is attached:

    {"n": node, "i": per-writer sequence number, "ts": wall clock, "ev": name,
     "args": {...},          # the input (a full message for "step")
     "out": [msg, ...],      # messages the call queued for sending
     "reads": [[ctx, idx]],  # ReadIndex results released by the call
     "logops": [...],        # log mutations: ["append", first, [entries]],
                             #                ["compact", index, term], ["restore", index, term]
     "err": "NotLeader",     # exception raised by the call, if any
     "post": {...}}          # protocol state after the call (see `state_of`)

"init" events additionally carry the recovered log and the node's options; "ready"
events mark the point at which the runtime persisted everything produced so far.

Tracing is off unless a `TraceWriter` is passed to `Raft(tracer=...)`; the simulator
takes `SimCluster(..., trace_path=...)` and a server process honours the
`RAFTSTORE_TRACE_DIR` environment variable (one `<node-id>.jsonl` per node).
The format is consumed by an external refinement checker that replays traces
against a TLA+ model of this Raft implementation.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time

from .types import ENTRY_CONFIG, ENTRY_NOOP, decode_conf

FORMAT_VERSION = 1


def data_digest(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()[:16]


def entry_json(e) -> list:
    """[term, index, kind, payload] with payload = config change dict / "" / data digest."""
    if e.kind == ENTRY_CONFIG:
        payload = decode_conf(e.data)
        payload = {"op": payload["op"], "id": payload.get("id"),
                   "voters": sorted(payload["voters"]) if "voters" in payload else None}
    elif e.kind == ENTRY_NOOP:
        payload = ""
    else:
        payload = data_digest(e.data)
    return [e.term, e.index, e.kind, payload]


def msg_json(m) -> dict:
    d = {"type": m.type, "src": m.src, "dst": m.dst, "term": m.term, "index": m.index,
         "log_term": m.log_term, "commit": m.commit, "reject": m.reject,
         "hint_index": m.hint_index, "hint_term": m.hint_term, "ctx": m.ctx,
         "transfer": m.transfer, "entries": [entry_json(e) for e in m.entries]}
    if m.snapshot is not None:
        d["snapshot"] = {"index": m.snapshot.index, "term": m.snapshot.term,
                         "voters": sorted(m.snapshot.config)}
    return d


def state_of(r) -> dict:
    lg = r.log
    st = {
        "term": r.term, "vote": r.vote, "state": r.state, "leader": r.leader,
        "commit": lg.commit, "snap": lg.snap_index, "snap_term": lg.snap_term,
        "last": lg.last_index(), "last_term": lg.last_term(),
        "voters": sorted(r.voters), "cidx": r.config_index,
        "votes": {k: v for k, v in sorted(r.votes.items())},
        "progress": {k: [p.match, p.next, p.acked_ctx] for k, p in sorted(r.progress.items())},
        "tstart": r.term_start_index, "xfer": r.lead_transferee, "rseq": r.read_seq,
        "pend": [[s, i, c] for s, i, c in r.pending_reads], "wait": list(r.waiting_reads),
    }
    return st


class TraceWriter:
    """Thread-safe JSON-lines sink. Each line is flushed, so a SIGKILL loses at most one line."""

    def __init__(self, path_or_file):
        if isinstance(path_or_file, (str, os.PathLike)):
            os.makedirs(os.path.dirname(os.path.abspath(path_or_file)), exist_ok=True)
            self._f = open(path_or_file, "a", encoding="utf-8")
            self._own = True
        else:
            self._f = path_or_file
            self._own = False
        self._lock = threading.Lock()
        self._seq = 0

    @classmethod
    def from_env(cls, node_id: str):
        d = os.environ.get("RAFTSTORE_TRACE_DIR")
        return cls(os.path.join(d, f"{node_id}.jsonl")) if d else None

    def write(self, rec: dict) -> None:
        with self._lock:
            self._seq += 1
            rec["i"] = self._seq
            self._f.write(json.dumps(rec, separators=(",", ":"), default=str) + "\n")
            self._f.flush()

    def event(self, r, ev: str, args: dict, n_msgs: int, n_reads: int, err: str | None,
              ops: list | None, extra: dict | None = None) -> None:
        rec = {"n": r.id, "ts": time.time(), "ev": ev, "args": args,
               "out": [msg_json(m) for m in r.msgs[n_msgs:]],
               "reads": [[rs.ctx, rs.index] for rs in r.read_states[n_reads:]],
               "logops": ops or [], "post": state_of(r)}
        if err:
            rec["err"] = err
        if extra:
            rec.update(extra)
        self.write(rec)

    def close(self) -> None:
        if self._own:
            self._f.close()

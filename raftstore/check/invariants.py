"""Raft safety invariants (Raft paper, Figure 3), checked after every simulated step.

* Election Safety      at most one leader per term
* Log Matching         equal (index, term) => identical entries and identical prefixes
* Leader Completeness  an entry committed in term T is in the log of every leader of term > T
* State Machine Safety no two nodes apply different commands at the same index
                       (and a snapshot installed at index i equals the state others had at i)
"""
from __future__ import annotations

from ..raft.types import LEADER


class SafetyViolation(AssertionError):
    pass


class InvariantChecker:
    def __init__(self):
        self.leaders: dict[int, str] = {}            # term -> leader id
        self.committed: dict[int, tuple] = {}        # index -> (term, kind, data, observed_term)
        self.max_committed = 0
        self.applied: dict[int, tuple] = {}          # index -> (term, data)
        self.digests: dict[int, object] = {}         # index -> state digest after applying index
        self._seen_commit: dict[str, int] = {}
        self.checks = 0

    def fail(self, what: str) -> None:
        raise SafetyViolation(what)

    # called by the harness when a node applies an entry / restores a snapshot
    def on_apply(self, node_id: str, e, digest, observed_term: int) -> None:
        c = self.committed.get(e.index)
        if c is None:
            self.committed[e.index] = (e.term, e.kind, e.data, observed_term)
            self.max_committed = max(self.max_committed, e.index)
        elif c[:3] != (e.term, e.kind, e.data):
            self.fail(f"commit safety: {node_id} applied {(e.term, e.data)!r} at {e.index}, "
                      f"committed entry is {c[:3]!r}")
        prev = self.applied.get(e.index)
        if prev is None:
            self.applied[e.index] = (e.term, e.data)
        elif prev != (e.term, e.data):
            self.fail(f"state machine safety: {node_id} applied {(e.term, e.data)!r} at {e.index}, "
                      f"another node applied {prev!r}")
        d = self.digests.get(e.index)
        if d is None:
            self.digests[e.index] = digest
        elif d != digest:
            self.fail(f"state machine safety: {node_id} state after {e.index} diverged")

    def on_snapshot(self, node_id: str, index: int, term: int, digest) -> None:
        c = self.committed.get(index)
        if c is not None and c[0] != term:
            self.fail(f"{node_id} installed snapshot ({index},{term}) but entry {index} committed in term {c[0]}")
        d = self.digests.get(index)
        if d is None:
            self.digests[index] = digest
        elif d != digest:
            self.fail(f"state machine safety: {node_id} snapshot state at {index} differs from applied state")

    def check(self, rafts: list) -> None:
        self.checks += 1
        # Election safety
        for r in rafts:
            if r.state == LEADER:
                prev = self.leaders.setdefault(r.term, r.id)
                if prev != r.id:
                    self.fail(f"election safety: {prev} and {r.id} both leader in term {r.term}")
        # Record newly committed entries (and check they agree with earlier observations)
        for r in rafts:
            lo = max(self._seen_commit.get(r.id, 0), r.log.snap_index) + 1
            for i in range(lo, r.log.commit + 1):
                e = r.log.entry(i)
                prev = self.committed.get(i)
                if prev is None:
                    self.committed[i] = (e.term, e.kind, e.data, r.term)
                    if i > self.max_committed:
                        self.max_committed = i
                elif prev[:3] != (e.term, e.kind, e.data):
                    self.fail(f"commit safety: {r.id} committed {(e.term, e.data)!r} at {i}, "
                              f"previously committed {prev[:3]!r}")
            self._seen_commit[r.id] = max(self._seen_commit.get(r.id, 0), r.log.commit)
        # Log matching (pairwise)
        for a_i in range(len(rafts)):
            for b_i in range(a_i + 1, len(rafts)):
                self._log_matching(rafts[a_i], rafts[b_i])
        # Leader completeness
        for r in rafts:
            if r.state != LEADER:
                continue
            for i in range(r.log.snap_index + 1, self.max_committed + 1):
                c = self.committed.get(i)
                if c is None or c[3] >= r.term:
                    continue
                t = r.log.term_at(i)
                if t != c[0]:
                    self.fail(f"leader completeness: leader {r.id} (term {r.term}) lacks entry {i} "
                              f"committed in term {c[0]} (has term {t})")
        # Every node's committed prefix must agree with the global record
        for r in rafts:
            for i in range(r.log.snap_index + 1, r.log.commit + 1):
                c = self.committed.get(i)
                if c is not None and r.log.term_at(i) != c[0]:
                    self.fail(f"{r.id} has entry {i} with term {r.log.term_at(i)}, committed term is {c[0]}")

    def _log_matching(self, a, b) -> None:
        la, lb = a.log, b.log
        lo = max(la.snap_index, lb.snap_index) + 1
        hi = min(la.last_index(), lb.last_index())
        i = hi
        while i >= lo and la.term_at(i) != lb.term_at(i):
            i -= 1
        while i >= lo:
            if la.entry(i) != lb.entry(i):
                self.fail(f"log matching: {a.id} and {b.id} agree on term at some index >= {i} "
                          f"but differ at {i}: {la.entry(i)!r} vs {lb.entry(i)!r}")
            i -= 1

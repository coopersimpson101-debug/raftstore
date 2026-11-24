import os
import random
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raftstore.raft.core import Raft  # noqa: E402
from raftstore.raft.types import LEADER  # noqa: E402


def sim_seeds(default: int) -> int:
    """Number of randomized schedules per simulation test (env RAFTSTORE_SIM_SEEDS)."""
    return int(os.environ.get("RAFTSTORE_SIM_SEEDS", default))


class Net:
    """Synchronous, lossless test network for hand-written Raft scenarios."""

    def __init__(self, ids, seed=0, **kw):
        voters = {i: {} for i in ids}
        hs, ents = Raft.bootstrap(voters)
        self.kw = kw
        self.rng = random.Random(seed)
        self.nodes = {i: Raft(i, hard_state=hs, entries=ents, rng=random.Random(self.rng.random()), **kw)
                      for i in ids}
        self.applied = {i: [] for i in ids}
        self.read_states = {i: [] for i in ids}
        self.cut = set()        # nodes isolated from everyone
        self.dropped_links = set()
        self.queue = []

    def collect(self):
        for i, r in self.nodes.items():
            while r.has_ready():
                rd = r.ready()
                self.queue.extend(rd.messages)
                self.applied[i].extend(rd.committed)
                self.read_states[i].extend(rd.read_states)

    def deliver(self, max_rounds=1000, filt=None):
        for _ in range(max_rounds):
            self.collect()
            if not self.queue:
                return
            q, self.queue = self.queue, []
            for m in q:
                if m.src in self.cut or m.dst in self.cut or (m.src, m.dst) in self.dropped_links:
                    continue
                if filt is not None and not filt(m):
                    continue
                if m.dst in self.nodes:
                    self.nodes[m.dst].step(m)
        raise AssertionError("network did not quiesce")

    def tick(self, ids=None, n=1):
        for _ in range(n):
            for i in ids or list(self.nodes):
                self.nodes[i].tick()
            self.deliver()

    def tick_filtered(self, ids=None, n=1, filt=None):
        for _ in range(n):
            for i in ids or list(self.nodes):
                self.nodes[i].tick()
            self.deliver(filt=filt)

    def elect(self, nid):
        r = self.nodes[nid]
        # Pretend an election timeout has passed for every reachable follower, so
        # leader stickiness (CheckQuorum) lets them vote for `nid`.
        for i, o in self.nodes.items():
            if i != nid and i not in self.cut and o.state != LEADER:
                o.election_elapsed = o.election_ticks
        for _ in range(3 * r.election_ticks):
            r.tick()
            self.deliver()
            if r.state == LEADER:
                return r
        raise AssertionError(f"{nid} did not become leader")

    def leader(self):
        ls = [r for r in self.nodes.values() if r.state == LEADER]
        return max(ls, key=lambda r: r.term) if ls else None


@pytest.fixture
def net3():
    return Net(["a", "b", "c"])


# ---- aggregate numbers printed at the end of the run (used for the README) ----
SIM_TOTALS = {"schedules": 0, "steps": 0, "invariant_checks": 0, "client_ops": 0, "acked_writes_verified": 0,
              "crashes": 0, "partitions": 0, "drive_pulls": 0, "msgs_dropped": 0, "msgs_duplicated": 0,
              "linearizability_histories": 0, "crash_points": 0}


def pytest_terminal_summary(terminalreporter):
    if SIM_TOTALS["schedules"] or SIM_TOTALS["crash_points"]:
        terminalreporter.write_sep("=", "raftstore randomized-testing totals")
        for k, v in SIM_TOTALS.items():
            terminalreporter.write_line(f"{k:>28}: {v}")

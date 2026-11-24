"""Seeded, deterministic cluster simulator.

The simulator drives real `Raft` instances (and, in `storage="disk"` mode, the
real WAL / snapshot code on top of `FaultyFS`) through a hostile network that
drops, delays, duplicates and reorders messages and can partition the cluster.
It crashes and restarts nodes, pulls drives mid-write, changes membership and
transfers leadership, while clients issue concurrent reads and writes.  After
every step the Raft safety invariants are checked; at the end the client
history is checked for linearizability and every acknowledged write is
verified to be present on every surviving replica.

Everything flows from one `random.Random(seed)`, so any failure is reproducible
by re-running the same seed.
"""
from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass, field

from ..check.invariants import InvariantChecker
from ..check.linearizability import Op, check_kv
from ..raft.core import Raft
from ..raft.types import ENTRY_NORMAL, LEADER, HardState, NotLeader, ProposalDropped
from ..storage.fs import FaultyFS
from ..storage.raftlog import DiskStorage, MemoryStorage
from ..storage.wal import CorruptLogError


@dataclass
class SimConfig:
    nodes: int = 3
    storage: str = "memory"          # "memory" | "disk" (real WAL on FaultyFS)
    drop: float = 0.05
    duplicate: float = 0.03
    max_delay: int = 4               # in steps; random delay => reordering
    p_tick: float = 0.35             # per node per step (unsynced clocks) or per step (synced)
    synced_ticks: bool = False
    max_deliveries: int = 6
    p_client: float = 0.25
    max_inflight_ops: int = 6
    op_timeout: int = 400
    keys: tuple = ("x", "y", "z")
    read_fraction: float = 0.5
    p_crash: float = 0.004
    p_restart: float = 0.03
    p_partition: float = 0.004
    p_heal: float = 0.02
    p_pull_drive: float = 0.0        # disk mode only
    p_transfer: float = 0.0
    p_membership: float = 0.0
    max_down: int | None = None      # cap on simultaneously crashed nodes (None = any)
    compact_every: int = 40
    raft: dict = field(default_factory=dict)
    # Deliberately injected bugs, used to prove the checkers can catch them:
    bug_forget_vote: bool = False    # restart without the persisted vote
    bug_stale_reads: bool = False    # serve reads from any replica's local state


class SimNetwork:
    def __init__(self, rng: random.Random, cfg: SimConfig):
        self.rng, self.cfg = rng, cfg
        self.inflight: list = []
        self.blocked: set = set()
        self.sent = self.dropped = self.duplicated = self.delivered = 0

    def send(self, m, now: int) -> None:
        self.sent += 1
        if self.rng.random() < self.cfg.drop:
            self.dropped += 1
            return
        copies = 1
        if self.rng.random() < self.cfg.duplicate:
            copies = 2
            self.duplicated += 1
        for _ in range(copies):
            self.inflight.append((now + self.rng.randint(0, self.cfg.max_delay), m))

    def pop(self, now: int):
        ready = [i for i, (t, _) in enumerate(self.inflight) if t <= now]
        if not ready:
            return None
        i = self.rng.choice(ready)
        _, m = self.inflight[i]
        self.inflight[i] = self.inflight[-1]
        self.inflight.pop()
        if (m.src, m.dst) in self.blocked:
            self.dropped += 1
            return None
        self.delivered += 1
        return m

    def partition(self, groups: list[set]) -> None:
        self.blocked = set()
        for g in groups:
            for h in groups:
                if g is not h:
                    self.blocked |= {(a, b) for a in g for b in h}

    def heal(self) -> None:
        self.blocked = set()


def _digest(kv: dict):
    return hash(tuple(sorted(kv.items())))


class SimNode:
    def __init__(self, cluster: "SimCluster", node_id: str, voters: dict | None):
        self.cluster = cluster
        self.id = node_id
        self.fs = FaultyFS(seed=cluster.rng.getrandbits(32)) if cluster.cfg.storage == "disk" else None
        self.mem = MemoryStorage() if self.fs is None else None
        self.alive = False
        self.retired = False
        self.unrecoverable = False
        self.drive_pulled = False
        self.raft: Raft | None = None
        self.storage = None
        self._open_storage()
        if voters is not None:
            hs, ents = Raft.bootstrap(voters)
            self.storage.load()
            self.storage.save(hs, ents, None, sync=True)
        self.start()

    def _open_storage(self):
        self.storage = self.mem if self.fs is None else DiskStorage(self.fs, "/data")

    def start(self) -> None:
        self._open_storage()
        try:
            hs, snap, ents = self.storage.load()
        except CorruptLogError as e:
            self.unrecoverable = True
            self.cluster.log(f"{self.id} cannot recover: {e}")
            return
        c = self.cluster
        if c.cfg.bug_forget_vote:
            hs = HardState(hs.term, None, hs.commit)
        self.raft = Raft(self.id, hard_state=hs, snapshot=snap, entries=ents,
                         rng=random.Random(c.rng.getrandbits(32)), **c.cfg.raft)
        self.kv = json.loads(snap.data)["kv"] if snap else {}
        self.pending_writes: dict[int, tuple] = {}
        self.pending_reads: dict[int, Op] = {}
        self.read_wait: list = []
        self.alive = True
        self.process()

    def crash(self, reason: str) -> None:
        if not self.alive:
            return
        self.alive = False
        c = self.cluster
        for _, op in self.pending_writes.values():
            c.inflight_ops.discard(op.id)      # outcome unknown: stays pending forever
        for op in list(self.pending_reads.values()) + [op for _, op in self.read_wait]:
            c.drop_op(op)
        self.pending_writes, self.pending_reads, self.read_wait = {}, {}, []
        if self.fs is not None and not self.drive_pulled:
            self.fs.crash()                     # power loss: torn tails, lost unsynced data
        self.raft = None
        c.log(f"{self.id} crashed ({reason})")

    def process(self) -> None:
        c = self.cluster
        while self.alive and self.raft.has_ready():
            rd = self.raft.ready()
            try:
                self.storage.save(rd.hard_state, rd.entries, rd.snapshot, sync=rd.must_sync)
            except OSError as e:
                self.crash(f"fail-stop on disk error: {e}")
                return
            for m in rd.messages:
                c.net.send(m, c.now)
            if rd.snapshot is not None:
                self.kv = json.loads(rd.snapshot.data)["kv"]
                c.checker.on_snapshot(self.id, rd.snapshot.index, rd.snapshot.term, _digest(self.kv))
                for idx in [i for i in self.pending_writes if i <= rd.snapshot.index]:
                    c.inflight_ops.discard(self.pending_writes.pop(idx)[1].id)
            for e in rd.committed:
                self._apply(e)
            for rs in rd.read_states:
                op = self.pending_reads.pop(rs.ctx, None)
                if op is not None:
                    self.read_wait.append((rs.index, op))
            for ctx in rd.failed_reads:
                op = self.pending_reads.pop(ctx, None)
                if op is not None:
                    c.drop_op(op)
            if self.read_wait:
                applied = self.raft.log.applied
                keep = []
                for idx, op in self.read_wait:
                    if idx <= applied:
                        op.output = self.kv.get(op.key)
                        c.complete(op)
                    else:
                        keep.append((idx, op))
                self.read_wait = keep
            log = self.raft.log
            if log.applied - log.snap_index >= c.cfg.compact_every:
                snap = self.raft.compact(log.applied, json.dumps({"kv": self.kv}, sort_keys=True).encode())
                try:
                    self.storage.compact(snap)
                except OSError as e:
                    self.crash(f"fail-stop on disk error during compaction: {e}")
                    return

    def _apply(self, e) -> None:
        c = self.cluster
        if e.kind == ENTRY_NORMAL and e.data:
            cmd = json.loads(e.data)
            self.kv[cmd["k"]] = cmd["v"]
        c.checker.on_apply(self.id, e, _digest(self.kv), self.raft.term)
        w = self.pending_writes.pop(e.index, None)
        if w is not None:
            term, op = w
            if term == e.term:
                op.meta["index"], op.meta["term"] = e.index, e.term
                c.complete(op)
            else:
                c.inflight_ops.discard(op.id)   # superseded; outcome treated as unknown


class SimCluster:
    def __init__(self, seed: int = 0, cfg: SimConfig | None = None, trace: bool = False):
        self.seed = seed
        self.cfg = cfg or SimConfig()
        self.rng = random.Random(seed)
        self.net = SimNetwork(self.rng, self.cfg)
        self.checker = InvariantChecker()
        self.now = 0
        self.trace = trace
        self.events: list[str] = []
        self.history: list[Op] = []
        self.inflight_ops: set[int] = set()
        self._op_seq = 0
        self._value_seq = 0
        self._next_node = self.cfg.nodes
        self.stats = {"crashes": 0, "restarts": 0, "partitions": 0, "drive_pulls": 0,
                      "transfers": 0, "member_adds": 0, "member_removes": 0, "steps": 0}
        ids = [f"n{i}" for i in range(self.cfg.nodes)]
        voters = {i: {} for i in ids}
        self.nodes: dict[str, SimNode] = {}
        for i in ids:
            self.nodes[i] = SimNode(self, i, voters)

    # ------------------------------------------------------------- utilities
    def log(self, msg: str) -> None:
        line = f"[t={self.now:5d}] {msg}"
        self.events.append(line)
        if self.trace:
            print(line)

    def live(self) -> list[SimNode]:
        return [n for n in self.nodes.values() if n.alive]

    def leaders(self) -> list[SimNode]:
        return [n for n in self.live() if n.raft.state == LEADER]

    def complete(self, op: Op) -> None:
        op.ret = self.now
        self.inflight_ops.discard(op.id)

    def drop_op(self, op: Op) -> None:
        """A read that never returned has no effect: remove it from the history."""
        op.meta["dropped"] = True
        self.inflight_ops.discard(op.id)

    # ---------------------------------------------------------------- clients
    def client_op(self, node: SimNode | None = None, kind: str | None = None, key: str | None = None):
        if len(self.inflight_ops) >= self.cfg.max_inflight_ops:
            return None
        if node is None:
            cands = self.leaders()
            if not cands or self.rng.random() < 0.1:
                cands = self.live()
            if not cands:
                return None
            node = self.rng.choice(cands)
        key = key or self.rng.choice(self.cfg.keys)
        kind = kind or ("r" if self.rng.random() < self.cfg.read_fraction else "w")
        self._op_seq += 1
        op = Op(self._op_seq, key, kind, call=self.now)
        try:
            if kind == "w":
                self._value_seq += 1
                op.value = self._value_seq
                data = json.dumps({"k": key, "v": op.value}).encode()
                idx, term = node.raft.propose(data)
                node.pending_writes[idx] = (term, op)
            elif self.cfg.bug_stale_reads:
                node = self.rng.choice(self.live())
                op.output = node.kv.get(key)
                op.ret = self.now
                op.meta["node"] = node.id
                self.history.append(op)
                return op
            else:
                node.pending_reads[op.id] = op
                node.raft.read_index(op.id)
        except (NotLeader, ProposalDropped):
            node.pending_reads.pop(op.id, None)
            return None
        op.meta["node"] = node.id
        self.history.append(op)
        self.inflight_ops.add(op.id)
        node.process()
        return op

    def _expire_ops(self) -> None:
        if not self.inflight_ops:
            return
        for op in self.history[-64:]:
            if op.id in self.inflight_ops and self.now - op.call > self.cfg.op_timeout:
                if op.kind == "r":
                    self.drop_op(op)
                else:
                    self.inflight_ops.discard(op.id)

    # ----------------------------------------------------------------- faults
    def crash(self, n: SimNode, reason: str = "kill -9") -> None:
        if n.alive:
            self.stats["crashes"] += 1
            n.crash(reason)

    def restart(self, n: SimNode) -> None:
        if n.alive or n.retired or n.unrecoverable:
            return
        if n.drive_pulled:
            n.fs.reinsert_drive()
            n.drive_pulled = False
            self.log(f"{n.id}: drive re-inserted")
        self.stats["restarts"] += 1
        n.start()
        if n.alive:
            self.log(f"{n.id} restarted (term={n.raft.term}, last={n.raft.log.last_index()}, "
                     f"commit={n.raft.log.commit})")

    def pull_drive(self, n: SimNode, after_writes: int = 0) -> None:
        if n.fs is None or not n.alive:
            return
        self.stats["drive_pulls"] += 1
        n.drive_pulled = True
        n.fs.pull_drive(after_writes)
        self.log(f"{n.id}: drive pulled (fails after {after_writes} more writes)")

    def random_partition(self) -> None:
        ids = [i for i, n in self.nodes.items() if not n.retired]
        self.rng.shuffle(ids)
        cut = self.rng.randint(1, max(1, len(ids) - 1))
        a, b = set(ids[:cut]), set(ids[cut:])
        self.net.partition([a, b])
        self.stats["partitions"] += 1
        self.log(f"partition {sorted(a)} | {sorted(b)}")

    def _membership_change(self) -> None:
        ls = self.leaders()
        if not ls:
            return
        leader = max(ls, key=lambda n: n.raft.term)
        voters = leader.raft.voters
        try:
            if len(voters) > 3 and self.rng.random() < 0.5:
                victim = self.rng.choice(sorted(voters))
                leader.raft.propose_conf_change({"op": "remove", "id": victim})
                self.stats["member_removes"] += 1
                self.log(f"propose remove {victim} via {leader.id}")
            elif len(voters) < 5:
                nid = f"n{self._next_node}"
                self._next_node += 1
                self.nodes[nid] = SimNode(self, nid, None)
                leader.raft.propose_conf_change({"op": "add", "id": nid, "meta": {}})
                self.stats["member_adds"] += 1
                self.log(f"propose add {nid} via {leader.id}")
        except (NotLeader, ProposalDropped, ValueError):
            return
        leader.process()

    def _retire_removed(self) -> None:
        """Shut down nodes whose removal has been committed (as an operator would)."""
        ls = self.leaders()
        if not ls:
            return
        leader = max(ls, key=lambda n: n.raft.term)
        if leader.raft.config_index > leader.raft.log.commit:
            return
        for nid, n in self.nodes.items():
            if not n.retired and nid not in leader.raft.voters and n.raft is not None \
                    and nid in [k for k in self.nodes] and self._was_member(nid):
                n.retired = True
                if n.alive:
                    n.crash("retired after removal")
                self.log(f"{nid} retired")

    def _was_member(self, nid: str) -> bool:
        n = self.nodes[nid]
        return n.raft is not None and n.raft.log.last_index() > 0

    def random_fault(self) -> None:
        r, cfg = self.rng, self.cfg
        live = self.live()
        down = [n for n in self.nodes.values() if not n.alive and not n.retired and not n.unrecoverable]
        can_crash = cfg.max_down is None or len(down) < cfg.max_down
        if live and can_crash and r.random() < cfg.p_crash:
            self.crash(r.choice(live))
        if down and r.random() < cfg.p_restart:
            self.restart(r.choice(down))
        if r.random() < cfg.p_partition:
            self.random_partition()
        if self.net.blocked and r.random() < cfg.p_heal:
            self.net.heal()
            self.log("network healed")
        if live and can_crash and cfg.storage == "disk" and r.random() < cfg.p_pull_drive:
            self.pull_drive(r.choice(live), r.randint(0, 3))
        if cfg.p_transfer and r.random() < cfg.p_transfer:
            ls = self.leaders()
            if ls:
                l = r.choice(ls)
                peers = sorted(l.raft.progress)
                if peers:
                    try:
                        l.raft.transfer_leadership(r.choice(peers))
                        self.stats["transfers"] += 1
                        l.process()
                    except Exception:
                        pass
        if cfg.p_membership and r.random() < cfg.p_membership:
            self._membership_change()
        if cfg.p_membership:
            self._retire_removed()

    # ------------------------------------------------------------------- run
    def step(self) -> None:
        self.now += 1
        self.stats["steps"] += 1
        r, cfg = self.rng, self.cfg
        if cfg.synced_ticks:
            if r.random() < cfg.p_tick:
                for n in self.live():
                    n.raft.tick()
                    n.process()
        else:
            for n in self.live():
                if r.random() < cfg.p_tick:
                    n.raft.tick()
                    n.process()
        for _ in range(r.randint(0, cfg.max_deliveries)):
            m = self.net.pop(self.now)
            if m is None:
                continue
            dst = self.nodes.get(m.dst)
            if dst is not None and dst.alive:
                dst.raft.step(m)
                dst.process()
        if r.random() < cfg.p_client:
            self.client_op()
        self.random_fault()
        self._expire_ops()
        self.checker.check([n.raft for n in self.live()])

    def run(self, steps: int) -> None:
        for _ in range(steps):
            self.step()

    def heal(self, max_steps: int = 6000) -> int:
        """Stop injecting faults, repair everything, and wait for convergence."""
        self.net.heal()
        self.cfg.drop = self.cfg.duplicate = 0.0
        for n in list(self.nodes.values()):
            self.restart(n)
        saved = (self.cfg.p_crash, self.cfg.p_partition, self.cfg.p_pull_drive,
                 self.cfg.p_transfer, self.cfg.p_membership, self.cfg.p_client)
        self.cfg.p_crash = self.cfg.p_partition = self.cfg.p_pull_drive = 0.0
        self.cfg.p_transfer = self.cfg.p_membership = self.cfg.p_client = 0.0
        probe = None
        try:
            for i in range(max_steps):
                self.step()
                ls = self.leaders()
                if not ls:
                    continue
                leader = max(ls, key=lambda n: n.raft.term)
                if probe is None or (probe.ret == math.inf and probe.id not in self.inflight_ops):
                    probe = self.client_op(leader, "w", "probe")
                    continue
                if probe.ret == math.inf:
                    continue
                if leader.raft.log.commit < leader.raft.term_start_index:
                    continue  # leader has not yet committed everything its predecessors did
                members = [self.nodes[v] for v in leader.raft.voters if v in self.nodes]
                target = leader.raft.log.commit
                if all(m.alive and m.raft.log.applied >= target and m.raft.log.commit >= target
                       for m in members):
                    return i + 1
        finally:
            (self.cfg.p_crash, self.cfg.p_partition, self.cfg.p_pull_drive,
             self.cfg.p_transfer, self.cfg.p_membership, self.cfg.p_client) = saved
        raise AssertionError(f"seed {self.seed}: cluster did not heal within {max_steps} steps")

    # ---------------------------------------------------------------- checks
    def client_history(self) -> list[Op]:
        return [op for op in self.history if not op.meta.get("dropped") and op.key != "probe"]

    def check_linearizable(self):
        return check_kv(self.client_history())

    def check_no_acked_write_lost(self) -> int:
        """Every acknowledged write must be committed and applied on every live member."""
        ls = self.leaders()
        leader = max(ls, key=lambda n: n.raft.term) if ls else None
        members = [self.nodes[v] for v in (leader.raft.voters if leader else self.nodes) if v in self.nodes]
        acked = [op for op in self.history if op.kind == "w" and op.ret != math.inf]
        for op in acked:
            idx = op.meta["index"]
            c = self.checker.committed.get(idx)
            if c is None:
                # committed but compacted before any check observed it: verify via applied record
                a = self.checker.applied.get(idx)
                if a is None:
                    raise AssertionError(f"acked write {op} never committed")
                term, data = a
            else:
                term, data = c[0], c[2]
            if term != op.meta["term"] or json.loads(data)["v"] != op.value:
                raise AssertionError(f"acked write {op} lost: index {idx} holds {data!r}")
            for m in members:
                if not m.alive or m.raft.log.applied < idx:
                    raise AssertionError(f"acked write {op} not applied on {m.id}")
        return len(acked)

    def summary(self) -> dict:
        h = self.client_history()
        return {
            "seed": self.seed, **self.stats,
            "ops": len(h), "acked": sum(1 for o in h if o.ret != math.inf),
            "msgs_sent": self.net.sent, "msgs_dropped": self.net.dropped,
            "msgs_duplicated": self.net.duplicated, "invariant_checks": self.checker.checks,
            "max_committed": self.checker.max_committed,
            "leaders_terms": len(self.checker.leaders),
        }

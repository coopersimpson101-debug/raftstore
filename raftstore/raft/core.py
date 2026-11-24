"""A deterministic, I/O-free Raft state machine.

The `Raft` object never touches a clock, a socket or a disk.  Time advances only
when the runtime calls `tick()`, messages arrive only through `step()`, and all
side effects are handed back through `ready()` (see `types.Ready` for the order
the runtime must perform them in).  This makes the whole protocol replayable
from a seed, which is what the simulation harness in `raftstore.sim` relies on.

Implemented: randomized election timeouts, PreVote, CheckQuorum + leader
stickiness, log replication with fast backtracking (conflict term/index),
commit of current-term entries only, snapshots / InstallSnapshot / log
compaction, single-server membership changes, ReadIndex and leader-lease
linearizable reads, and leadership transfer (TimeoutNow).
"""
from __future__ import annotations

import bisect
import random
from dataclasses import dataclass

from .types import (
    CANDIDATE, ENTRY_CONFIG, ENTRY_NOOP, ENTRY_NORMAL, FOLLOWER, LEADER, PRE_CANDIDATE,
    MSG_APPEND, MSG_APPEND_RESP, MSG_PREVOTE, MSG_PREVOTE_RESP, MSG_SNAPSHOT,
    MSG_TIMEOUT_NOW, MSG_VOTE, MSG_VOTE_RESP,
    Entry, HardState, Message, NotLeader, ProposalDropped, ReadState, Ready, Snapshot,
    apply_conf, decode_conf, encode_conf,
)

_TERM_KEY = lambda e: e.term  # noqa: E731


class RaftLog:
    """In-memory view of the log: a snapshot prefix plus the entries after it."""

    __slots__ = ("snap_index", "snap_term", "entries", "stable", "commit", "applied")

    def __init__(self, snapshot: Snapshot | None, entries: list[Entry]):
        self.snap_index = snapshot.index if snapshot else 0
        self.snap_term = snapshot.term if snapshot else 0
        ents = [e for e in entries if e.index > self.snap_index]
        for i, e in enumerate(ents):
            if e.index != self.snap_index + 1 + i:
                raise ValueError("non-contiguous log handed to RaftLog")
        self.entries: list[Entry] = ents
        self.stable = self.last_index()  # entries <= stable are known durable
        self.commit = self.snap_index
        self.applied = self.snap_index

    def last_index(self) -> int:
        return self.snap_index + len(self.entries)

    def last_term(self) -> int:
        return self.entries[-1].term if self.entries else self.snap_term

    def term_at(self, i: int) -> int | None:
        if i == self.snap_index:
            return self.snap_term
        if i < self.snap_index or i > self.snap_index + len(self.entries):
            return None
        return self.entries[i - self.snap_index - 1].term

    def entry(self, i: int) -> Entry:
        return self.entries[i - self.snap_index - 1]

    def slice(self, lo: int, hi: int) -> list[Entry]:
        """Entries with lo <= index < hi (clamped to what is available)."""
        lo = max(lo, self.snap_index + 1)
        hi = min(hi, self.last_index() + 1)
        if lo >= hi:
            return []
        return self.entries[lo - self.snap_index - 1: hi - self.snap_index - 1]

    def append(self, ents: list[Entry]) -> None:
        """Append, truncating any existing suffix starting at ents[0].index."""
        if not ents:
            return
        first = ents[0].index
        if first <= self.commit:
            raise AssertionError(f"attempt to overwrite committed entry {first} (commit={self.commit})")
        if not (self.snap_index < first <= self.last_index() + 1):
            raise AssertionError(f"append at {first} leaves a gap (last={self.last_index()})")
        del self.entries[first - self.snap_index - 1:]
        self.entries.extend(ents)
        self.stable = min(self.stable, first - 1)

    def unstable(self) -> list[Entry]:
        return self.entries[self.stable - self.snap_index:]

    def compact(self, index: int) -> None:
        term = self.term_at(index)
        assert term is not None and index > self.snap_index
        del self.entries[: index - self.snap_index]
        self.snap_index, self.snap_term = index, term
        self.stable = max(self.stable, index)

    def restore(self, s: Snapshot) -> None:
        self.entries = []
        self.snap_index, self.snap_term = s.index, s.term
        self.commit = self.applied = self.stable = s.index

    def is_up_to_date(self, index: int, term: int) -> bool:
        lt = self.last_term()
        return term > lt or (term == lt and index >= self.last_index())

    def first_index_of_term(self, term: int, upto: int) -> int:
        """Smallest index whose term == `term` (terms are non-decreasing)."""
        hi = upto - self.snap_index  # entries[:hi] have index <= upto
        pos = bisect.bisect_left(self.entries, term, 0, max(0, hi), key=_TERM_KEY)
        return self.snap_index + 1 + pos

    def last_index_of_term(self, term: int, upto: int) -> int | None:
        hi = min(upto, self.last_index()) - self.snap_index
        pos = bisect.bisect_right(self.entries, term, 0, max(0, hi), key=_TERM_KEY)
        if pos > 0 and self.entries[pos - 1].term == term:
            return self.snap_index + pos
        if pos == 0 and self.snap_term == term:
            return self.snap_index
        return None


@dataclass(slots=True)
class Progress:
    match: int = 0
    next: int = 1
    acked_ctx: int = 0
    recent_active: bool = True


class Raft:
    def __init__(
        self,
        node_id: str,
        *,
        hard_state: HardState | None = None,
        snapshot: Snapshot | None = None,
        entries=(),
        rng: random.Random | None = None,
        election_ticks: int = 10,
        heartbeat_ticks: int = 1,
        prevote: bool = True,
        check_quorum: bool = True,
        read_only: str = "safe",  # "safe" (ReadIndex) or "lease"
        lease_ticks: int | None = None,
        max_msg_entries: int = 64,
        max_msg_bytes: int = 1 << 20,
        unsafe_commit_any_term: bool = False,  # mutation-testing switch; never enable in production
    ):
        if read_only not in ("safe", "lease"):
            raise ValueError("read_only must be 'safe' or 'lease'")
        self.id = node_id
        self.rng = rng if rng is not None else random.Random()
        self.election_ticks = election_ticks
        self.heartbeat_ticks = heartbeat_ticks
        self.prevote = prevote
        self.check_quorum = check_quorum or read_only == "lease"
        self.read_only = read_only
        self.lease_ticks = lease_ticks if lease_ticks is not None else max(1, election_ticks - 2)
        self.max_msg_entries = max_msg_entries
        self.max_msg_bytes = max_msg_bytes
        self.unsafe_commit_any_term = unsafe_commit_any_term

        self.log = RaftLog(snapshot, list(entries))
        self.snapshot = snapshot
        hs = hard_state or HardState()
        self.term = hs.term
        self.vote = hs.vote
        self.log.commit = min(max(hs.commit, self.log.snap_index), self.log.last_index())

        self.voters: dict = {}
        self.config_index = 0
        self.state = FOLLOWER
        self._recompute_config()

        self.msgs: list[Message] = []
        self.read_states: list[ReadState] = []
        self.failed_reads: list = []
        self.pending_reads: list = []   # (seq, index, ctx) waiting for quorum heartbeat ack
        self.waiting_reads: list = []   # ctx waiting for the leader's first commit in its term
        self.pending_snapshot: Snapshot | None = None
        self._last_hs = HardState(self.term, self.vote, self.log.commit)
        self.ticks = 0
        self.read_seq = 0
        self.seq_tick: dict[int, int] = {}
        self._seq_lo = 1
        self.term_start_index = 0
        self._lease_disabled_until = 0
        self.progress: dict[str, Progress] = {}
        self.state = FOLLOWER
        self.leader: str | None = None
        self._reset()

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def bootstrap(voters: dict) -> tuple[HardState, list[Entry]]:
        """Initial durable state shared by every founding member of a cluster."""
        e = Entry(1, 1, ENTRY_CONFIG, encode_conf({"op": "bootstrap", "voters": voters}))
        return HardState(1, None, 1), [e]

    def _quorum(self) -> int:
        return len(self.voters) // 2 + 1

    def _promotable(self) -> bool:
        return self.id in self.voters

    def _send(self, m: Message) -> None:
        self.msgs.append(m)

    def _recompute_config(self) -> None:
        cfg = dict(self.snapshot.config) if self.snapshot else {}
        cidx = self.log.snap_index if self.snapshot else 0
        for e in self.log.entries:
            if e.kind == ENTRY_CONFIG:
                cfg = apply_conf(cfg, decode_conf(e.data))
                cidx = e.index
        self.voters = cfg
        self.config_index = cidx
        if self.state == LEADER:
            for v in cfg:
                if v != self.id and v not in self.progress:
                    self.progress[v] = Progress(match=0, next=self.log.last_index() + 1)
            for v in list(self.progress):
                if v not in cfg:
                    del self.progress[v]

    def config_at(self, index: int) -> dict:
        cfg = dict(self.snapshot.config) if self.snapshot else {}
        for e in self.log.entries:
            if e.index > index:
                break
            if e.kind == ENTRY_CONFIG:
                cfg = apply_conf(cfg, decode_conf(e.data))
        return cfg

    def _hs(self) -> HardState:
        return HardState(self.term, self.vote, self.log.commit)

    def _fail_pending_reads(self) -> None:
        for _, _, ctx in getattr(self, "pending_reads", []):
            self.failed_reads.append(ctx)
        for ctx in getattr(self, "waiting_reads", []):
            self.failed_reads.append(ctx)
        self.pending_reads = []
        self.waiting_reads = []

    def _reset(self) -> None:
        self.election_elapsed = 0
        self.heartbeat_elapsed = 0
        self.randomized_timeout = self.election_ticks + self.rng.randrange(self.election_ticks)
        self.votes: dict[str, bool] = {}
        self.lead_transferee: str | None = None
        self.progress = {}
        self._fail_pending_reads()

    # ------------------------------------------------------------- transitions
    def _become_follower(self, term: int, leader: str | None) -> None:
        if term > self.term:
            self.term = term
            self.vote = None
        self.state = FOLLOWER
        self.leader = leader
        self._reset()

    def _become_pre_candidate(self) -> None:
        self.state = PRE_CANDIDATE
        self.leader = None
        self._reset()

    def _become_candidate(self) -> None:
        self.term += 1
        self.vote = self.id
        self.state = CANDIDATE
        self.leader = None
        self._reset()

    def _become_leader(self) -> None:
        self.state = LEADER
        self.leader = self.id
        self._reset()
        last = self.log.last_index()
        self.progress = {v: Progress(match=0, next=last + 1) for v in self.voters if v != self.id}
        noop = Entry(self.term, last + 1, ENTRY_NOOP, b"")
        self.log.append([noop])
        self.term_start_index = noop.index
        self._maybe_commit()
        self._bcast_append()

    def _campaign(self, pre: bool, transfer: bool = False) -> None:
        if pre:
            self._become_pre_candidate()
            vote_term, mtype = self.term + 1, MSG_PREVOTE
        else:
            self._become_candidate()
            vote_term, mtype = self.term, MSG_VOTE
        if self._poll(self.id, True) == "won":
            if pre:
                self._campaign(False, transfer)
            else:
                self._become_leader()
            return
        li, lt = self.log.last_index(), self.log.last_term()
        for v in sorted(self.voters):
            if v != self.id:
                self._send(Message(mtype, self.id, v, vote_term, index=li, log_term=lt, transfer=transfer))

    def _poll(self, src: str, granted: bool) -> str:
        self.votes.setdefault(src, granted)
        g = sum(1 for v in self.voters if self.votes.get(v) is True)
        r = sum(1 for v in self.voters if self.votes.get(v) is False)
        q = self._quorum()
        if g >= q:
            return "won"
        if r > len(self.voters) - q:
            return "lost"
        return "pending"

    # --------------------------------------------------------------- public API
    def tick(self) -> None:
        self.ticks += 1
        if self.state == LEADER:
            self._tick_leader()
        else:
            self.election_elapsed += 1
            if self._promotable() and self.election_elapsed >= self.randomized_timeout:
                self._campaign(pre=self.prevote)

    def _tick_leader(self) -> None:
        self.heartbeat_elapsed += 1
        self.election_elapsed += 1
        if self.election_elapsed >= self.election_ticks:
            self.election_elapsed = 0
            if self.check_quorum:
                active = sum(
                    1 for v in self.voters
                    if v == self.id or (v in self.progress and self.progress[v].recent_active)
                )
                if active < self._quorum():
                    self._become_follower(self.term, None)
                    return
                for pr in self.progress.values():
                    pr.recent_active = False
            self.lead_transferee = None  # abort a transfer that did not finish in time
        if self.heartbeat_elapsed >= self.heartbeat_ticks:
            self.heartbeat_elapsed = 0
            self._bcast_heartbeat()

    def propose(self, data: bytes, kind: int = ENTRY_NORMAL) -> tuple[int, int]:
        """Append a command; returns (index, term).  Raises NotLeader."""
        if self.state != LEADER:
            raise NotLeader(self.leader)
        if self.lead_transferee is not None:
            raise ProposalDropped("leadership transfer in progress")
        e = Entry(self.term, self.log.last_index() + 1, kind, data)
        self.log.append([e])
        if kind == ENTRY_CONFIG:
            self._recompute_config()
        self._maybe_commit()
        self._bcast_append(allow_empty=False)
        return e.index, e.term

    def propose_conf_change(self, cc: dict) -> tuple[int, int]:
        """Single-server membership change: {"op": "add"|"remove", "id": ..., "meta": {...}}."""
        if self.state != LEADER:
            raise NotLeader(self.leader)
        if self.log.commit < self.term_start_index:
            raise ProposalDropped("leader has not committed an entry in its term yet")
        if self.config_index > self.log.commit:
            raise ProposalDropped("another membership change is still uncommitted")
        op = cc.get("op")
        if op == "add":
            if cc["id"] in self.voters:
                raise ValueError(f"{cc['id']} is already a member")
        elif op == "remove":
            if cc["id"] not in self.voters:
                raise ValueError(f"{cc['id']} is not a member")
            if len(self.voters) <= 1:
                raise ValueError("cannot remove the last member")
        else:
            raise ValueError("op must be add or remove")
        return self.propose(encode_conf(cc), ENTRY_CONFIG)

    def read_index(self, ctx) -> None:
        """Request a linearizable read point; result appears as a ReadState in ready()."""
        if self.state != LEADER:
            raise NotLeader(self.leader)
        if self.log.commit < self.term_start_index:
            self.waiting_reads.append(ctx)
            return
        self._register_read(ctx)

    def _register_read(self, ctx) -> None:
        if self.voters.keys() == {self.id}:
            self.read_states.append(ReadState(ctx, self.log.commit))
            return
        if self.read_only == "lease" and self._lease_valid():
            self.read_states.append(ReadState(ctx, self.log.commit))
            return
        self._next_seq()
        self.pending_reads.append((self.read_seq, self.log.commit, ctx))
        for p in self.progress:
            self._send_append(p)

    def transfer_leadership(self, target: str) -> None:
        if self.state != LEADER:
            raise NotLeader(self.leader)
        if target == self.id or target not in self.progress:
            raise ValueError(f"cannot transfer leadership to {target!r}")
        self.lead_transferee = target
        self.election_elapsed = 0
        # the transferee may win without waiting out the lease, so stop lease reads
        self._lease_disabled_until = self.ticks + 2 * self.election_ticks
        pr = self.progress[target]
        if pr.match == self.log.last_index():
            self._send(Message(MSG_TIMEOUT_NOW, self.id, target, self.term))
        else:
            self._send_append(target)

    def compact(self, index: int, data: bytes) -> Snapshot:
        """Called by the application after it applied `index`; drops log <= index."""
        if not (self.log.snap_index < index <= self.log.applied):
            raise ValueError(f"cannot compact at {index} (snap={self.log.snap_index}, applied={self.log.applied})")
        snap = Snapshot(index, self.log.term_at(index), self.config_at(index), data)
        self.log.compact(index)
        self.snapshot = snap
        return snap

    # -------------------------------------------------------------------- step
    def step(self, m: Message) -> None:
        if m.dst != self.id:
            return
        if m.term > self.term:
            if m.type in (MSG_VOTE, MSG_PREVOTE):
                in_lease = self.check_quorum and (
                    (self.leader is not None and self.election_elapsed < self.election_ticks)
                    # lease mode: a freshly (re)started node may have acked a lease before
                    # it crashed, so it must not vote until one election timeout has passed
                    or (self.read_only == "lease" and self.ticks < self.election_ticks))
                if in_lease and not m.transfer:
                    return  # leader stickiness: ignore disruptive candidates
            if m.type == MSG_PREVOTE or (m.type == MSG_PREVOTE_RESP and not m.reject):
                pass  # pre-votes never change our term
            else:
                leader = m.src if m.type in (MSG_APPEND, MSG_SNAPSHOT) else None
                self._become_follower(m.term, leader)
        elif m.term < self.term:
            if m.type in (MSG_APPEND, MSG_SNAPSHOT):
                # tell a stale leader that a newer term exists so it steps down
                self._send(Message(MSG_APPEND_RESP, self.id, m.src, self.term, reject=True))
            elif m.type == MSG_PREVOTE:
                self._send(Message(MSG_PREVOTE_RESP, self.id, m.src, self.term, reject=True))
            return

        t = m.type
        if t in (MSG_VOTE, MSG_PREVOTE):
            self._handle_vote(m)
        elif self.state == LEADER:
            if t == MSG_APPEND_RESP:
                self._handle_append_resp(m)
        elif self.state in (CANDIDATE, PRE_CANDIDATE):
            if t in (MSG_APPEND, MSG_SNAPSHOT):
                self._become_follower(m.term, m.src)
                self._step_follower(m)
            elif (t == MSG_PREVOTE_RESP and self.state == PRE_CANDIDATE) or \
                 (t == MSG_VOTE_RESP and self.state == CANDIDATE):
                res = self._poll(m.src, not m.reject)
                if res == "won":
                    if self.state == PRE_CANDIDATE:
                        self._campaign(False)
                    else:
                        self._become_leader()
                elif res == "lost":
                    self._become_follower(self.term, None)
        else:
            self._step_follower(m)

    def _step_follower(self, m: Message) -> None:
        if m.type == MSG_APPEND:
            self.election_elapsed = 0
            self.leader = m.src
            self._handle_append(m)
        elif m.type == MSG_SNAPSHOT:
            self.election_elapsed = 0
            self.leader = m.src
            self._handle_snapshot(m)
        elif m.type == MSG_TIMEOUT_NOW:
            if self._promotable():
                self._campaign(False, transfer=True)

    def _handle_vote(self, m: Message) -> None:
        can = (self.vote == m.src
               or (self.vote is None and self.leader is None)
               or (m.type == MSG_PREVOTE and m.term > self.term))
        rtype = MSG_VOTE_RESP if m.type == MSG_VOTE else MSG_PREVOTE_RESP
        if can and self.log.is_up_to_date(m.index, m.log_term):
            self._send(Message(rtype, self.id, m.src, m.term))
            if m.type == MSG_VOTE:
                self.election_elapsed = 0
                self.vote = m.src
        else:
            self._send(Message(rtype, self.id, m.src, self.term, reject=True))

    def _handle_append(self, m: Message) -> None:
        prev, prev_term, ents = m.index, m.log_term, m.entries
        commit = self.log.commit
        if prev < commit:
            # Everything up to our commit index is committed and therefore identical
            # to the leader's log; skip that part of the message.
            skip = commit - prev
            ents = ents[skip:] if skip < len(ents) else ()
            prev, prev_term = commit, self.log.term_at(commit)
        t = self.log.term_at(prev)
        if t != prev_term:
            if t is None:
                hi, ht = self.log.last_index() + 1, 0
            else:
                ht, hi = t, self.log.first_index_of_term(t, prev)
            self._send(Message(MSG_APPEND_RESP, self.id, m.src, self.term, reject=True,
                               index=m.index, hint_index=hi, hint_term=ht, ctx=m.ctx))
            return
        last_new = prev + len(ents)
        for i, e in enumerate(ents):
            lt = self.log.term_at(e.index)
            if lt != e.term:
                rest = list(ents[i:])
                truncating = lt is not None
                self.log.append(rest)
                if any(x.kind == ENTRY_CONFIG for x in rest) or (truncating and e.index <= self.config_index):
                    self._recompute_config()
                break
        new_commit = min(m.commit, last_new)
        if new_commit > self.log.commit:
            self.log.commit = new_commit
        self._send(Message(MSG_APPEND_RESP, self.id, m.src, self.term, index=last_new, ctx=m.ctx))

    def _handle_snapshot(self, m: Message) -> None:
        s = m.snapshot
        if s.index <= self.log.commit:
            self._send(Message(MSG_APPEND_RESP, self.id, m.src, self.term, index=self.log.commit))
            return
        if self.log.term_at(s.index) == s.term:
            self.log.commit = s.index
        else:
            self.log.restore(s)
            self.snapshot = s
            self.pending_snapshot = s
            self._recompute_config()
        self._send(Message(MSG_APPEND_RESP, self.id, m.src, self.term, index=s.index))

    def _handle_append_resp(self, m: Message) -> None:
        pr = self.progress.get(m.src)
        if pr is None:
            return
        pr.recent_active = True
        if m.ctx > pr.acked_ctx:
            pr.acked_ctx = m.ctx
        last = self.log.last_index()
        if m.reject:
            if m.index > pr.match:  # otherwise a stale rejection
                if m.hint_term:
                    idx = self.log.last_index_of_term(m.hint_term, m.index)
                    new = idx + 1 if idx is not None else m.hint_index
                else:
                    new = m.hint_index
                pr.next = max(pr.match + 1, min(new, m.index, last + 1))
                self._send_append(m.src)
        else:
            if m.index > pr.match:
                pr.match = m.index
                if pr.next <= pr.match:
                    pr.next = pr.match + 1
                if self._maybe_commit():
                    self._bcast_append()
                elif self.state != LEADER:
                    return  # committed our own removal and stepped down
                elif pr.next <= last:
                    self._send_append(m.src, allow_empty=False)
            if m.src == self.lead_transferee and pr.match == last:
                self._send(Message(MSG_TIMEOUT_NOW, self.id, m.src, self.term))
        self._release_reads()

    # ------------------------------------------------------------- replication
    def _next_seq(self) -> None:
        self.read_seq += 1
        self.seq_tick[self.read_seq] = self.ticks

    def _bcast_heartbeat(self) -> None:
        self._next_seq()
        lo = min((pr.acked_ctx for pr in self.progress.values()), default=self.read_seq)
        while self._seq_lo < lo:
            self.seq_tick.pop(self._seq_lo, None)
            self._seq_lo += 1
        for p in self.progress:
            self._send_append(p)

    def _bcast_append(self, allow_empty: bool = True) -> None:
        for p in self.progress:
            self._send_append(p, allow_empty=allow_empty)

    def _send_append(self, to: str, allow_empty: bool = True) -> None:
        pr = self.progress[to]
        last = self.log.last_index()
        if pr.next > last + 1:
            pr.next = last + 1
        prev = pr.next - 1
        pt = self.log.term_at(prev)
        if pt is None:  # prev was compacted away: ship the snapshot
            self._send(Message(MSG_SNAPSHOT, self.id, to, self.term, snapshot=self.snapshot))
            pr.next = self.snapshot.index + 1
            return
        ents = self.log.slice(pr.next, last + 1)
        if not ents and not allow_empty:
            return
        if ents:
            n, size = 0, 0
            for e in ents:
                if n >= self.max_msg_entries or (n > 0 and size + len(e.data) > self.max_msg_bytes):
                    break
                n += 1
                size += len(e.data)
            ents = ents[:n]
            pr.next = ents[-1].index + 1  # optimistic pipelining
        self._send(Message(MSG_APPEND, self.id, to, self.term, index=prev, log_term=pt,
                           entries=tuple(ents), commit=self.log.commit, ctx=self.read_seq))

    def _match_of(self, v: str) -> int:
        return self.log.last_index() if v == self.id else self.progress[v].match

    def _maybe_commit(self) -> bool:
        if not self.voters:
            return False
        matches = sorted((self._match_of(v) for v in self.voters if v == self.id or v in self.progress),
                         reverse=True)
        if len(matches) < self._quorum():
            return False
        n = matches[self._quorum() - 1]
        if n <= self.log.commit:
            return False
        # Raft §5.4.2: only entries from the current term are committed by counting replicas.
        if self.log.term_at(n) != self.term and not self.unsafe_commit_any_term:
            return False
        self.log.commit = n
        if self.waiting_reads and self.log.commit >= self.term_start_index:
            waiting, self.waiting_reads = self.waiting_reads, []
            for ctx in waiting:
                self._register_read(ctx)
        if self.id not in self.voters and self.log.commit >= self.config_index:
            self._become_follower(self.term, None)  # we were removed; step down
            return False
        return True

    # ------------------------------------------------------------------- reads
    def _quorum_value(self, mine, peer_values) -> int:
        vals = [mine] if self.id in self.voters else []
        vals += [peer_values(pr) for v, pr in self.progress.items() if v in self.voters]
        vals.sort(reverse=True)
        q = self._quorum()
        return vals[q - 1] if len(vals) >= q else -(1 << 62)

    def _release_reads(self) -> None:
        if not self.pending_reads:
            return
        acked = self._quorum_value(self.read_seq, lambda pr: pr.acked_ctx)
        keep = []
        for seq, idx, ctx in self.pending_reads:
            if seq <= acked:
                self.read_states.append(ReadState(ctx, idx))
            else:
                keep.append((seq, idx, ctx))
        self.pending_reads = keep

    def _lease_valid(self) -> bool:
        if self.ticks < self._lease_disabled_until:
            return False
        t = self._quorum_value(self.ticks, lambda pr: self.seq_tick.get(pr.acked_ctx, -(1 << 62)))
        return t >= self.ticks - self.lease_ticks

    # ------------------------------------------------------------------- ready
    def has_ready(self) -> bool:
        return bool(self.msgs or self.read_states or self.failed_reads or self.pending_snapshot
                    or self.log.stable < self.log.last_index() or self.log.applied < self.log.commit
                    or self._hs() != self._last_hs)

    def ready(self) -> Ready:
        hs = self._hs()
        changed = hs != self._last_hs
        ents = self.log.unstable()
        snap = self.pending_snapshot
        committed = self.log.slice(self.log.applied + 1, self.log.commit + 1)
        must_sync = bool(ents) or snap is not None or (
            changed and (hs.term != self._last_hs.term or hs.vote != self._last_hs.vote))
        rd = Ready(hs if changed else None, ents, snap, self.msgs, committed,
                   self.read_states, self.failed_reads, must_sync)
        self._last_hs = hs
        self.log.stable = self.log.last_index()
        self.pending_snapshot = None
        if committed:
            self.log.applied = committed[-1].index
        self.msgs, self.read_states, self.failed_reads = [], [], []
        return rd

    # ------------------------------------------------------------------ status
    def status(self) -> dict:
        st = {
            "id": self.id, "state": self.state, "term": self.term, "vote": self.vote,
            "leader": self.leader, "commit": self.log.commit, "applied": self.log.applied,
            "last_index": self.log.last_index(), "snapshot_index": self.log.snap_index,
            "voters": sorted(self.voters), "config_index": self.config_index,
        }
        if self.state == LEADER:
            st["progress"] = {k: {"match": p.match, "next": p.next} for k, p in self.progress.items()}
            st["lease_valid"] = self._lease_valid()
            st["transferring_to"] = self.lead_transferee
        return st

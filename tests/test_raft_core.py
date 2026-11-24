"""Hand-written scenarios for the deterministic Raft core."""
import random

import pytest

from conftest import Net
from raftstore.codec import decode_message, encode_message
from raftstore.raft.core import Raft
from raftstore.raft.types import (
    ENTRY_CONFIG, ENTRY_NOOP, FOLLOWER, LEADER, MSG_APPEND, MSG_APPEND_RESP, MSG_PREVOTE_RESP,
    MSG_SNAPSHOT, MSG_VOTE_RESP, Entry, HardState, Message, NotLeader, ProposalDropped, Snapshot,
    encode_conf,
)


def test_single_node_elects_itself_and_commits():
    hs, ents = Raft.bootstrap({"solo": {}})
    r = Raft("solo", hard_state=hs, entries=ents, rng=random.Random(1))
    for _ in range(30):
        r.tick()
    assert r.state == LEADER
    idx, _ = r.propose(b"hello")
    rd = r.ready()
    assert rd.must_sync and any(e.index == idx for e in rd.entries)
    assert any(e.data == b"hello" for e in rd.committed)


def test_three_node_election_and_replication(net3):
    a = net3.elect("a")
    assert a.term == 2 and all(r.leader == "a" for r in net3.nodes.values())
    for i in range(5):
        a.propose(f"v{i}".encode())
    net3.deliver()
    for i, r in net3.nodes.items():
        assert r.log.commit == a.log.last_index()
        assert [e.data for e in net3.applied[i] if e.data.startswith(b"v")] == [f"v{i}".encode() for i in range(5)]


def test_prevote_keeps_partitioned_node_from_inflating_term(net3):
    a = net3.elect("a")
    net3.cut = {"c"}
    term_before = net3.nodes["c"].term
    net3.tick(["c"], n=200)
    assert net3.nodes["c"].term == term_before  # pre-votes never succeed, term stays put
    net3.cut = set()
    net3.tick(n=3)
    assert a.state == LEADER and a.term == term_before  # rejoining node did not disrupt the leader


def test_without_prevote_partitioned_node_bumps_term():
    net = Net(["a", "b", "c"], prevote=False)
    net.elect("a")
    net.cut = {"c"}
    t0 = net.nodes["c"].term
    net.tick(["c"], n=200)
    assert net.nodes["c"].term > t0 + 3


def test_check_quorum_leader_steps_down_when_isolated(net3):
    a = net3.elect("a")
    net3.cut = {"a"}
    net3.tick(["a"], n=2 * a.election_ticks + 1)
    assert a.state == FOLLOWER


def test_commit_only_current_term_entries():
    """Raft §5.4.2 / Figure 8: counting replicas never commits an entry from an older term."""
    for unsafe in (False, True):
        ids = ["s1", "s2", "s3", "s4", "s5"]
        cfg = Entry(1, 1, ENTRY_CONFIG, encode_conf({"op": "bootstrap", "voters": {i: {} for i in ids}}))
        ents = [cfg, Entry(2, 2, ENTRY_NOOP), Entry(2, 3, 0, b"old-term")]
        r = Raft("s1", hard_state=HardState(3, None, 1), entries=ents, rng=random.Random(0),
                 unsafe_commit_any_term=unsafe)
        while r.state == FOLLOWER:
            r.tick()
        for p in ("s2", "s3"):
            r.step(Message(MSG_PREVOTE_RESP, p, "s1", r.term + 1))
        for p in ("s2", "s3"):
            r.step(Message(MSG_VOTE_RESP, p, "s1", r.term))
        assert r.state == LEADER and r.term == 4
        # s2 and s3 acknowledge up to index 3 (the term-2 entry) but not the term-4 no-op at 4
        for p in ("s2", "s3"):
            r.step(Message(MSG_APPEND_RESP, p, "s1", 4, index=3))
        if unsafe:
            assert r.log.commit == 3   # the bug the rule prevents
        else:
            assert r.log.commit == 1
            r.step(Message(MSG_APPEND_RESP, "s2", "s1", 4, index=4))
            r.step(Message(MSG_APPEND_RESP, "s3", "s1", 4, index=4))
            assert r.log.commit == 4   # committing the current-term no-op commits 3 indirectly


def test_fast_backtracking_needs_few_round_trips():
    net = Net(["a", "b", "c"])
    a = net.elect("a")
    for i in range(5):
        a.propose(b"x%d" % i)
    net.deliver()
    net.cut = {"a"}
    for i in range(200):          # 200 uncommittable entries on the old leader
        a.propose(b"doomed%d" % i)
    net.deliver()
    b = net.elect("b")
    for i in range(3):
        b.propose(b"y%d" % i)
    net.deliver()
    net.cut = set()
    rejects = []
    net.tick_filtered(n=3, filt=lambda m: (rejects.append(m) if m.type == MSG_APPEND_RESP and m.reject
                                           and m.src == "a" and m.term == b.term else None) or True)
    assert a.state == FOLLOWER and a.log.last_index() == b.log.last_index()
    assert a.log.term_at(a.log.last_index()) == b.term
    assert len(rejects) <= 3, f"{len(rejects)} rejections: backtracking is not using conflict hints"


def test_snapshot_install_for_lagging_follower():
    net = Net(["a", "b", "c"])
    a = net.elect("a")
    net.cut = {"c"}
    for i in range(40):
        a.propose(b"e%d" % i)
    net.deliver()
    snap = a.compact(a.log.applied, b"STATE")
    assert a.log.snap_index == snap.index and not a.log.slice(1, snap.index + 1)
    seen = []
    net.cut = set()
    net.tick_filtered(n=3, filt=lambda m: (seen.append(m.type)) or True)
    c = net.nodes["c"]
    assert MSG_SNAPSHOT in seen
    assert c.log.snap_index >= snap.index and c.log.commit == a.log.commit
    assert c.snapshot.data == b"STATE" and c.voters.keys() == a.voters.keys()


def test_membership_add_then_remove_leader():
    net = Net(["a", "b", "c"])
    a = net.elect("a")
    a.propose(b"before")
    net.deliver()
    net.nodes["d"] = Raft("d", rng=random.Random(9))      # brand-new empty node
    net.applied["d"], net.read_states["d"] = [], []
    a.propose_conf_change({"op": "add", "id": "d", "meta": {"addr": "10.0.0.4"}})
    with pytest.raises(ProposalDropped):
        a.propose_conf_change({"op": "add", "id": "e"})   # one change at a time
    net.deliver()
    net.tick(n=3)
    assert set(net.nodes["d"].voters) == {"a", "b", "c", "d"}
    assert net.nodes["d"].log.commit == a.log.commit
    a.propose_conf_change({"op": "remove", "id": "a"})
    net.deliver()
    net.tick(n=3)
    assert a.state == FOLLOWER and "a" not in a.voters
    survivors = [net.nodes[i] for i in "bcd"]
    for _ in range(100):
        net.tick(list("bcd"))
        if any(r.state == LEADER for r in survivors):
            break
    new = net.leader()
    assert new.id in "bcd" and set(new.voters) == {"b", "c", "d"}


def test_read_index_confirms_leadership_with_quorum(net3):
    a = net3.elect("a")
    a.read_index("r1")
    net3.collect()
    assert not net3.read_states["a"]          # needs a heartbeat round first
    net3.deliver()
    assert [rs.ctx for rs in net3.read_states["a"]] == ["r1"]
    # an isolated leader can never confirm a read, and fails it when it steps down
    net3.cut = {"a"}
    a.read_index("r2")
    net3.tick(["a"], n=2 * a.election_ticks + 1)
    assert a.state == FOLLOWER
    with pytest.raises(NotLeader):
        a.read_index("r3")


def test_lease_read_served_without_round_trip():
    net = Net(["a", "b", "c"], read_only="lease")
    for _ in range(12):
        net.tick()                           # pass the startup no-vote window
    a = net.elect("a")
    net.tick(n=2)                            # heartbeats acked => lease valid
    assert a._lease_valid()
    a.read_index("fast")
    assert not a.msgs and a.read_states and a.read_states[0].ctx == "fast"


def test_leadership_transfer(net3):
    a = net3.elect("a")
    a.propose(b"x")
    net3.deliver()
    a.transfer_leadership("c")
    net3.deliver()
    c = net3.nodes["c"]
    assert c.state == LEADER and c.term == a.term and a.state == FOLLOWER


def test_follower_truncates_conflicting_uncommitted_suffix():
    net = Net(["a", "b", "c"])
    a = net.elect("a")
    net.cut = {"a"}
    a.propose(b"lost")
    net.deliver()
    b = net.elect("b")
    b.propose(b"kept")
    net.deliver()
    net.cut = set()
    net.tick(n=3)
    datas = [e.data for e in a.log.entries]
    assert b"lost" not in datas and b"kept" in datas


def test_codec_roundtrip_property():
    rng = random.Random(7)
    for _ in range(300):
        ents = tuple(Entry(rng.randint(1, 9), i, rng.choice([0, 1, 2]), rng.randbytes(rng.randint(0, 64)))
                     for i in range(1, rng.randint(0, 5) + 1))
        snap = Snapshot(rng.randint(1, 99), rng.randint(1, 9), {"a": {"raft": "10.0.0.1:7000"}},
                        rng.randbytes(rng.randint(0, 100))) if rng.random() < 0.3 else None
        m = Message(rng.choice([MSG_APPEND, MSG_APPEND_RESP, MSG_SNAPSHOT]), "a", "b", rng.randint(0, 50),
                    index=rng.randint(0, 99), log_term=rng.randint(0, 9), entries=ents,
                    commit=rng.randint(0, 99), reject=rng.random() < 0.5, hint_index=rng.randint(0, 9),
                    hint_term=rng.randint(0, 9), ctx=rng.randint(0, 999), snapshot=snap,
                    transfer=rng.random() < 0.5)
        assert decode_message(encode_message(m)) == m

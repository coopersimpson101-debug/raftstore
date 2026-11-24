"""The owner's acceptance test: pull a drive mid-write and show nothing is lost.

These run the real Raft core *and* the real WAL/snapshot code on FaultyFS inside
the deterministic simulator, so every run is reproducible from its seed.
"""
import math

import pytest

from conftest import SIM_TOTALS
from raftstore.raft.types import NotLeader, ProposalDropped
from raftstore.sim.cluster import SimCluster, SimConfig, SimNode


def quiet_cfg(**kw):
    base = dict(storage="disk", p_crash=0.0, p_restart=0.0, p_partition=0.0, p_client=0.6,
                drop=0.02, duplicate=0.01, compact_every=25)
    base.update(kw)
    return SimConfig(**base)


def wait_for(c: SimCluster, pred, steps=3000):
    for _ in range(steps):
        c.step()
        if pred():
            return
    raise AssertionError("condition not reached")


def current_leader(c):
    ls = c.leaders()
    return max(ls, key=lambda n: n.raft.term) if ls else None


@pytest.mark.parametrize("after_writes", range(0, 16))
@pytest.mark.parametrize("seed", range(4))
def test_pull_leader_drive_mid_write_then_reinsert(seed, after_writes):
    c = SimCluster(seed, quiet_cfg())
    wait_for(c, lambda: current_leader(c) is not None)
    c.run(250)
    victim = current_leader(c)
    acked_before = sum(1 for o in c.history if o.kind == "w" and o.ret != math.inf)
    c.pull_drive(victim, after_writes)
    wait_for(c, lambda: not victim.alive)          # fail-stop on the first EIO
    wait_for(c, lambda: current_leader(c) is not None and current_leader(c) is not victim)
    c.run(300)                                     # cluster keeps serving writes without it
    acked_after = sum(1 for o in c.history if o.kind == "w" and o.ret != math.inf)
    assert acked_after > acked_before, "cluster stopped accepting writes"
    c.heal()                                       # drive re-inserted, node restarts and catches up
    assert victim.alive
    assert c.check_linearizable().ok
    n = c.check_no_acked_write_lost()
    SIM_TOTALS["acked_writes_verified"] += n
    SIM_TOTALS["drive_pulls"] += 1


@pytest.mark.parametrize("seed", range(6))
def test_dead_drive_node_replaced_and_data_rereplicated(seed):
    c = SimCluster(seed, quiet_cfg())
    wait_for(c, lambda: current_leader(c) is not None)
    c.run(300)
    victim = current_leader(c)
    c.pull_drive(victim, 2)
    wait_for(c, lambda: not victim.alive)
    victim.retired = True                         # this drive is dead for good
    wait_for(c, lambda: current_leader(c) is not None)
    c.run(200)

    def change(cc):
        for _ in range(3000):
            c.step()
            ld = current_leader(c)
            if ld is None:
                continue
            try:
                ld.raft.propose_conf_change(cc)
                ld.process()
                break
            except (NotLeader, ProposalDropped):
                continue
        wait_for(c, lambda: (ld := current_leader(c)) is not None and ld.raft.log.commit >= ld.raft.config_index
                 and (cc["id"] in ld.raft.voters) == (cc["op"] == "add"))

    change({"op": "remove", "id": victim.id})       # remove first: 2-node quorum is still a majority
    new = SimNode(c, "replacement", None)            # empty disk, no state at all
    c.nodes["replacement"] = new
    change({"op": "add", "id": "replacement", "meta": {}})
    c.run(300)
    c.heal()
    leader = current_leader(c)
    assert set(leader.raft.voters) == set(c.nodes) - {victim.id}
    assert new.raft.log.applied >= leader.raft.log.commit - 0
    assert new.raft.log.snap_index > 0 or new.raft.log.applied > 0
    assert new.kv == leader.kv                      # replicated state equal on the replacement
    assert c.check_linearizable().ok
    SIM_TOTALS["acked_writes_verified"] += c.check_no_acked_write_lost()


@pytest.mark.parametrize("seed", range(10))
def test_whole_cluster_power_loss_with_torn_writes(seed):
    """All nodes lose power at the same instant, mid-write; every acknowledged write survives."""
    c = SimCluster(seed, quiet_cfg())
    wait_for(c, lambda: current_leader(c) is not None)
    c.run(400)
    for n in c.live():
        c.crash(n, "power cut")                      # FaultyFS.crash(): torn tails everywhere
    for n in list(c.nodes.values()):
        c.restart(n)
    c.heal()
    assert c.check_linearizable().ok
    SIM_TOTALS["acked_writes_verified"] += c.check_no_acked_write_lost()

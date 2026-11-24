"""Randomized deterministic simulation: thousands of steps per seed, invariants checked every step.

Scale with RAFTSTORE_SIM_SEEDS (seeds per scenario) and RAFTSTORE_SIM_STEPS.
"""
import os

import pytest

from conftest import SIM_TOTALS, sim_seeds
from raftstore.sim.cluster import SimCluster, SimConfig

STEPS = int(os.environ.get("RAFTSTORE_SIM_STEPS", 3000))

SCENARIOS = {
    # name: (config factory, default seed count)
    "lossy-net+crashes+partitions": (lambda: SimConfig(), 60),
    "five-nodes+leadership-transfer": (lambda: SimConfig(nodes=5, p_transfer=0.005), 20),
    "membership-changes": (lambda: SimConfig(p_membership=0.01, p_transfer=0.005), 30),
    "lease-reads(synced-clocks)": (lambda: SimConfig(synced_ticks=True, raft={"read_only": "lease"}), 30),
    "no-prevote/no-checkquorum": (lambda: SimConfig(raft={"prevote": False, "check_quorum": False}), 25),
    "real-WAL-on-faulty-disk+drive-pulls": (lambda: SimConfig(storage="disk", p_pull_drive=0.003), 40),
    "everything-at-once": (lambda: SimConfig(storage="disk", p_pull_drive=0.002, p_membership=0.008,
                                             p_transfer=0.004), 30),
}


def run_schedule(seed: int, cfg: SimConfig, steps: int = STEPS) -> SimCluster:
    c = SimCluster(seed, cfg)
    c.run(steps)
    c.heal()
    res = c.check_linearizable()
    assert res.ok, f"seed {seed}: history for key {res.key!r} is not linearizable"
    acked = c.check_no_acked_write_lost()
    s = c.summary()
    SIM_TOTALS["schedules"] += 1
    SIM_TOTALS["steps"] += s["steps"]
    SIM_TOTALS["invariant_checks"] += s["invariant_checks"]
    SIM_TOTALS["client_ops"] += s["ops"]
    SIM_TOTALS["acked_writes_verified"] += acked
    SIM_TOTALS["crashes"] += s["crashes"]
    SIM_TOTALS["partitions"] += s["partitions"]
    SIM_TOTALS["drive_pulls"] += s["drive_pulls"]
    SIM_TOTALS["msgs_dropped"] += s["msgs_dropped"]
    SIM_TOTALS["msgs_duplicated"] += s["msgs_duplicated"]
    SIM_TOTALS["linearizability_histories"] += 1
    return c


@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_random_schedules(scenario):
    factory, default = SCENARIOS[scenario]
    for seed in range(sim_seeds(default)):
        run_schedule(seed, factory())


def test_simulation_is_deterministic():
    a = SimCluster(42, SimConfig(storage="disk", p_pull_drive=0.003))
    b = SimCluster(42, SimConfig(storage="disk", p_pull_drive=0.003))
    a.run(1500)
    b.run(1500)
    assert a.events == b.events and a.summary() == b.summary()


def test_checker_catches_injected_stale_read_bug():
    """Mutation test: serving reads from any replica must be caught by the linearizability checker."""
    for seed in range(30):
        c = SimCluster(seed, SimConfig(bug_stale_reads=True))
        c.run(2000)
        c.heal()
        if not c.check_linearizable().ok:
            return
    pytest.fail("linearizability checker missed stale reads in 30 schedules")

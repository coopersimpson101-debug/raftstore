"""Opt-in structured execution traces (raftstore.raft.trace)."""
import io
import json
from collections import Counter

from raftstore.raft.core import Raft
from raftstore.raft.trace import TraceWriter, msg_json
from raftstore.sim.cluster import SimCluster, SimConfig

REQUIRED = {"n", "i", "ts", "ev", "args", "out", "reads", "logops", "post"}


def load(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def test_tracing_is_off_by_default():
    hs, ents = Raft.bootstrap({"a": {}})
    r = Raft("a", hard_state=hs, entries=ents)
    assert r.tracer is None
    r.tick()
    r.ready()


def test_simulator_trace_is_complete_and_well_formed(tmp_path):
    p = tmp_path / "t.jsonl"
    c = SimCluster(3, SimConfig(p_transfer=0.005), trace_path=str(p))
    c.run(800)
    c.heal()
    c.close_trace()
    evs = load(p)
    assert evs[0]["ev"] == "meta" and evs[0]["seed"] == 3
    kinds = Counter(e["ev"] for e in evs[1:])
    for k in ("init", "tick", "step", "ready", "propose", "read_index"):
        assert kinds[k] > 0, k
    for e in evs[1:]:
        assert REQUIRED <= e.keys()
        assert e["post"]["commit"] <= e["post"]["last"]
    # every delivered message was emitted by an earlier traced call of its sender
    sent = Counter()
    for e in evs[1:]:
        if e["ev"] == "step":
            key = json.dumps(e["args"]["m"], sort_keys=True)
            assert sent[key] > 0, e["args"]["m"]
        for m in e["out"]:
            sent[json.dumps(m, sort_keys=True)] += 1
    # sequence numbers are dense
    assert [e["i"] for e in evs] == list(range(1, len(evs) + 1))


def test_tracing_does_not_change_the_simulation(tmp_path):
    a = SimCluster(11, SimConfig(storage="disk", p_pull_drive=0.003))
    b = SimCluster(11, SimConfig(storage="disk", p_pull_drive=0.003), trace_path=str(tmp_path / "b.jsonl"))
    a.run(1000)
    b.run(1000)
    b.close_trace()
    assert a.events == b.events and a.summary() == b.summary()


def test_log_mutations_are_recorded():
    buf = io.StringIO()
    hs, ents = Raft.bootstrap({"a": {}})
    r = Raft("a", hard_state=hs, entries=ents, tracer=TraceWriter(buf))
    while r.state != "leader":
        r.tick()
    r.ready()
    r.propose(b"x")
    r.ready()
    r.compact(2, b"{}")
    evs = [json.loads(line) for line in buf.getvalue().splitlines()]
    prop = [e for e in evs if e["ev"] == "propose"][0]
    assert prop["logops"][0][0] == "append" and prop["logops"][0][1] == 3
    assert prop["args"]["kind"] == 0 and len(prop["args"]["digest"]) == 16
    comp = [e for e in evs if e["ev"] == "compact"][0]
    assert comp["logops"] == [["compact", 2, 2]]  # index 2 is the term-2 no-op
    assert comp["post"]["snap"] == 2


def test_errors_are_recorded():
    buf = io.StringIO()
    hs, ents = Raft.bootstrap({"a": {}, "b": {}, "c": {}})
    r = Raft("a", hard_state=hs, entries=ents, tracer=TraceWriter(buf))
    try:
        r.propose(b"x")
    except Exception:  # noqa: BLE001
        pass
    last = json.loads(buf.getvalue().splitlines()[-1])
    assert last["ev"] == "propose" and last["err"] == "NotLeader"


def test_trace_dir_from_environment(tmp_path, monkeypatch):
    monkeypatch.delenv("RAFTSTORE_TRACE_DIR", raising=False)
    assert TraceWriter.from_env("n1") is None
    monkeypatch.setenv("RAFTSTORE_TRACE_DIR", str(tmp_path))
    w = TraceWriter.from_env("n1")
    w.write({"ev": "x"})
    w.close()
    assert json.loads((tmp_path / "n1.jsonl").read_text())["ev"] == "x"


def test_msg_json_round_trips_fields():
    from raftstore.raft.types import MSG_APPEND, Entry, Message
    m = Message(MSG_APPEND, "a", "b", 3, index=4, log_term=2, entries=(Entry(3, 5, 0, b"v"),), commit=4, ctx=7)
    d = msg_json(m)
    assert d["type"] == "append" and d["entries"][0][:3] == [3, 5, 0] and d["ctx"] == 7

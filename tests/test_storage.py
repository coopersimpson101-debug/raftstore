"""WAL, snapshot and chunk-store durability: torn writes, bit rot, crash at every fsync, pulled drives."""
import random

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from conftest import SIM_TOTALS
from raftstore.raft.types import Entry, HardState, Snapshot
from raftstore.storage.chunks import ChunkStore, sha256_hex
from raftstore.storage.fs import FaultyFS, RealFS, SimulatedCrash
from raftstore.storage.raftlog import DiskStorage, MemoryStorage, SnapshotStore
from raftstore.storage.wal import WAL, CorruptLogError, encode_record, scan

# --------------------------------------------------------------------- WAL format


def _records(n, rng):
    return [encode_record(rng.choice([1, 2, 3]), rng.randbytes(rng.randint(0, 300))) for _ in range(n)]


def test_wal_torn_tail_truncated_at_every_byte_offset():
    rng = random.Random(1)
    recs = _records(12, rng)
    blob = b"".join(recs)
    bounds = [0]
    for r in recs:
        bounds.append(bounds[-1] + len(r))
    for cut in range(len(blob) + 1):
        fs = FaultyFS()
        fs.makedirs("/w")
        f = fs.open_write("/w/0000000000000000.wal")
        f.write(blob[:cut])
        f.sync()
        fs.sync_dir("/w")
        wal = WAL(fs, "/w")
        got = wal.open()
        complete = max(i for i, b in enumerate(bounds) if b <= cut)
        assert len(got) == complete
        assert fs.size("/w/0000000000000000.wal") == bounds[complete]  # torn bytes removed
        wal.close()
    SIM_TOTALS["crash_points"] += len(blob) + 1


def test_wal_corruption_before_tail_refuses_to_start():
    rng = random.Random(2)
    recs = _records(10, rng)
    blob = bytearray(b"".join(recs))
    off = len(recs[0]) + len(recs[1]) + 5     # inside record #3, valid records follow
    blob[off] ^= 0x40
    fs = FaultyFS()
    fs.makedirs("/w")
    f = fs.open_write("/w/0000000000000000.wal")
    f.write(bytes(blob))
    f.sync()
    fs.sync_dir("/w")
    with pytest.raises(CorruptLogError):
        WAL(fs, "/w").open()


@given(st.lists(st.tuples(st.sampled_from([1, 2, 3]), st.binary(max_size=200)), max_size=20),
       st.binary(max_size=40))
@settings(max_examples=200, deadline=None)
def test_wal_scan_property(records, garbage):
    blob = b"".join(encode_record(t, p) for t, p in records)
    got, valid, _ = scan(blob)
    assert got == records and valid == len(blob)
    got2, valid2, _ = scan(blob + garbage)
    assert got2[: len(records)] == records and valid2 >= len(blob)


# ----------------------------------------------------- DiskStorage vs. a model

def gen_workload(seed, n_ops=40):
    """A random but Raft-legal sequence of storage operations, with the expected state after each."""
    rng = random.Random(seed)
    model = MemoryStorage()
    term, vote, commit = 1, None, 0
    ops, states = [], []

    def last():
        base = model.snapshot.index if model.snapshot else 0
        return base + len(model.entries)

    for _ in range(n_ops):
        snap_idx = model.snapshot.index if model.snapshot else 0
        k = rng.random()
        if k < 0.45:
            ents = [Entry(term, last() + 1 + i, rng.choice([0, 0, 1, 2]), rng.randbytes(rng.randint(0, 80)))
                    for i in range(rng.randint(1, 5))]
            op = ("save", None, ents, None)
        elif k < 0.6 and last() > commit:
            term += 1
            start = rng.randint(max(commit, snap_idx) + 1, last() + 1)
            ents = [Entry(term, start + i, 0, rng.randbytes(20)) for i in range(rng.randint(1, 4))]
            if rng.random() < 0.5:   # follower learns the new entries and their commit in one batch
                commit = rng.randint(commit, ents[-1].index)
            op = ("save", HardState(term, vote, commit), ents, None)
        elif k < 0.75:
            if rng.random() < 0.5:
                term += 1
                vote = rng.choice([None, "a", "b", "c"])
            commit = rng.randint(commit, last())
            op = ("save", HardState(term, vote, commit), [], None)
        elif k < 0.9 and commit > snap_idx:
            idx = rng.randint(snap_idx + 1, commit)
            t = model.entries[idx - snap_idx - 1].term
            op = ("compact", Snapshot(idx, t, {"a": {}}, rng.randbytes(30)))
        else:
            idx = max(commit, last()) + rng.randint(1, 10)
            term += 1
            commit = idx
            op = ("save", HardState(term, vote, commit), [], Snapshot(idx, term, {"a": {}, "b": {}}, rng.randbytes(50)))
        if op[0] == "save":
            model.save(op[1], op[2], op[3])
        else:
            model.compact(op[1])
        ops.append(op)
        states.append((model.hs, model.snapshot, list(model.entries)))
    return ops, states


def apply_op(storage, op):
    if op[0] == "save":
        storage.save(op[1], op[2], op[3], sync=True)
    else:
        storage.compact(op[1])


def logmap(snapshot, entries):
    m = {}
    if snapshot is not None:
        m[snapshot.index] = snapshot.term
    for e in entries:
        m[e.index] = e.term
    return m


def check_recovered(recovered, before, after):
    """`before` = last acknowledged state, `after` = state had the in-flight op finished.

    The recovered state may be anything in between (a torn in-flight batch), but it
    must never lose an acknowledged entry, invent one, regress term/vote, or claim a
    commit index whose entries it does not have.
    """
    hs, snap, ents = recovered
    g_lo = snap.index if snap else 0
    for i, e in enumerate(ents):
        assert e.index == g_lo + 1 + i, "recovered log is not contiguous"
    got = logmap(snap, ents)
    b_map, a_map = logmap(before[1], before[2]), logmap(after[1], after[2])
    b_lo = before[1].index if before[1] else 0
    a_lo = after[1].index if after[1] else 0
    assert g_lo >= b_lo, "snapshot went backwards"
    assert g_lo in (b_lo, a_lo), "snapshot that was never written"
    if snap is not None:
        assert snap.term == (before[1] if g_lo == b_lo and before[1] else after[1]).term
    for i, t in b_map.items():          # nothing acknowledged is lost
        if i > g_lo and a_map.get(i) == t:
            assert got.get(i) == t, f"acknowledged entry {i} (term {t}) lost"
    for i, t in got.items():            # nothing invented
        if i > g_lo:
            assert t in (b_map.get(i), a_map.get(i)), f"entry {i} has term {t}, never written"
    assert hs.term >= before[0].term, "term regressed"
    if hs.term == before[0].term and before[0].vote is not None:
        assert hs.vote == before[0].vote, "vote forgotten"
    last = g_lo + len(ents)
    if hs.commit > g_lo:                # every committed index must hold the committed entry
        assert hs.commit <= last, "commit index beyond the recovered log"
        expect = a_map if hs.commit > before[0].commit else b_map
        for i in range(g_lo + 1, hs.commit + 1):
            assert got[i] == expect.get(i), f"commit={hs.commit} but entry {i} is not the committed one"


@pytest.mark.parametrize("seed", range(25))
def test_disk_storage_matches_model(seed):
    ops, states = gen_workload(seed)
    fs = FaultyFS(seed)
    s = DiskStorage(fs, "/d")
    s.load()
    for op, want in zip(ops, states):
        apply_op(s, op)
        r = DiskStorage(fs, "/d")          # re-open after every op (as after a clean restart)
        hs, snap, ents = r.load()
        assert (hs, snap, ents) == want
        r.close()
        s = DiskStorage(fs, "/d")
        s.load()


def _count_syncs(ops):
    fs = FaultyFS(0)
    s = DiskStorage(fs, "/d")
    s.load()
    start = fs.sync_count
    w0 = fs.write_count
    for op in ops:
        apply_op(s, op)
    return start, fs.sync_count, w0, fs.write_count


@pytest.mark.parametrize("seed", range(12))
@pytest.mark.parametrize("after", [False, True])
def test_crash_at_every_fsync_boundary(seed, after):
    """Power-cut before/after every single fsync of a random workload, with torn tails."""
    ops, states = gen_workload(seed, n_ops=30)
    first, total, _, _ = _count_syncs(ops)
    empty = (HardState(), None, [])
    for k in range(first + 1, total + 1):
        fs = FaultyFS(seed * 1000 + k)
        s = DiskStorage(fs, "/d")
        s.load()
        fs.crash_at_sync, fs.crash_after_sync = k, after
        done = 0
        try:
            for op in ops:
                apply_op(s, op)
                done += 1
        except SimulatedCrash:
            pass
        fs.crash()
        recovered = DiskStorage(fs, "/d").load()
        before = states[done - 1] if done else empty
        nxt = states[done] if done < len(states) else before
        check_recovered(recovered, before, nxt)
        SIM_TOTALS["crash_points"] += 1


@pytest.mark.parametrize("seed", range(12))
def test_pull_drive_mid_write_at_every_write(seed):
    """The drive disappears in the middle of write #k (partial data hits the platter), then EIO forever."""
    ops, states = gen_workload(seed, n_ops=30)
    _, _, w0, wtotal = _count_syncs(ops)
    empty = (HardState(), None, [])
    for k in range(0, wtotal - w0):
        fs = FaultyFS(seed * 7919 + k)
        s = DiskStorage(fs, "/d")
        s.load()
        fs.pull_drive(after_writes=k)
        done = 0
        with pytest.raises(OSError):
            for op in ops:
                apply_op(s, op)
                done += 1
            raise OSError("drive survived the workload")  # (only if k exceeded the write count)
        with pytest.raises(OSError):
            s.save(HardState(99, None, 0), [], None)     # stays dead: nothing more can be acknowledged
        fs.reinsert_drive()
        recovered = DiskStorage(fs, "/d").load()
        before = states[done - 1] if done else empty
        nxt = states[done] if done < len(states) else before
        check_recovered(recovered, before, nxt)
        SIM_TOTALS["crash_points"] += 1


def test_snapshot_crc_fallback_and_missing_snapshot_detection():
    fs = FaultyFS()
    s = DiskStorage(fs, "/d")
    s.load()
    s.save(HardState(1, None, 3), [Entry(1, i, 0, b"x") for i in range(1, 6)], None)
    s.compact(Snapshot(2, 1, {}, b"S2"))
    s.compact(Snapshot(4, 1, {}, b"S4"))
    newest = sorted(n for n in fs.files() if n.endswith(".snap"))[-1]
    fs.corrupt(newest, 20)
    with pytest.raises(CorruptLogError):        # WAL says "starts after 4" but snapshot 4 is rotten
        DiskStorage(fs, "/d").load()
    store = SnapshotStore(fs, "/d/snap")
    snap, bad = store.load_latest()
    assert snap.index == 2 and len(bad) == 1


def test_real_filesystem_roundtrip(tmp_path):
    fs = RealFS()
    d = str(tmp_path / "node")
    s = DiskStorage(fs, d)
    s.load()
    ents = [Entry(1, i, 0, bytes([i]) * 100) for i in range(1, 50)]
    s.save(HardState(3, "b", 40), ents, None)
    s.compact(Snapshot(30, 1, {"a": {}}, b"state"))
    s.save(None, [Entry(3, 50, 0, b"tail")], None)
    s.close()
    hs, snap, got = DiskStorage(fs, d).load()
    assert hs == HardState(3, "b", 40) and snap.index == 30
    assert [e.index for e in got] == list(range(31, 51)) and got[-1].data == b"tail"


# ----------------------------------------------------------------- chunk store

def test_chunk_store_verify_quarantine_and_crash_atomicity():
    data = random.Random(3).randbytes(5000)
    h = sha256_hex(data)
    for k in range(1, 4):
        for after in (False, True):
            fs = FaultyFS(k)
            cs = ChunkStore(fs, "/c")
            fs.crash_at_sync, fs.crash_after_sync = k, after
            try:
                cs.put(h, data)
            except SimulatedCrash:
                pass
            fs.crash()
            assert ChunkStore(fs, "/c").verify(h) in ("ok", "missing")   # never a half chunk
    fs = FaultyFS()
    cs = ChunkStore(fs, "/c")
    cs.put(h, data)
    fs.corrupt(cs.path(h), 100)
    assert cs.verify(h) == "corrupt"
    cs.quarantine(h)
    assert cs.verify(h) == "missing" and cs.put(h, data) and cs.get(h) == data
    with pytest.raises(ValueError):
        cs.put(h, data + b"!")

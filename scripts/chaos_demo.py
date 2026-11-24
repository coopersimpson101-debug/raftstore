#!/usr/bin/env python3
"""Narrated chaos run against a REAL 3-process raftstore cluster on localhost.

    python scripts/chaos_demo.py [--keep]

What it does (all against real OS processes, real TCP, real files):
  1. boots nodes named nas / pi5 / server
  2. starts concurrent writers and uploads a multi-chunk file
  3. "pulls the drive" on the leader mid-write (FaultInjectingFS -> EIO); the leader fail-stops
  4. checks a new leader takes over and that every acknowledged write is still readable
  5. flips a byte inside a chunk file on a follower's disk; the scrubber detects and repairs it
  6. replaces the dead node with a brand-new empty one (remove + add); data is re-replicated
  7. kill -9's the current leader during writes, restarts it, and verifies convergence
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import shutil
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raftstore.client import Client, ClientError  # noqa: E402
from raftstore.devcluster import DevCluster  # noqa: E402

T0 = time.monotonic()


def say(msg: str) -> None:
    print(f"[{time.monotonic() - T0:6.2f}s] {msg}", flush=True)


class Writers:
    def __init__(self, dc: DevCluster, prefix: str, n: int = 3):
        self.dc, self.n, self.prefix = dc, n, prefix
        self.acked: dict[str, bytes] = {}
        self.failed = 0
        self.stop = threading.Event()
        self.lock = threading.Lock()

    def _run(self, wid: int) -> None:
        c = Client(self.dc.urls(), timeout=5, retries=60)
        rng = random.Random(wid)
        i = 0
        while not self.stop.is_set():
            key = f"{self.prefix}/writer{wid}/item{i:05d}"
            val = rng.randbytes(rng.randint(100, 20_000))
            try:
                c.put(key, val)
                with self.lock:
                    self.acked[key] = val
            except Exception:  # noqa: BLE001 - unknown outcome, not counted as acknowledged
                with self.lock:
                    self.failed += 1
            i += 1

    def start(self):
        self.threads = [threading.Thread(target=self._run, args=(w,), daemon=True) for w in range(self.n)]
        for t in self.threads:
            t.start()

    def halt(self):
        self.stop.set()
        for t in self.threads:
            t.join()


def verify_all(dc: DevCluster, acked: dict, label: str) -> None:
    c = dc.client(timeout=10)
    bad = 0
    for k, v in acked.items():
        if c.get(k) != v:
            bad += 1
    say(f"verify[{label}]: {len(acked)} acknowledged writes checked with linearizable reads -> "
        f"{'ALL PRESENT' if not bad else f'{bad} MISSING'}")
    if bad:
        raise SystemExit(1)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="keep the data directories")
    args = ap.parse_args()
    work = tempfile.mkdtemp(prefix="raftstore-chaos-")
    dc = DevCluster(work, names=("nas", "pi5", "server"), tick_ms=50)
    say("booting 3 local processes (nas, pi5, server); data dirs in a fresh temp directory")
    dc.start_all()
    try:
        leader = dc.wait_leader(30)
        say(f"leader elected: {leader}")
        c = dc.client(timeout=10)

        photo = random.Random(0).randbytes(3 * 1024 * 1024 + 123)
        r = c.put("photos/2026/holiday.raw", photo)
        say(f"uploaded photos/2026/holiday.raw: {r['size']} bytes in {r['chunks']} content-addressed chunks "
            f"(sha256 {r['sha256'][:16]}...)")

        w = Writers(dc, "phase1")
        w.start()
        time.sleep(2.0)
        say(f"{len(w.acked)} writes acknowledged so far by 3 concurrent writers")

        # ---------------------------------------------------------- pull the drive
        victim = dc.wait_leader()
        n_before = len(w.acked)
        say(f"PULLING THE DRIVE on leader '{victim}' (EIO after 5 more writes, last one torn)")
        Client([dc.url(victim)], retries=1).admin("/debug/fault", {"after_writes": 5})
        proc = dc.procs[victim]
        t_pull = time.monotonic()
        code = proc.wait(15)
        say(f"'{victim}' fail-stopped with exit code {code} after the I/O error "
            f"({(time.monotonic() - t_pull) * 1000:.0f} ms) -- it acknowledged nothing it had not fsynced")
        dc.procs.pop(victim)
        new_leader = dc.wait_leader(15)
        say(f"new leader: '{new_leader}' ({(time.monotonic() - t_pull) * 1000:.0f} ms after the pull)")
        time.sleep(2.0)
        say(f"writers kept going: {len(w.acked) - n_before} more writes acknowledged without '{victim}'")
        w.halt()
        say(f"writers stopped: {len(w.acked)} acknowledged, {w.failed} attempts with unknown outcome")
        verify_all(dc, w.acked, "after drive pull")
        assert c.get("photos/2026/holiday.raw") == photo
        say("photos/2026/holiday.raw still byte-identical")

        # ----------------------------------------------------------- bit rot
        follower = next(n for n in dc.procs if n != new_leader)
        st = Client([dc.url(follower)]).status(dc.url(follower))
        chunk_dir = os.path.join(dc.cfgs[follower]["data_dir"], "chunks")
        files = sorted(os.path.join(dp, f) for dp, _, fs in os.walk(chunk_dir) for f in fs
                       if len(f) == 64 and "quarantine" not in dp)
        target = files[len(files) // 2]
        with open(target, "r+b") as f:
            f.seek(1000)
            b = f.read(1)
            f.seek(1000)
            f.write(bytes([b[0] ^ 0x01]))
        say(f"BIT ROT: flipped one bit in chunk {os.path.basename(target)[:16]}... on '{follower}' "
            f"({st['chunks_referenced']} chunks referenced there)")
        res = Client([dc.url(follower)], timeout=60, retries=1).admin("/admin/scrub", {})
        say(f"scrubber on '{follower}': checked={res['checked']} corrupt={res['corrupt']} "
            f"repaired={res['repaired']} unrepairable={res['unrepairable']}")
        with open(target, "rb") as f:
            ok = hashlib.sha256(f.read()).hexdigest() == os.path.basename(target)
        say(f"chunk re-fetched from a healthy peer and verified by hash: {ok}")

        # ------------------------------------------------------ node replacement
        repl = f"{victim}-new"
        say(f"REPLACING '{victim}': its drive is dead; removing it and adding an empty node '{repl}'")
        print(json.dumps(c.admin("/admin/members", {"op": "remove", "id": victim})["result"]), flush=True)
        dc.add_node(repl)
        dc.start(repl)
        cfg = dc.cfgs[repl]
        c.admin("/admin/members", {"op": "add", "id": repl, "raft": cfg["raft_listen"], "http": cfg["http_listen"]})
        c.admin("/admin/snapshot", {})
        t_add = time.monotonic()
        while True:
            st = Client([dc.url(repl)]).status(dc.url(repl))
            if st["applied"] >= json.loads(c.request("GET", "/status")[1])["commit"] - 1 and st["keys"] > 0:
                break
            time.sleep(0.2)
        data = Client([dc.url(repl)]).get("photos/2026/holiday.raw", consistency="stale")
        st = Client([dc.url(repl)]).status(dc.url(repl))
        say(f"'{repl}' caught up in {time.monotonic() - t_add:.1f}s: snapshot_index={st['snapshot_index']} "
            f"keys={st['keys']} chunks_repaired_from_peers={st['repair']['repaired']}; "
            f"photo identical on {repl}: {data == photo}")
        say(f"voters now: {st['voters']}")
        dc.cfgs.pop(victim)  # the dead node is gone for good

        # --------------------------------------------------------------- kill -9
        w2 = Writers(dc, "phase2")
        w2.start()
        time.sleep(1.0)
        lead = dc.wait_leader()
        say(f"kill -9 the leader '{lead}' during writes")
        dc.kill(lead)
        t_kill = time.monotonic()
        nl = dc.wait_leader(15)
        say(f"new leader '{nl}' after {(time.monotonic() - t_kill) * 1000:.0f} ms")
        time.sleep(1.5)
        dc.start(lead)
        say(f"restarted '{lead}' from its WAL")
        time.sleep(1.5)
        w2.halt()
        say(f"phase-2 writers: {len(w2.acked)} acknowledged, {w2.failed} attempts with unknown outcome")
        sts = dc.wait_converged(timeout=30)
        say("all nodes converged: " + ", ".join(f"{n}=commit {s['commit']}" for n, s in sorted(sts.items())))
        acked = dict(w.acked)
        acked.update(w2.acked)
        verify_all(dc, acked, "final")
        say("DONE: no acknowledged write was lost through a pulled drive, bit rot, node replacement and kill -9")
        return 0
    except ClientError as e:
        say(f"client error: {e}")
        return 1
    finally:
        dc.stop_all()
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())

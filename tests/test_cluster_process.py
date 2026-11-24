"""Real OS processes on localhost: kill -9 the leader during concurrent writes, then check
the recorded client history for linearizability (Jepsen-style) and verify every node."""
import json
import math
import random
import threading
import time
import urllib.error
import urllib.request

import pytest

from conftest import SIM_TOTALS
from raftstore.check.linearizability import Op, check_kv
from raftstore.devcluster import DevCluster

pytestmark = pytest.mark.cluster


class Recorder:
    def __init__(self, cluster: DevCluster):
        self.cluster = cluster
        self.ops: list[Op] = []
        self.lock = threading.Lock()
        self.seq = 0

    def attempt(self, method, path, body=None):
        """Retry only when the request certainly had no effect (refused / redirected / not-leader)."""
        urls = self.cluster.urls()
        start = random.randrange(len(urls))
        for i in range(60):
            base = urls[(start + i) % len(urls)]
            req = urllib.request.Request(base + path, data=body, method=method)
            try:
                with urllib.request.urlopen(req, timeout=5) as r:
                    return r.status, r.read()
            except urllib.error.HTTPError as e:
                data = e.read()
                if e.code == 307:
                    hint = json.loads(data).get("leader_http")
                    if hint:
                        urls = [hint] + [u for u in urls if u != hint]
                        start = -(i + 1)  # next attempt goes to the hinted leader
                    time.sleep(0.02)
                    continue
                if e.code == 503 and b"retry:" in data:   # proposal was rejected, not appended
                    time.sleep(0.05)
                    continue
                return e.code, data                       # 504/other 503: outcome unknown
            except urllib.error.URLError as e:
                if isinstance(e.reason, ConnectionRefusedError):
                    time.sleep(0.05)
                    continue
                return None, b""                          # timeout / reset: unknown
            except (ConnectionError, TimeoutError, OSError):
                return None, b""
        return None, b""

    def run_client(self, cid, keys, deadline):
        rng = random.Random(cid)
        while time.monotonic() < deadline:
            with self.lock:
                self.seq += 1
                oid = self.seq
            key = rng.choice(keys)
            if rng.random() < 0.5:
                op = Op(oid, key, "w", value=oid, call=time.monotonic(), client=cid)
                st, _ = self.attempt("PUT", f"/kv/{key}", str(oid).encode())
                if st == 201:
                    op.ret = time.monotonic()
                with self.lock:
                    self.ops.append(op)       # failed/unknown writes stay pending (ret = inf)
            else:
                op = Op(oid, key, "r", call=time.monotonic(), client=cid)
                st, data = self.attempt("GET", f"/kv/{key}")
                if st in (200, 404):
                    op.ret = time.monotonic()
                    op.output = int(data) if st == 200 else None
                    with self.lock:
                        self.ops.append(op)   # reads that failed have no effect: omitted


def test_kill_minus_nine_leader_during_writes_is_linearizable(tmp_path):
    dc = DevCluster(str(tmp_path), tick_ms=50)
    dc.start_all()
    try:
        first = dc.wait_leader(30)
        rec = Recorder(dc)
        keys = ["x", "y", "z"]
        deadline = time.monotonic() + 14
        threads = [threading.Thread(target=rec.run_client, args=(i, keys, deadline)) for i in range(4)]
        for t in threads:
            t.start()
        time.sleep(3)
        dc.kill(first)                               # kill -9 / TerminateProcess
        killed_at = time.monotonic()
        time.sleep(4)
        second = dc.wait_leader(15)
        assert second != first
        dc.start(first)                              # it recovers from its WAL and rejoins
        time.sleep(3)
        dc.kill(second)                              # and once more for good measure
        time.sleep(2)
        dc.start(second)
        for t in threads:
            t.join()
        dc.wait_converged(timeout=30)

        ops = rec.ops
        acked_w = [o for o in ops if o.kind == "w" and o.ret != math.inf]
        after_kill = [o for o in acked_w if o.call > killed_at]
        assert len(acked_w) > 50 and after_kill, "cluster did not make progress across the kills"
        res = check_kv(ops)
        assert res.ok, f"non-linearizable history on key {res.key}: {sorted(res.ops, key=lambda o: o.call)[:20]}"
        # every replica converged to the same final state
        finals = {}
        for name in dc.names:
            for k in keys:
                with urllib.request.urlopen(dc.url(name) + f"/kv/{k}?consistency=stale", timeout=5) as r:
                    finals.setdefault(k, set()).add(r.read())
        assert all(len(v) == 1 for v in finals.values()), finals
        SIM_TOTALS["linearizability_histories"] += 1
        print(f"\nreal cluster: {len(ops)} ops recorded, {len(acked_w)} acked writes, "
              f"{sum(1 for o in ops if o.kind == 'w' and o.ret == math.inf)} indeterminate writes, "
              f"{sum(1 for o in ops if o.kind == 'r')} reads -> linearizable")
    finally:
        dc.stop_all()

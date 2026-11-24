#!/usr/bin/env python3
"""Small write/read benchmark.

    # against a throwaway local 3-process cluster:
    python scripts/bench.py --local
    # against a real deployment:
    python scripts/bench.py --url http://10.0.0.10:8080,http://10.0.0.11:8080,http://10.0.0.12:8080

Reports per-operation latency percentiles and throughput for sequential and
concurrent PUTs of a given size, plus linearizable GETs.
"""
from __future__ import annotations

import argparse
import os
import shutil
import statistics
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raftstore.client import Client  # noqa: E402
from raftstore.devcluster import DevCluster  # noqa: E402


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))] * 1000


def run_phase(urls, label, n, size, threads, op):
    lat, lock = [], threading.Lock()
    payload = os.urandom(size)

    def worker(tid):
        c = Client(urls, timeout=30)
        for i in range(n // threads):
            t = time.perf_counter()
            if op == "put":
                c.put(f"bench/{label}/{tid}/{i}", payload)
            else:
                c.get(f"bench/seq-{size}/0/{i % max(1, n // threads)}")
            with lock:
                lat.append(time.perf_counter() - t)
    t0 = time.perf_counter()
    ts = [threading.Thread(target=worker, args=(k,)) for k in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    wall = time.perf_counter() - t0
    print(f"{label:<22} n={len(lat):<5} size={size:<8} threads={threads:<3} "
          f"ops/s={len(lat) / wall:8.1f}  MB/s={len(lat) * size / wall / 1e6:7.2f}  "
          f"p50={pct(lat, .5):6.1f}ms p99={pct(lat, .99):6.1f}ms mean={statistics.mean(lat) * 1000:6.1f}ms",
          flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url")
    ap.add_argument("--local", action="store_true")
    ap.add_argument("-n", type=int, default=300)
    args = ap.parse_args()
    work = None
    dc = None
    if args.local:
        work = tempfile.mkdtemp(prefix="raftstore-bench-")
        dc = DevCluster(work, tick_ms=50, fault_injection=False)
        dc.start_all()
        dc.wait_leader(30)
        urls = dc.urls()
    else:
        urls = [u for u in args.url.split(",") if u]
    try:
        n = args.n
        run_phase(urls, "seq-1024", n, 1024, 1, "put")
        run_phase(urls, "conc8-1024", n, 1024, 8, "put")
        run_phase(urls, "seq-1048576", max(10, n // 10), 1 << 20, 1, "put")
        run_phase(urls, "get-linearizable", n, 1024, 1, "get")
    finally:
        if dc:
            dc.stop_all()
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()

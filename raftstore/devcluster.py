"""Spawn a real multi-process cluster on localhost (used by tests and the chaos demo)."""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time

from .client import Client


def free_ports(n: int) -> list[int]:
    socks, ports = [], []
    for _ in range(n):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        socks.append(s)
        ports.append(s.getsockname()[1])
    for s in socks:
        s.close()
    return ports


class DevCluster:
    def __init__(self, workdir: str, names=("nas", "pi5", "server"), tick_ms: int = 50,
                 fault_injection: bool = True, extra: dict | None = None):
        self.workdir = workdir
        self.names = list(names)
        self.tick_ms = tick_ms
        self.fault_injection = fault_injection
        self.extra = extra or {}
        self.procs: dict[str, subprocess.Popen] = {}
        self.cfgs: dict[str, dict] = {}
        ports = free_ports(2 * len(self.names))
        self.initial = {n: {"raft": f"127.0.0.1:{ports[2 * i]}", "http": f"127.0.0.1:{ports[2 * i + 1]}"}
                        for i, n in enumerate(self.names)}
        for n in self.names:
            self._write_cfg(n, self.initial[n], join=False)

    def _write_cfg(self, name: str, addrs: dict, join: bool) -> None:
        cfg = {
            "id": name, "data_dir": os.path.join(self.workdir, name, "data"),
            "raft_listen": addrs["raft"], "http_listen": addrs["http"],
            "initial_cluster": self.initial, "join": join, "tick_ms": self.tick_ms,
            "election_ticks": 10, "heartbeat_ticks": 1, "snapshot_every": 200,
            "chunk_size": 64 * 1024, "scrub_interval_s": 3600, "fault_injection": self.fault_injection,
            "log_level": "INFO", **self.extra,
        }
        os.makedirs(os.path.join(self.workdir, name), exist_ok=True)
        path = os.path.join(self.workdir, name, "config.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=1)
        self.cfgs[name] = cfg

    def url(self, name: str) -> str:
        return "http://" + self.cfgs[name]["http_listen"]

    def urls(self) -> list[str]:
        return [self.url(n) for n in self.cfgs]

    def client(self, **kw) -> Client:
        return Client(self.urls(), **kw)

    def start(self, name: str) -> None:
        log = open(os.path.join(self.workdir, name, "node.log"), "ab")
        env = dict(os.environ)
        env["PYTHONPATH"] = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + os.pathsep + env.get("PYTHONPATH", "")
        self.procs[name] = subprocess.Popen(
            [sys.executable, "-m", "raftstore", "serve", "--config",
             os.path.join(self.workdir, name, "config.json")],
            stdout=log, stderr=subprocess.STDOUT, env=env)
        log.close()

    def start_all(self) -> None:
        for n in self.names:
            self.start(n)

    def kill(self, name: str) -> None:
        """SIGKILL on POSIX, TerminateProcess on Windows: no cleanup code runs."""
        p = self.procs.pop(name, None)
        if p is not None:
            p.kill()
            p.wait(10)

    def add_node(self, name: str) -> None:
        port_r, port_h = free_ports(2)
        self._write_cfg(name, {"raft": f"127.0.0.1:{port_r}", "http": f"127.0.0.1:{port_h}"}, join=True)

    def status(self, name: str) -> dict | None:
        try:
            return Client([self.url(name)], timeout=2, retries=1).status(self.url(name))
        except Exception:  # noqa: BLE001
            return None

    def leader(self) -> str | None:
        for n in list(self.procs):
            st = self.status(n)
            if st and st.get("state") == "leader":
                return n
        return None

    def wait_leader(self, timeout: float = 20.0) -> str:
        end = time.time() + timeout
        while time.time() < end:
            lead = self.leader()
            if lead:
                return lead
            time.sleep(0.1)
        raise TimeoutError("no leader elected")

    def wait_converged(self, names=None, timeout: float = 30.0) -> dict:
        names = names or list(self.procs)
        end = time.time() + timeout
        while time.time() < end:
            sts = {n: self.status(n) for n in names}
            if all(sts.values()):
                commits = {s["commit"] for s in sts.values()}
                applied = {s["applied"] for s in sts.values()}
                if len(commits) == 1 and applied == commits:
                    return sts
            time.sleep(0.2)
        raise TimeoutError(f"nodes did not converge: { {n: (s and (s['commit'], s['applied'])) for n, s in sts.items()} }")

    def stop_all(self) -> None:
        for n in list(self.procs):
            self.kill(n)

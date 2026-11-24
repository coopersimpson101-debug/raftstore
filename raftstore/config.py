"""Node configuration (JSON file)."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, fields


@dataclass
class NodeConfig:
    id: str
    data_dir: str
    raft_listen: str                 # "0.0.0.0:7000"
    http_listen: str                 # "0.0.0.0:8080"
    initial_cluster: dict = field(default_factory=dict)  # id -> {"raft": "10.0.0.10:7000", "http": "10.0.0.10:8080"}
    join: bool = False               # true for a node being added to an existing cluster
    tick_ms: int = 100
    election_ticks: int = 10
    heartbeat_ticks: int = 1
    read_mode: str = "safe"          # "safe" (ReadIndex) | "lease"
    fsync: bool = True
    chunk_size: int = 256 * 1024
    snapshot_every: int = 2000       # entries applied since the last snapshot
    snapshot_wal_bytes: int = 64 << 20
    proposal_timeout_s: float = 10.0
    scrub_interval_s: float = 3600.0
    scrub_rate_bytes: int = 32 << 20  # bytes/s the scrubber may read
    tls: dict | None = None          # {"cert": ..., "key": ..., "ca": ...}
    fault_injection: bool = False    # enables POST /debug/fault (tests and demos only!)
    log_level: str = "INFO"

    @classmethod
    def from_dict(cls, d: dict) -> "NodeConfig":
        known = {f.name for f in fields(cls)}
        unknown = set(d) - known
        if unknown:
            raise ValueError(f"unknown config keys: {sorted(unknown)}")
        cfg = cls(**d)
        if cfg.read_mode not in ("safe", "lease"):
            raise ValueError("read_mode must be 'safe' or 'lease'")
        if not cfg.join and cfg.id not in cfg.initial_cluster:
            raise ValueError("a founding node must appear in initial_cluster (or set join: true)")
        return cfg

    @classmethod
    def load(cls, path: str) -> "NodeConfig":
        with open(path, encoding="utf-8") as f:
            return cls.from_dict(json.load(f))

    def to_dict(self) -> dict:
        return asdict(self)

# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] - 2026-11-25

First public release of self-healing replicated storage on a from-scratch Raft implementation.

### Added
- Deterministic, I/O-free Raft core: PreVote, fast backtracking, snapshots, membership changes, ReadIndex, leadership transfer
- CRC32 write-ahead log with torn-write recovery, atomic snapshots, SHA-256 chunk store with scrub-and-repair
- Simulation harness: 2,100 randomized schedules, 6.4M steps, invariants checked every step, histories linearizable
- 3,716 storage crash points tested (power cut at every fsync, drive pulled inside every write)

[0.1.0]: https://github.com/coopersimpson101-debug/raftstore/releases/tag/v0.1.0

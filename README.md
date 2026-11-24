# raftstore

**Self-healing replicated storage for a home lab, on a Raft implementation written from scratch —
and tested like a database.**

Put a NAS, a Raspberry Pi 5 and a server PC in one cluster. Every file you store is split into
SHA-256-addressed chunks, replicated through Raft to a majority of machines, and fsynced before you get
an acknowledgement. Unplug a drive in the middle of a write: the node fail-stops, the others carry on,
and nothing that was acknowledged is lost. Let a bit rot on disk: the scrubber notices the hash mismatch
and re-fetches the chunk from a healthy peer. Lose a drive for good: swap in an empty node and it is
re-populated from the others.

* **Raft, by hand** — a deterministic, I/O-free state machine (`message in → messages/actions out`):
  randomized elections, PreVote, CheckQuorum, fast log backtracking, current-term commit rule,
  snapshots + InstallSnapshot + compaction, single-server membership changes, ReadIndex and lease reads,
  leadership transfer. No Raft library; no runtime dependencies at all (Python 3.11+ stdlib).
* **Storage, by hand** — CRC-framed write-ahead log with torn-write truncation and corruption
  detection, atomic snapshot files, content-addressed chunk store, background scrubber, peer repair.
* **Tested to destruction** — a seeded network/disk simulator that checks Raft's safety invariants after
  every step, a Wing–Gong/Knossos-style linearizability checker, power cuts at every fsync, drives pulled
  in the middle of every write, and a real three-process cluster that gets `kill -9`'d.

> Status: complete and passing its test-suite on a development machine (three local processes).
> **It has not yet been run on the actual NAS / Pi 5 / server** — see
> [Hardware validation / TODO](#hardware-validation--todo-for-the-author).

---

## Architecture

```
                 clients:  raftstore CLI · curl · scripts/bench.py
                                     │  HTTP(S)  PUT/GET/DELETE /kv/<key>, /status, /admin/*
     ┌───────────────────────────────┼─────────────────────────────────────────────┐
     │ node "server" (leader)        ▼                                             │
     │  ┌────────────────────────────────────────────┐   ┌──────────────────────┐  │
     │  │ HTTP API (asyncio)                         │   │ Scrubber             │  │
     │  │  PUT: split → chunk entries + manifest      │   │ re-hash every chunk, │  │
     │  │  GET: ReadIndex → verify chunks → assemble  │   │ quarantine + repair  │  │
     │  └───────────────┬────────────────────────────┘   └─────────┬────────────┘  │
     │      propose /   │ read_index                    peer repair │ GET /internal/chunk/<sha>
     │                  ▼                                           │              │
     │  ┌────────────────────────────────────────────┐              │              │
     │  │ Raft core  (pure, deterministic)           │◄── tick() ── asyncio timer   │
     │  │  step(msg) → Ready{persist, send, apply}   │                             │
     │  └──────┬───────────────┬───────────────┬─────┘                             │
     │  ① persist + fsync   ② send          ③ apply                                │
     │         ▼               ▼               ▼                                   │
     │  ┌─────────────┐  ┌───────────┐  ┌──────────────────────────────────────┐   │
     │  │ WAL (CRC32) │  │ TCP/TLS   │  │ Blob state machine                   │   │
     │  │ + snapshots │  │ transport │  │  manifests: key → [sha256…]          │   │
     │  └─────────────┘  └─────┬─────┘  │  chunk store: chunks/<aa>/<sha256>   │   │
     │                         │        └──────────────────────────────────────┘   │
     └─────────────────────────┼───────────────────────────────────────────────────┘
                 Raft RPCs     │   (append / vote / pre-vote / snapshot / timeout-now)
              ┌────────────────┴────────────────┐
              ▼                                 ▼
   node "nas"  (follower)              node "pi5"  (follower)
   same components, own disk           same components, own disk
```

The single most important rule is visible in the picture: a node **persists and fsyncs (①) before it
sends anything (②)**, and acknowledges a client only after the entry is committed on a majority and
applied (③). The Raft core cannot violate this, because it has no way to send — it can only return a
`Ready` that the runtime processes in that order.

Code map:

| path | what |
|---|---|
| `raftstore/raft/core.py` | the Raft state machine (≈700 lines) |
| `raftstore/storage/wal.py`, `raftlog.py` | WAL format + recovery, snapshot files, `DiskStorage` |
| `raftstore/storage/chunks.py`, `statemachine.py` | content-addressed chunks, replicated blob/KV state machine |
| `raftstore/storage/fs.py` | filesystem interface: `RealFS`, `FaultInjectingFS`, simulated `FaultyFS` |
| `raftstore/node.py`, `transport.py`, `httpd.py`, `codec.py` | the server: asyncio TCP/TLS transport, HTTP API, scrubber, repair |
| `raftstore/sim/cluster.py` | deterministic cluster simulator |
| `raftstore/check/` | safety-invariant checker, linearizability checker |
| `raftstore/cli.py`, `client.py` | `raftstore` command line and client library |

## Quick start (local, three processes)

```sh
python3 scripts/chaos_demo.py          # boots nas/pi5/server locally and tortures them, narrated
```

or with Docker on one host (the CLI runs inside the containers, where the node names resolve):

```sh
docker compose -f deploy/docker-compose.yml up --build -d
docker compose -f deploy/docker-compose.yml exec nas sh -c   'echo hello > /tmp/h && python -m raftstore --url http://nas:8080,http://pi5:8080,http://server:8080 put greeting /tmp/h'
docker compose -f deploy/docker-compose.yml exec pi5 python -m raftstore --url http://pi5:8080 get greeting
```

On the real machines (see [docs/operations.md](docs/operations.md)):

```sh
python3 -m raftstore serve --config /etc/raftstore/node.json          # via deploy/systemd/raftstore.service
export RAFTSTORE_URLS=https://10.0.0.10:8080,https://10.0.0.11:8080,https://10.0.0.12:8080
python3 -m raftstore put photos/cat.jpg ./cat.jpg
python3 -m raftstore get photos/cat.jpg -o /tmp/cat.jpg
python3 -m raftstore status --all
```

API reference: [docs/api.md](docs/api.md). Deployment on the three machines: [docs/operations.md](docs/operations.md).

## Raft, explained (as implemented here)

Full detail with code pointers: [docs/raft.md](docs/raft.md).

**Elections.** Followers that hear nothing from a leader for a *randomized* election timeout become
candidates, increment the term and ask for votes; a majority makes a leader. Each node votes at most
once per term (the vote is fsynced before the reply) and only for candidates whose log is at least as
up-to-date as its own. Two additions keep a home network calm: **PreVote** — a node first asks whether
it *would* win, without bumping its term, so a Pi that drops off Wi-Fi does not come back with a huge
term and depose the leader — and **CheckQuorum** — a leader that cannot reach a majority steps down, and
followers that have a live leader ignore vote requests.

**Log replication.** The leader appends client commands to its log and sends them with the index and
term of the preceding entry. A follower accepts only if it has that preceding entry, truncating any
conflicting *uncommitted* suffix. On rejection it returns a conflict hint (term + first index of that
term) so the leader skips a whole term per round trip instead of one entry.

**Commit rules.** An entry is committed once a majority stores it **and** it belongs to the leader's
current term; older entries commit indirectly with it (Raft §5.4.2 — the "Figure 8" trap, reproduced in
a test). New leaders append a no-op to get a current-term entry committed quickly. Clients are
acknowledged only after commit + apply.

**Safety argument.** One vote per term + majority intersection ⇒ one leader per term. The prev-entry
consistency check ⇒ logs that agree on an (index, term) agree on the whole prefix. The up-to-date voting
rule + majority intersection ⇒ every committed entry is in every later leader's log. Hence all nodes
apply the same commands in the same order. Each of these four properties is *checked* after every step
of every simulated schedule.

**Snapshots.** Applied state is periodically snapshotted and the log prefix discarded (WAL rewritten
atomically). A follower too far behind receives InstallSnapshot. Snapshots carry only manifests; chunk
bytes are re-fetched from peers by hash — which is exactly the mechanism that re-populates a replacement
node.

**Membership changes.** One server at a time (any old and new majority overlap), effective on append,
at most one uncommitted change, and only after the leader committed an entry in its own term. Founding
membership is log entry 1, so joiners learn the full history.

**Reads.** Default **ReadIndex**: the leader notes its commit index, proves it is still leader with one
heartbeat round to a majority, waits until applied ≥ that index, then reads — linearizable without
writing to the log. Optional **lease** mode skips the round trip while a recent quorum heartbeat makes
another leader impossible (assumes bounded clock drift). **Leadership transfer** moves the leader off a
machine before maintenance.

## Storage layer design

Full detail: [docs/storage.md](docs/storage.md).

* **WAL**: `len | crc32 | type | payload` records; one batch and one fsync per Raft `Ready`, written in
  the order *reset → entries → hard state* so that every torn prefix is a legal pre-reply state. On
  recovery an invalid tail with nothing valid after it is a torn write → truncated; an invalid record with
  valid records after it is corruption of acknowledged data → the node refuses to start (fail-stop) and
  should be replaced.
* **Snapshots**: `temp file → fsync → rename → fsync(dir)`, CRC-checked, newest two kept; the WAL is
  re-based onto the snapshot if a crash landed between the two.
* **Chunks**: files named by their SHA-256, written atomically; every read is verified, so a node can
  never return rotten data — it repairs from a peer instead.
* **Scrubber**: re-reads every chunk at a configurable rate, detects bit rot by hash mismatch,
  quarantines the bad copy and repairs from a peer. **GC** of orphaned chunks is a replicated command.
* **Fail-stop on I/O errors**: any EIO from the disk stops the node (exit code 75) — after a failed fsync
  the page cache can no longer be trusted, so the only safe thing is to never acknowledge anything again.

## Fault model

Full table: [docs/fault-model.md](docs/fault-model.md). With 3 voters raftstore tolerates any **one**
node failing in any of these ways: crash / `kill -9`, power loss (also of all nodes at once — only
acknowledged data must survive), a drive pulled mid-write, a dead drive (node replacement), bit rot in
chunks or snapshots, message loss/duplication/reordering/delay, and partitions. It does **not** tolerate
Byzantine nodes, drives that lie about fsync, or losing a majority of drives; lease reads additionally
assume bounded clock drift.

## Testing — and the numbers

Everything below was **measured on one Windows 10 development machine** (Python 3.12, three local
processes / in-process simulation). Nothing here was measured on the NAS, Pi 5 or server.

```sh
python -m venv .venv && .venv/bin/pip install pytest hypothesis    # test-only deps
.venv/bin/python -m pytest -q                                      # whole suite
RAFTSTORE_SIM_SEEDS=300 .venv/bin/python -m pytest -q tests/test_simulation.py   # deeper sweep
python scripts/chaos_demo.py
```

| suite | what it does | result here |
|---|---|---|
| default `pytest -q` | 187 tests: core scenarios, simulator, storage faults, linearizability, in-process nodes, mutual TLS, real 3-process `kill -9` cluster | **187 passed** in 127 s |
| simulator, default seeds | 235 randomized schedules × 3000 steps (716,757 simulated steps incl. heal phase, invariants checked after each; 111,228 client ops; 2,754 crashes, 2,792 partitions, 552 random drive pulls, 183,224 dropped + 49,872 duplicated messages) |  all invariants held, every history linearizable, no acknowledged write lost |
| simulator, 300 seeds/scenario | **2,100 schedules** (300 per scenario × 7): 6,423,903 steps, 934,611 client ops, 25,177 crashes, 25,076 partitions, 4,386 drive pulls, 1.68 M dropped messages | **7/7 passed** in 13 min 53 s: zero invariant violations, 2,100/2,100 histories linearizable, 454,005 acknowledged writes verified present on every member |
| storage crash points | power cut before/after every fsync, drive pulled inside every write, torn WAL tail at every byte: 3,716 distinct crash points | all recovered without losing an acknowledged entry |
| real processes | 4 client threads, leader `kill -9`'d twice, restarted; wall-clock history checked | latest run: 1,229 ops (595 acked writes, 4 indeterminate, 630 reads) → **linearizable**, replicas identical |

How the simulator works, what the checkers check, the mutation tests that prove they catch real bugs,
and the six bugs they found during development: [docs/testing.md](docs/testing.md).

### Chaos demo (real output, local machine)

`python scripts/chaos_demo.py` — three real processes named nas / pi5 / server on localhost:

```
[  0.00s] booting 3 local processes (nas, pi5, server); data dirs in a fresh temp directory
[  1.00s] leader elected: server
[  1.30s] uploaded photos/2026/holiday.raw: 3145851 bytes in 49 content-addressed chunks (sha256 0b597619dcef9c80...)
[  3.30s] 243 writes acknowledged so far by 3 concurrent writers
[  3.31s] PULLING THE DRIVE on leader 'server' (EIO after 5 more writes, last one torn)
[  3.34s] 'server' fail-stopped with exit code 75 after the I/O error (32 ms) -- it acknowledged nothing it had not fsynced
[  4.16s] new leader: 'pi5' (844 ms after the pull)
[  6.16s] writers kept going: 86 more writes acknowledged without 'server'
[  6.19s] writers stopped: 332 acknowledged, 0 attempts with unknown outcome
[  7.98s] verify[after drive pull]: 332 acknowledged writes checked with linearizable reads -> ALL PRESENT
[ 10.36s] photos/2026/holiday.raw still byte-identical
[ 10.41s] BIT ROT: flipped one bit in chunk 88f3d32edde696a3... on 'nas' (381 chunks referenced there)
[ 14.19s] scrubber on 'nas': checked=381 corrupt=1 repaired=1 unrepairable=0
[ 14.19s] chunk re-fetched from a healthy peer and verified by hash: True
[ 14.19s] REPLACING 'server': its drive is dead; removing it and adding an empty node 'server-new'
{"config": {"id": "server", "op": "remove"}}
[ 15.50s] 'server-new' caught up in 1.3s: snapshot_index=721 keys=333 chunks_repaired_from_peers=151; photo identical on server-new: True
[ 15.50s] voters now: ['nas', 'pi5', 'server-new']
[ 16.50s] kill -9 the leader 'pi5' during writes
[ 17.33s] new leader 'nas' after 828 ms
[ 18.84s] restarted 'pi5' from its WAL
[ 20.36s] phase-2 writers: 434 acknowledged, 0 attempts with unknown outcome
[ 20.36s] all nodes converged: nas=commit 1592, pi5=commit 1592, server-new=commit 1592
[ 21.70s] verify[final]: 766 acknowledged writes checked with linearizable reads -> ALL PRESENT
[ 21.70s] DONE: no acknowledged write was lost through a pulled drive, bit rot, node replacement and kill -9
```

### Local micro-benchmark (`python scripts/bench.py --local`)

Three processes on one Windows 10 laptop-class machine, loopback network, fsync on (NTFS). Only useful
as a baseline for the real hardware runs.

```
seq-1024               n=300   size=1024     threads=1   ops/s=   137.4  MB/s=   0.14  p50=   6.0ms p99=  27.9ms
conc8-1024             n=296   size=1024     threads=8   ops/s=   250.0  MB/s=   0.26  p50=  31.0ms p99=  57.3ms
seq-1048576            n=30    size=1048576  threads=1   ops/s=    21.6  MB/s=  22.62  p50=  38.8ms p99= 156.7ms
get-linearizable       n=300   size=1024     threads=1   ops/s=   546.1  MB/s=   0.56  p50=   1.4ms p99=  18.6ms
```

## Hardware validation / TODO for the author

**Not done yet — no results from the real NAS, Pi 5 or server PC exist in this repository.** The exact
procedure (install, bring-up, LAN benchmark, physically pulling the leader's drive during writes, power
cuts, bit rot on a real disk, Pi-specific checks) and **empty result tables** to fill in are in
[docs/operations.md → Hardware validation](docs/operations.md#hardware-validation--todo-for-the-author).
Summary of the tables to fill:

| test | nas | pi5 | server |
|---|---|---|---|
| bring-up / joined cluster | | | |
| `bench.py` seq PUT 1 KiB p50 (ms) | | | |
| `bench.py` seq PUT 1 MiB MB/s | | | |
| drive pulled mid-write: acked writes missing | | | |
| power cut: acked writes missing | | | |
| scrub: corrupt chunk found + repaired | | | |

Other open items: an end-to-end run with TLS across the LAN (TLS is tested on localhost only), long-run
soak test (days) with the scrubber on spinning disks, and tuning `tick_ms`/`election_ticks` for Wi-Fi if
the Pi is not wired.

## Deploy

`deploy/` contains example configs for the three machines (`deploy/config/{nas,pi5,server}.json`,
10.0.0.10–12 placeholders, mutual TLS), a replacement-node config, systemd units (the service refuses to
restart-loop after a fail-stop: exit code 75), a scrub timer, a Dockerfile and a docker-compose file
for a single-host trial. See [docs/operations.md](docs/operations.md).

## License

MIT — see [LICENSE](LICENSE).

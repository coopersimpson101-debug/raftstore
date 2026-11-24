# Testing strategy

The protocol core is deterministic and I/O-free, so most of the testing happens in a simulator where
time, the network and the disk are all controlled by one seeded PRNG. A failing seed replays exactly.

## 1. Deterministic simulation (`raftstore/sim/cluster.py`, `tests/test_simulation.py`)

Each schedule runs 3000 steps. In a step: nodes tick (independent clocks, or synchronized for lease
tests), up to 6 random in-flight messages are delivered (chosen at random ⇒ reordering), clients submit
reads/writes to whoever thinks it is leader (sometimes to a random node), and faults fire:

* network: drop (5 %), duplicate (3 %), delay 0–4 steps, random two-way partitions and heals;
* nodes: crash (state not yet fsynced is lost), restart, leadership transfer, add/remove members;
* disk (scenarios with `storage="disk"`): the node runs the **real WAL and snapshot code on FaultyFS**;
  crashes tear the unsynced tail at a random byte; "pull drive" makes the k-th next write partial and
  everything after it EIO.

After **every step** the [`InvariantChecker`](../raftstore/check/invariants.py) checks election safety,
log matching (pairwise), leader completeness, that every node's committed prefix matches the global
record, and state machine safety (same entry and same state digest at every applied index, including
snapshot installs). At the end the cluster is healed and must converge (liveness), every acknowledged
write must be present on every member, and the client history must be linearizable.

Scenarios: lossy network + crashes + partitions; 5 nodes + leadership transfer; membership changes;
lease reads with synchronized clocks; PreVote and CheckQuorum disabled; real WAL on a faulty disk with
drive pulls; and everything at once.

## 2. Linearizability checker (`raftstore/check/linearizability.py`)

Wing–Gong search with Lowe's memoisation (as in Knossos/Porcupine) over per-key register histories.
Writes with unknown outcome stay open forever (they may or may not have happened); reads that never
returned are dropped. Validated against a brute-force permutation oracle on 1500 random small histories
(`test_matches_brute_force_oracle`) and on a 3000-operation history.

## 3. Storage fault injection (`tests/test_storage.py`)

* torn tail at **every byte offset** of a WAL;
* corruption in the middle of the WAL is refused, not silently truncated;
* a Raft-legal random workload (appends, conflicting truncations with commit advances, term/vote
  changes, compactions, snapshot installs) re-opened after every operation must equal an in-memory model;
* **power cut before and after every single fsync** of that workload, with torn tails: the recovered
  state must lose nothing acknowledged, invent nothing, never regress term/vote, and never hold a commit
  index whose entries are not the committed ones;
* **drive pulled in the middle of every single write** of that workload: EIO, stays dead, re-insert,
  same recovery checks;
* chunk writes are atomic under crashes; corrupt chunks are detected and quarantined.

## 4. Real processes (`tests/test_cluster_process.py`, `scripts/chaos_demo.py`)

Three `python -m raftstore serve` processes on localhost. Four client threads issue reads and writes on
three keys while the leader is killed with `kill -9` (TerminateProcess on Windows), restarted, and the
new leader killed again. The wall-clock history is checked for linearizability; afterwards all replicas
must hold identical values. A write is retried only when it certainly had no effect (connection
refused, redirect, explicit "retry" from a non-leader); timeouts make it indeterminate.

## Mutation testing: do the checkers actually catch bugs?

| injected bug | caught by |
|---|---|
| serve reads from any replica's local state (`bug_stale_reads`) | linearizability checker (`test_checker_catches_injected_stale_read_bug`) |
| commit entries from older terms by counting replicas (`unsafe_commit_any_term`) | `test_commit_only_current_term_entries` |
| WAL batch order HARDSTATE before ENTRIES | `test_crash_at_every_fsync_boundary`, `test_pull_drive_mid_write_at_every_write` (11 failures when reintroduced) |

## Bugs the tests found during development

1. **WAL batch order** (disk-mode simulation): HARDSTATE written before ENTRIES let a torn tail keep a
   commit index without its entries ⇒ a node applied a never-committed entry. Fixed by writing entries first.
2. **WAL gap after crash between snapshot and WAL record** (disk-mode simulation): recovery reconciled
   the in-memory view with a newer snapshot but left the WAL untouched, so the next append left a gap and
   the node could never restart. Fixed by rewriting the WAL onto the snapshot during recovery.
3. **Leader that commits its own removal** crashed while handling the same response (membership
   simulation). Fixed.
4. **Redirects pointing at the wrong path** (real-process Jepsen test): a follower's `307 Location`
   omitted the request path, so redirected linearizable GETs hit `/` and returned 404 — recorded as reads
   of a missing key, which the linearizability checker flagged. Fixed; leaderless nodes now answer 503.
5. **Repair stopped at the first half-closed peer** (node replacement test, flaky): an
   `IncompleteReadError` from a dying peer aborted the whole repair loop. Fixed.
6. **Replacement node missing chunks** whose chunk entries were compacted into a snapshot before it
   joined but whose manifest applied after. Fixed by queueing repairs when a manifest references absent chunks.

## Numbers (measured on the Windows 10 development machine, not on the target hardware)

* Default suite: 187 tests passed in ~2 min; 235 simulated schedules, 3,716 storage crash points.
* Deep sweep `RAFTSTORE_SIM_SEEDS=300 pytest tests/test_simulation.py`: 2,100 schedules,
  6,423,903 simulated steps with invariants checked after each, 934,611 client operations,
  25,177 crashes, 25,076 partitions, 4,386 drive pulls, 1,681,785 dropped and 454,760 duplicated
  messages — zero violations, all 2,100 histories linearizable, 454,005 acknowledged writes verified.
* Real processes: e.g. 1,229 operations across two leader `kill -9`s → linearizable.

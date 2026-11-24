# Fault model

## Tolerated (and tested)

| Fault | Behaviour | Where it is tested |
|---|---|---|
| Process crash / `kill -9` of any node, including the leader mid-write | Survivors elect a leader within ~1–2 election timeouts; the crashed node recovers from its WAL and catches up | `test_cluster_process.py` (real processes), simulator |
| Power loss on one node or **all nodes at once** | Only fsynced state survives; torn tails are truncated; no acknowledged write is lost | `test_whole_cluster_power_loss_with_torn_writes`, `test_crash_at_every_fsync_boundary` |
| Drive **pulled mid-write** (partial write, then EIO forever) | The node fail-stops on the first I/O error (exit code 75) and never acknowledges anything it did not fsync; the cluster continues if a majority remains | `test_pull_the_drive.py`, `test_pull_drive_mid_write_at_every_write`, `test_node.py`, chaos demo |
| Drive re-inserted after being pulled | Node recovers like after a power cut | `test_pull_leader_drive_mid_write_then_reinsert` |
| Drive dead for good | Operator removes the node and adds an empty replacement; data is re-replicated (snapshot + chunk repair) | `test_dead_drive_node_replaced_and_data_rereplicated`, `test_replace_dead_node_rereplicates_everything`, chaos demo |
| Bit rot in a chunk file | Detected by SHA-256 on read or by the scrubber; quarantined and repaired from a peer | `test_scrubber_detects_bitrot_and_repairs_from_peer`, chaos demo |
| Bit rot in a snapshot file | CRC fails; older snapshot used if the WAL allows it, otherwise refuse to start | `test_snapshot_crc_fallback_and_missing_snapshot_detection` |
| Bit rot inside the WAL (not in the last record) | Detected (valid records after an invalid one) → refuse to start, replace node | `test_wal_corruption_before_tail_refuses_to_start` |
| Message loss, duplication, reordering, arbitrary delay | Raft retransmits; duplicates and stale responses are ignored | simulator (all scenarios) |
| Network partitions (any split) | Minority side cannot commit or serve linearizable reads; PreVote/CheckQuorum prevent disruption on heal | simulator |
| Membership changes and leadership transfer under faults | Single-server changes keep majorities overlapping | simulator (`membership-changes`, `everything-at-once`) |

With `n` voters the cluster stays available while a majority (`⌊n/2⌋+1`) is up and connected; with 3
nodes it tolerates one failure, with 5 nodes two.

## Not tolerated / assumptions

* **Byzantine faults.** Nodes are assumed honest; TLS protects the wire, not against a compromised node.
* **Disks that lie about fsync** (volatile write caches without power-loss protection). raftstore can
  only be as durable as `fsync()`; check drive cache settings on the NAS (see operations.md).
* **Simultaneous loss of a majority of drives.** Data on a majority is the definition of committed.
* **Bit rot in the last WAL record** is treated as a torn write (see storage.md).
* **Lease reads** additionally assume bounded clock drift; the default ReadIndex mode does not.
* **Silent corruption of in-memory state** (bad RAM). ECC memory recommended on the NAS/server.
* `fsync: false` disables the durability guarantees entirely (benchmarking only).

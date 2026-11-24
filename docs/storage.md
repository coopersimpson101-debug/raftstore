# Storage layer

```
<data_dir>/
  raft/
    wal/0000000000000007.wal      newest segment = the whole log (older ones are superseded)
    snap/00000000000000004211-00000000000000000009.snap   <index>-<term>, newest two kept
  chunks/
    3f/3f9a…e1                     content-addressed chunk files (name = SHA-256)
    quarantine/<hash>.<n>          corrupt copies moved aside by the scrubber / read path
```

All file I/O goes through a 12-method filesystem interface (`storage/fs.py`) so the exact same code
runs on the real disk (`RealFS`), on the real disk with a "pull the drive" switch
(`FaultInjectingFS`), and on a simulated disk with a power-loss model (`FaultyFS`).

## Write-ahead log

Record: `u32 length | u32 crc32(type ‖ payload) | u8 type | payload`.

| type | payload |
|---|---|
| 1 HARDSTATE | `u64 term, u64 commit, vote (utf-8)` |
| 2 ENTRY | `u64 term, u64 index, u8 kind, data` (an entry at index i replaces everything ≥ i) |
| 3 RESET | `u64 index, u64 term` — log now starts after this snapshot point |

**One batch per Ready, one fsync per batch.** Order inside a batch: `RESET → ENTRIES → HARDSTATE`.
Every prefix of that batch is a state the node may legally be in before it has replied to anyone.
In particular a surviving commit index always has its entries before it. (The first version wrote
HARDSTATE first; the disk-mode simulation found a schedule where a torn tail kept `commit=109` but
lost the entries that overwrote a stale suffix, and the node applied a never-committed entry after
restart. The storage crash test `test_crash_at_every_fsync_boundary` now fails if the order is reverted —
verified by mutation.)

**fsync policy.** `fsync: true` (default) fsyncs when the batch contains entries, a snapshot, or a
term/vote change. Commit-index-only updates are written without fsync: commit is recomputable from the
cluster, so losing it on power cut is safe. `fsync: false` exists for benchmarking only; with it the node
can acknowledge data that a power cut destroys, and Raft's safety argument no longer holds.

**Recovery and torn writes.** Only the newest segment is read. Parsing stops at the first invalid
record (short header, length beyond EOF, unknown type, CRC mismatch, zero fill).

* If **no valid record follows** that point, it is the torn tail of a write that was never acknowledged:
  the file is truncated there (and the truncation fsynced) and the node starts.
* If a **valid record follows**, acknowledged data in the middle of the log is damaged (bit rot,
  misdirected write). The node refuses to start (`CorruptLogError`) rather than silently forgetting
  entries it may have voted with; the operator replaces the node (its peers hold all committed data).

Known limitation: a bit flip inside the *last* record is indistinguishable from a torn write and is
truncated. If that record had been acknowledged this node forgets one entry; Raft tolerates this only
because the other replicas hold it — it is one reason to run 3+ nodes on independent disks.

**Compaction** writes `RESET(snapshot) + HARDSTATE + entries after the snapshot` into
`<seq+1>.wal.tmp`, fsyncs it, renames it to `<seq+1>.wal`, fsyncs the directory, then deletes older
segments. A crash at any point leaves either the old or the new segment as the newest complete file.

## Snapshots

`<index>-<term>.snap` = `"RSNAP1\n" | crc32 | u32 meta_len | meta JSON (index, term, config) | data`,
written with **temp file + fsync + rename + fsync(dir)**. Recovery takes the newest file whose CRC
verifies. Consistency rules applied on load:

* WAL `RESET(i)` but no valid snapshot ≥ i → refuse to start (would have a hole).
* Snapshot newer than the WAL's base (crash between writing the snapshot and the WAL record): keep WAL
  entries after the snapshot only if the WAL has the snapshot's index with the snapshot's term,
  otherwise drop them; then *rewrite the WAL onto the snapshot* before anything is appended (the
  simulator found the gap that otherwise appears).

Windows note: NTFS cannot fsync a directory handle; `RealFS.sync_dir` is a no-op there (NTFS journals
metadata). On Linux it is a real `fsync(dirfd)`.

## Content-addressed chunks and the blob state machine

`PUT /kv/<key>` on the leader splits the body into `chunk_size` pieces (default 256 KiB, 1 MiB in the
deploy configs) and proposes one log entry per chunk (`"C" ‖ sha256 ‖ bytes`), pipelined, then one
manifest entry (`{"op":"put","key","chunks":[…],"size","sha256"}`). A reader can never see a
half-written object: the key only appears when the manifest applies. Applying a chunk entry writes
`chunks/<aa>/<hash>` atomically (verified against its hash first); identical chunks are stored once.

GET assembles the chunks, verifying each chunk's SHA-256 and finally the object's SHA-256.

## Self-healing

* **Read path.** A chunk that fails verification on read is moved to `quarantine/` and fetched from a
  peer (`GET /internal/chunk/<hash>`, verified by hash before it is stored). Peers never serve a chunk
  that fails their own verification.
* **Scrubber.** Every `scrub_interval_s` the node re-reads every chunk at most `scrub_rate_bytes`/s,
  quarantines corrupt ones and repairs every *referenced* chunk that is missing or corrupt. Stats are in
  `/status` (`scrub`, `repair`). `raftstore scrub` runs a pass on demand.
* **Re-replication after replacement.** A new node is seeded by InstallSnapshot (manifests only), then
  fetches every referenced chunk it lacks from peers. Manifests applied later that reference chunks
  whose chunk entries were compacted away before the node joined are queued for repair as well.
* **GC.** `raftstore gc --grace N` proposes a `gc` entry; every replica deterministically deletes chunks
  that no manifest references and that were last written more than `N` log entries earlier (so in-flight
  uploads are not collected).

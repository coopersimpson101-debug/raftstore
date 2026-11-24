# The Raft implementation in detail

Source: [`raftstore/raft/core.py`](../raftstore/raft/core.py) (≈700 lines, no I/O, no clock, no threads).

## Shape of the core

```
          tick()                step(msg)            propose()/read_index()/transfer_leadership()
            │                      │                          │
            └──────────────┬───────┴──────────────────────────┘
                           ▼
                 ┌───────────────────┐
                 │   Raft (pure)     │   term, vote, log, commit, progress, votes, read queue
                 └─────────┬─────────┘
                           │ ready()
                           ▼
   Ready{ hard_state, entries, snapshot, messages, committed, read_states, failed_reads, must_sync }
```

The runtime (real node or simulator) must handle a `Ready` in this order:

1. **persist** `snapshot`, `entries`, `hard_state` and fsync if `must_sync`;
2. **send** `messages`;
3. **apply** the snapshot, then `committed` entries;
4. **serve** `read_states` once `applied >= index`.

Because messages leave only after step 1, *every* RPC response (vote granted, append
accepted, snapshot installed) is backed by durable state. That is the whole of Raft's
"persist before responding" rule, enforced structurally rather than by discipline at each call site.

## Leader election

* Every follower counts `election_elapsed` ticks; when it reaches a **randomized** timeout in
  `[election_ticks, 2·election_ticks)` it campaigns. Randomization makes split votes rare.
* **PreVote** (Ongaro thesis §9.6). A would-be candidate first asks "would you vote for me in
  term T+1?" *without* incrementing its own term. Voters answer yes only if the candidate's log is at
  least as up-to-date as theirs and they have not heard from a leader recently. Only after a quorum
  of pre-votes does the node become a real candidate. A partitioned node therefore never inflates its
  term and cannot depose a healthy leader when it reconnects (`test_prevote_keeps_partitioned_node_from_inflating_term`).
* **CheckQuorum + leader stickiness.** A leader that has not heard from a quorum within an election
  timeout steps down. A follower that heard from a leader within the last election timeout ignores
  vote requests (unless they carry the leadership-transfer flag). Together with PreVote this keeps
  removed or flaky nodes from disrupting the cluster.
* **Voting rule.** At most one vote per term, persisted (`HardState.vote`) before the response is sent;
  a vote is granted only to a candidate whose log is at least as up-to-date (§5.4.1: higher last term,
  or equal last term and ≥ last index).
* A new leader immediately appends a **no-op** entry in its term; this is what lets it commit entries
  from earlier terms (see below) and what ReadIndex waits for.

## Log replication

* The leader keeps `Progress{match, next}` per follower and sends `append(prev_index, prev_term,
  entries, commit)`. Sends are pipelined: `next` advances optimistically when a batch leaves.
  Heartbeats are appends too, so lost messages are retransmitted on the next heartbeat.
* The follower checks that it has `prev_index` with `prev_term`; if so it appends, truncating any
  conflicting *uncommitted* suffix, and sets `commit = min(leader_commit, prev_index + len(entries))`
  (never beyond what it has verified to match).
* **Fast backtracking.** On a mismatch the follower answers with a hint: if its log is too short,
  `hint_index = last+1`; otherwise `hint_term` = its term at `prev_index` and `hint_index` = the first
  index of that term. The leader jumps `next` to just after its own last entry of `hint_term` (if it
  has that term) or to `hint_index`. One round trip per conflicting *term* instead of per entry —
  `test_fast_backtracking_needs_few_round_trips` reconciles a 200-entry divergent suffix with ≤ 3 rejections.
* Entries a follower already knows are committed are skipped, which makes duplicated and reordered
  appends harmless.

## Commit rule

The leader sorts the match indices of the voters (itself counting as its last index) and takes the
quorum-th largest `N`. It commits `N` **only if `log[N].term == currentTerm`** (§5.4.2). Entries from
earlier terms become committed indirectly, when a current-term entry after them commits.

Why: in Figure 8 of the Raft paper an entry from term 2 can be on a majority and still be overwritten
by a leader of term 3 that never saw it. `test_commit_only_current_term_entries` constructs exactly that
state and shows that with the `unsafe_commit_any_term` mutation switch the leader would commit it.

## Safety argument (sketch)

The four properties of Figure 3, which the simulator checks after every step:

1. **Election safety** — one leader per term: each node votes once per term (vote is durable before
   the reply), and two majorities intersect.
2. **Log matching** — if two logs have an entry with the same index and term, the logs are identical up
   to it: a leader creates at most one entry per index in its term, and the `prev_index/prev_term`
   consistency check makes followers extend only matching prefixes (induction on appends).
3. **Leader completeness** — a committed entry is in every later leader's log: it is on a majority;
   any later leader got votes from a majority; some voter had the entry, and the up-to-date check means
   the candidate's log is at least as new as that voter's, so it contains the entry (using the
   current-term commit rule to make "committed" well-defined).
4. **State machine safety** — no two nodes apply different entries at an index: nodes only apply
   committed prefixes, and by (2)+(3) committed prefixes agree.

Persistence matters for all of them; the storage layer's job is to make "durable" true even across
torn writes and lost drives (see [storage.md](storage.md)).

## Snapshots, InstallSnapshot and compaction

* After applying index `i`, the runtime may call `compact(i, state_bytes)`. The core records
  `Snapshot(i, term(i), config_at(i), data)` and drops log entries `≤ i`; the storage layer writes the
  snapshot file atomically and rewrites the WAL without the covered prefix.
* When a follower needs an entry the leader has compacted (`term_at(next-1)` unknown), the leader sends
  the whole snapshot. The follower:
  * ignores it if it already committed past it;
  * if it has an entry at `snapshot.index` with the same term, just advances commit (keeps its suffix);
  * otherwise discards its log and restores from the snapshot (delivered through `Ready.snapshot`).
* The blob state machine keeps only manifests in the snapshot. Chunk bytes are content-addressed and
  re-fetched from peers after a snapshot install (this is how a replaced node is re-populated).

## Membership changes (single-server)

* Config entries (`ENTRY_CONFIG`) take effect as soon as they are **appended**, not when committed
  (dissertation §4.1). If an uncommitted config entry is truncated, the configuration is recomputed
  from the log/snapshot.
* Only one change at a time: the leader refuses a new change while `config_index > commit`, and refuses
  any change until it has committed an entry in its own term (the fix for the single-server change bug
  described by Ongaro on raft-dev, 2015).
* Adding one server or removing one server at a time keeps every old majority and new majority
  overlapping, so no two leaders can be elected by disjoint quorums.
* A leader that removes itself keeps leading until the change commits (not counting itself), then
  steps down.
* The founding configuration is log entry 1 (`bootstrap`), identical on all founding members, so a
  node that joins later learns the full membership history by replication.

## Linearizable reads

* **ReadIndex (default, `read_mode: safe`).** The leader records `read_index = commit`, then confirms
  it is still leader by a heartbeat round: each heartbeat carries a sequence number `ctx`, followers echo
  it, and once a quorum has echoed a `ctx ≥` the read's sequence number the read is released. The
  runtime serves it after applying up to `read_index`. Reads before the leader's first commit in its term
  wait for that commit. No log write, no fsync.
* **Lease (`read_mode: lease`).** If a quorum acknowledged heartbeats *sent* within the last
  `lease_ticks < election_ticks` ticks, the leader serves from local state with no round trip. Followers
  that heard from a leader within an election timeout refuse to vote, so no other leader can exist
  during the lease. Assumes bounded clock drift between nodes. Extra guards: a freshly restarted node
  refuses votes for one election timeout (it may have acked a lease before crashing), and a leader
  that starts a leadership transfer disables lease reads for two election timeouts.

## Leadership transfer

`transfer_leadership(target)`: the leader stops accepting proposals, brings the target up to date,
then sends `TimeoutNow`; the target campaigns immediately (skipping PreVote, with the `transfer` flag
that overrides stickiness). If it does not complete within an election timeout the leader resumes.

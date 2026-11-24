# HTTP API and CLI

Every node serves the API. Mutations must go to the leader; other nodes answer
`307` with `{"leader": id, "leader_http": url}` and a `Location` header pointing at the same path on
the leader, or `503 {"error": "retry: …"}` while no leader is known. The CLI/`Client` follow both.

| method & path | description |
|---|---|
| `PUT /kv/<key>` | store the request body; chunked, replicated, acknowledged after commit + apply on the leader → `201 {key, size, sha256, chunks, index}` |
| `GET /kv/<key>` | linearizable read (ReadIndex or lease, per `read_mode`) → bytes; `404` if absent |
| `GET /kv/<key>?consistency=stale` | read this node's local state (any node, may be behind) |
| `DELETE /kv/<key>` | → `{deleted, index}` |
| `GET /kv?prefix=p` | list keys with size / sha256 / index |
| `GET /status` | Raft state, progress, membership, WAL size, scrub/repair stats, recovery notes |
| `POST /admin/members` | `{"op":"add","id","raft","http"}` or `{"op":"remove","id"}` (single-server change) |
| `POST /admin/transfer` | `{"target": id}` — leadership transfer |
| `POST /admin/snapshot` | snapshot + WAL compaction now |
| `POST /admin/scrub` | run a scrub pass on *this* node, returns its stats |
| `POST /admin/gc` | `{"grace": N}` — delete orphaned chunks older than N log entries |
| `GET /internal/chunk/<sha256>` | peer repair endpoint (only intact chunks are served) |
| `POST /debug/fault` | `{"after_writes": k}` — "pull the drive" after k writes; **only if `fault_injection: true`** |

Error codes: `503` retryable (no leader / proposal dropped / node fail-stopped), `504` timed out with
**unknown outcome** (the write may still commit), `400` bad request.

```
raftstore [--url URLS] [--cacert CA --cert CERT --key KEY] <command>
  serve --config node.json
  put KEY [FILE|-]            get KEY [-o FILE] [--consistency stale]
  delete KEY                  ls [--prefix P]
  status [--all] [--json]     member add ID --raft H:P --http H:P | member remove ID
  transfer ID                 snapshot | scrub | gc [--grace N]
```
`RAFTSTORE_URLS`, `RAFTSTORE_CACERT`, `RAFTSTORE_CERT`, `RAFTSTORE_KEY` may be set in the environment.

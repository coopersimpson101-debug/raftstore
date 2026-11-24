"""The raftstore server: Raft core + WAL + chunk store + transport + HTTP API + scrubber."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from urllib.parse import quote

from .config import NodeConfig
from .httpd import HttpServer, Request, Response, http_request
from .raft.core import Raft
from .raft.types import ENTRY_CONFIG, ENTRY_NORMAL, LEADER, NotLeader, ProposalDropped, decode_conf
from .statemachine import BlobStateMachine, encode_chunk_cmd, encode_json_cmd
from .storage.chunks import ChunkCorrupt, ChunkMissing, ChunkStore
from .storage.fs import FaultInjectingFS, RealFS
from .storage.raftlog import DiskStorage
from .transport import Transport, make_ssl_contexts

log = logging.getLogger("raftstore.node")


class Unavailable(Exception):
    pass


def _exit_process(exc: BaseException) -> None:
    logging.getLogger("raftstore").critical("FAIL-STOP: %s -- exiting so that no unsafe ack is ever sent", exc)
    logging.shutdown()
    os._exit(75)


class StoreNode:
    def __init__(self, cfg: NodeConfig, fs=None, on_fatal=None):
        self.cfg = cfg
        self.fs = fs or (FaultInjectingFS() if cfg.fault_injection else RealFS())
        self.on_fatal = on_fatal or _exit_process
        self.dead = False
        self.fatal_error: str | None = None
        self.applied = 0
        self.waiters: dict[int, tuple] = {}
        self.read_waiters: dict[int, asyncio.Future] = {}
        self.apply_waiters: list = []
        self._ctx = 0
        self._kicked = False
        self.addr_book: dict[str, dict] = {k: dict(v) for k, v in cfg.initial_cluster.items()}
        self.scrub_stats: dict = {"runs": 0}
        self.repair_stats = {"repaired": 0, "failed": 0, "corrupt_reads": 0}
        self.started = time.time()
        self.tasks: list = []
        self._repairing: set = set()
        _, self.client_ssl = make_ssl_contexts(cfg.tls)

    # --------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        cfg, fs = self.cfg, self.fs
        fs.makedirs(cfg.data_dir)
        self.storage = DiskStorage(fs, fs.join(cfg.data_dir, "raft"), sync=cfg.fsync)
        self.chunks = ChunkStore(fs, fs.join(cfg.data_dir, "chunks"), sync=cfg.fsync)
        self.sm = BlobStateMachine(self.chunks)
        hs, snap, ents = self.storage.load()
        for note in self.storage.recovery_notes:
            log.warning("%s: recovery: %s", cfg.id, note)
        if hs.term == 0 and snap is None and not ents and not cfg.join:
            hs, ents = Raft.bootstrap(cfg.initial_cluster)
            self.storage.save(hs, ents, None, sync=True)
            log.info("%s: bootstrapped new cluster %s", cfg.id, sorted(cfg.initial_cluster))
        self.raft = Raft(cfg.id, hard_state=hs, snapshot=snap, entries=ents,
                         election_ticks=cfg.election_ticks, heartbeat_ticks=cfg.heartbeat_ticks,
                         read_only=cfg.read_mode, max_msg_bytes=max(1 << 20, cfg.chunk_size * 4))
        if snap is not None:
            self.sm.restore(snap.data)
            self.applied = snap.index
            self._learn(snap.config)
        self._learn(self.raft.voters)
        self.transport = Transport(cfg.id, cfg.raft_listen, self._raft_addr, self._on_message, cfg.tls)
        await self.transport.start()
        server_ssl, _ = make_ssl_contexts(cfg.tls)
        self.http = HttpServer(cfg.http_listen, self.handle_http, server_ssl)
        await self.http.start()
        self.tasks = [asyncio.ensure_future(self._ticker()), asyncio.ensure_future(self._scrub_loop())]
        if snap is not None:
            self._schedule_repair()
        self._drive()
        log.info("%s: started (term=%d last=%d commit=%d) raft=%s http=%s", cfg.id, self.raft.term,
                 self.raft.log.last_index(), self.raft.log.commit, cfg.raft_listen, cfg.http_listen)

    async def stop(self) -> None:
        self.dead = True
        for t in self.tasks:
            t.cancel()
        await self.transport.close()
        await self.http.close()
        self.storage.close()

    def _fatal(self, exc: BaseException) -> None:
        if self.dead:
            return
        self.dead = True
        self.fatal_error = f"{type(exc).__name__}: {exc}"
        log.critical("%s: fatal storage error, fail-stopping: %s", self.cfg.id, exc)
        for _, fut in self.waiters.values():
            if not fut.done():
                fut.set_exception(Unavailable("node failed; outcome unknown"))
        for fut in self.read_waiters.values():
            if not fut.done():
                fut.set_exception(Unavailable("node failed"))
        for t in self.tasks:
            t.cancel()
        asyncio.ensure_future(self.transport.close())
        asyncio.ensure_future(self.http.close())
        self.on_fatal(exc)

    # ------------------------------------------------------------ addresses
    def _learn(self, config: dict) -> None:
        for k, v in config.items():
            if v:
                self.addr_book[k] = dict(v)

    def _meta(self, nid: str) -> dict:
        return self.raft.voters.get(nid) or self.addr_book.get(nid) or {}

    def _raft_addr(self, nid: str) -> str | None:
        return self._meta(nid).get("raft")

    def http_url(self, nid: str) -> str | None:
        a = self._meta(nid).get("http")
        if not a:
            return None
        return a if "://" in a else (("https://" if self.cfg.tls else "http://") + a)

    # ---------------------------------------------------------- raft driving
    def _on_message(self, m) -> None:
        if self.dead:
            return
        self.raft.step(m)
        self._kick()

    def _kick(self) -> None:
        """Coalesce many steps into one Ready/fsync (group commit)."""
        if not self._kicked and not self.dead:
            self._kicked = True
            asyncio.get_running_loop().call_soon(self._drive)

    async def _ticker(self) -> None:
        interval = self.cfg.tick_ms / 1000.0
        while not self.dead:
            await asyncio.sleep(interval)
            if self.dead:
                return
            self.raft.tick()
            self._drive()

    def _drive(self) -> None:
        self._kicked = False
        if self.dead:
            return
        try:
            while self.raft.has_ready():
                rd = self.raft.ready()
                # 1. durability first: nothing below may be observed before this returns
                self.storage.save(rd.hard_state, rd.entries, rd.snapshot, sync=rd.must_sync)
                # 2. now it is safe to talk
                for m in rd.messages:
                    self.transport.send(m)
                # 3. apply
                if rd.snapshot is not None:
                    self._install_snapshot(rd.snapshot)
                for e in rd.committed:
                    self._apply(e)
                for rs in rd.read_states:
                    fut = self.read_waiters.pop(rs.ctx, None)
                    if fut is not None and not fut.done():
                        fut.set_result(rs.index)
                for ctx in rd.failed_reads:
                    fut = self.read_waiters.pop(ctx, None)
                    if fut is not None and not fut.done():
                        fut.set_exception(NotLeader(self.raft.leader))
                self._wake_apply_waiters()
                self._maybe_snapshot()
            if self.sm.pending_repair:
                hs, self.sm.pending_repair = sorted(self.sm.pending_repair), set()
                self._schedule_repair(hs)
        except OSError as e:
            self._fatal(e)
        except Exception as e:  # noqa: BLE001 - an internal bug must stop the node, not freeze it
            log.exception("%s: unexpected error in the Raft loop", self.cfg.id)
            self._fatal(e)

    def _install_snapshot(self, snap) -> None:
        self.sm.restore(snap.data)
        self.applied = snap.index
        self._learn(snap.config)
        for idx in [i for i in self.waiters if i <= snap.index]:
            _, fut = self.waiters.pop(idx)
            if not fut.done():
                fut.set_exception(Unavailable("outcome unknown (superseded by snapshot)"))
        log.info("%s: installed snapshot at %d (%d keys)", self.cfg.id, snap.index, len(self.sm.kv))
        self._schedule_repair()

    def _apply(self, e) -> None:
        res = None
        if e.kind == ENTRY_NORMAL:
            res = self.sm.apply(e.index, e.data)
        elif e.kind == ENTRY_CONFIG:
            cc = decode_conf(e.data)
            if cc["op"] == "bootstrap":
                self._learn(cc["voters"])
            elif cc["op"] == "add":
                self._learn({cc["id"]: cc.get("meta", {})})
            res = {"config": cc}
            log.info("%s: membership change committed at %d: %s", self.cfg.id, e.index, cc)
        self.applied = e.index
        w = self.waiters.pop(e.index, None)
        if w is not None:
            term, fut = w
            if not fut.done():
                if term == e.term:
                    fut.set_result(res)
                else:
                    fut.set_exception(ProposalDropped("entry was overwritten by a new leader"))

    def _wake_apply_waiters(self) -> None:
        if not self.apply_waiters:
            return
        keep = []
        for idx, fut in self.apply_waiters:
            if idx <= self.applied:
                if not fut.done():
                    fut.set_result(None)
            else:
                keep.append((idx, fut))
        self.apply_waiters = keep

    def _maybe_snapshot(self, force: bool = False) -> bool:
        lg = self.raft.log
        due = (lg.applied - lg.snap_index >= self.cfg.snapshot_every
               or (self.storage.wal_bytes() > self.cfg.snapshot_wal_bytes and lg.applied > lg.snap_index))
        if not (due or (force and lg.applied > lg.snap_index)):
            return False
        snap = self.raft.compact(lg.applied, self.sm.snapshot_bytes())
        self.storage.compact(snap)
        log.info("%s: snapshot at %d, WAL compacted", self.cfg.id, snap.index)
        return True

    # -------------------------------------------------------- client-facing
    async def propose(self, data: bytes, kind: int = ENTRY_NORMAL):
        idx, term = self.raft.propose(data, kind)
        fut = asyncio.get_running_loop().create_future()
        self.waiters[idx] = (term, fut)
        self._kick()
        return await asyncio.wait_for(fut, self.cfg.proposal_timeout_s)

    async def read_barrier(self) -> None:
        """Linearizable read point: ReadIndex (or lease), then wait for apply."""
        self._ctx += 1
        ctx = self._ctx
        fut = asyncio.get_running_loop().create_future()
        self.read_waiters[ctx] = fut
        try:
            self.raft.read_index(ctx)
        except NotLeader:
            self.read_waiters.pop(ctx, None)
            raise
        self._kick()
        index = await asyncio.wait_for(fut, self.cfg.proposal_timeout_s)
        await self.wait_applied(index)

    async def wait_applied(self, index: int) -> None:
        if self.applied >= index:
            return
        fut = asyncio.get_running_loop().create_future()
        self.apply_waiters.append((index, fut))
        await asyncio.wait_for(fut, self.cfg.proposal_timeout_s)

    async def put_blob(self, key: str, data: bytes) -> dict:
        cs = self.cfg.chunk_size
        pieces = [data[i:i + cs] for i in range(0, len(data), cs)] or [b""]
        hashes = []
        batch = []
        for p in pieces:
            h, cmd = encode_chunk_cmd(p)
            hashes.append(h)
            batch.append(self.propose(cmd))
            if len(batch) >= 16:
                await asyncio.gather(*batch)
                batch = []
        if batch:
            await asyncio.gather(*batch)
        manifest = {"op": "put", "key": key, "chunks": hashes, "size": len(data),
                    "sha256": hashlib.sha256(data).hexdigest()}
        res = await self.propose(encode_json_cmd(manifest))
        return {"key": key, "size": len(data), "sha256": manifest["sha256"], "chunks": len(hashes),
                "index": res["index"]}

    async def read_chunk(self, h: str) -> bytes:
        try:
            return self.chunks.get(h)
        except ChunkCorrupt:
            self.repair_stats["corrupt_reads"] += 1
            log.warning("%s: chunk %s failed verification on read; quarantining and repairing", self.cfg.id, h[:12])
            self.chunks.quarantine(h)
        except ChunkMissing:
            pass
        if await self.repair_chunk(h):
            return self.chunks.get(h)
        raise Unavailable(f"chunk {h} unavailable on this node and all peers")

    async def get_blob(self, key: str) -> bytes | None:
        m = self.sm.get_manifest(key)
        if m is None:
            return None
        data = b"".join([await self.read_chunk(h) for h in m["chunks"]])
        if hashlib.sha256(data).hexdigest() != m["sha256"]:
            raise Unavailable("assembled object failed end-to-end hash check")
        return data

    # ------------------------------------------------------- self-healing
    async def repair_chunk(self, h: str) -> bool:
        # current members first (the leader first of all), then any other address we know
        voters = sorted(self.raft.voters, key=lambda v: v != self.raft.leader)
        peers = voters + sorted(set(self.addr_book) - set(voters))
        for p in peers:
            if p == self.cfg.id:
                continue
            url = self.http_url(p)
            if not url:
                continue
            try:
                status, _, data = await http_request(f"{url}/internal/chunk/{h}", timeout=10, ssl_ctx=self.client_ssl)
            except Exception:  # noqa: BLE001 - a dead/half-closed peer must not stop the repair
                continue
            if status == 200 and hashlib.sha256(data).hexdigest() == h:
                try:
                    self.chunks.put(h, data)
                except OSError as e:
                    self._fatal(e)
                    return False
                self.repair_stats["repaired"] += 1
                log.info("%s: repaired chunk %s from %s", self.cfg.id, h[:12], p)
                return True
        self.repair_stats["failed"] += 1
        return False

    def _schedule_repair(self, hashes=None) -> None:
        async def go():
            for h in (hashes if hashes is not None else self.sm.missing_chunks()):
                if self.dead:
                    return
                if h not in self._repairing:
                    self._repairing.add(h)
                    try:
                        await self.repair_chunk(h)
                    finally:
                        self._repairing.discard(h)
        self.tasks.append(asyncio.ensure_future(go()))

    async def scrub_once(self) -> dict:
        st = {"checked": 0, "bytes": 0, "corrupt": 0, "missing": 0, "repaired": 0, "unrepairable": 0,
              "started": time.time()}
        refs = self.sm.referenced_chunks()
        delay = self.cfg.chunk_size / max(1, self.cfg.scrub_rate_bytes)
        try:
            for h in sorted(refs | set(self.chunks.list())):
                if self.dead:
                    break
                v = self.chunks.verify(h)
                st["checked"] += 1
                if v == "corrupt":
                    st["corrupt"] += 1
                    log.warning("%s: scrubber found bit-rot in chunk %s", self.cfg.id, h[:12])
                    self.chunks.quarantine(h)
                elif v == "missing":
                    st["missing"] += 1
                else:
                    st["bytes"] += self.cfg.chunk_size
                if v != "ok" and h in refs:
                    if await self.repair_chunk(h):
                        st["repaired"] += 1
                    else:
                        st["unrepairable"] += 1
                await asyncio.sleep(delay)
        except OSError as e:
            self._fatal(e)
        st["finished"] = time.time()
        st["runs"] = self.scrub_stats.get("runs", 0) + 1
        self.scrub_stats = st
        return st

    async def _scrub_loop(self) -> None:
        while not self.dead:
            await asyncio.sleep(self.cfg.scrub_interval_s)
            await self.scrub_once()

    # ----------------------------------------------------------------- HTTP
    def _not_leader(self, req: Request) -> Response:
        leader = self.raft.leader
        url = self.http_url(leader) if leader else None
        if url is None:
            # no known leader (election in progress): a retryable error, never a redirect to nowhere
            return Response.json({"error": "retry: no leader known", "leader": None}, 503)
        qs = "&".join(f"{k}={v}" for k, v in req.query.items())
        location = url + quote(req.path) + (f"?{qs}" if qs else "")
        return Response.json({"error": "not leader", "leader": leader, "leader_http": url}, 307,
                             {"Location": location})

    def status(self) -> dict:
        st = self.raft.status()
        st.update({
            "node_applied": self.applied, "keys": len(self.sm.kv), "chunks_referenced": len(self.sm.referenced_chunks()),
            "wal_bytes": self.storage.wal_bytes(), "scrub": self.scrub_stats, "repair": self.repair_stats,
            "uptime_s": round(time.time() - self.started, 1), "dead": self.dead, "fatal_error": self.fatal_error,
            "recovery_notes": self.storage.recovery_notes, "read_mode": self.cfg.read_mode,
            "members": {k: self._meta(k) for k in sorted(self.raft.voters)},
        })
        return st

    async def handle_http(self, req: Request) -> Response:
        if self.dead:
            return Response.json({"error": "node has fail-stopped", "fatal": self.fatal_error}, 503)
        try:
            return await self._route(req)
        except NotLeader:
            return self._not_leader(req)
        except ProposalDropped as e:
            return Response.json({"error": f"retry: {e}"}, 503)
        except Unavailable as e:
            return Response.json({"error": str(e)}, 503)
        except asyncio.TimeoutError:
            return Response.json({"error": "timed out; outcome unknown"}, 504)
        except (ValueError, KeyError) as e:
            return Response.json({"error": f"bad request: {e}"}, 400)

    async def _route(self, req: Request) -> Response:
        p, m = req.path, req.method
        if p == "/status" and m == "GET":
            return Response.json(self.status())
        if p.startswith("/kv/") and len(p) > 4:
            key = p[4:]
            if m == "PUT":
                if self.raft.state != LEADER:
                    raise NotLeader(self.raft.leader)
                return Response.json(await self.put_blob(key, req.body), 201)
            if m == "GET":
                if req.query.get("consistency", "linearizable") != "stale":
                    await self.read_barrier()
                data = await self.get_blob(key)
                if data is None:
                    return Response.json({"error": "not found", "key": key}, 404)
                return Response(200, data, {"Content-Type": "application/octet-stream",
                                            "X-Raftstore-Applied": str(self.applied)})
            if m == "DELETE":
                res = await self.propose(encode_json_cmd({"op": "del", "key": key}))
                return Response.json(res)
            return Response.json({"error": "method not allowed"}, 405)
        if p in ("/kv", "/kv/") and m == "GET":
            if req.query.get("consistency", "linearizable") != "stale":
                await self.read_barrier()
            prefix = req.query.get("prefix", "")
            return Response.json({"keys": [{"key": k, **{f: self.sm.kv[k][f] for f in ("size", "sha256", "index")}}
                                           for k in self.sm.keys(prefix)]})
        if p.startswith("/internal/chunk/") and m == "GET":
            h = p.rsplit("/", 1)[-1]
            try:
                return Response(200, self.chunks.get(h), {"Content-Type": "application/octet-stream"})
            except (ChunkMissing, ChunkCorrupt, ValueError):
                return Response.json({"error": "no intact copy here"}, 404)
        if p == "/admin/members" and m == "POST":
            body = json.loads(req.body or b"{}")
            cc = {"op": body["op"], "id": body["id"]}
            if body["op"] == "add":
                cc["meta"] = {"raft": body["raft"], "http": body["http"]}
                self.addr_book[body["id"]] = dict(cc["meta"])
            idx, term = self.raft.propose_conf_change(cc)
            fut = asyncio.get_running_loop().create_future()
            self.waiters[idx] = (term, fut)
            self._kick()
            res = await asyncio.wait_for(fut, self.cfg.proposal_timeout_s)
            return Response.json({"ok": True, "index": idx, "result": res})
        if p == "/admin/transfer" and m == "POST":
            body = json.loads(req.body or b"{}")
            self.raft.transfer_leadership(body["target"])
            self._kick()
            return Response.json({"ok": True, "transferring_to": body["target"]})
        if p == "/admin/snapshot" and m == "POST":
            try:
                took = self._maybe_snapshot(force=True)
            except OSError as e:
                self._fatal(e)
                raise Unavailable("storage failed") from e
            return Response.json({"ok": True, "snapshot_taken": took, "snapshot_index": self.raft.log.snap_index})
        if p == "/admin/scrub" and m == "POST":
            return Response.json(await self.scrub_once())
        if p == "/admin/gc" and m == "POST":
            body = json.loads(req.body or b"{}")
            res = await self.propose(encode_json_cmd({"op": "gc", "grace": int(body.get("grace", 1000))}))
            return Response.json(res)
        if p == "/debug/fault" and m == "POST":
            if not self.cfg.fault_injection or not isinstance(self.fs, FaultInjectingFS):
                return Response.json({"error": "fault injection disabled"}, 403)
            body = json.loads(req.body or b"{}")
            self.fs.pull(int(body.get("after_writes", 0)))
            log.warning("%s: DEBUG: drive will disappear after %s more writes", self.cfg.id, body.get("after_writes", 0))
            return Response.json({"ok": True})
        return Response.json({"error": "no such endpoint"}, 404)


async def serve(cfg: NodeConfig) -> None:
    node = StoreNode(cfg)
    await node.start()
    try:
        while not node.dead:
            await asyncio.sleep(3600)
    finally:
        if not node.dead:
            await node.stop()

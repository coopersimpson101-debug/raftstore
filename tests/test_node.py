"""In-process cluster of real StoreNodes: real TCP, real HTTP, real files on disk."""
import asyncio
import hashlib
import json
import os
import random

import pytest

from raftstore.config import NodeConfig
from raftstore.devcluster import free_ports
from raftstore.httpd import http_request
from raftstore.node import StoreNode
from raftstore.raft.types import LEADER
from raftstore.storage.fs import FaultInjectingFS


class Cluster:
    def __init__(self, tmp, names=("nas", "pi5", "server"), **extra):
        self.tmp = tmp
        self.extra = extra
        ports = free_ports(2 * len(names))
        self.initial = {n: {"raft": f"127.0.0.1:{ports[2 * i]}", "http": f"127.0.0.1:{ports[2 * i + 1]}"}
                        for i, n in enumerate(names)}
        self.nodes: dict[str, StoreNode] = {}
        self.fatal: dict[str, BaseException] = {}

    def cfg(self, name, addrs=None, join=False):
        addrs = addrs or self.initial[name]
        d = dict(id=name, data_dir=os.path.join(self.tmp, name), raft_listen=addrs["raft"],
                 http_listen=addrs["http"], initial_cluster=self.initial, join=join, tick_ms=20,
                 chunk_size=4096, snapshot_every=40, scrub_interval_s=3600, fault_injection=True)
        d.update(self.extra)
        return NodeConfig.from_dict(d)

    async def start(self, name, addrs=None, join=False):
        n = StoreNode(self.cfg(name, addrs, join), fs=FaultInjectingFS(),
                      on_fatal=lambda e, name=name: self.fatal.__setitem__(name, e))
        await n.start()
        self.nodes[name] = n
        return n

    async def start_all(self):
        for name in self.initial:
            await self.start(name)

    async def leader(self, timeout=10):
        for _ in range(int(timeout / 0.02)):
            ls = [n for n in self.nodes.values() if not n.dead and n.raft.state == LEADER]
            if ls:
                return max(ls, key=lambda n: n.raft.term)
            await asyncio.sleep(0.02)
        raise AssertionError("no leader")

    def url(self, n):
        return "http://" + n.cfg.http_listen

    async def request(self, method, path, body=b"", tries=200):
        """Client with leader discovery + retry (like raftstore.client, but async)."""
        last = None
        for _ in range(tries):
            live = [n for n in self.nodes.values() if not n.dead]
            target = next((n for n in live if n.raft.state == LEADER), None) or random.choice(live)
            try:
                status, _, data = await http_request(self.url(target) + path, method, body, timeout=5)
            except (OSError, asyncio.TimeoutError) as e:
                last = e
                await asyncio.sleep(0.02)
                continue
            if status in (307, 503, 504):
                last = (status, data)
                await asyncio.sleep(0.02)
                continue
            return status, data
        raise AssertionError(f"request failed: {last}")

    async def converge(self, names=None, timeout=15):
        names = names or [k for k, n in self.nodes.items() if not n.dead]
        for _ in range(int(timeout / 0.05)):
            ns = [self.nodes[k] for k in names]
            commits = {n.raft.log.commit for n in ns}
            if len(commits) == 1 and all(n.applied == n.raft.log.commit for n in ns):
                return
            await asyncio.sleep(0.05)
        raise AssertionError({k: (self.nodes[k].raft.log.commit, self.nodes[k].applied) for k in names})

    async def stop(self):
        for n in self.nodes.values():
            if not n.dead:
                await n.stop()
            else:
                n.storage.close()
        await asyncio.sleep(0.05)


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, 120))


def corrupt_file(path, offset=10):
    with open(path, "r+b") as f:
        f.seek(offset)
        b = f.read(1)
        f.seek(offset)
        f.write(bytes([b[0] ^ 0xFF]))


def test_put_get_delete_list_and_redirects(tmp_path):
    async def main():
        c = Cluster(str(tmp_path))
        await c.start_all()
        try:
            leader = await c.leader()
            blob = os.urandom(50_000)
            st, body = await c.request("PUT", "/kv/photos/cat.jpg", blob)
            assert st == 201 and json.loads(body)["chunks"] == 13
            st, got = await c.request("GET", "/kv/photos/cat.jpg")
            assert st == 200 and got == blob
            follower = next(n for n in c.nodes.values() if n is not leader)
            st, _, body = await http_request(c.url(follower) + "/kv/photos/cat.jpg", "PUT", b"x")
            assert st == 307 and json.loads(body)["leader"] == leader.cfg.id
            await c.converge()
            st, _, got = await http_request(c.url(follower) + "/kv/photos/cat.jpg?consistency=stale")
            assert st == 200 and got == blob
            st, body = await c.request("GET", "/kv?prefix=photos/")
            assert [k["key"] for k in json.loads(body)["keys"]] == ["photos/cat.jpg"]
            st, body = await c.request("DELETE", "/kv/photos/cat.jpg")
            assert json.loads(body)["deleted"] is True
            st, _ = await c.request("GET", "/kv/photos/cat.jpg")
            assert st == 404
            st, body = await c.request("GET", "/status")
            assert json.loads(body)["voters"] == ["nas", "pi5", "server"]
        finally:
            await c.stop()
    run(main())


def test_scrubber_detects_bitrot_and_repairs_from_peer(tmp_path):
    async def main():
        c = Cluster(str(tmp_path))
        await c.start_all()
        try:
            leader = await c.leader()
            blob = os.urandom(30_000)
            await c.request("PUT", "/kv/doc", blob)
            await c.converge()
            victim = next(n for n in c.nodes.values() if n is not leader)
            h = victim.sm.get_manifest("doc")["chunks"][3]
            corrupt_file(victim.chunks.path(h))            # bit rot on the follower's disk
            assert victim.chunks.verify(h) == "corrupt"
            st = await victim.scrub_once()
            assert st["corrupt"] == 1 and st["repaired"] == 1 and st["unrepairable"] == 0
            assert victim.chunks.verify(h) == "ok"
            # read-path repair: rot on the leader is healed transparently by GET
            h2 = leader.sm.get_manifest("doc")["chunks"][0]
            corrupt_file(leader.chunks.path(h2), 0)
            s, got = await c.request("GET", "/kv/doc")
            assert s == 200 and got == blob and leader.repair_stats["corrupt_reads"] == 1
            assert os.listdir(os.path.join(leader.cfg.data_dir, "chunks", "quarantine"))
        finally:
            await c.stop()
    run(main())


def test_pull_leader_drive_mid_write_nothing_acknowledged_is_lost(tmp_path):
    async def main():
        c = Cluster(str(tmp_path))
        await c.start_all()
        acked: dict[str, bytes] = {}
        stop = asyncio.Event()

        async def writer(wid):
            i = 0
            while not stop.is_set():
                key, val = f"w{wid}-{i}", os.urandom(random.randint(10, 9000))
                try:
                    st, _ = await c.request("PUT", f"/kv/{key}", val, tries=400)
                    if st == 201:
                        acked[key] = val
                except AssertionError:
                    pass
                i += 1
        try:
            leader = await c.leader()
            tasks = [asyncio.ensure_future(writer(w)) for w in range(4)]
            await asyncio.sleep(0.6)
            leader.fs.pull(after_writes=7)               # the drive vanishes mid-write
            for _ in range(500):
                if leader.dead:
                    break
                await asyncio.sleep(0.01)
            assert leader.dead and leader.cfg.id in c.fatal, "leader did not fail-stop on EIO"
            n_at_pull = len(acked)
            new = await c.leader()
            assert new is not leader
            await asyncio.sleep(0.8)
            stop.set()
            await asyncio.gather(*tasks)
            assert len(acked) > n_at_pull, "no progress after the leader's drive died"
            for key, val in acked.items():
                st, got = await c.request("GET", f"/kv/{key}")
                assert st == 200 and got == val, f"acknowledged write {key} lost"
            # re-insert the drive: the old leader recovers from its torn WAL and catches up
            old = leader.cfg.id
            leader.storage.close()
            await asyncio.sleep(0.2)
            await c.start(old)
            await c.converge()
            node = c.nodes[old]
            for key, val in acked.items():
                assert await node.get_blob(key) == val
        finally:
            stop.set()
            await c.stop()
    run(main())


def test_replace_dead_node_rereplicates_everything(tmp_path):
    async def main():
        c = Cluster(str(tmp_path), snapshot_every=15)
        await c.start_all()
        try:
            leader = await c.leader()
            data = {f"f{i}": os.urandom(5000 + i) for i in range(12)}
            for k, v in data.items():
                await c.request("PUT", f"/kv/{k}", v)
            await c.converge()
            victim = next(n for n in c.nodes.values() if n is not leader)
            victim.fs.pull()                              # this drive is gone for good
            await c.request("PUT", "/kv/after-failure", b"still writable")
            for _ in range(200):
                if victim.dead:
                    break
                await asyncio.sleep(0.02)
            if not victim.dead:
                await victim.stop()
                victim.dead = True
            st, _ = await c.request("POST", "/admin/members", json.dumps({"op": "remove", "id": victim.cfg.id}).encode())
            assert st == 200
            p1, p2 = free_ports(2)
            addrs = {"raft": f"127.0.0.1:{p1}", "http": f"127.0.0.1:{p2}"}
            await c.start("replacement", addrs, join=True)
            st, _ = await c.request("POST", "/admin/members",
                                    json.dumps({"op": "add", "id": "replacement", **addrs}).encode())
            assert st == 200
            await c.converge([k for k in c.nodes if k != victim.cfg.id])
            new = c.nodes["replacement"]
            for _ in range(300):
                if not new.sm.missing_chunks():
                    break
                await asyncio.sleep(0.05)
            assert new.raft.log.snap_index > 0, "expected the replacement to be seeded by a snapshot"
            assert not new.sm.missing_chunks(), "chunks were not re-replicated to the replacement"
            for k, v in data.items():
                assert await new.get_blob(k) == v
                assert hashlib.sha256(await new.get_blob(k)).hexdigest() == new.sm.get_manifest(k)["sha256"]
            assert sorted(new.raft.voters) == sorted(set(c.initial) - {victim.cfg.id} | {"replacement"})
        finally:
            await c.stop()
    run(main())


def test_leadership_transfer_and_snapshot_admin(tmp_path):
    async def main():
        c = Cluster(str(tmp_path))
        await c.start_all()
        try:
            leader = await c.leader()
            await c.request("PUT", "/kv/a", b"1")
            target = next(k for k in c.nodes if k != leader.cfg.id)
            st, _ = await c.request("POST", "/admin/transfer", json.dumps({"target": target}).encode())
            assert st == 200
            for _ in range(200):
                if c.nodes[target].raft.state == LEADER:
                    break
                await asyncio.sleep(0.02)
            assert c.nodes[target].raft.state == LEADER
            st, body = await c.request("POST", "/admin/snapshot", b"{}")
            assert json.loads(body)["snapshot_index"] > 0
            st, got = await c.request("GET", "/kv/a")
            assert got == b"1"
        finally:
            await c.stop()
    run(main())


@pytest.mark.parametrize("mode", ["safe", "lease"])
def test_read_modes(tmp_path, mode):
    async def main():
        c = Cluster(str(tmp_path), read_mode=mode)
        await c.start_all()
        try:
            await c.leader()
            for i in range(10):
                await c.request("PUT", "/kv/k", str(i).encode())
                st, got = await c.request("GET", "/kv/k")
                assert got == str(i).encode()
        finally:
            await c.stop()
    run(main())


def _make_certs(d, names):
    import shutil
    import subprocess
    if not shutil.which("openssl"):
        pytest.skip("openssl CLI not available to create test certificates")
    run_ = lambda *a: subprocess.run(a, cwd=d, check=True, capture_output=True)  # noqa: E731
    ec = ["-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes"]
    # Python >= 3.13 sets VERIFY_X509_STRICT by default: the CA needs keyUsage and every leaf needs
    # an authority key identifier, otherwise every handshake fails and no leader is ever elected.
    run_("openssl", "req", "-x509", *ec, "-days", "2", "-subj", "/CN=test-ca",
         "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign",
         "-keyout", "ca.key", "-out", "ca.pem")
    for n in names:
        with open(os.path.join(d, f"{n}.ext"), "w") as f:
            f.write("basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\n"
                    "extendedKeyUsage=serverAuth,clientAuth\nsubjectKeyIdentifier=hash\n"
                    f"authorityKeyIdentifier=keyid\nsubjectAltName=DNS:{n},IP:127.0.0.1\n")
        run_("openssl", "req", *ec, "-subj", f"/CN={n}", "-keyout", f"{n}.key", "-out", f"{n}.csr")
        run_("openssl", "x509", "-req", "-in", f"{n}.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
             "-CAcreateserial", "-days", "2", "-extfile", f"{n}.ext", "-out", f"{n}.pem")


def test_mutual_tls_cluster(tmp_path):
    import ssl
    certs = tmp_path / "tls"
    certs.mkdir()
    names = ("nas", "pi5", "server")
    _make_certs(str(certs), names + ("client",))

    async def main():
        c = Cluster(str(tmp_path / "data"))
        for n in names:
            c.extra = {"tls": {"cert": str(certs / f"{n}.pem"), "key": str(certs / f"{n}.key"),
                               "ca": str(certs / "ca.pem")}}
            await c.start(n)
        try:
            leader = await c.leader()
            ctx = ssl.create_default_context(cafile=str(certs / "ca.pem"))
            ctx.check_hostname = False
            ctx.load_cert_chain(str(certs / "client.pem"), str(certs / "client.key"))
            url = "https://" + leader.cfg.http_listen
            st, _, _ = await http_request(url + "/kv/secret", "PUT", b"over tls", ssl_ctx=ctx)
            assert st == 201
            await c.converge()
            st, _, got = await http_request(url + "/kv/secret", ssl_ctx=ctx)
            assert st == 200 and got == b"over tls"
            with pytest.raises(Exception):        # no client certificate -> rejected
                plain = ssl.create_default_context(cafile=str(certs / "ca.pem"))
                plain.check_hostname = False
                await http_request(url + "/status", ssl_ctx=plain)
        finally:
            await c.stop()
    run(main())

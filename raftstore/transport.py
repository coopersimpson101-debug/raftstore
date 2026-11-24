"""asyncio TCP transport for Raft messages (optionally mutual TLS).

One outbound connection per peer with a bounded queue.  Raft tolerates loss,
so when a peer is slow or down the queue simply drops the oldest traffic
instead of applying back-pressure to the consensus loop.
"""
from __future__ import annotations

import asyncio
import logging
import ssl

from .codec import CodecError, decode_message, encode_message, frame, read_frame

log = logging.getLogger("raftstore.transport")


def split_addr(addr: str) -> tuple[str, int]:
    host, _, port = addr.rpartition(":")
    return host.strip("[]") or "0.0.0.0", int(port)


def make_ssl_contexts(tls: dict | None):
    """tls = {"cert": path, "key": path, "ca": path}; returns (server_ctx, client_ctx)."""
    if not tls:
        return None, None
    server = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=tls["ca"])
    server.load_cert_chain(tls["cert"], tls["key"])
    server.verify_mode = ssl.CERT_REQUIRED
    client = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=tls["ca"])
    client.load_cert_chain(tls["cert"], tls["key"])
    client.check_hostname = bool(tls.get("check_hostname", False))
    return server, client


class _Peer:
    def __init__(self, transport: "Transport", peer_id: str):
        self.t = transport
        self.id = peer_id
        self.q: asyncio.Queue = asyncio.Queue(maxsize=transport.queue_size)
        self.task = asyncio.ensure_future(self._run())
        self.dropped = 0

    def enqueue(self, body: bytes) -> None:
        if self.q.full():
            try:
                self.q.get_nowait()
                self.dropped += 1
            except asyncio.QueueEmpty:
                pass
        self.q.put_nowait(body)

    async def _run(self) -> None:
        backoff = 0.05
        while not self.t.closed:
            addr = self.t.addr_of(self.id)
            if addr is None:
                await asyncio.sleep(0.5)
                continue
            host, port = split_addr(addr)
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port, ssl=self.t.client_ssl), timeout=2.0)
            except (OSError, asyncio.TimeoutError, ssl.SSLError):
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 1.0)
                continue
            backoff = 0.05
            try:
                while not self.t.closed:
                    body = await self.q.get()
                    writer.write(frame(body))
                    if self.q.empty():
                        await writer.drain()
            except (OSError, ConnectionError, asyncio.CancelledError):
                pass
            finally:
                writer.close()
            if self.t.closed:
                return

    def close(self) -> None:
        self.task.cancel()


class Transport:
    def __init__(self, node_id: str, listen: str, addr_of, on_message, tls: dict | None = None,
                 queue_size: int = 4096):
        self.id = node_id
        self.listen = listen
        self.addr_of = addr_of
        self.on_message = on_message
        self.server_ssl, self.client_ssl = make_ssl_contexts(tls)
        self.queue_size = queue_size
        self.peers: dict[str, _Peer] = {}
        self.server = None
        self.closed = False
        self._conns: set = set()

    async def start(self) -> None:
        host, port = split_addr(self.listen)
        self.server = await asyncio.start_server(self._handle, host, port, ssl=self.server_ssl)

    async def _handle(self, reader, writer) -> None:
        self._conns.add(writer)
        try:
            while not self.closed:
                body = await read_frame(reader)
                m = decode_message(body)
                if m.dst == self.id:
                    self.on_message(m)
        except (asyncio.IncompleteReadError, ConnectionError, OSError, CodecError, ssl.SSLError):
            pass
        except asyncio.CancelledError:
            pass
        finally:
            self._conns.discard(writer)
            writer.close()

    def send(self, m) -> None:
        if self.closed or m.dst == self.id:
            return
        p = self.peers.get(m.dst)
        if p is None:
            p = self.peers[m.dst] = _Peer(self, m.dst)
        p.enqueue(encode_message(m))

    async def close(self) -> None:
        self.closed = True
        for p in self.peers.values():
            p.close()
        if self.server is not None:
            self.server.close()
        for w in list(self._conns):
            w.close()
        await asyncio.sleep(0)

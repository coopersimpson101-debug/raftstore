"""Minimal asyncio HTTP/1.1 server and client (stdlib only, Connection: close)."""
from __future__ import annotations

import asyncio
import json
import ssl
from dataclasses import dataclass, field
from urllib.parse import parse_qs, unquote, urlsplit

MAX_BODY = 1 << 30
REASONS = {200: "OK", 201: "Created", 204: "No Content", 307: "Temporary Redirect", 400: "Bad Request",
           403: "Forbidden", 404: "Not Found", 405: "Method Not Allowed", 409: "Conflict",
           413: "Payload Too Large", 500: "Internal Server Error", 503: "Service Unavailable",
           504: "Gateway Timeout"}


@dataclass
class Request:
    method: str
    path: str
    query: dict
    headers: dict
    body: bytes


@dataclass
class Response:
    status: int = 200
    body: bytes = b""
    headers: dict = field(default_factory=dict)

    @classmethod
    def json(cls, obj, status: int = 200, headers: dict | None = None) -> "Response":
        h = {"Content-Type": "application/json"}
        h.update(headers or {})
        return cls(status, (json.dumps(obj, indent=1, sort_keys=True) + "\n").encode(), h)


class HttpServer:
    def __init__(self, listen: str, handler, ssl_ctx: ssl.SSLContext | None = None):
        self.listen, self.handler, self.ssl_ctx = listen, handler, ssl_ctx
        self.server = None

    async def start(self) -> None:
        host, _, port = self.listen.rpartition(":")
        self.server = await asyncio.start_server(self._conn, host or "0.0.0.0", int(port), ssl=self.ssl_ctx)

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()

    async def _conn(self, reader, writer) -> None:
        try:
            line = await reader.readline()
            if not line:
                return
            method, target, _ = line.decode("latin-1").split(" ", 2)
            headers = {}
            while True:
                h = await reader.readline()
                if h in (b"\r\n", b"\n", b""):
                    break
                k, _, v = h.decode("latin-1").partition(":")
                headers[k.strip().lower()] = v.strip()
            n = int(headers.get("content-length", "0") or 0)
            if n > MAX_BODY:
                resp = Response.json({"error": "body too large"}, 413)
            else:
                body = await reader.readexactly(n) if n else b""
                u = urlsplit(target)
                q = {k: v[-1] for k, v in parse_qs(u.query).items()}
                try:
                    resp = await self.handler(Request(method.upper(), unquote(u.path), q, headers, body))
                except Exception as e:  # noqa: BLE001
                    resp = Response.json({"error": f"{type(e).__name__}: {e}"}, 500)
            head = [f"HTTP/1.1 {resp.status} {REASONS.get(resp.status, 'Status')}",
                    f"Content-Length: {len(resp.body)}", "Connection: close"]
            head += [f"{k}: {v}" for k, v in resp.headers.items()]
            writer.write(("\r\n".join(head) + "\r\n\r\n").encode("latin-1") + resp.body)
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError, ValueError, OSError, ssl.SSLError):
            pass
        finally:
            try:
                writer.close()
            except Exception:  # noqa: BLE001
                pass


async def http_request(url: str, method: str = "GET", body: bytes = b"", timeout: float = 5.0,
                       ssl_ctx: ssl.SSLContext | None = None) -> tuple[int, dict, bytes]:
    u = urlsplit(url)
    port = u.port or (443 if u.scheme == "https" else 80)
    ctx = ssl_ctx if u.scheme == "https" else None

    async def go():
        reader, writer = await asyncio.open_connection(u.hostname, port, ssl=ctx)
        try:
            path = u.path + (f"?{u.query}" if u.query else "")
            req = (f"{method} {path} HTTP/1.1\r\nHost: {u.hostname}:{port}\r\n"
                   f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode() + body
            writer.write(req)
            await writer.drain()
            status_line = await reader.readline()
            status = int(status_line.split()[1])
            headers = {}
            while True:
                h = await reader.readline()
                if h in (b"\r\n", b"\n", b""):
                    break
                k, _, v = h.decode("latin-1").partition(":")
                headers[k.strip().lower()] = v.strip()
            n = int(headers.get("content-length", "0"))
            data = await reader.readexactly(n) if n else b""
            return status, headers, data
        finally:
            writer.close()

    return await asyncio.wait_for(go(), timeout)

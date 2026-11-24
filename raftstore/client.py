"""Blocking HTTP client that finds the leader and fails over between nodes."""
from __future__ import annotations

import json
import ssl
import time
import urllib.error
import urllib.request


class ClientError(Exception):
    def __init__(self, status: int, body: bytes):
        super().__init__(f"HTTP {status}: {body[:300]!r}")
        self.status, self.body = status, body


class Client:
    def __init__(self, urls: list[str] | str, timeout: float = 10.0, retries: int = 20,
                 cafile: str | None = None, certfile: str | None = None, keyfile: str | None = None):
        self.ssl_ctx = None
        if cafile or certfile:
            self.ssl_ctx = ssl.create_default_context(cafile=cafile)
            self.ssl_ctx.check_hostname = False  # node certs are issued per node name, not per IP
            if certfile:
                self.ssl_ctx.load_cert_chain(certfile, keyfile)
        if isinstance(urls, str):
            urls = [u for u in urls.split(",") if u]
        self.urls = [u.rstrip("/") if "://" in u else "http://" + u.rstrip("/") for u in urls]
        self.timeout = timeout
        self.retries = retries
        self.preferred = self.urls[0]

    def _once(self, base: str, method: str, path: str, body: bytes | None):
        req = urllib.request.Request(base + path, data=body, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout, context=self.ssl_ctx) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def request(self, method: str, path: str, body: bytes | None = None, retry_unknown: bool = True):
        """Returns (status, body).  Follows leader redirects; retries on 503 / connection errors.

        Non-idempotent requests that time out (504) are *not* retried unless
        `retry_unknown` -- PUT/DELETE of a key are idempotent, so the default is to retry.
        """
        last_err = None
        candidates = [self.preferred] + [u for u in self.urls if u != self.preferred]
        i = 0
        for attempt in range(self.retries):
            base = candidates[i % len(candidates)]
            try:
                status, data = self._once(base, method, path, body)
            except (urllib.error.URLError, OSError, TimeoutError) as e:
                last_err = e
                i += 1
                time.sleep(min(0.05 * (attempt + 1), 0.5))
                continue
            if status == 307:
                try:
                    hint = json.loads(data).get("leader_http")
                except ValueError:
                    hint = None
                if hint:
                    self.preferred = hint.rstrip("/")
                    if self.preferred not in candidates:
                        candidates.insert(0, self.preferred)
                    i = candidates.index(self.preferred)
                else:
                    i += 1
                    time.sleep(min(0.05 * (attempt + 1), 0.5))  # election in progress
                continue
            if status == 503 or (status == 504 and retry_unknown):
                last_err = ClientError(status, data)
                i += 1
                time.sleep(min(0.05 * (attempt + 1), 0.5))
                continue
            self.preferred = base
            return status, data
        raise last_err or RuntimeError("no node reachable")

    # convenience wrappers
    def put(self, key: str, data: bytes) -> dict:
        s, b = self.request("PUT", f"/kv/{key}", data)
        if s != 201:
            raise ClientError(s, b)
        return json.loads(b)

    def get(self, key: str, consistency: str = "linearizable") -> bytes | None:
        s, b = self.request("GET", f"/kv/{key}?consistency={consistency}")
        if s == 404:
            return None
        if s != 200:
            raise ClientError(s, b)
        return b

    def delete(self, key: str) -> dict:
        s, b = self.request("DELETE", f"/kv/{key}")
        if s != 200:
            raise ClientError(s, b)
        return json.loads(b)

    def status(self, url: str | None = None) -> dict:
        base = url or self.preferred
        s, b = self._once(base if "://" in base else "http://" + base, "GET", "/status", None)
        return json.loads(b)

    def admin(self, path: str, obj: dict) -> dict:
        s, b = self.request("POST", path, json.dumps(obj).encode(), retry_unknown=False)
        if s != 200:
            raise ClientError(s, b)
        return json.loads(b)

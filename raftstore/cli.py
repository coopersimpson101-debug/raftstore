"""Command line interface: `raftstore serve | put | get | delete | ls | status | member | ...`."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys

from .client import Client, ClientError

DEFAULT_URLS = os.environ.get("RAFTSTORE_URLS", "http://127.0.0.1:8080")


def _client(args) -> Client:
    return Client(args.url, timeout=args.timeout, cafile=args.cacert, certfile=args.cert, keyfile=args.key)


def cmd_serve(args) -> int:
    from .config import NodeConfig
    from .node import serve
    cfg = NodeConfig.load(args.config)
    logging.basicConfig(level=getattr(logging, cfg.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(serve(cfg))
    except KeyboardInterrupt:
        pass
    return 0


def cmd_put(args) -> int:
    if args.file in (None, "-"):
        data = sys.stdin.buffer.read()
    else:
        with open(args.file, "rb") as f:
            data = f.read()
    print(json.dumps(_client(args).put(args.key, data), indent=1))
    return 0


def cmd_get(args) -> int:
    data = _client(args).get(args.key, args.consistency)
    if data is None:
        print(f"not found: {args.key}", file=sys.stderr)
        return 1
    if args.output and args.output != "-":
        with open(args.output, "wb") as f:
            f.write(data)
    else:
        sys.stdout.buffer.write(data)
    return 0


def cmd_delete(args) -> int:
    print(json.dumps(_client(args).delete(args.key)))
    return 0


def cmd_ls(args) -> int:
    c = _client(args)
    s, b = c.request("GET", f"/kv?prefix={args.prefix}")
    for k in json.loads(b)["keys"]:
        print(f"{k['size']:>12}  {k['sha256'][:12]}  {k['key']}")
    return 0


def cmd_status(args) -> int:
    c = _client(args)
    urls = c.urls if args.all else [c.urls[0]]
    for u in urls:
        try:
            st = c.status(u)
        except Exception as e:  # noqa: BLE001
            print(f"{u}: unreachable ({e})")
            continue
        if args.json:
            print(json.dumps(st, indent=1, sort_keys=True))
        else:
            print(f"{u}: id={st['id']} state={st['state']} term={st['term']} leader={st['leader']} "
                  f"commit={st['commit']} applied={st['applied']} keys={st['keys']} "
                  f"voters={','.join(st['voters'])}")
    return 0


def cmd_member(args) -> int:
    c = _client(args)
    if args.op == "add":
        if not (args.raft and args.http):
            print("member add needs --raft and --http", file=sys.stderr)
            return 2
        res = c.admin("/admin/members", {"op": "add", "id": args.id, "raft": args.raft, "http": args.http})
    else:
        res = c.admin("/admin/members", {"op": "remove", "id": args.id})
    print(json.dumps(res, indent=1))
    return 0


def cmd_simple_admin(path: str, body_fn=lambda a: {}):
    def run(args) -> int:
        print(json.dumps(_client(args).admin(path, body_fn(args)), indent=1))
        return 0
    return run


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="raftstore", description="Self-healing replicated storage on Raft")
    p.add_argument("--url", default=DEFAULT_URLS,
                   help="comma-separated node HTTP URLs (env RAFTSTORE_URLS); the leader is found automatically")
    p.add_argument("--timeout", type=float, default=30.0)
    p.add_argument("--cacert", default=os.environ.get("RAFTSTORE_CACERT"), help="CA bundle for https:// nodes")
    p.add_argument("--cert", default=os.environ.get("RAFTSTORE_CERT"), help="client certificate (mutual TLS)")
    p.add_argument("--key", default=os.environ.get("RAFTSTORE_KEY"), help="client private key (mutual TLS)")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="run a node")
    s.add_argument("--config", required=True)
    s.set_defaults(fn=cmd_serve)

    s = sub.add_parser("put", help="store a file (or stdin) under KEY")
    s.add_argument("key")
    s.add_argument("file", nargs="?")
    s.set_defaults(fn=cmd_put)

    s = sub.add_parser("get", help="fetch KEY")
    s.add_argument("key")
    s.add_argument("-o", "--output")
    s.add_argument("--consistency", choices=["linearizable", "stale"], default="linearizable")
    s.set_defaults(fn=cmd_get)

    s = sub.add_parser("delete", help="delete KEY")
    s.add_argument("key")
    s.set_defaults(fn=cmd_delete)

    s = sub.add_parser("ls", help="list keys")
    s.add_argument("--prefix", default="")
    s.set_defaults(fn=cmd_ls)

    s = sub.add_parser("status", help="cluster status")
    s.add_argument("--all", action="store_true", help="query every URL given in --url")
    s.add_argument("--json", action="store_true")
    s.set_defaults(fn=cmd_status)

    s = sub.add_parser("member", help="single-server membership change")
    s.add_argument("op", choices=["add", "remove"])
    s.add_argument("id")
    s.add_argument("--raft", help="raft address host:port of the new node")
    s.add_argument("--http", help="http address host:port of the new node")
    s.set_defaults(fn=cmd_member)

    s = sub.add_parser("transfer", help="transfer leadership")
    s.add_argument("target")
    s.set_defaults(fn=cmd_simple_admin("/admin/transfer", lambda a: {"target": a.target}))

    s = sub.add_parser("snapshot", help="force a snapshot + WAL compaction on the leader")
    s.set_defaults(fn=cmd_simple_admin("/admin/snapshot"))

    s = sub.add_parser("scrub", help="run the scrubber now on the node at the first --url")
    s.set_defaults(fn=cmd_simple_admin("/admin/scrub"))

    s = sub.add_parser("gc", help="garbage-collect unreferenced chunks")
    s.add_argument("--grace", type=int, default=1000, help="log entries an orphan chunk must age")
    s.set_defaults(fn=cmd_simple_admin("/admin/gc", lambda a: {"grace": a.grace}))
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.fn(args)
    except ClientError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

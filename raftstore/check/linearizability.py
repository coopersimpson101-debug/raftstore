"""Linearizability checker for key/value register histories.

Algorithm: Wing & Gong's backtracking search with Lowe's "just-in-time
linearization" memoisation (the same approach as Knossos' `wgl` and Porcupine).
The history is a doubly linked list of call/return events in real-time order.
We walk it, trying to linearize the earliest pending call; a return event that
is reached before its call was linearized forces a backtrack.  The cache of
(set-of-linearized-ops, model-state) pairs prunes equivalent branches, which
keeps typical histories polynomial.

Keys are independent registers, so the history is split per key and each part
is checked separately (P-compositionality, Herlihy & Wing 1990).

Operations whose outcome is unknown (client timed out, server crashed) are
recorded with `ret = math.inf`: they may take effect at any point after their
call, or not at all.
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class Op:
    id: int
    key: str
    kind: str            # "w" (write value) or "r" (read -> output)
    value: object = None
    call: float = 0.0
    ret: float = math.inf
    output: object = None
    client: int = 0
    meta: dict = field(default_factory=dict)

    def __repr__(self) -> str:
        r = "inf" if self.ret == math.inf else f"{self.ret:g}"
        what = f"w({self.value!r})" if self.kind == "w" else f"r()->{self.output!r}"
        return f"<op{self.id} {self.key} {what} [{self.call:g},{r}]>"


class _Ev:
    __slots__ = ("op", "idx", "is_call", "match", "prev", "next")

    def __init__(self, op=None, idx=-1, is_call=False):
        self.op, self.idx, self.is_call = op, idx, is_call
        self.match = self.prev = self.next = None


def _step(state, op: Op):
    if op.kind == "w":
        return True, op.value
    return state == op.output, state


@dataclass
class Result:
    ok: bool
    key: str | None = None
    ops: list | None = None
    explored: int = 0

    def __bool__(self) -> bool:
        return self.ok


def check_register(ops: list[Op], init=None, key: str | None = None, max_explored: int = 5_000_000) -> Result:
    if not ops:
        return Result(True, key)
    evs = []
    for i, op in enumerate(ops):
        if op.ret < op.call:
            raise ValueError(f"{op} returns before it is called")
        evs.append((op.call, 0, i))
        evs.append((op.ret, 1, i))
    evs.sort()  # at equal timestamps calls come first => treated as concurrent (never a false alarm)
    head = _Ev()
    tail = head
    calls: dict[int, _Ev] = {}
    for _, kind, i in evs:
        ev = _Ev(ops[i], i, kind == 0)
        if kind == 0:
            calls[i] = ev
        else:
            calls[i].match = ev
        ev.prev = tail
        tail.next = ev
        tail = ev

    def lift(ev):
        ev.prev.next = ev.next
        if ev.next is not None:
            ev.next.prev = ev.prev
        m = ev.match
        m.prev.next = m.next
        if m.next is not None:
            m.next.prev = m.prev

    def unlift(ev):
        m = ev.match
        m.prev.next = m
        if m.next is not None:
            m.next.prev = m
        ev.prev.next = ev
        if ev.next is not None:
            ev.next.prev = ev

    state = init
    linearized = 0
    cache: set = set()
    stack: list = []
    entry = head.next
    explored = 0
    while head.next is not None:
        explored += 1
        if explored > max_explored:
            raise RuntimeError(f"linearizability search budget exceeded for key {key!r}")
        if entry.is_call:
            ok, new_state = _step(state, entry.op)
            if ok:
                nl = linearized | (1 << entry.idx)
                k = (nl, new_state)
                if k not in cache:
                    cache.add(k)
                    stack.append((entry, state))
                    state, linearized = new_state, nl
                    lift(entry)
                    entry = head.next
                    continue
            entry = entry.next
        else:
            if not stack:
                return Result(False, key, ops, explored)
            entry, state = stack.pop()
            linearized &= ~(1 << entry.idx)
            unlift(entry)
            entry = entry.next
    return Result(True, key, None, explored)


def check_kv(ops: list[Op], init=None) -> Result:
    by_key: dict[str, list[Op]] = defaultdict(list)
    for op in ops:
        by_key[op.key].append(op)
    total = 0
    for k in sorted(by_key):
        r = check_register(by_key[k], init=init, key=k)
        total += r.explored
        if not r.ok:
            r.explored = total
            return r
    return Result(True, None, None, total)

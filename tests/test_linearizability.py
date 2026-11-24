"""The linearizability checker, validated against hand-made histories and a brute-force oracle."""
import itertools
import math
import random

from hypothesis import given, settings
from hypothesis import strategies as st

from raftstore.check.linearizability import Op, check_kv, check_register

INF = math.inf


def W(i, v, a, b, key="k"):
    return Op(i, key, "w", value=v, call=a, ret=b)


def R(i, out, a, b, key="k"):
    return Op(i, key, "r", output=out, call=a, ret=b)


def test_sequential_history_is_linearizable():
    assert check_register([W(0, 1, 0, 1), R(1, 1, 2, 3), W(2, 2, 4, 5), R(3, 2, 6, 7)])


def test_stale_read_is_not_linearizable():
    assert not check_register([W(0, 1, 0, 1), W(1, 2, 2, 3), R(2, 1, 4, 5)])


def test_concurrent_write_may_be_seen_either_way():
    assert check_register([W(0, 1, 0, 10), R(1, 1, 2, 3)])
    assert check_register([W(0, 1, 0, 10), R(1, None, 2, 3)])


def test_read_cannot_flip_back():
    # once a read observed the new value, a later read must not see the old one
    assert not check_register([W(0, 1, 0, 1), W(1, 2, 2, 20), R(2, 2, 3, 4), R(3, 1, 5, 6)])


def test_pending_write_may_or_may_not_take_effect():
    assert check_register([W(0, 1, 0, 1), W(1, 2, 2, INF), R(2, 1, 3, 4)])
    assert check_register([W(0, 1, 0, 1), W(1, 2, 2, INF), R(2, 2, 3, 4)])
    assert not check_register([W(0, 1, 0, 1), W(1, 2, 2, INF), R(2, 2, 3, 4), R(3, 1, 5, 6)])


def test_keys_are_checked_independently():
    ops = [W(0, 1, 0, 1, "a"), W(1, 9, 0, 1, "b"), R(2, 1, 2, 3, "a"), R(3, 1, 2, 3, "b")]
    r = check_kv(ops)
    assert not r.ok and r.key == "b"


def brute_force(ops, init=None):
    """Try every total order consistent with real time."""
    n = len(ops)
    for perm in itertools.permutations(range(n)):
        pos = {p: i for i, p in enumerate(perm)}
        if any(ops[a].ret < ops[b].call and pos[a] > pos[b] for a in range(n) for b in range(n)):
            continue
        state, ok = init, True
        for i in perm:
            o = ops[i]
            if o.kind == "w":
                state = o.value
            elif o.output != state:
                ok = False
                break
        if ok:
            return True
    return False


@st.composite
def histories(draw):
    n = draw(st.integers(1, 6))
    ops = []
    for i in range(n):
        a = draw(st.integers(0, 12))
        b = a + draw(st.integers(0, 6))
        if draw(st.booleans()):
            ops.append(W(i, draw(st.integers(0, 2)), a, b))
        else:
            ops.append(R(i, draw(st.sampled_from([None, 0, 1, 2])), a, b))
    return ops


@given(histories())
@settings(max_examples=1500, deadline=None)
def test_matches_brute_force_oracle(ops):
    assert check_register(ops).ok == brute_force(ops)


def test_large_linearizable_history_is_fast():
    rng = random.Random(5)
    ops, t, val = [], 0, None
    for i in range(3000):          # concurrent clients over a single register
        a = t + rng.random()
        b = a + rng.random() * 3
        if rng.random() < 0.5:
            val = i
            ops.append(W(i, i, a, b))
        else:
            ops.append(R(i, val, a, b))
        t = b
    r = check_register(ops)
    assert r.ok and r.explored < 200_000

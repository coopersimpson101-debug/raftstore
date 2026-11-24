"""Plain data types shared by the Raft core, the storage layer and the transport."""
from __future__ import annotations

import json
from dataclasses import dataclass

FOLLOWER = "follower"
PRE_CANDIDATE = "pre-candidate"
CANDIDATE = "candidate"
LEADER = "leader"

ENTRY_NORMAL = 0
ENTRY_CONFIG = 1
ENTRY_NOOP = 2

MSG_VOTE = "vote"
MSG_VOTE_RESP = "vote_resp"
MSG_PREVOTE = "prevote"
MSG_PREVOTE_RESP = "prevote_resp"
MSG_APPEND = "append"
MSG_APPEND_RESP = "append_resp"
MSG_SNAPSHOT = "snapshot"
MSG_TIMEOUT_NOW = "timeout_now"

ALL_MSG_TYPES = (
    MSG_VOTE, MSG_VOTE_RESP, MSG_PREVOTE, MSG_PREVOTE_RESP,
    MSG_APPEND, MSG_APPEND_RESP, MSG_SNAPSHOT, MSG_TIMEOUT_NOW,
)


@dataclass(frozen=True, slots=True)
class Entry:
    term: int
    index: int
    kind: int = ENTRY_NORMAL
    data: bytes = b""


@dataclass(frozen=True, slots=True)
class HardState:
    """State that must be durable before the node answers any RPC."""
    term: int = 0
    vote: str | None = None
    commit: int = 0


@dataclass(frozen=True, slots=True)
class Snapshot:
    index: int
    term: int
    config: dict  # node id -> opaque metadata (addresses); the voter set as of `index`
    data: bytes = b""


@dataclass(slots=True)
class Message:
    """One Raft RPC. Field meaning depends on `type`:

    vote / prevote:        index, log_term = candidate's last log index/term
    vote_resp / prevote:   reject = vote refused
    append:                index, log_term = prev log index/term; entries; commit; ctx
    append_resp:           reject; index = match index (success) or rejected prev index;
                           hint_index / hint_term = fast-backtracking conflict hint; ctx echo
    snapshot:              snapshot
    timeout_now:           (leadership transfer) campaign immediately
    """
    type: str
    src: str
    dst: str
    term: int
    index: int = 0
    log_term: int = 0
    entries: tuple = ()
    commit: int = 0
    reject: bool = False
    hint_index: int = 0
    hint_term: int = 0
    ctx: int = 0
    snapshot: Snapshot | None = None
    transfer: bool = False


@dataclass(frozen=True, slots=True)
class ReadState:
    """The leader confirmed it was leader when commit index was `index`.
    Serving a read once `applied >= index` is linearizable."""
    ctx: object
    index: int


@dataclass(slots=True)
class Ready:
    """Everything the runtime must do, *in this order*:

    1. persist `snapshot` (if any), `hard_state` (if any) and `entries` -- fsync if `must_sync`
    2. send `messages`
    3. apply `snapshot` to the state machine, then apply `committed` entries
    4. serve `read_states` once applied >= their index; fail `failed_reads`
    """
    hard_state: HardState | None
    entries: list
    snapshot: Snapshot | None
    messages: list
    committed: list
    read_states: list
    failed_reads: list
    must_sync: bool


class NotLeader(Exception):
    def __init__(self, leader: str | None):
        super().__init__(f"not leader (leader hint: {leader})")
        self.leader = leader


class ProposalDropped(Exception):
    pass


# ---- membership change records (stored as ENTRY_CONFIG data) -----------------

def encode_conf(cc: dict) -> bytes:
    return json.dumps(cc, sort_keys=True, separators=(",", ":")).encode()


def decode_conf(data: bytes) -> dict:
    return json.loads(data.decode())


def apply_conf(config: dict, cc: dict) -> dict:
    op = cc["op"]
    if op == "bootstrap":
        return dict(cc["voters"])
    if op == "add":
        new = dict(config)
        new[cc["id"]] = cc.get("meta", {})
        return new
    if op == "remove":
        new = dict(config)
        new.pop(cc["id"], None)
        return new
    raise ValueError(f"unknown conf change op {op!r}")

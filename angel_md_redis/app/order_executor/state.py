"""Execution state machine data (DECISION.md §7 E12). Serialisable to Redis JSON."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import List, Optional

RECEIVED = "RECEIVED"
WORKING = "WORKING"
PARTIAL = "PARTIAL"
CLOSING = "CLOSING"                 # cancel sent, waiting for the broker's ack

FILLED = "FILLED"
PARTIAL_FILL_TIMEOUT = "PARTIAL_FILL_TIMEOUT"
PARTIAL_FILL_STOPPED = "PARTIAL_FILL_STOPPED"
PARTIAL_KILLED = "PARTIAL_KILLED"
CANCELLED_TIMEOUT = "CANCELLED_TIMEOUT"
ABORTED_SLIPPAGE_LIMIT = "ABORTED_SLIPPAGE_LIMIT"
ABORTED_PRICE_BAND = "ABORTED_PRICE_BAND"
REJECTED_BY_BROKER = "REJECTED_BY_BROKER"
KILLED = "KILLED"
REJECTED_BEFORE_EXECUTION = "REJECTED_BEFORE_EXECUTION"

TERMINAL = frozenset({
    FILLED, PARTIAL_FILL_TIMEOUT, PARTIAL_FILL_STOPPED, PARTIAL_KILLED, CANCELLED_TIMEOUT,
    ABORTED_SLIPPAGE_LIMIT, ABORTED_PRICE_BAND, REJECTED_BY_BROKER, KILLED, REJECTED_BEFORE_EXECUTION,
})
# terminal states that leave a position (the journal opens it)
WITH_FILL = frozenset({FILLED, PARTIAL_FILL_TIMEOUT, PARTIAL_FILL_STOPPED, PARTIAL_KILLED})


@dataclass(frozen=True)
class OrderUpdate:
    """Broker view of one order. filled_qty / avg_price are CUMULATIVE for the order."""
    order_id: str
    status: str                     # OPEN / COMPLETE / CANCELLED / REJECTED
    filled_qty: float = 0.0
    avg_price: Optional[float] = None
    reason: str = ""


@dataclass
class ExecState:
    trade_id: str
    command: dict
    status: str = RECEIVED
    received_ms: int = 0
    started_ms: int = 0
    deadline_ms: int = 0
    reference: Optional[float] = None       # ask at the first snapshot (fixed)
    arrival_mid: Optional[float] = None
    initial_bid: Optional[float] = None
    initial_ask: Optional[float] = None
    cap: Optional[float] = None
    step: Optional[float] = None
    target_lots: int = 0
    limiting_factor: str = ""
    lot_limits: dict = field(default_factory=dict)
    reductions: List[str] = field(default_factory=list)
    fills: List[list] = field(default_factory=list)       # [price, units, ts_ms, order_id]
    orders: List[dict] = field(default_factory=list)      # every order sent (audit)
    working: Optional[dict] = None
    closing: str = ""                                     # why we are winding down
    block_reason: str = ""                                # last reason no order was working
    decision: dict = field(default_factory=dict)          # last continue/stop detail
    lpp_rejects: int = 0
    reject_reasons: List[str] = field(default_factory=list)
    finished_ms: int = 0
    report: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "ExecState":
        return ExecState(**{k: d[k] for k in ExecState.__dataclass_fields__ if k in d})

    @property
    def filled_units(self) -> float:
        return sum(f[1] for f in self.fills)

"""
app/stock_entry_exit.py
───────────────────────
Entry & Exit Based on Stock Bid-Ask — core logic.

ENTRY (all 5 must align on the same tick):
  1. spread_pct  < its own 20-period rolling average
  2. bid_top3 size exceeds ask_top3 size by > 20%
  3. last 5 trade-prints' net delta (signed by aggressor side) all positive
  4. no ask level within $0.10 of the current ask is a "wall"
     (> 3x that level's own 50-tick rolling average size)
  5. most recent trade print was classified as a BUY (hit the ask)

EXIT (any single condition triggers):
  1. ask_top3 size exceeds bid_top3 size by > 30% (imbalance flip)
  2. last 3 trade-prints' net delta all negative
  3. an ask wall (> 3x its 50-tick rolling avg) sits at/just above price
     (same detector as entry condition 4 — the state, not a fresh event)
  4. spread_pct > 1.5x its own 20-period rolling average
  5. a confirmed sell-side sweep hits the bid (>= 3 bid levels cleared by
     one trade, with a volume surge vs rolling avg trade size)

Signal-only: no position state is tracked here. exit_trigger reflects
whether exit CONDITIONS are currently true on the tape, not whether a
trade is open — the caller decides whether that's actionable.

No I/O here — pure dataclasses + functions/classes. Redis wiring lives in
run_stock_entry_exit.py.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

SPREAD_AVG_WINDOW = 20
SPREAD_EXIT_MULT = 1.5

BID_IMBALANCE_ENTRY_PCT = 20.0
ASK_IMBALANCE_EXIT_PCT = 30.0

NET_DELTA_ENTRY_TICKS = 5
NET_DELTA_EXIT_TICKS = 3

WALL_WINDOW = 50
WALL_MULTIPLIER = 3.0
WALL_PRICE_RANGE = 0.10

SWEEP_MIN_LEVELS = 3
SWEEP_VOL_MULT = 2.0
TICK_VOL_AVG_WINDOW = 100


@dataclass(frozen=True)
class DepthLevel:
    price: float
    qty: float


@dataclass(frozen=True)
class SpreadResult:
    raw_spread: float
    spread_pct: float
    mid: float


def compute_spread(bid: Optional[float], ask: Optional[float]) -> SpreadResult:
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return SpreadResult(0.0, 0.0, 0.0)
    mid = (bid + ask) / 2.0
    raw = ask - bid
    pct = (raw / mid) * 100.0 if mid > 0 else 0.0
    return SpreadResult(round(raw, 4), round(pct, 4), round(mid, 4))


def classify_trade(
    trade_price: Optional[float],
    prev_trade_price: Optional[float],
    bid: Optional[float],
    ask: Optional[float],
) -> str:
    """Quote rule (hit the ask = BUY, hit the bid = SELL), tick-rule fallback."""
    if trade_price is None or trade_price <= 0:
        return "NONE"
    if ask and trade_price >= ask:
        return "BUY"
    if bid and trade_price <= bid:
        return "SELL"
    if prev_trade_price is not None:
        if trade_price > prev_trade_price:
            return "BUY"
        if trade_price < prev_trade_price:
            return "SELL"
    return "NONE"


@dataclass
class RollingStat:
    window: int
    _buf: Deque[float] = field(default_factory=deque, repr=False)

    def __post_init__(self) -> None:
        self._buf = deque(maxlen=max(1, self.window))

    def push(self, v: float) -> None:
        if v and v > 0:
            self._buf.append(v)

    @property
    def avg(self) -> Optional[float]:
        if not self._buf:
            return None
        return sum(self._buf) / len(self._buf)


@dataclass(frozen=True)
class EntryExitResult:
    spread_pct: float
    spread_avg: Optional[float]
    spread_ratio: Optional[float]
    bid_top3: float
    ask_top3: float
    bid_imbalance_pct: Optional[float]
    ask_imbalance_pct: Optional[float]
    net_delta_recent: List[float]
    last_print_side: str
    ask_wall_near: bool
    sweep_sell_confirmed: bool

    entry_conditions: Dict[str, bool]
    entry_trigger: bool

    exit_conditions: Dict[str, bool]
    exit_trigger: bool
    exit_reasons: List[str]


class StockEntryExitDetector:
    """Per-symbol stateful tracker."""

    def __init__(self):
        self.spread_avg = RollingStat(SPREAD_AVG_WINDOW)
        self.wall_trackers: Dict[int, RollingStat] = {}
        self.delta_hist: Deque[float] = deque(maxlen=NET_DELTA_ENTRY_TICKS)
        self.last_print_side = "NONE"
        self.avg_tick_vol = RollingStat(TICK_VOL_AVG_WINDOW)

        self._prev_trade_price: Optional[float] = None
        self._prev_bid_levels: List[DepthLevel] = []

    def analyze(
        self,
        trade_price: Optional[float],
        trade_qty: float,
        bid: Optional[float],
        ask: Optional[float],
        bid_levels: List[DepthLevel],
        ask_levels: List[DepthLevel],
    ) -> EntryExitResult:
        sr = compute_spread(bid, ask)
        spread_avg = self.spread_avg.avg
        spread_ratio = (sr.spread_pct / spread_avg) if spread_avg else None
        self.spread_avg.push(sr.spread_pct)

        bid_top3 = sum(lv.qty for lv in bid_levels[:3])
        ask_top3 = sum(lv.qty for lv in ask_levels[:3])
        bid_imbalance_pct = ((bid_top3 - ask_top3) / ask_top3 * 100.0) if ask_top3 > 0 else None
        ask_imbalance_pct = ((ask_top3 - bid_top3) / bid_top3 * 100.0) if bid_top3 > 0 else None

        # Wall check: any ask level within WALL_PRICE_RANGE of the current
        # ask whose size exceeds WALL_MULTIPLIER x ITS OWN 50-tick rolling
        # average. Trackers are updated for every visible level regardless
        # of range, so each level's own history stays accurate.
        ask_wall_near = False
        ref = ask or (trade_price or 0.0)
        for i, lv in enumerate(ask_levels[:5]):
            tr = self.wall_trackers.setdefault(i, RollingStat(WALL_WINDOW))
            avg = tr.avg
            if (
                avg
                and lv.qty > WALL_MULTIPLIER * avg
                and ref > 0
                and lv.price > 0
                and ref <= lv.price <= ref + WALL_PRICE_RANGE
            ):
                ask_wall_near = True
            tr.push(lv.qty)

        side = classify_trade(trade_price, self._prev_trade_price, bid, ask)
        if side in ("BUY", "SELL") and trade_qty > 0:
            signed = trade_qty if side == "BUY" else -trade_qty
            self.delta_hist.append(signed)
            self.last_print_side = side
        if trade_price:
            self._prev_trade_price = trade_price

        recent = list(self.delta_hist)
        entry_delta_ok = len(recent) >= NET_DELTA_ENTRY_TICKS and all(
            x > 0 for x in recent[-NET_DELTA_ENTRY_TICKS:]
        )
        exit_delta_ok = len(recent) >= NET_DELTA_EXIT_TICKS and all(
            x < 0 for x in recent[-NET_DELTA_EXIT_TICKS:]
        )

        # Sweep into the bid: this trade cleared >= SWEEP_MIN_LEVELS of the
        # PREVIOUS tick's bid ladder, confirmed by a volume surge.
        avg_vol = self.avg_tick_vol.avg
        sweep_sell_confirmed = False
        if trade_price is not None and self._prev_bid_levels:
            crossed = sum(1 for lv in self._prev_bid_levels if lv.price > 0 and lv.price >= trade_price)
            if crossed >= SWEEP_MIN_LEVELS:
                sweep_sell_confirmed = bool(avg_vol) and trade_qty > SWEEP_VOL_MULT * avg_vol
        self.avg_tick_vol.push(trade_qty or 0.0)
        self._prev_bid_levels = bid_levels

        entry_conditions = {
            "spread_below_avg": spread_avg is not None and sr.spread_pct < spread_avg,
            "bid_imbalance": bid_imbalance_pct is not None and bid_imbalance_pct > BID_IMBALANCE_ENTRY_PCT,
            "net_delta_positive": entry_delta_ok,
            "no_ask_wall": not ask_wall_near,
            "last_print_buy": self.last_print_side == "BUY",
        }
        entry_trigger = all(entry_conditions.values())

        exit_conditions = {
            "ask_imbalance_flip": ask_imbalance_pct is not None and ask_imbalance_pct > ASK_IMBALANCE_EXIT_PCT,
            "net_delta_negative": exit_delta_ok,
            "ask_wall_appeared": ask_wall_near,
            "spread_spike": bool(spread_avg) and sr.spread_pct > SPREAD_EXIT_MULT * spread_avg,
            "bid_sweep": sweep_sell_confirmed,
        }
        exit_trigger = any(exit_conditions.values())
        exit_reasons = [k for k, v in exit_conditions.items() if v]

        return EntryExitResult(
            spread_pct=sr.spread_pct,
            spread_avg=spread_avg,
            spread_ratio=spread_ratio,
            bid_top3=bid_top3,
            ask_top3=ask_top3,
            bid_imbalance_pct=bid_imbalance_pct,
            ask_imbalance_pct=ask_imbalance_pct,
            net_delta_recent=recent,
            last_print_side=self.last_print_side,
            ask_wall_near=ask_wall_near,
            sweep_sell_confirmed=sweep_sell_confirmed,
            entry_conditions=entry_conditions,
            entry_trigger=entry_trigger,
            exit_conditions=exit_conditions,
            exit_trigger=exit_trigger,
            exit_reasons=exit_reasons,
        )

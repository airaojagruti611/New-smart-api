"""
app/order_flow.py
───────────────────────
Direction + Support & Resistance from Bid-Ask — core logic.

Direction:
  Classify each trade as hitting the bid or the ask (quote rule, falling
  back to the tick rule when price sits between bid/ask), accumulate
  buy/sell pressure over a rolling window, and derive a directional bias
  from whether net delta is positive AND rising (UP) or negative AND
  falling (DOWN).

Support / Resistance:
  A resting bid/ask level whose size exceeds 2x the average size across
  the visible levels (this snapshot) is flagged as support/resistance.
  Levels are tracked across ticks by price: a level that keeps
  reappearing accumulates a "streak"; one that vanishes after having
  held emits a DISAPPEARED event (bearish for support, bullish for
  resistance) — and one that gets tested by price and holds emits a
  HELD event (the SPY $510.50 example in the brief).

Refresh rate:
  Per (side, level_index), track the time between successive size
  changes at that slot. Faster-than-its-own-rolling-average = active
  defense; slower = passive/low conviction.

No I/O here — pure dataclasses + functions/classes. Redis wiring lives
in run_order_flow.py.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

# ── Config defaults ──────────────────────────────────────────────────────
DIRECTION_WINDOW = 50

SR_MULTIPLIER = 2.0

LEVEL_MIN_STREAK_FOR_EVENT = 2      # a level must have held >=N ticks before
                                     # its disappearance/hold is worth flagging
LEVEL_TOUCH_TOL = 0.0                # price within this distance = "touched"
LEVEL_HOLD_CONFIRM_DIST_MULT = 3.0   # must move this many x tol away to confirm a "hold"

REFRESH_WINDOW = 50
REFRESH_FAST_MULT = 0.7             # interval < 0.7x avg -> ACTIVE_DEFENSE
REFRESH_SLOW_MULT = 1.5             # interval > 1.5x avg -> PASSIVE


@dataclass(frozen=True)
class DepthLevel:
    price: float
    qty: float


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


# ── Trade classification (quote rule + tick-rule fallback) ────────────────

def classify_trade(
    trade_price: Optional[float],
    prev_trade_price: Optional[float],
    bid: Optional[float],
    ask: Optional[float],
) -> str:
    """
    Buy pressure  = trades that hit the ask (aggressive buyer lifting offers)
    Sell pressure = trades that hit the bid (aggressive seller hitting bids)
    Falls back to the tick rule (price vs previous print) when the trade
    prints inside the spread and can't be quote-classified.
    """
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


@dataclass(frozen=True)
class DirectionResult:
    buy_pressure: float
    sell_pressure: float
    net_delta: float
    prev_net_delta: Optional[float]
    bias: str  # "UP" / "DOWN" / "NEUTRAL"


class DirectionTracker:
    """Rolling buy/sell pressure -> net delta -> directional bias."""

    def __init__(self, window: int = DIRECTION_WINDOW):
        self._buf: Deque[Tuple[float, float]] = deque(maxlen=window)
        self._last_net_delta: Optional[float] = None

    def update(self, buy_vol: float, sell_vol: float) -> DirectionResult:
        self._buf.append((buy_vol, sell_vol))
        buy_sum = sum(b for b, _ in self._buf)
        sell_sum = sum(s for _, s in self._buf)
        net = buy_sum - sell_sum

        prev = self._last_net_delta
        if prev is None:
            bias = "NEUTRAL"
        elif net > 0 and net > prev:
            bias = "UP"
        elif net < 0 and net < prev:
            bias = "DOWN"
        else:
            bias = "NEUTRAL"

        self._last_net_delta = net
        return DirectionResult(
            buy_pressure=buy_sum, sell_pressure=sell_sum,
            net_delta=net, prev_net_delta=prev, bias=bias,
        )


# ── Support / Resistance from resting depth ────────────────────────────────

@dataclass(frozen=True)
class SRLevel:
    price: float
    side: str  # "SUPPORT" / "RESISTANCE"
    size: float
    ratio: float  # size / avg_level_size this snapshot


def detect_sr_levels(levels: List[DepthLevel], side: str, multiplier: float = SR_MULTIPLIER) -> List[SRLevel]:
    """
    A level is support/resistance if its resting size exceeds `multiplier` x
    the average size across the currently visible levels (this snapshot).
    """
    sizes = [lv.qty for lv in levels if lv.qty]
    if not sizes:
        return []
    avg = sum(sizes) / len(sizes)
    if avg <= 0:
        return []
    out = []
    for lv in levels:
        if lv.qty > multiplier * avg:
            out.append(SRLevel(price=lv.price, side=side, size=lv.qty, ratio=round(lv.qty / avg, 2)))
    return out


class LevelTracker:
    """
    Tracks SR levels across ticks by price (per side). Emits:
      DISAPPEARED - a level that had held for >= LEVEL_MIN_STREAK_FOR_EVENT
                     ticks vanished (size collapsed / dropped out of depth)
                     -> bearish if SUPPORT, bullish if RESISTANCE
      HELD        - price touched within LEVEL_TOUCH_TOL of the level and
                     then moved back away without breaking it
                     -> the SPY $510.50 "bid wall held" example
    """

    def __init__(
        self,
        touch_tol: float = LEVEL_TOUCH_TOL,
        min_streak: int = LEVEL_MIN_STREAK_FOR_EVENT,
        hold_confirm_mult: float = LEVEL_HOLD_CONFIRM_DIST_MULT,
    ):
        self.touch_tol = touch_tol
        self.min_streak = min_streak
        self.hold_confirm_mult = hold_confirm_mult
        self._levels: Dict[float, dict] = {}

    def update(
        self,
        side: str,
        current_levels: List[SRLevel],
        ts_ms: int,
        trade_price: Optional[float] = None,
    ) -> List[dict]:
        events: List[dict] = []
        seen = set()

        for lv in current_levels:
            price_key = round(lv.price, 2)
            seen.add(price_key)
            st = self._levels.get(price_key)
            if st is None:
                self._levels[price_key] = {
                    "size": lv.size, "streak": 1, "first_ts": ts_ms,
                    "last_ts": ts_ms, "touched": False,
                }
            else:
                st["streak"] += 1
                st["size"] = lv.size
                st["last_ts"] = ts_ms

            # Touch/hold check: did price approach this level and bounce off it?
            st = self._levels[price_key]
            tol = max(self.touch_tol, price_key * 0.0005)  # small relative tolerance
            if trade_price is not None:
                near = abs(trade_price - price_key) <= tol
                away = abs(trade_price - price_key) > tol * self.hold_confirm_mult
                broke_through = (
                    (side == "SUPPORT" and trade_price < price_key - tol * self.hold_confirm_mult)
                    or (side == "RESISTANCE" and trade_price > price_key + tol * self.hold_confirm_mult)
                )
                if near:
                    st["touched"] = True
                elif st.get("touched") and away and not broke_through and st["streak"] >= self.min_streak:
                    events.append({
                        "event": "HELD", "side": side, "price": price_key,
                        "size": st["size"], "streak": st["streak"],
                    })
                    st["touched"] = False
                elif broke_through:
                    st["touched"] = False

        # Disappearance: previously tracked levels not seen this tick.
        for price_key in list(self._levels.keys()):
            if price_key in seen:
                continue
            st = self._levels.pop(price_key)
            if st["streak"] >= self.min_streak:
                events.append({
                    "event": "DISAPPEARED", "side": side, "price": price_key,
                    "size": st["size"], "streak": st["streak"],
                })

        return events


# ── Refresh rate ────────────────────────────────────────────────────────

class RefreshTracker:
    """
    Per (side, level_index): time between successive size changes at that
    slot, compared against its own rolling average interval.
    """

    def __init__(self, window: int = REFRESH_WINDOW):
        self.window = window
        self._last_qty: Dict[Tuple[str, int], float] = {}
        self._last_ts: Dict[Tuple[str, int], int] = {}
        self._interval_avg: Dict[Tuple[str, int], RollingStat] = {}

    def update(self, side: str, level_idx: int, qty: float, ts_ms: int) -> Optional[dict]:
        key = (side, level_idx)
        last_qty = self._last_qty.get(key)
        last_ts = self._last_ts.get(key)
        result = None

        if last_qty is not None and last_ts is not None and qty != last_qty:
            interval = ts_ms - last_ts
            stat = self._interval_avg.setdefault(key, RollingStat(self.window))
            avg = stat.avg
            classification = "NORMAL"
            if avg:
                if interval < REFRESH_FAST_MULT * avg:
                    classification = "ACTIVE_DEFENSE"
                elif interval > REFRESH_SLOW_MULT * avg:
                    classification = "PASSIVE"
            stat.push(interval)
            result = {
                "side": side, "level": level_idx, "interval_ms": interval,
                "avg_interval_ms": round(avg, 0) if avg else None,
                "classification": classification,
            }

        self._last_qty[key] = qty
        self._last_ts[key] = ts_ms
        return result


# ── Composite ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class OrderFlowSignal:
    direction: DirectionResult
    supports: List[SRLevel]
    resistances: List[SRLevel]
    support_events: List[dict]
    resistance_events: List[dict]
    refresh_events: List[dict]


class OrderFlowDetector:
    """Per-symbol (or per-contract) stateful composite: direction + S/R + refresh."""

    def __init__(self, direction_window: int = DIRECTION_WINDOW):
        self.direction = DirectionTracker(direction_window)
        self.support_tracker = LevelTracker()
        self.resistance_tracker = LevelTracker()
        self.refresh = RefreshTracker()
        self._prev_trade_price: Optional[float] = None

    def analyze(
        self,
        ts_ms: int,
        trade_price: Optional[float],
        trade_qty: float,
        bid: Optional[float],
        ask: Optional[float],
        bid_levels: List[DepthLevel],
        ask_levels: List[DepthLevel],
    ) -> OrderFlowSignal:
        side = classify_trade(trade_price, self._prev_trade_price, bid, ask)
        buy_vol = trade_qty if side == "BUY" else 0.0
        sell_vol = trade_qty if side == "SELL" else 0.0
        direction = self.direction.update(buy_vol, sell_vol)
        if trade_price:
            self._prev_trade_price = trade_price

        supports = detect_sr_levels(bid_levels, "SUPPORT")
        resistances = detect_sr_levels(ask_levels, "RESISTANCE")

        support_events = self.support_tracker.update("SUPPORT", supports, ts_ms, trade_price)
        resistance_events = self.resistance_tracker.update("RESISTANCE", resistances, ts_ms, trade_price)

        refresh_events: List[dict] = []
        for i, lv in enumerate(bid_levels):
            ev = self.refresh.update("BID", i, lv.qty, ts_ms)
            if ev:
                refresh_events.append(ev)
        for i, lv in enumerate(ask_levels):
            ev = self.refresh.update("ASK", i, lv.qty, ts_ms)
            if ev:
                refresh_events.append(ev)

        return OrderFlowSignal(
            direction=direction,
            supports=supports,
            resistances=resistances,
            support_events=support_events,
            resistance_events=resistance_events,
            refresh_events=refresh_events,
        )

"""
app/smart_money.py
───────────────────────
Smart Money Entry Detection — core logic.

A. Size anomaly (wall/iceberg)  — a level's size > 3x its own 50-tick rolling avg
B. Absorption                   — a wall holds (size doesn't deplete) while
                                   price fails to move through it despite
                                   meaningful volume trading against it;
                                   later resolved as broken / absorbed / expired
C. Sweep                        — a single trade clears >=3 levels of the
                                   PRIOR ask/bid ladder in one tick, confirmed
                                   by a volume surge vs rolling avg tick volume
D. Time-and-sales clustering    — >=2 round-lot prints at the same price
                                   within 200ms (algorithmic accumulation signature)

No I/O here — pure dataclasses + functions/classes, unit-testable in
isolation. Redis wiring lives in run_smart_money.py.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

# ── Config defaults (overridable via constructor args) ─────────────────────
ANOMALY_WINDOW = 50
ANOMALY_MULTIPLIER = 3.0

ABSORPTION_WINDOW = 30
ABSORPTION_MIN_TICKS = 15
ABSORPTION_PRICE_TOL = 0.0            # price must not move against the wall beyond this
ABSORPTION_SIZE_HOLD_RATIO = 0.8      # wall must stay >= 80% of its starting size to flag
ABSORPTION_DEPLETE_RATIO = 0.5        # wall dropping below 50% of start = meaningfully depleted
ABSORPTION_VOL_TRIGGER_RATIO = 0.5    # volume traded through must be >= 50% of wall size
ABSORPTION_BREAK_VOL_MULT = 2.0       # tick-volume surge multiplier confirming a break
ABSORPTION_TIMEOUT_TICKS = 100

SWEEP_MIN_LEVELS = 3                  # trade must clear >= this many prior levels
SWEEP_VOL_MULT = 2.0                  # tick volume vs rolling avg tick volume to "confirm"

ROUND_LOTS = frozenset({500, 1000, 2500})
CLUSTER_WINDOW_MS = 200
CLUSTER_MIN_PRINTS = 2


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

    @property
    def n(self) -> int:
        return len(self._buf)


# ── A. Size anomaly (wall / iceberg) ────────────────────────────────────────

class SizeAnomalyDetector:
    """
    Per-level rolling average of size (last ANOMALY_WINDOW ticks). Flags a
    "wall" when a level's current size is > ANOMALY_MULTIPLIER x its own
    rolling average.
    """

    def __init__(self, window: int = ANOMALY_WINDOW, multiplier: float = ANOMALY_MULTIPLIER):
        self.window = window
        self.multiplier = multiplier
        self._bid_stats: Dict[int, RollingStat] = {}
        self._ask_stats: Dict[int, RollingStat] = {}

    def _check_side(self, stats: Dict[int, RollingStat], sizes: List[float]) -> List[dict]:
        alerts: List[dict] = []
        for i, sz in enumerate(sizes):
            st = stats.setdefault(i, RollingStat(self.window))
            avg = st.avg
            if avg and sz > self.multiplier * avg and st.n >= max(10, self.window // 5):
                alerts.append({"level": i, "size": sz, "avg": round(avg, 1), "ratio": round(sz / avg, 2)})
            st.push(sz)
        return alerts

    def update(self, bid_sizes: List[float], ask_sizes: List[float]) -> Tuple[List[dict], List[dict]]:
        return (
            self._check_side(self._bid_stats, bid_sizes),
            self._check_side(self._ask_stats, ask_sizes),
        )


# ── B. Absorption ────────────────────────────────────────────────────────

@dataclass
class AbsorptionState:
    status: str = "NONE"   # NONE / TESTING / BROKEN_BULLISH / BROKEN_BEARISH / ABSORBED_NEUTRAL / EXPIRED
    side: str = ""         # "ASK" / "BID" / ""
    wall_price: float = 0.0
    wall_size: float = 0.0
    vol_against: float = 0.0
    ticks: int = 0


class AbsorptionTracker:
    """
    ASK absorption: price hasn't risen, wall size held, meaningful volume
    traded through it -> resistance test / distribution.
    BID absorption: symmetric -> support test / accumulation.

    Once flagged, tracks resolution on subsequent ticks:
      - price breaks the wall level WITH a volume surge -> BROKEN_BULLISH / BROKEN_BEARISH
      - wall collapses (< ABSORPTION_DEPLETE_RATIO of start) without a clean break -> ABSORBED_NEUTRAL
      - no resolution within ABSORPTION_TIMEOUT_TICKS -> EXPIRED
    """

    def __init__(
        self,
        window: int = ABSORPTION_WINDOW,
        min_ticks: int = ABSORPTION_MIN_TICKS,
        price_tol: float = ABSORPTION_PRICE_TOL,
        size_hold_ratio: float = ABSORPTION_SIZE_HOLD_RATIO,
        deplete_ratio: float = ABSORPTION_DEPLETE_RATIO,
        vol_trigger_ratio: float = ABSORPTION_VOL_TRIGGER_RATIO,
        break_vol_mult: float = ABSORPTION_BREAK_VOL_MULT,
        timeout_ticks: int = ABSORPTION_TIMEOUT_TICKS,
    ):
        self.window = window
        self.min_ticks = min_ticks
        self.price_tol = price_tol
        self.size_hold_ratio = size_hold_ratio
        self.deplete_ratio = deplete_ratio
        self.vol_trigger_ratio = vol_trigger_ratio
        self.break_vol_mult = break_vol_mult
        self.timeout_ticks = timeout_ticks

        self._buf: Deque[dict] = deque(maxlen=window)
        self._active: Optional[AbsorptionState] = None
        self._avg_tick_vol = RollingStat(window=100)

    def _try_flag(self) -> Optional[AbsorptionState]:
        if len(self._buf) < self.min_ticks:
            return None

        start, end = self._buf[0], self._buf[-1]
        vol_sum = sum(x["tick_vol"] for x in self._buf)

        if start["ask_size"] > 0:
            price_rise = end["ask_price"] - start["ask_price"]
            size_ratio = end["ask_size"] / start["ask_size"]
            vol_vs_size = vol_sum / start["ask_size"]
            if (
                price_rise <= self.price_tol
                and size_ratio >= self.size_hold_ratio
                and vol_vs_size >= self.vol_trigger_ratio
            ):
                return AbsorptionState(
                    status="TESTING", side="ASK",
                    wall_price=start["ask_price"], wall_size=start["ask_size"],
                    vol_against=vol_sum, ticks=len(self._buf),
                )

        if start["bid_size"] > 0:
            price_fall = start["bid_price"] - end["bid_price"]
            size_ratio = end["bid_size"] / start["bid_size"]
            vol_vs_size = vol_sum / start["bid_size"]
            if (
                price_fall <= self.price_tol
                and size_ratio >= self.size_hold_ratio
                and vol_vs_size >= self.vol_trigger_ratio
            ):
                return AbsorptionState(
                    status="TESTING", side="BID",
                    wall_price=start["bid_price"], wall_size=start["bid_size"],
                    vol_against=vol_sum, ticks=len(self._buf),
                )
        return None

    def update(
        self,
        bid_price: float, bid_size: float,
        ask_price: float, ask_size: float,
        tick_vol: float,
    ) -> AbsorptionState:
        self._buf.append({
            "bid_price": bid_price, "bid_size": bid_size,
            "ask_price": ask_price, "ask_size": ask_size,
            "tick_vol": tick_vol,
        })

        if self._active is None:
            flagged = self._try_flag()
            self._active = flagged
            self._avg_tick_vol.push(tick_vol)
            return self._active or AbsorptionState()

        avg_vol = self._avg_tick_vol.avg
        self._avg_tick_vol.push(tick_vol)

        a = self._active
        a.ticks += 1
        a.vol_against += tick_vol
        surge = bool(avg_vol) and tick_vol > self.break_vol_mult * avg_vol

        if a.side == "ASK":
            if ask_price > a.wall_price and surge:
                a.status = "BROKEN_BULLISH"
                self._active = None
                return a
            if ask_size < self.deplete_ratio * a.wall_size:
                a.status = "ABSORBED_NEUTRAL"
                self._active = None
                return a
        else:  # BID
            if bid_price < a.wall_price and surge:
                a.status = "BROKEN_BEARISH"
                self._active = None
                return a
            if bid_size < self.deplete_ratio * a.wall_size:
                a.status = "ABSORBED_NEUTRAL"
                self._active = None
                return a

        if a.ticks >= self.timeout_ticks:
            a.status = "EXPIRED"
            self._active = None
            return a

        a.status = "TESTING"
        return a


# ── C. Sweep detection ──────────────────────────────────────────────────

def detect_sweep(
    prev_ask_levels: List[DepthLevel],
    prev_bid_levels: List[DepthLevel],
    trade_price: Optional[float],
    tick_vol: float,
    avg_tick_vol: Optional[float],
    min_levels: int = SWEEP_MIN_LEVELS,
    vol_mult: float = SWEEP_VOL_MULT,
) -> Tuple[str, int, bool]:
    """
    A single trade price crosses >= min_levels of the PREVIOUS tick's ask
    (buy sweep) or bid (sell sweep) ladder in one step.
    Returns (signal, levels_crossed, volume_confirmed).
    """
    if trade_price is None:
        return "NONE", 0, False

    ask_crossed = sum(1 for lv in prev_ask_levels if lv.price > 0 and lv.price <= trade_price)
    if ask_crossed >= min_levels:
        confirmed = bool(avg_tick_vol) and tick_vol > vol_mult * avg_tick_vol
        return "SWEEP_BUY", ask_crossed, confirmed

    bid_crossed = sum(1 for lv in prev_bid_levels if lv.price > 0 and lv.price >= trade_price)
    if bid_crossed >= min_levels:
        confirmed = bool(avg_tick_vol) and tick_vol > vol_mult * avg_tick_vol
        return "SWEEP_SELL", bid_crossed, confirmed

    return "NONE", 0, False


# ── D. Time-and-sales clustering ────────────────────────────────────────

def is_round_lot(qty: float) -> bool:
    q = int(round(qty))
    if q <= 0:
        return False
    return q in ROUND_LOTS or (q % 500 == 0)


class TimeSalesCluster:
    """
    Multiple round-lot prints at the SAME price within CLUSTER_WINDOW_MS of
    each other -> algorithmic accumulation/distribution signature.
    """

    def __init__(self, window_ms: int = CLUSTER_WINDOW_MS, min_prints: int = CLUSTER_MIN_PRINTS):
        self.window_ms = window_ms
        self.min_prints = min_prints
        self._recent: Deque[dict] = deque(maxlen=50)

    def update(self, ts_ms: int, price: float, qty: float) -> Optional[dict]:
        if price <= 0:
            return None

        is_round = is_round_lot(qty)
        self._recent.append({"ts_ms": ts_ms, "price": price, "qty": qty, "round": is_round})

        if not is_round:
            return None

        matches = [
            x for x in self._recent
            if x["round"] and x["price"] == price and (ts_ms - x["ts_ms"]) <= self.window_ms
        ]
        if len(matches) >= self.min_prints:
            return {
                "price": price,
                "prints": len(matches),
                "total_qty": sum(x["qty"] for x in matches),
            }
        return None


# ── Composite ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SmartMoneySignal:
    wall_bid: List[dict]
    wall_ask: List[dict]
    absorption_status: str
    absorption_side: str
    sweep_signal: str
    sweep_levels: int
    sweep_confirmed: bool
    cluster: Optional[dict]
    composite: str  # SMART_MONEY_BUY / SMART_MONEY_SELL / WATCH_ACCUMULATION /
                    # WATCH_DISTRIBUTION / WATCH / NEUTRAL


class SmartMoneyDetector:
    """Per-symbol (or per-contract) stateful composite detector, A+B+C+D."""

    def __init__(self):
        self.anomaly = SizeAnomalyDetector()
        self.absorption = AbsorptionTracker()
        self.cluster = TimeSalesCluster()
        self._avg_tick_vol = RollingStat(window=100)
        self._prev_ask_levels: List[DepthLevel] = []
        self._prev_bid_levels: List[DepthLevel] = []

    def analyze(
        self,
        ts_ms: int,
        trade_price: Optional[float],
        tick_vol: float,
        bid_price: float, bid_size: float,
        ask_price: float, ask_size: float,
        bid_levels: List[DepthLevel],
        ask_levels: List[DepthLevel],
    ) -> SmartMoneySignal:
        bid_sizes = [lv.qty for lv in bid_levels]
        ask_sizes = [lv.qty for lv in ask_levels]
        wall_bid, wall_ask = self.anomaly.update(bid_sizes, ask_sizes)

        absorb = self.absorption.update(bid_price, bid_size, ask_price, ask_size, tick_vol)

        avg_vol = self._avg_tick_vol.avg
        sweep_sig, sweep_levels, sweep_conf = detect_sweep(
            self._prev_ask_levels, self._prev_bid_levels, trade_price, tick_vol, avg_vol
        )
        self._avg_tick_vol.push(tick_vol)
        self._prev_ask_levels = ask_levels
        self._prev_bid_levels = bid_levels

        cl = self.cluster.update(ts_ms, trade_price or 0.0, tick_vol) if trade_price else None

        composite = "NEUTRAL"
        if sweep_sig == "SWEEP_BUY" and sweep_conf:
            composite = "SMART_MONEY_BUY"
        elif sweep_sig == "SWEEP_SELL" and sweep_conf:
            composite = "SMART_MONEY_SELL"
        elif absorb.status == "BROKEN_BULLISH":
            composite = "SMART_MONEY_BUY"
        elif absorb.status == "BROKEN_BEARISH":
            composite = "SMART_MONEY_SELL"
        elif absorb.status == "TESTING" and absorb.side == "BID":
            composite = "WATCH_ACCUMULATION"
        elif absorb.status == "TESTING" and absorb.side == "ASK":
            composite = "WATCH_DISTRIBUTION"
        elif wall_bid or wall_ask or cl:
            composite = "WATCH"

        return SmartMoneySignal(
            wall_bid=wall_bid,
            wall_ask=wall_ask,
            absorption_status=absorb.status,
            absorption_side=absorb.side,
            sweep_signal=sweep_sig,
            sweep_levels=sweep_levels,
            sweep_confirmed=sweep_conf,
            cluster=cl,
            composite=composite,
        )

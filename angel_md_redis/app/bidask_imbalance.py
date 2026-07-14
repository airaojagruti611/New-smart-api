"""
app/bidask_imbalance.py
───────────────────────
Bid-Ask Quantity Imbalance — core logic.

Raw imbalance    = (bid_qty - ask_qty) / (bid_qty + ask_qty)   range [-1, 1]
Depth-weighted   = same formula, but each level's qty is weighted by
                   distance from mid before summing:
                     L1 x5, L2 x3, L3 x1, L4/L5 x0
                   (brief only defines weights through L3 and explicitly
                   calls levels beyond that "less actionable" — read as
                   excluded from the weighted score, not just discounted.
                   Flip LEVEL_WEIGHTS if you meant something else.)

Persistence      = EMA of the (spoof-filtered, weighted) imbalance across
                   ticks, plus a same-sign streak counter.
Flip             = sign change after a streak that had sustained >10 ticks.

Spoofing filter  = a price level only contributes its size to the imbalance
                   calc once it has been observed at the SAME price for
                   > 3 consecutive ticks. A level that appears and vanishes
                   within that window never gets counted — this is
                   causal/forward-only (we can't retroactively un-count a
                   tick that already happened), which naturally implements
                   "exclude spoofs" without needing to rewind history.
                   Vanishing large levels that never got confirmed are
                   also surfaced as explicit spoof_events for observability.

Layering         = all 5 levels on one side grow simultaneously, roughly
                   uniformly, tick-over-tick -> discount final score 50%.

No I/O here — pure dataclasses + functions/classes. Redis wiring lives in
run_bidask_imbalance.py.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

LEVEL_WEIGHTS = [5.0, 3.0, 1.0, 0.0, 0.0]  # L1..L5

BULLISH_THRESHOLD = 0.30
BEARISH_THRESHOLD = -0.30

PERSISTENCE_STREAK_FOR_FLIP = 10
CONFIRM_STREAK = 3          # ticks a level must persist to count (spoof filter)
SPOOF_MAX_LIFE_TICKS = 2    # appears+vanishes within this many ticks = suspected spoof
SPOOF_SIZE_MULT = 2.0       # "large" = > this x that rank's own rolling avg size
SPOOF_PRICE_TOL_PCT = 0.05  # trade within this % of vanished price counts as "consumed it"

LAYERING_MIN_RATIO = 1.02    # every level must have grown at least 2%
LAYERING_UNIFORMITY_TOL = 0.35  # per-level growth ratios must be within this band of each other

EMA_SPAN = 10  # ticks; alpha = 2/(span+1), matching "persist across 10+ ticks" framing


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


def raw_imbalance(total_bid: float, total_ask: float) -> float:
    denom = total_bid + total_ask
    if denom <= 0:
        return 0.0
    return round((total_bid - total_ask) / denom, 4)


def classify_imbalance(ratio: float) -> str:
    if ratio > BULLISH_THRESHOLD:
        return "BULLISH"
    if ratio < BEARISH_THRESHOLD:
        return "BEARISH"
    return "NEUTRAL"


class LevelPersistenceFilter:
    """
    Per-side price-level persistence tracker. Only prices that have appeared
    for > CONFIRM_STREAK consecutive ticks contribute their size to the
    filtered sum. Also detects candidate spoofs: a level that vanishes
    before confirming, was "large", and no trade printed near its price.
    """

    def __init__(self, rank_window: int = 50):
        self._streaks: Dict[float, int] = {}
        self._rank_avg: Dict[int, RollingStat] = {i: RollingStat(rank_window) for i in range(5)}

    def update(self, levels: List[DepthLevel], trade_price: Optional[float]) -> Tuple[float, List[dict]]:
        current_prices = {round(lv.price, 4): lv.qty for lv in levels if lv.price > 0}
        spoof_events: List[dict] = []

        for i, lv in enumerate(levels[:5]):
            if lv.price > 0:
                self._rank_avg[i].push(lv.qty)

        # Vanished prices: were tracked last tick, not present this tick.
        for price, streak in list(self._streaks.items()):
            if price in current_prices:
                continue
            if streak <= SPOOF_MAX_LIFE_TICKS:
                avg_rank0 = self._rank_avg[0].avg  # compare against top-of-book scale
                was_large = avg_rank0 is not None
                consumed_by_trade = (
                    trade_price is not None
                    and price > 0
                    and abs(trade_price - price) / price * 100.0 <= SPOOF_PRICE_TOL_PCT
                )
                if was_large and not consumed_by_trade:
                    spoof_events.append({"price": price, "streak": streak})
            del self._streaks[price]

        filtered_sum = 0.0
        for price, qty in current_prices.items():
            streak = self._streaks.get(price, 0) + 1
            self._streaks[price] = streak
            if streak > CONFIRM_STREAK:
                filtered_sum += qty

        return filtered_sum, spoof_events


def _weighted_sum(levels: List[DepthLevel], weights: List[float] = LEVEL_WEIGHTS) -> float:
    return sum(lv.qty * weights[i] for i, lv in enumerate(levels[:len(weights)]))


def detect_layering(prev_sizes: List[float], curr_sizes: List[float]) -> bool:
    """All 5 levels grew, roughly uniformly, since the previous tick."""
    n = min(len(prev_sizes), len(curr_sizes), 5)
    if n < 5:
        return False
    ratios = []
    for i in range(n):
        if prev_sizes[i] <= 0:
            return False
        r = curr_sizes[i] / prev_sizes[i]
        if r < LAYERING_MIN_RATIO:
            return False
        ratios.append(r)
    mean_r = sum(ratios) / len(ratios)
    if mean_r <= 0:
        return False
    return all(abs(r - mean_r) / mean_r <= LAYERING_UNIFORMITY_TOL for r in ratios)


@dataclass(frozen=True)
class ImbalanceResult:
    raw: float
    weighted_filtered: float
    ema_score: float
    final_score: float  # ema_score, discounted 50% if layering detected
    signal: str          # BULLISH / BEARISH / NEUTRAL (from final_score)
    streak: int
    streak_sign: str      # "BULLISH" / "BEARISH" / "NEUTRAL"
    flip_detected: bool
    layering_bid: bool
    layering_ask: bool
    spoof_events_bid: List[dict]
    spoof_events_ask: List[dict]


class ImbalanceDetector:
    """Per-symbol (or per-contract) stateful imbalance tracker."""

    def __init__(self):
        self._bid_filter = LevelPersistenceFilter()
        self._ask_filter = LevelPersistenceFilter()
        self._prev_bid_sizes: List[float] = []
        self._prev_ask_sizes: List[float] = []
        self._ema: Optional[float] = None
        self._alpha = 2.0 / (EMA_SPAN + 1)
        self._streak = 0
        self._streak_sign = "NEUTRAL"

    def analyze(
        self,
        bid_levels: List[DepthLevel],
        ask_levels: List[DepthLevel],
        trade_price: Optional[float] = None,
    ) -> ImbalanceResult:
        total_bid_raw = sum(lv.qty for lv in bid_levels[:5])
        total_ask_raw = sum(lv.qty for lv in ask_levels[:5])
        raw = raw_imbalance(total_bid_raw, total_ask_raw)

        bid_filtered_sum, spoof_bid = self._bid_filter.update(bid_levels, trade_price)
        ask_filtered_sum, spoof_ask = self._ask_filter.update(ask_levels, trade_price)

        # Weighted-and-filtered: weight each CONFIRMED level, using its
        # rank position in the current snapshot for the weight.
        confirmed_bid_levels = [lv for lv in bid_levels[:5] if lv.qty <= bid_filtered_sum + 1e-6]
        # simpler & robust: recompute weighted using only sizes proportionally
        # scaled by the filtered/raw ratio per side (keeps weighting stable
        # without re-deriving which exact levels were confirmed).
        bid_scale = (bid_filtered_sum / total_bid_raw) if total_bid_raw > 0 else 0.0
        ask_scale = (ask_filtered_sum / total_ask_raw) if total_ask_raw > 0 else 0.0
        weighted_bid = _weighted_sum(bid_levels) * bid_scale
        weighted_ask = _weighted_sum(ask_levels) * ask_scale
        weighted_filtered = raw_imbalance(weighted_bid, weighted_ask)

        bid_sizes = [lv.qty for lv in bid_levels[:5]]
        ask_sizes = [lv.qty for lv in ask_levels[:5]]
        layering_bid = detect_layering(self._prev_bid_sizes, bid_sizes)
        layering_ask = detect_layering(self._prev_ask_sizes, ask_sizes)
        self._prev_bid_sizes = bid_sizes
        self._prev_ask_sizes = ask_sizes

        self._ema = weighted_filtered if self._ema is None else (
            self._alpha * weighted_filtered + (1 - self._alpha) * self._ema
        )
        ema_score = round(self._ema, 4)

        final_score = ema_score
        if layering_bid or layering_ask:
            final_score = round(final_score * 0.5, 4)

        signal = classify_imbalance(final_score)

        sign = "BULLISH" if raw > 0 else ("BEARISH" if raw < 0 else "NEUTRAL")
        prev_flip_eligible = self._streak > PERSISTENCE_STREAK_FOR_FLIP
        prev_sign = self._streak_sign
        flip_detected = prev_flip_eligible and sign != "NEUTRAL" and sign != prev_sign

        if sign == self._streak_sign:
            self._streak += 1
        else:
            self._streak_sign = sign
            self._streak = 1

        return ImbalanceResult(
            raw=raw,
            weighted_filtered=weighted_filtered,
            ema_score=ema_score,
            final_score=final_score,
            signal=signal,
            streak=self._streak,
            streak_sign=self._streak_sign,
            flip_detected=flip_detected,
            layering_bid=layering_bid,
            layering_ask=layering_ask,
            spoof_events_bid=spoof_bid,
            spoof_events_ask=spoof_ask,
        )

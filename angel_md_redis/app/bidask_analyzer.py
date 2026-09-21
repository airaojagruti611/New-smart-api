"""
app/bidask_analyzer.py
───────────────────────
Bid-Ask Intelligence Module — core logic.

Treats the order book as a sensor array:
  - Spread / spread% as a liquidity proxy
  - Size-weighted top-5 depth, normalized against its own rolling 20-period
    average, as the real liquidity signal (0-100 score)
  - Stock spreads classified against fixed thresholds
  - Option spreads classified against the CONTRACT'S OWN 10-day average
    spread% (falling back to the session mean until daily history exists).
    Options carry structurally wider spreads; a fixed threshold is
    meaningless across strikes/expiries, and a 20-tick window flickers on
    a single tick-size change.

No I/O here — pure dataclasses + functions, unit-testable in isolation.
Redis wiring lives in run_bidask_analyzer.py.
"""

from __future__ import annotations

import datetime as dt
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, List, Optional, Sequence, Tuple

try:
    from zoneinfo import ZoneInfo

    IST = ZoneInfo("Asia/Kolkata")
except Exception:  # pragma: no cover
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

# ── Thresholds (per design brief) ──────────────────────────────────────────
STOCK_HIGH_LIQ_SPREAD_PCT = 0.05     # spread% < 0.05        -> HIGH_LIQUIDITY
STOCK_MODERATE_SPREAD_PCT = 0.20     # 0.05 <= spread% <=0.20-> MODERATE_LIQUIDITY
# spread% > 0.20                                             -> THIN_AVOID

OPTION_CAUTION_RATIO = 1.5           # current/avg >= 1.5x   -> CAUTION
OPTION_AVOID_RATIO = 2.0             # current/avg >= 2.0x   -> EXIT_TERRITORY
OPTION_SPREAD_DAYS = 10              # spec: option's own 10-day average
SESSION_MIN_SAMPLES = 20             # session fallback needs a stable mean

DEPTH_LEVELS = 5


def ist_today() -> str:
    return dt.datetime.now(IST).date().isoformat()


@dataclass(frozen=True)
class SpreadResult:
    raw_spread: float
    spread_pct: float
    mid: float


def compute_spread(bid: Optional[float], ask: Optional[float]) -> SpreadResult:
    """
    Raw spread = Ask - Bid; Spread% = Raw spread / Mid * 100.
    Invalid/crossed/missing quotes -> zeroed result (caller should treat as NO_DATA).
    """
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return SpreadResult(raw_spread=0.0, spread_pct=0.0, mid=0.0)
    mid = (bid + ask) / 2.0
    raw = ask - bid
    pct = (raw / mid) * 100.0 if mid > 0 else 0.0
    return SpreadResult(raw_spread=round(raw, 4), spread_pct=round(pct, 4), mid=round(mid, 4))


def depth_sum(sizes: Sequence[float], levels: int = DEPTH_LEVELS) -> float:
    """Sum of top-N level sizes (bid or ask side)."""
    return float(sum(s for s in list(sizes)[:levels] if s))


def liquidity_score(total_depth: float, rolling_avg_depth: Optional[float]) -> float:
    """
    0-100 liquidity score: size-weighted top-5 depth (bid+ask) normalized
    against its own rolling 20-period average. 100 = at/above-average book
    depth; scales down linearly for a thinner-than-usual book. Capped at 100.
    """
    if not rolling_avg_depth or rolling_avg_depth <= 0:
        return 0.0
    return round(min((total_depth / rolling_avg_depth) * 100.0, 100.0), 2)


def classify_stock_liquidity(spread_pct: float) -> str:
    if spread_pct <= 0:
        return "NO_DATA"
    if spread_pct < STOCK_HIGH_LIQ_SPREAD_PCT:
        return "HIGH_LIQUIDITY"
    if spread_pct <= STOCK_MODERATE_SPREAD_PCT:
        return "MODERATE_LIQUIDITY"
    return "THIN_AVOID"


def classify_option_liquidity(
    spread_pct: float,
    avg_spread_pct: Optional[float],
) -> Tuple[str, Optional[float]]:
    """
    Options: compare current spread% to the contract's own 10-day (or session)
    average spread%, not a fixed value. Returns (signal, ratio).
      ratio >= 2.0 -> EXIT_TERRITORY
      ratio >= 1.5 -> CAUTION
      else         -> NORMAL
    """
    if spread_pct <= 0:
        return "NO_DATA", None
    if not avg_spread_pct or avg_spread_pct <= 0:
        return "WARMING_UP", None  # not enough history yet to normalize against

    ratio = spread_pct / avg_spread_pct
    if ratio >= OPTION_AVOID_RATIO:
        signal = "EXIT_TERRITORY"
    elif ratio >= OPTION_CAUTION_RATIO:
        signal = "CAUTION"
    else:
        signal = "NORMAL"
    return signal, round(ratio, 2)


def option_spread_baseline(
    daily_avgs: Sequence[float],
    session_avg: Optional[float],
    max_days: int = OPTION_SPREAD_DAYS,
) -> Tuple[Optional[float], str, int]:
    """
    Prefer the mean of up to `max_days` prior daily averages (the spec's
    10-day window). Until that history exists, fall back to the session mean
    so a 20-tick window cannot flip CAUTION/EXIT on a single tick-size change.
    """
    vals = [float(x) for x in daily_avgs if x and float(x) > 0][-max_days:]
    if vals:
        return sum(vals) / len(vals), "ten_day", len(vals)
    if session_avg and session_avg > 0:
        return float(session_avg), "session", 0
    return None, "", 0


@dataclass
class RollingStat:
    """Fixed-window rolling average (in-memory, per-symbol/contract)."""

    window: int
    _buf: Deque[float] = field(default_factory=deque, repr=False)

    def __post_init__(self) -> None:
        self._buf = deque(maxlen=max(1, self.window))

    def push(self, value: float) -> None:
        if value and value > 0:
            self._buf.append(value)

    @property
    def avg(self) -> Optional[float]:
        if not self._buf:
            return None
        return sum(self._buf) / len(self._buf)

    @property
    def n(self) -> int:
        return len(self._buf)


@dataclass
class SessionAvg:
    """Cumulative mean since session start; resets on IST date change."""

    min_samples: int = SESSION_MIN_SAMPLES
    day: str = field(default_factory=ist_today)
    _sum: float = 0.0
    _n: int = 0

    def take_day_and_reset(self) -> Optional[Tuple[str, float, int]]:
        """If the IST date rolled, return (old_date, avg, n) and start a new day."""
        today = ist_today()
        if today == self.day:
            return None
        out = None
        if self._n > 0 and self._sum > 0:
            out = (self.day, self._sum / self._n, self._n)
        self.day = today
        self._sum = 0.0
        self._n = 0
        return out

    def push(self, value: float) -> None:
        if value and value > 0:
            self._sum += value
            self._n += 1

    @property
    def avg(self) -> Optional[float]:
        if self._n < self.min_samples:
            return None
        return self._sum / self._n

    @property
    def n(self) -> int:
        return self._n

    def snapshot(self) -> Optional[Tuple[str, float, int]]:
        """Current day's running mean, even before min_samples (caller decides)."""
        if self._n <= 0 or self._sum <= 0:
            return None
        return self.day, self._sum / self._n, self._n


@dataclass(frozen=True)
class BidAskResult:
    bid: float
    ask: float
    raw_spread: float
    spread_pct: float
    mid: float
    depth: float
    liquidity_score: float
    signal: str
    spread_ratio: Optional[float]  # options only; None for stocks
    spread_avg: Optional[float] = None
    spread_avg_source: str = ""    # "ten_day" | "session" | ""
    spread_days: int = 0           # how many prior daily avgs fed the baseline


class BidAskAnalyzer:
    """
    Per-symbol (or per-contract) stateful analyzer.

    is_option=False -> stock path: liquidity_score + fixed spread% thresholds
    is_option=True  -> option path: liquidity_score + spread normalized
                        against the contract's own 10-day average spread%
                        (session mean until daily history exists)
    """

    def __init__(
        self,
        depth_avg_window: int = 20,
        spread_avg_window: int = 20,  # kept for call-site compat; unused for options
        is_option: bool = False,
        daily_spread_avgs: Optional[Sequence[float]] = None,
        session_min_samples: int = SESSION_MIN_SAMPLES,
        max_spread_days: int = OPTION_SPREAD_DAYS,
    ):
        self.depth_avg = RollingStat(depth_avg_window)
        self.spread_avg = RollingStat(spread_avg_window)  # stocks unused; depth is the 20-period path
        self.session_spread = SessionAvg(min_samples=session_min_samples)
        self.is_option = is_option
        self.max_spread_days = max_spread_days
        self.daily_spread_avgs: List[float] = [
            float(x) for x in (daily_spread_avgs or []) if x and float(x) > 0
        ][-max_spread_days:]

    def roll_if_new_day(self) -> Optional[Tuple[str, float, int]]:
        """Close yesterday into the 10-day buffer. Returns (date, avg, n) if rolled."""
        snap = self.session_spread.take_day_and_reset()
        if not snap:
            return None
        _date, avg, _n = snap
        self.daily_spread_avgs.append(avg)
        self.daily_spread_avgs = self.daily_spread_avgs[-self.max_spread_days:]
        return snap

    def session_snapshot(self, min_samples: Optional[int] = None) -> Optional[Tuple[str, float, int]]:
        snap = self.session_spread.snapshot()
        if not snap:
            return None
        _date, _avg, n = snap
        need = self.session_spread.min_samples if min_samples is None else min_samples
        if n < need:
            return None
        return snap

    def analyze(
        self,
        bid: Optional[float],
        ask: Optional[float],
        bid_sizes: Sequence[float],
        ask_sizes: Sequence[float],
    ) -> BidAskResult:
        self.roll_if_new_day()
        sr = compute_spread(bid, ask)
        depth = depth_sum(bid_sizes) + depth_sum(ask_sizes)

        liq_score = liquidity_score(depth, self.depth_avg.avg)

        spread_avg: Optional[float] = None
        avg_source = ""
        spread_days = 0
        if self.is_option:
            spread_avg, avg_source, spread_days = option_spread_baseline(
                self.daily_spread_avgs,
                self.session_spread.avg,
                max_days=self.max_spread_days,
            )
            signal, ratio = classify_option_liquidity(sr.spread_pct, spread_avg)
        else:
            signal, ratio = classify_stock_liquidity(sr.spread_pct), None

        # Update rolling stats AFTER classifying against the prior average,
        # so the current sample doesn't bias its own comparison.
        if depth > 0:
            self.depth_avg.push(depth)
        if sr.spread_pct > 0:
            self.spread_avg.push(sr.spread_pct)
            if self.is_option:
                self.session_spread.push(sr.spread_pct)

        return BidAskResult(
            bid=bid or 0.0,
            ask=ask or 0.0,
            raw_spread=sr.raw_spread,
            spread_pct=sr.spread_pct,
            mid=sr.mid,
            depth=depth,
            liquidity_score=liq_score,
            signal=signal,
            spread_ratio=ratio,
            spread_avg=round(spread_avg, 4) if spread_avg else None,
            spread_avg_source=avg_source,
            spread_days=spread_days,
        )

"""
app/market_structure.py
───────────────────────
Market-structure helpers for Module 18 (Adaptive Trailing Stop Loss &
Re-entry, DECISION.md §5 / P9). Pure functions, no I/O.

  swing_points()     fractal swing highs / lows (N bars each side)
  last_swing_high()  most recent confirmed swing high (or None)
  last_swing_low()   most recent confirmed swing low  (or None)
  fib_levels()       Fibonacci retracements of a high/low range
  atr_pct()          Wilder ATR as % of the last close
  update_bars()      1-minute OHLC bars built from (ts, price) samples —
                     used for the option premium, which has no candles in Redis

Bars are plain dicts {"t","o","h","l","c"} so they persist as JSON.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

from .candle_types import Candle
from .supertrend import atr_wilder

SWING_LEFT = 2
SWING_RIGHT = 2
FIB_RATIOS = (0.236, 0.382, 0.5, 0.618, 0.786)
BAR_MS = 60_000
MAX_BARS = 240


def swing_points(
    candles: Sequence[Candle], left: int = SWING_LEFT, right: int = SWING_RIGHT
) -> Tuple[List[Tuple[int, float]], List[Tuple[int, float]]]:
    """
    Fractal swings: bar i is a swing high when its high is strictly above the
    `left` bars before and >= the `right` bars after (lows mirrored). The last
    `right` bars cannot be confirmed yet. Returns ([(ts, high)], [(ts, low)]).
    """
    highs: List[Tuple[int, float]] = []
    lows: List[Tuple[int, float]] = []
    n = len(candles)
    for i in range(left, n - right):
        c = candles[i]
        before = candles[i - left:i]
        after = candles[i + 1:i + 1 + right]
        if all(c.h > b.h for b in before) and all(c.h >= a.h for a in after):
            highs.append((c.ts_ms, c.h))
        if all(c.l < b.l for b in before) and all(c.l <= a.l for a in after):
            lows.append((c.ts_ms, c.l))
    return highs, lows


def last_swing_high(candles: Sequence[Candle], left: int = SWING_LEFT, right: int = SWING_RIGHT) -> Optional[float]:
    highs, _ = swing_points(candles, left, right)
    return highs[-1][1] if highs else None


def last_swing_low(candles: Sequence[Candle], left: int = SWING_LEFT, right: int = SWING_RIGHT) -> Optional[float]:
    _, lows = swing_points(candles, left, right)
    return lows[-1][1] if lows else None


def fib_levels(high: Optional[float], low: Optional[float], ratios: Sequence[float] = FIB_RATIOS) -> Dict[str, float]:
    """Retracements measured down from `high`: level = high - ratio x (high - low)."""
    if high is None or low is None or high <= low:
        return {}
    rng = high - low
    return {f"{r:.3f}": round(high - r * rng, 4) for r in ratios}


def atr_pct(candles: Sequence[Candle], period: int = 14) -> Optional[float]:
    if len(candles) < period + 1:
        return None
    atr = atr_wilder(list(candles), period)[-1]
    close = candles[-1].c
    if atr is None or not close:
        return None
    return round(atr / close * 100.0, 6)


def bars_to_candles(bars: Sequence[dict]) -> List[Candle]:
    return [Candle(ts_ms=int(b["t"]) + BAR_MS, o=b["o"], h=b["h"], l=b["l"], c=b["c"], v=0.0) for b in bars]


def update_bars(bars: List[dict], ts_ms: int, price: Optional[float], max_bars: int = MAX_BARS) -> List[dict]:
    """
    Fold one price sample into 1-minute bars (returns a new list). The last
    bar is the one still forming; every earlier bar is complete. Out-of-order
    samples older than the forming bar are ignored.
    """
    if price is None or price <= 0:
        return list(bars)
    start = ts_ms - ts_ms % BAR_MS
    out = list(bars)
    if out and int(out[-1]["t"]) == start:
        b = dict(out[-1])
        b["h"] = max(b["h"], price)
        b["l"] = min(b["l"], price)
        b["c"] = price
        out[-1] = b
    elif not out or start > int(out[-1]["t"]):
        out.append({"t": start, "o": price, "h": price, "l": price, "c": price})
    return out[-max_bars:]


def completed_bars(bars: Sequence[dict], now_ms: int) -> List[dict]:
    """Bars whose minute has ended by `now_ms`."""
    return [b for b in bars if int(b["t"]) + BAR_MS <= now_ms]

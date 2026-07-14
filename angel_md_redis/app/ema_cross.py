from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from .candle_types import Candle
from .ema import ema


@dataclass(frozen=True)
class EmaCrossPoint:
    ts_ms: int
    ema_fast: float
    ema_slow: float
    signal: str  # "bullish_cross" / "bearish_cross" / "none"
    state: str  # "bullish" / "bearish"


def ema_cross(
    candles: List[Candle],
    fast: int = 9,
    slow: int = 26,
) -> List[Optional[EmaCrossPoint]]:
    """
    EMA9 / EMA26 cross detector on candle closes.

    signal:
      - bullish_cross: EMA_fast crosses above EMA_slow
      - bearish_cross: EMA_fast crosses below EMA_slow
      - none: no cross on this bar
    state:
      - bullish if EMA_fast > EMA_slow else bearish
    """
    n = len(candles)
    out: List[Optional[EmaCrossPoint]] = [None] * n
    if n == 0:
        return out

    closes = [c.c for c in candles]
    fast_s = ema(closes, fast)
    slow_s = ema(closes, slow)

    prev_fast: Optional[float] = None
    prev_slow: Optional[float] = None

    for i in range(n):
        f = fast_s[i]
        s = slow_s[i]
        if f is None or s is None:
            continue

        state = "bullish" if f > s else "bearish"
        signal = "none"

        if prev_fast is not None and prev_slow is not None:
            if prev_fast <= prev_slow and f > s:
                signal = "bullish_cross"
            elif prev_fast >= prev_slow and f < s:
                signal = "bearish_cross"

        out[i] = EmaCrossPoint(
            ts_ms=int(candles[i].ts_ms),
            ema_fast=float(f),
            ema_slow=float(s),
            signal=signal,
            state=state,
        )
        prev_fast = f
        prev_slow = s

    return out


def last_ema_cross_signal(
    candles: List[Candle],
    fast: int = 9,
    slow: int = 26,
) -> Optional[EmaCrossPoint]:
    pts = ema_cross(candles, fast=fast, slow=slow)
    for p in reversed(pts):
        if p is not None:
            return p
    return None

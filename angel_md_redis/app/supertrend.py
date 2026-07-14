from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from .candle_types import Candle


def _true_range(curr: Candle, prev_close: float) -> float:
    return max(
        curr.h - curr.l,
        abs(curr.h - prev_close),
        abs(curr.l - prev_close),
    )


def atr_wilder(candles: List[Candle], period: int) -> List[Optional[float]]:
    """
    Wilder's ATR, aligned to the input candles.

    Returns a list of length len(candles) where values are None until enough
    history exists to seed the ATR.
    """
    n = len(candles)
    if n == 0:
        return []

    out: List[Optional[float]] = [None] * n
    if n < period + 1:
        return out

    tr: List[Optional[float]] = [None] * n
    for i in range(1, n):
        tr[i] = _true_range(candles[i], candles[i - 1].c)

    seed = sum(x for x in tr[1 : period + 1] if x is not None) / period
    out[period] = seed

    for i in range(period + 1, n):
        prev = out[i - 1]
        assert prev is not None
        tri = tr[i]
        assert tri is not None
        out[i] = (prev * (period - 1) + tri) / period

    return out


@dataclass(frozen=True)
class SupertrendPoint:
    ts_ms: int
    supertrend: float
    direction: str  # "bullish" or "bearish"


def supertrend(
    candles: List[Candle],
    atr_period: int = 7,
    multiplier: float = 1.0,
) -> List[Optional[SupertrendPoint]]:
    """
    Supertrend indicator (classic bands + final bands + trend flip logic).

    Output is aligned to candles with None until ATR is available.
    """
    n = len(candles)
    out: List[Optional[SupertrendPoint]] = [None] * n
    if n == 0:
        return out

    atr = atr_wilder(candles, atr_period)

    fub: List[Optional[float]] = [None] * n  # final upper band
    flb: List[Optional[float]] = [None] * n  # final lower band

    prev_st: Optional[float] = None

    for i in range(n):
        ai = atr[i]
        if ai is None:
            continue

        c = candles[i]
        hl2 = (c.h + c.l) / 2.0
        basic_ub = hl2 + multiplier * ai
        basic_lb = hl2 - multiplier * ai

        if i == 0 or fub[i - 1] is None or flb[i - 1] is None:
            fub[i] = basic_ub
            flb[i] = basic_lb
        else:
            prev_c = candles[i - 1]
            prev_fub = fub[i - 1]
            prev_flb = flb[i - 1]
            assert prev_fub is not None and prev_flb is not None

            fub[i] = basic_ub if (basic_ub < prev_fub or prev_c.c > prev_fub) else prev_fub
            flb[i] = basic_lb if (basic_lb > prev_flb or prev_c.c < prev_flb) else prev_flb

        curr_fub = fub[i]
        curr_flb = flb[i]
        assert curr_fub is not None and curr_flb is not None

        if prev_st is None:
            if c.c >= curr_flb:
                st = curr_flb
                direction = "bullish"
            else:
                st = curr_fub
                direction = "bearish"
        else:
            prev_fub = fub[i - 1]
            prev_flb = flb[i - 1]
            assert prev_fub is not None and prev_flb is not None

            if prev_st == prev_fub:
                if c.c <= curr_fub:
                    st = curr_fub
                    direction = "bearish"
                else:
                    st = curr_flb
                    direction = "bullish"
            else:
                if c.c >= curr_flb:
                    st = curr_flb
                    direction = "bullish"
                else:
                    st = curr_fub
                    direction = "bearish"

        out[i] = SupertrendPoint(ts_ms=int(c.ts_ms), supertrend=float(st), direction=direction)
        prev_st = st

    return out


def last_supertrend_signal(
    candles: List[Candle],
    atr_period: int = 7,
    multiplier: float = 1.0,
) -> Optional[SupertrendPoint]:
    pts = supertrend(candles, atr_period=atr_period, multiplier=multiplier)
    for p in reversed(pts):
        if p is not None:
            return p
    return None


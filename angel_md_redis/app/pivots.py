from __future__ import annotations

from .candle_types import Candle, PivotLevels


def classic_pivots(prev_day: Candle, date: str) -> PivotLevels:
    """
    Classic floor pivots from previous day's H/L/C.
    """
    P = (prev_day.h + prev_day.l + prev_day.c) / 3.0
    R1 = (2.0 * P) - prev_day.l
    S1 = (2.0 * P) - prev_day.h
    R2 = P + (prev_day.h - prev_day.l)
    S2 = P - (prev_day.h - prev_day.l)
    return PivotLevels(date=date, P=P, R1=R1, S1=S1, R2=R2, S2=S2)


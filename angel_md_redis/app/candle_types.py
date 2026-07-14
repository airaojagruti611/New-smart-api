from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Candle:
    """
    Generic OHLCV candle.

    ts_ms is the candle close timestamp (end of the bucket) in epoch milliseconds.
    """

    ts_ms: int
    o: float
    h: float
    l: float
    c: float
    v: float


@dataclass(frozen=True)
class PivotLevels:
    date: str  # YYYY-MM-DD (the source day the pivots were computed from)
    P: float
    R1: float
    S1: float
    R2: float
    S2: float


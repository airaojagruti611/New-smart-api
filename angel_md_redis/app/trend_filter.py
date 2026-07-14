from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from .candle_types import Candle
from .supertrend import last_supertrend_signal


@dataclass(frozen=True)
class MtfTrendResult:
    per_tf: Dict[str, str]  # tf -> "bullish"/"bearish"/"na"
    bullish: int
    bearish: int
    bias: str  # "CALL" / "PUT" / "NEUTRAL"


def mtf_supertrend_bias(
    candles_by_tf: Dict[str, List[Candle]],
    atr_period: int = 7,
    multiplier: float = 1.0,
    majority: int = 3,
) -> MtfTrendResult:
    """
    Multi-timeframe Supertrend bias.

    Example candles_by_tf keys: {"30m": [...], "10m": [...], "5m": [...], "1m": [...]}
    majority=3 means 3-of-4 timeframes must agree for CALL/PUT, else NEUTRAL.
    """
    per_tf: Dict[str, str] = {}
    bull = 0
    bear = 0

    for tf, candles in candles_by_tf.items():
        p = last_supertrend_signal(candles, atr_period=atr_period, multiplier=multiplier)
        if p is None:
            per_tf[tf] = "na"
            continue

        per_tf[tf] = p.direction
        if p.direction == "bullish":
            bull += 1
        elif p.direction == "bearish":
            bear += 1

    if bull >= majority:
        bias = "CALL"
    elif bear >= majority:
        bias = "PUT"
    else:
        bias = "NEUTRAL"

    return MtfTrendResult(per_tf=per_tf, bullish=bull, bearish=bear, bias=bias)


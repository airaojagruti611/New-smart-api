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
    majority: int = 0,
) -> MtfTrendResult:
    """
    Multi-timeframe Supertrend bias.

    Example candles_by_tf keys: {"30m": [...], "10m": [...], "5m": [...], "1m": [...]}
    majority > 0: that many TFs must agree (legacy 3-of-4).
    majority <= 0: strict majority of TFs that already have a Supertrend
    reading, requiring at least 2 ready TFs. If fewer TFs are ready than
    the configured majority, falls back to this adaptive rule so a cold
    start can still emit CALL/PUT once 1m+5m exist.
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

    ready = bull + bear
    if majority > 0 and ready >= majority:
        need = majority
    elif ready >= 2:
        need = (ready // 2) + 1
    else:
        need = 99

    if bull >= need:
        bias = "CALL"
    elif bear >= need:
        bias = "PUT"
    else:
        bias = "NEUTRAL"

    return MtfTrendResult(per_tf=per_tf, bullish=bull, bearish=bear, bias=bias)


"""
Spec Module 1 scoring board (-2 .. +2) from Supertrend + EMA + pivot strength.

  Strong Bullish +2 | Bullish +1 | Neutral 0 | Bearish -1 | Strong Bearish -2
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IndicatorScoreResult:
    score: float
    label: str
    reason: str


def compute_indicator_score(
    st_bias: str,
    ema_state: str,
    st_bullish: int = 0,
    st_bearish: int = 0,
    level_signal: str = "",
    level: str = "",
    strength: str = "",
) -> IndicatorScoreResult:
    bias = (st_bias or "").strip().upper()
    state = (ema_state or "").strip().lower()
    lvl_sig = (level_signal or "").strip().upper()
    lvl = (level or "").strip().upper()
    strength_in = (strength or "").strip().lower()
    try:
        n_bull = int(st_bullish or 0)
    except (TypeError, ValueError):
        n_bull = 0
    try:
        n_bear = int(st_bearish or 0)
    except (TypeError, ValueError):
        n_bear = 0

    strong_call = (
        lvl_sig == "BUY CALL" and lvl in ("R1", "R2")
    ) or strength_in == "strong" or n_bull >= 3
    strong_put = (
        lvl_sig == "BUY PUT" and lvl in ("S1", "S2")
    ) or strength_in == "strong" or n_bear >= 3

    if bias == "CALL" and state == "bullish":
        if strong_call:
            return IndicatorScoreResult(2.0, "strong_bullish", "st_call+ema_bullish+strong")
        return IndicatorScoreResult(1.0, "bullish", "st_call+ema_bullish")

    if bias == "PUT" and state == "bearish":
        if strong_put:
            return IndicatorScoreResult(-2.0, "strong_bearish", "st_put+ema_bearish+strong")
        return IndicatorScoreResult(-1.0, "bearish", "st_put+ema_bearish")

    return IndicatorScoreResult(
        0.0,
        "neutral",
        f"unaligned(st={bias or 'na'},ema={state or 'na'})",
    )

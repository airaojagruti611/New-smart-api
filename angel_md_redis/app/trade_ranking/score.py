"""
Trade Ranking Engine — final trade score and decision bands
(DECISION.md §6 R12, brief §16–§17).

    weighted    = sum(w_i x component_i) / sum(w_i)   over present components
    trade_score = weighted x conflict_factor x risk_factor x dq

    < 50 REJECT | 50-59 WATCH | 60-69 CONDITIONAL | 70-79 TAKE
    | 80-89 HIGH_CONVICTION | >= 90 EXCEPTIONAL
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from app.trade_ranking.config import (
    BAND_CONDITIONAL_MIN,
    BAND_EXCEPTIONAL_MIN,
    BAND_HIGH_MIN,
    BAND_REJECT_MAX,
    BAND_TAKE_MIN,
    Profile,
    RankConfig,
)


def weighted(components: Dict[str, Optional[float]], weights: Dict[str, float]) -> Optional[float]:
    known = {k: float(v) for k, v in components.items() if k in weights and v is not None}
    wsum = sum(weights[k] for k in known)
    if wsum <= 0:
        return None
    return round(sum(v * weights[k] for k, v in known.items()) / wsum, 4)


def trade_score(weighted_score: float, conflict_factor: float, risk_factor: float, dq: float) -> float:
    return round(weighted_score * conflict_factor * risk_factor * dq, 2)


def confidence_band(score: float) -> str:
    if score >= BAND_EXCEPTIONAL_MIN:
        return "EXCEPTIONAL"
    if score >= BAND_HIGH_MIN:
        return "HIGH_CONVICTION"
    if score >= BAND_TAKE_MIN:
        return "TAKE"
    if score >= BAND_CONDITIONAL_MIN:
        return "CONDITIONAL"
    if score >= BAND_REJECT_MAX:
        return "WATCH"
    return "REJECT"


def decide(score: float, profile: Profile) -> Tuple[str, str, List[str]]:
    """(decision, confidence, reject/watch reasons) from the score alone (gates handled elsewhere)."""
    band = confidence_band(score)
    if band == "REJECT":
        return "REJECT", "", ["SCORE_BELOW_50"]
    if band == "WATCH":
        return "WATCH", "", ["SCORE_WATCH_BAND"]
    if band == "CONDITIONAL" and not profile.allow_conditional:
        return "WATCH", "CONDITIONAL", ["CONDITIONAL_NOT_ALLOWED"]
    if score < profile.min_trade_score:
        return "WATCH", band, ["BELOW_PROFILE_MIN_SCORE"]
    return "TAKE_TRADE", band, []


_LABELS = {
    "probability": ("High probability", "Low probability"),
    "indicator": ("Indicators confirm the direction", "Indicators do not confirm"),
    "volume": ("Participation increasing", "Weak volume participation"),
    "market_regime": ("Supportive market regime", "Unsupportive market regime"),
    "bidask": ("Strong bid-ask confirmation", "Poor bid-ask / execution quality"),
    "oi": ("Positive OI behaviour", "OI positioning against the trade"),
    "greeks": ("Favourable Greeks profile", "Unfavourable Greeks profile"),
    "liquidity": ("Healthy liquidity", "Thin liquidity"),
    "expected_move": ("Favourable expected move", "Expected move does not support"),
    "greeks_change": ("Projected premium gain is large", "Small projected premium gain"),
    "strike": ("Strong strike selection", "Weak strike selection"),
    "lot_size": ("Comfortable risk capacity", "Tight risk capacity"),
    "trailing_stop": ("Good reward vs stop distance", "Stop distance large vs reward"),
}


def reasons_and_warnings(components: Dict[str, Optional[float]], agreement: Optional[float],
                         cfg: RankConfig) -> Tuple[List[str], List[str]]:
    reasons: List[str] = []
    warnings: List[str] = []
    if agreement is not None and agreement >= cfg.agreement_full:
        reasons.append("Strong directional agreement")
    elif agreement is not None and agreement < cfg.agreement_full:
        warnings.append(f"Directional agreement {agreement * 100:.0f}%")
    for k, (good, bad) in _LABELS.items():
        v = components.get(k)
        if v is None:
            continue
        if v >= cfg.reason_min:
            reasons.append(good)
        elif v < cfg.warning_max:
            warnings.append(bad)
    return reasons, warnings

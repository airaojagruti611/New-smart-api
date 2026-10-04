"""
Trade Ranking Engine — score normaliser (DECISION.md §6 R4, R5).

Every component → 0..100, side-aligned (100 = supports this trade).
Signed inputs use (x - min) / (max - min) x 100 after flipping the sign for
BUY PUT. Missing input → None (never 0, R6).
"""

from __future__ import annotations

from typing import Dict, Optional

from app.probability_engine import (
    VOLUME_SURGE_BONUS,
    _VOLUME_MAP,
    normalize_side,
    oi_score,
    signed_to_score,
)
from app.trade_ranking.candidate import Candidate
from app.trade_ranking.config import PHASE_FIT, RankConfig


def clip(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def lin(v: Optional[float], lo: float, hi: float) -> Optional[float]:
    """0 at `lo`, 100 at `hi`, clipped."""
    if v is None or hi == lo:
        return None
    return round(clip((float(v) - lo) / (hi - lo) * 100.0), 4)


def signed_range_to_score(x: Optional[float], lo: float, hi: float) -> Optional[float]:
    """Brief §5: (x - min) / (max - min) x 100; -100..+100 -> (x + 100) / 200 x 100."""
    return lin(x, lo, hi)


def label_alignment(label: Optional[str], side: str) -> Optional[str]:
    """
    Underlying-direction label (BULLISH / STRONG_BEARISH / CALL / PUT /
    *_POSITIONING / build-ups) -> FOR / AGAINST the trade side, NEUTRAL, or
    None when absent.
    """
    v = (label or "").strip().upper()
    if not v:
        return None
    bull = any(t in v for t in ("BULL", "CALL", "LONG_BUILDUP", "SHORT_COVERING"))
    bear = any(t in v for t in ("BEAR", "PUT", "SHORT_BUILDUP", "LONG_UNWINDING"))
    if bull == bear:
        return "NEUTRAL"
    is_ce = normalize_side(side) == "CE"
    return "FOR" if bull == is_ce else "AGAINST"


_ALIGN_SCORE = {"FOR": 100.0, "NEUTRAL": 50.0, "AGAINST": 0.0}


def regime_score(c: Candidate) -> Optional[float]:
    parts = []
    a = label_alignment(c.regime, c.side)
    if a is not None:
        parts.append(_ALIGN_SCORE[a])
    fit = PHASE_FIT.get((c.market_phase or "").strip().upper())
    if fit is not None:
        parts.append(fit)
    return round(sum(parts) / len(parts), 4) if parts else None


def bidask_score(c: Candidate) -> Optional[float]:
    exe = None if c.execution_quality is None else clip(float(c.execution_quality))
    a = label_alignment(c.imbalance, c.side)
    imb = None if a is None else _ALIGN_SCORE[a]
    if exe is None and imb is None:
        return None
    if exe is None:
        return imb
    if imb is None:
        return round(exe, 4)
    return round(0.7 * exe + 0.3 * imb, 4)


def volume_score(c: Candidate) -> Optional[float]:
    v = _VOLUME_MAP.get((c.volume_signal or "").strip().upper())
    s = signed_to_score(v, normalize_side(c.side), scale=2.0)
    if s is None:
        return None
    return round(clip(s + (VOLUME_SURGE_BONUS if c.volume_surge else 0.0)), 4)


def em_opposes(c: Candidate) -> bool:
    return label_alignment(c.em_direction, c.side) == "AGAINST"


def expected_move_score(c: Candidate) -> Optional[float]:
    conf = None
    if c.em_confidence is not None:
        conf = 0.0 if em_opposes(c) else clip(float(c.em_confidence))
    parts = [p for p in (conf, c.em_fit) if p is not None]
    return round(sum(parts) / len(parts), 4) if parts else None


def greeks_change_score(c: Candidate, cfg: RankConfig) -> Optional[float]:
    if c.projected_gain is None or not c.premium or c.premium <= 0:
        return None
    s = lin(c.projected_gain / c.premium * 100.0, 0.0, cfg.greeks_change_full_pct)
    if s is not None and c.projected_gain_iv_down is not None and c.projected_gain_iv_down <= 0:
        s = round(s * 0.5, 4)
    return s


def _opt(v: Optional[float]) -> Optional[float]:
    return None if v is None else round(clip(float(v)), 4)


def normalize(c: Candidate, cfg: RankConfig) -> Dict[str, Optional[float]]:
    """The 11 market components; lot_size and trailing_stop come from economics / sl_tsl."""
    side = normalize_side(c.side)
    oi = _opt(c.p_oi)
    if oi is None:
        oi = oi_score(c.oi_positioning, side) if c.oi_positioning else None
    return {
        "probability": _opt(c.probability),
        "indicator": signed_to_score(c.indicator_score, side, scale=2.0),
        "volume": volume_score(c),
        "market_regime": regime_score(c),
        "bidask": bidask_score(c),
        "oi": oi,
        "greeks": _opt(c.greeks_score),
        "liquidity": _opt(c.liquidity_score),
        "expected_move": expected_move_score(c),
        "greeks_change": greeks_change_score(c, cfg),
        "strike": _opt(c.strike_score),
    }

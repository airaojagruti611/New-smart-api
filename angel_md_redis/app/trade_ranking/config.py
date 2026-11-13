"""
Trade Ranking Engine (Module 13) — weights, thresholds and trading profiles
(DECISION.md §6 R4, R8). Every value is overridable by env in run_trade_ranking.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Tuple

# R4 — brief §4 weights (sum 100). Normalised to fractions by score.weighted().
WEIGHTS: Dict[str, float] = {
    "probability": 20.0,
    "indicator": 8.0,
    "volume": 8.0,
    "market_regime": 10.0,
    "bidask": 8.0,
    "oi": 8.0,
    "greeks": 6.0,
    "liquidity": 8.0,
    "expected_move": 7.0,
    "greeks_change": 5.0,
    "strike": 5.0,
    "lot_size": 4.0,
    "trailing_stop": 3.0,
}

# R6 — missing / stale → DATA_INSUFFICIENT instead of a score.
CRITICAL_COMPONENTS: Tuple[str, ...] = ("probability", "liquidity", "bidask", "greeks", "strike")

# R4 — SIE market phase fit for the regime component.
PHASE_FIT: Dict[str, float] = {
    "STRONG_TREND": 100.0,
    "NORMAL_TREND": 70.0,
    "HIGH_VOLATILITY": 50.0,
    "EXPIRY_DAY": 50.0,
}

# R12 — decision bands (lower bound inclusive).
BAND_REJECT_MAX = 50.0          # < 50 REJECT
BAND_CONDITIONAL_MIN = 60.0     # 60-69 CONDITIONAL
BAND_TAKE_MIN = 70.0            # 70-79 TAKE
BAND_HIGH_MIN = 80.0            # 80-89 HIGH_CONVICTION
BAND_EXCEPTIONAL_MIN = 90.0     # >= 90 EXCEPTIONAL


@dataclass(frozen=True)
class Profile:
    """R8 — our probability is a 0-100 score (< 65 never trades in ICARE)."""
    name: str
    min_probability: float
    min_trade_score: float
    min_ev_r: float
    allow_conditional: bool


PROFILES: Dict[str, Profile] = {
    "conservative": Profile("conservative", 75.0, 75.0, 0.30, False),
    "normal_intraday": Profile("normal_intraday", 65.0, 70.0, 0.15, False),
    "aggressive": Profile("aggressive", 65.0, 60.0, 0.05, True),
}
DEFAULT_PROFILE = "normal_intraday"


def get_profile(name: str) -> Profile:
    return PROFILES.get((name or "").strip().lower(), PROFILES[DEFAULT_PROFILE])


@dataclass(frozen=True)
class RankConfig:
    profile: Profile = field(default_factory=lambda: PROFILES[DEFAULT_PROFILE])
    weights: Dict[str, float] = field(default_factory=lambda: dict(WEIGHTS))
    # R6 data quality
    dq_missing_penalty: float = 0.5          # dq = 1 - 0.5 x missing weight share
    # R7 direction
    agreement_full: float = 0.80
    min_agreement: float = 0.50
    min_decisive_votes: int = 3
    low_evidence_factor: float = 0.90
    conflict_floor: float = 0.50
    # R9 economics
    risk_factor_base: float = 0.70
    risk_factor_slope: float = 0.15          # full (1.0) at reward/risk >= 2
    # R10 lot feasibility
    target_lots: int = 2
    # R11 TSL quality: gain / max(SL %, TSL %) -> 0 at 0.5, 100 at 3
    tsl_quality_lo: float = 0.5
    tsl_quality_hi: float = 3.0
    # R4 greeks change: projected gain % of premium, 100 at >= 30 %
    greeks_change_full_pct: float = 30.0
    # R13 hard gates
    min_liquidity: float = 70.0
    max_spread_pct: float = 3.0
    circuit_band_pct: float = 1.0
    # R14 portfolio filter
    max_per_sector: int = 1
    # R15 reasons / warnings thresholds
    reason_min: float = 80.0
    warning_max: float = 40.0

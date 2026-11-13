"""
Trade Ranking Engine — proposed SL / TSL quality (DECISION.md §6 R11).

The TSL % is Module 18's own selector (app/adaptive_tsl) run on a snapshot of
the contract before entry; activation = tsl % (T7: trail starts once the stop
would reach breakeven). Ranking only SCORES the proposal:
    tsl_quality = expected_gain % / max(initial SL %, TSL %)  -> 0 at lo, 100 at hi
It never gates entry by itself (brief §27).
"""

from __future__ import annotations

from typing import Optional, Tuple

from app.adaptive_tsl import (
    Snapshot,
    TSLConfig,
    gamma_component,
    select_tsl_pct,
    trend_strength,
    volatility_score,
)
from app.trade_ranking.config import RankConfig
from app.trade_ranking.normalizer import lin


def propose_tsl(snap: Snapshot, side: str, cfg: TSLConfig) -> Tuple[float, str]:
    """(trailing %, rule) Module 18 would pick for this contract right now."""
    vol, band, _, _ = volatility_score(snap, cfg)
    _, trend, _, _ = trend_strength(snap, side, cfg)
    return select_tsl_pct(trend, vol, band, snap.dte, gamma_component(snap.gamma, snap.spot, cfg), snap.sideways, cfg)


def tsl_quality(expected_gain_pct: Optional[float], initial_sl_pct: Optional[float],
                tsl_pct: Optional[float], cfg: RankConfig) -> Optional[float]:
    if expected_gain_pct is None:
        return None
    risk = max(x for x in (initial_sl_pct, tsl_pct, 0.0) if x is not None)
    if risk <= 0:
        return None
    return lin(expected_gain_pct / risk, cfg.tsl_quality_lo, cfg.tsl_quality_hi)

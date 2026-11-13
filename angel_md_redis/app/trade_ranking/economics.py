"""
Trade Ranking Engine — expected value, reward/risk and lot feasibility
(DECISION.md §6 R9, R10). Reuses ICARE's pure helpers so SL, EV and lot
formulas have one source of truth.

  initial SL   = icare.stop_loss_points (SIE adverse repricing, clamped 10-30 %)
  EV per lot   = icare.ev_breakdown NET of round-trip charges for the feasible lots
                 (model until the journal bucket has >= 30 trades); gross_ev_per_lot /
                 charges reported alongside
  ev_per_risk  = EV per lot / risk per lot
  reward_risk  = projected gain / SL points
  risk_factor  = clamp(base + slope x reward_risk, base, 1)
  lots         = MIN(icare.presize_lots)  (no quality class / portfolio here; ICARE decides),
                 incl. the remaining daily-loss room (same formula as ICARE)
  lot_score    = 60 + 40 x min(1, lots / target_lots); 0 lots -> LOT_NOT_FEASIBLE
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from app.icare import ICAREConfig, ICAREInputs, daily_loss_room, ev_breakdown, presize_lots, stop_loss_points
from app.trade_ranking.candidate import Candidate, Context
from app.trade_ranking.config import RankConfig


@dataclass(frozen=True)
class Economics:
    sl_pts: Optional[float]
    initial_sl_pct: Optional[float]
    expected_gain_pct: Optional[float]
    reward_risk: Optional[float]
    risk_factor: float
    ev_per_lot: Optional[float]
    ev_source: str
    win_probability: Optional[float]
    risk_per_lot: Optional[float]
    ev_per_risk: Optional[float]
    lot_limits: Dict[str, int]
    feasible_lots: int
    lot_score: Optional[float]
    capital_required: Optional[float]
    risk_amount: Optional[float]
    expected_value: Optional[float]           # net EV x feasible lots
    gross_ev_per_lot: Optional[float] = None  # before charges
    charges: Optional[float] = None           # expected round-trip charges for the feasible lots


def risk_factor(reward_risk: Optional[float], cfg: RankConfig) -> float:
    if reward_risk is None:
        return cfg.risk_factor_base
    return round(max(cfg.risk_factor_base, min(1.0, cfg.risk_factor_base + cfg.risk_factor_slope * reward_risk)), 4)


def lot_score(lots: int, cfg: RankConfig) -> Optional[float]:
    if lots <= 0:
        return None
    return round(60.0 + 40.0 * min(1.0, lots / max(cfg.target_lots, 1)), 4)


def evaluate(c: Candidate, ctx: Context, cfg: RankConfig, icfg: ICAREConfig) -> Economics:
    premium, lot = c.premium, c.lot_size
    if not premium or premium <= 0 or not lot or lot <= 0:
        return Economics(None, None, None, None, cfg.risk_factor_base, None, "none", None, None, None,
                         {}, 0, None, None, None, None)
    sl_pts = stop_loss_points(premium, c.adverse_change, icfg)
    gain = c.projected_gain if c.projected_gain is not None and c.projected_gain > 0 else None
    rr = round(gain / sl_pts, 4) if gain and sl_pts else None
    ev = p_win = gross = charges = None
    source = "none"
    risk_lot = round(sl_pts * lot, 2)
    room = daily_loss_room(ctx.total_capital, ctx.day_pnl, ctx.daily_loss_limit_pct) if ctx.total_capital > 0 else None
    limits = presize_lots(premium, lot, sl_pts, ctx.available_margin, ctx.total_capital, c.liquidity_max_lots, icfg,
                          risk_room=room)
    lots = min(limits.values()) if limits else 0
    if gain:
        inp = ICAREInputs(
            symbol=c.symbol, side=c.side, tradingsymbol=c.tradingsymbol, strike=c.strike or 0.0,
            premium=premium, lot_size=lot, probability=c.probability,
            probability_decision=c.probability_decision,
            history_samples=c.history_samples, history_win_rate=c.history_win_rate,
            history_avg_win_pct=c.history_avg_win_pct, history_avg_loss_pct=c.history_avg_loss_pct,
        )
        bd = ev_breakdown(inp, premium, lot, sl_pts, gain, icfg, lots=max(lots, 1))
        if bd is not None:
            ev, source, p_win = bd["net_ev"], bd["source"], bd["p_win"]
            gross, charges = bd["gross_ev"], bd["charges"]
    return Economics(
        sl_pts=sl_pts,
        initial_sl_pct=round(sl_pts / premium * 100.0, 4),
        expected_gain_pct=None if gain is None else round(gain / premium * 100.0, 4),
        reward_risk=rr,
        risk_factor=risk_factor(rr, cfg),
        ev_per_lot=ev,
        ev_source=source,
        win_probability=p_win,
        risk_per_lot=risk_lot,
        ev_per_risk=None if ev is None or risk_lot <= 0 else round(ev / risk_lot, 4),
        lot_limits=limits,
        feasible_lots=lots,
        lot_score=lot_score(lots, cfg),
        capital_required=round(lots * premium * lot, 2),
        risk_amount=round(lots * risk_lot, 2),
        expected_value=None if ev is None else round(ev * max(lots, 1), 2),
        gross_ev_per_lot=gross,
        charges=charges,
    )

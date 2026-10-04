"""
Trade Ranking Engine — hard gates, ranking and correlation filter
(DECISION.md §6 R13, R14, brief §18–§21).

Gates can never be overridden by the score. Ranking sorts eligible
candidates (score, then EV per risk, then probability), keeps one side per
underlying, at most `max_per_sector` new trades per sector (open positions
count), and no more than the free slots. Nothing is ever forced: an empty
survivor list is NO_TRADE.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import Dict, List, Optional, Tuple

from app.trade_ranking.candidate import Candidate, Context, RankResult
from app.trade_ranking.config import RankConfig

GATE_REASONS = {
    "kill_switch": "KILL_SWITCH_ACTIVE",
    "probability": "PROBABILITY_BELOW_PROFILE",
    "liquidity": "LIQUIDITY_BELOW_MIN",
    "spread": "SPREAD_ABOVE_MAX",
    "lot_fit": "LOT_NOT_FEASIBLE",
    "duplicate": "DUPLICATE_POSITION",
    "reentry_block": "REENTRY_BLOCKED",
    "daily_loss": "DAILY_LOSS_LIMIT",
    "exposure": "PORTFOLIO_EXPOSURE_LIMIT",
    "circuit": "CIRCUIT_LIMIT",
    "direction": "DIRECTION_CONFLICT",
    "ev": "NEGATIVE_EV",
    "ev_min": "LOW_EV",
}


def hard_gates(
    c: Candidate, ctx: Context, cfg: RankConfig, *, feasible_lots: int, direction_conflict: bool,
    ev_per_lot: Optional[float], ev_per_risk: Optional[float],
) -> Tuple[Dict[str, bool], List[str]]:
    """(gate -> passed, flags). Unknown inputs pass the gate only where the brief allows (flagged)."""
    flags: List[str] = []
    capital = max(ctx.total_capital, 0.0)
    g: Dict[str, bool] = {}
    g["kill_switch"] = not ctx.kill_switch
    g["probability"] = c.probability is not None and c.probability >= cfg.profile.min_probability
    g["liquidity"] = (
        c.liquidity_score is not None and c.liquidity_score >= cfg.min_liquidity
        and (c.liquidity_band or "").strip().upper() != "RED"
    )
    g["spread"] = c.spread_pct is not None and c.spread_pct <= cfg.max_spread_pct
    g["lot_fit"] = feasible_lots > 0
    g["duplicate"] = c.symbol.upper() not in ctx.open_symbols
    g["reentry_block"] = f"{c.symbol.upper()}:{c.side.upper()}" not in ctx.blocked
    g["daily_loss"] = not (capital > 0 and ctx.day_pnl <= -capital * ctx.daily_loss_limit_pct / 100.0)
    g["exposure"] = (
        ctx.open_positions < ctx.max_open_trades
        and ctx.open_risk < capital * ctx.max_portfolio_risk_pct / 100.0
    )
    if c.ltp is not None and c.upper_circuit and c.lower_circuit:
        band = cfg.circuit_band_pct / 100.0
        g["circuit"] = c.lower_circuit * (1 + band) < c.ltp < c.upper_circuit * (1 - band)
    else:
        g["circuit"] = True
        flags.append("CIRCUIT_UNCHECKED")
    g["direction"] = not direction_conflict
    g["ev"] = ev_per_lot is not None and ev_per_lot > 0
    g["ev_min"] = ev_per_risk is not None and ev_per_risk >= cfg.profile.min_ev_r
    return g, flags


def failed_reasons(gates: Dict[str, bool]) -> List[str]:
    out = [GATE_REASONS[k] for k, ok in gates.items() if not ok]
    if "NEGATIVE_EV" in out and "LOW_EV" in out:
        out.remove("LOW_EV")
    return out


def _sort_key(r: RankResult):
    return (
        -(r.trade_score if r.trade_score is not None else -1.0),
        -(r.ev_per_risk if r.ev_per_risk is not None else -1e9),
        -(r.probability if r.probability is not None else -1.0),
        r.symbol,
        r.side,
    )


@dataclass(frozen=True)
class CycleSummary:
    scanned: int
    data_insufficient: int
    rejected: int
    watch: int
    eligible: int
    taken: int
    outcome: str                          # TRADE / NO_TRADE
    taken_ids: List[str]

    def to_dict(self) -> dict:
        return asdict(self)


def _demote(r: RankResult, reason: str) -> RankResult:
    return replace(r, decision="WATCH", reject_reasons=r.reject_reasons + [reason])


def rank(results: List[RankResult], ctx: Context, cfg: RankConfig) -> Tuple[List[RankResult], CycleSummary]:
    scored = sorted([r for r in results if r.trade_score is not None], key=_sort_key)
    unscored = [r for r in results if r.trade_score is None]
    eligible = sum(1 for r in scored if r.decision == "TAKE_TRADE")

    slots = max(ctx.max_open_trades - ctx.open_positions, 0)
    sector_count = dict(ctx.open_sectors)
    taken_syms: set = set()
    out: List[RankResult] = []
    taken: List[str] = []
    for i, r in enumerate(scored, start=1):
        r = replace(r, rank=i)
        if r.decision == "TAKE_TRADE":
            sec = r.sector
            if r.symbol in taken_syms:
                r = _demote(r, "CORRELATED_SAME_UNDERLYING")
            elif sec and sector_count.get(sec, 0) >= cfg.max_per_sector:
                r = _demote(r, "CORRELATED_SECTOR")
            elif len(taken) >= slots:
                r = _demote(r, "NO_SLOT")
            else:
                taken.append(r.candidate_id)
                taken_syms.add(r.symbol)
                if sec:
                    sector_count[sec] = sector_count.get(sec, 0) + 1
        out.append(r)
    out.extend(unscored)

    summary = CycleSummary(
        scanned=len(results),
        data_insufficient=sum(1 for r in out if r.decision == "DATA_INSUFFICIENT"),
        rejected=sum(1 for r in out if r.decision == "REJECT"),
        watch=sum(1 for r in out if r.decision == "WATCH"),
        eligible=eligible,
        taken=len(taken),
        outcome="TRADE" if taken else "NO_TRADE",
        taken_ids=taken,
    )
    return out, summary

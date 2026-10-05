"""
Trade Ranking Engine (Module 13) — controller (DECISION.md §6, brief §24).

    validate -> normalise -> direction -> economics (EV, RR, lots) -> SL/TSL
    -> trade score -> hard gates -> decision            [evaluate(), per candidate]
    -> rank -> correlation / slot filter -> NO_TRADE    [rank(), all candidates]

Pure: no Redis, no clock. Never places orders, never picks strikes, never
sets the real SL — ICARE and Module 18 own those.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

from app.icare import ICAREConfig
from app.probability_engine import normalize_side
from app.trade_ranking import direction as direction_mod
from app.trade_ranking import economics as economics_mod
from app.trade_ranking.candidate import Candidate, Context, RankResult
from app.trade_ranking.config import RankConfig
from app.trade_ranking.normalizer import normalize
from app.trade_ranking.portfolio import CycleSummary, failed_reasons, hard_gates
from app.trade_ranking.portfolio import rank as rank_results
from app.trade_ranking.score import decide, reasons_and_warnings, trade_score, weighted
from app.trade_ranking.sl_tsl import tsl_quality
from app.trade_ranking.validator import validate


class TradeRankingEngine:
    def __init__(self, cfg: Optional[RankConfig] = None, icare_cfg: Optional[ICAREConfig] = None):
        self.cfg = cfg or RankConfig()
        self.icare_cfg = icare_cfg or ICAREConfig()

    def evaluate(self, c: Candidate, ctx: Context) -> RankResult:
        cfg = self.cfg
        side = normalize_side(c.side)
        comps = normalize(c, cfg)
        eco = economics_mod.evaluate(c, ctx, cfg, self.icare_cfg)
        comps["lot_size"] = eco.lot_score
        comps["trailing_stop"] = tsl_quality(eco.expected_gain_pct, eco.initial_sl_pct, c.tsl_pct, cfg)
        for k in c.stale:
            if k in comps:
                comps[k] = None

        val = validate(c, comps, cfg)
        d = direction_mod.analyze(c, cfg)
        gates, gate_flags = hard_gates(
            c, ctx, cfg, feasible_lots=eco.feasible_lots, direction_conflict=d.conflict,
            ev_per_lot=eco.ev_per_lot, ev_per_risk=eco.ev_per_risk,
        )
        flags = list(d.flags) + gate_flags
        if not c.sector:
            flags.append("SECTOR_UNKNOWN")

        w = weighted(comps, cfg.weights)
        score = None
        confidence = ""
        reject: List[str] = []
        if not val.sufficient:
            status, decision = "DATA_INSUFFICIENT", "DATA_INSUFFICIENT"
            reject = [f"MISSING_{k.upper()}" for k in val.critical_missing]
        else:
            score = trade_score(w or 0.0, d.conflict_factor, eco.risk_factor, val.dq)
            failed = failed_reasons(gates)
            decision, confidence, band_reasons = decide(score, cfg.profile)
            if failed:
                status, decision = "REJECT", "REJECT"
                reject = failed
            else:
                status = "SCORED"
                reject = band_reasons
        reasons, warnings = reasons_and_warnings(comps, d.agreement, cfg)
        if eco.ev_source == "model":
            warnings.append("EV from model (journal bucket < min samples)")

        tsl = c.tsl_pct
        return RankResult(
            symbol=c.symbol.upper(),
            option=c.tradingsymbol,
            side=side,
            direction="CALL" if side == "CE" else "PUT",
            candidate_id=c.candidate_id,
            status=status,
            decision=decision,
            confidence=confidence if decision == "TAKE_TRADE" or confidence == "CONDITIONAL" else "",
            trade_score=score,
            weighted_score=w,
            rank=None,
            probability=c.probability,
            components=comps,
            agreement=d.agreement,
            votes=d.votes,
            conflict_factor=d.conflict_factor,
            risk_factor=eco.risk_factor,
            dq=val.dq,
            missing=val.missing,
            gates=gates,
            expected_gain_pct=eco.expected_gain_pct,
            expected_value=eco.expected_value,
            ev_source=eco.ev_source,
            ev_per_risk=eco.ev_per_risk,
            reward_risk=eco.reward_risk,
            feasible_lots=eco.feasible_lots,
            capital_required=eco.capital_required,
            risk_amount=eco.risk_amount,
            initial_stop_loss_pct=eco.initial_sl_pct,
            trailing_stop_pct=tsl,
            trailing_activation_pct=tsl,
            tsl_rule=c.tsl_rule,
            reasons=reasons,
            warnings=warnings,
            reject_reasons=reject,
            flags=flags,
            sector=c.sector,
            gross_ev=eco.gross_ev_per_lot,
            charges=eco.charges,
        )

    def rank(self, candidates: List[Candidate], ctx: Context) -> Tuple[List[RankResult], CycleSummary]:
        return rank_results([self.evaluate(c, ctx) for c in candidates], ctx, self.cfg)

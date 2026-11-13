"""
Trade Ranking Engine — directional agreement & conflict penalty
(DECISION.md §6 R7, brief §7–§8).

Each module votes FOR / AGAINST / NEUTRAL relative to the trade side.
    agreement       = FOR / (FOR + AGAINST)          (neutral ignored)
    conflict_factor = clamp(agreement / agreement_full, conflict_floor, 1)
agreement < min_agreement -> hard reject DIRECTION_CONFLICT.
Fewer than min_decisive_votes decisive votes -> factor low_evidence_factor.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from app.probability_engine import _VOLUME_MAP, normalize_side
from app.trade_ranking.candidate import Candidate
from app.trade_ranking.config import RankConfig
from app.trade_ranking.normalizer import contract_alignment, label_alignment

FOR, AGAINST, NEUTRAL = "FOR", "AGAINST", "NEUTRAL"


def _sign_vote(value: Optional[float], side: str) -> Optional[str]:
    if value is None:
        return None
    if value == 0:
        return NEUTRAL
    return FOR if (value > 0) == (normalize_side(side) == "CE") else AGAINST


def amd_vote(phase: Optional[str]) -> Optional[str]:
    """Greeks phase of the contract itself (we only buy): MARKUP / ACCUMULATION favour it."""
    p = (phase or "").strip().upper()
    if not p:
        return None
    if p in ("MARKUP", "ACCUMULATION"):
        return FOR
    if p == "DISTRIBUTION":
        return AGAINST
    return NEUTRAL


def collect_votes(c: Candidate) -> Dict[str, str]:
    raw = {
        "indicator": _sign_vote(c.indicator_score, c.side),
        "volume": _sign_vote(_VOLUME_MAP.get((c.volume_signal or "").strip().upper()), c.side),
        "market_regime": label_alignment(c.regime, c.side),
        "bidask": contract_alignment(c.imbalance),      # the contract's own book: not side-flipped
        "oi": label_alignment(c.oi_positioning, c.side),
        "greeks": amd_vote(c.amd_phase),
        "expected_move": label_alignment(c.em_direction, c.side),
        "htf": label_alignment(c.htf_bias, c.side),
        "supertrend": label_alignment(c.st_bias, c.side),
    }
    return {k: v for k, v in raw.items() if v is not None}


@dataclass(frozen=True)
class Direction:
    votes: Dict[str, str]
    n_for: int
    n_against: int
    n_neutral: int
    agreement: Optional[float]
    conflict_factor: float
    conflict: bool
    flags: List[str]


def analyze(c: Candidate, cfg: RankConfig) -> Direction:
    votes = collect_votes(c)
    n_for = sum(1 for v in votes.values() if v == FOR)
    n_against = sum(1 for v in votes.values() if v == AGAINST)
    n_neutral = sum(1 for v in votes.values() if v == NEUTRAL)
    decisive = n_for + n_against
    flags: List[str] = []
    agreement = round(n_for / decisive, 4) if decisive else None
    if decisive < cfg.min_decisive_votes:
        flags.append("LOW_DIRECTIONAL_EVIDENCE")
        factor = cfg.low_evidence_factor
        conflict = agreement is not None and agreement < cfg.min_agreement and decisive >= 2
    else:
        ratio = n_for / decisive
        factor = max(cfg.conflict_floor, min(1.0, ratio / cfg.agreement_full))
        conflict = ratio < cfg.min_agreement
    return Direction(
        votes=votes, n_for=n_for, n_against=n_against, n_neutral=n_neutral,
        agreement=agreement, conflict_factor=round(factor, 4), conflict=conflict, flags=flags,
    )

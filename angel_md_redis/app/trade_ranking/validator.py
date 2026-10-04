"""
Trade Ranking Engine — candidate validator (DECISION.md §6 R6).

Missing ≠ 0: a missing / stale CRITICAL input makes the candidate
DATA_INSUFFICIENT (not scored, not rejected); other missing components are
excluded, the weights renormalised, and a data-quality factor applied:
    dq = 1 - dq_missing_penalty x (missing weight / total weight)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from app.trade_ranking.candidate import Candidate
from app.trade_ranking.config import CRITICAL_COMPONENTS, RankConfig


@dataclass(frozen=True)
class Validation:
    missing: List[str]
    critical_missing: List[str]
    dq: float

    @property
    def sufficient(self) -> bool:
        return not self.critical_missing


def validate(c: Candidate, components: Dict[str, Optional[float]], cfg: RankConfig) -> Validation:
    missing = sorted(k for k in cfg.weights if components.get(k) is None or k in c.stale)
    critical = [k for k in CRITICAL_COMPONENTS if k in missing]
    if not c.premium or c.premium <= 0:
        critical.append("premium")
    if not c.lot_size or c.lot_size <= 0:
        critical.append("lot_size_contract")
    total = sum(cfg.weights.values()) or 1.0
    share = sum(cfg.weights[k] for k in missing if k not in CRITICAL_COMPONENTS) / total
    dq = round(max(0.0, 1.0 - cfg.dq_missing_penalty * share), 4)
    return Validation(missing=missing, critical_missing=critical, dq=dq)

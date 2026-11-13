"""
Bucket statistics over paper/real trade outcomes (pure). See DECISION.MD C.

hist_score (0-100): 50 = neutral. Blend of shrunk win rate and shrunk
expectancy; both are empirical-Bayes shrunk toward the global mean with
prior strength LEARN_PRIOR_N (pseudo-trades), so small buckets stay near the
global average. Consumers must gate on n >= LEARN_MIN_SAMPLES themselves.
"""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List, Optional

PRIOR_N = float(os.getenv("LEARN_PRIOR_N", "20"))
EXP_SCALE_PCT = float(os.getenv("LEARN_EXP_SCALE_PCT", "20"))  # +/-20% mean return => 0/100


def _f(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class BucketStats:
    n: int
    wins: int
    win_rate: float          # raw, 0-1
    expectancy: float        # raw mean pnl_pct
    avg_mfe: float           # mean mfe_pct
    avg_mae: float           # mean mae_pct
    shrunk_win_rate: float
    shrunk_expectancy: float
    hist_score: float        # 0-100

    def to_dict(self) -> Dict[str, float]:
        return asdict(self)


def shrink(sum_x: float, n: int, prior_mean: float, prior_n: float = PRIOR_N) -> float:
    """Empirical-Bayes posterior mean: (sum + prior_n*prior_mean) / (n + prior_n)."""
    d = n + prior_n
    return (sum_x + prior_n * prior_mean) / d if d > 0 else prior_mean


def hist_score(shr_wr: float, shr_exp_pct: float, scale: float = EXP_SCALE_PCT) -> float:
    wr_part = max(0.0, min(100.0, shr_wr * 100.0))
    exp_part = max(0.0, min(100.0, 50.0 + 50.0 * shr_exp_pct / scale)) if scale > 0 else 50.0
    return round(0.5 * wr_part + 0.5 * exp_part, 2)


def usable(row: Dict[str, str]) -> bool:
    if str(row.get("excluded_from_stats", "")).lower() == "true":
        return False
    return _f(row.get("pnl_pct")) is not None and bool(row.get("bucket_key"))


def compute_stats(rows: Iterable[Dict[str, str]], prior_n: float = PRIOR_N) -> Dict[str, BucketStats]:
    good: List[Dict[str, str]] = [r for r in rows if usable(r)]
    if not good:
        return {}
    N = len(good)
    g_wr = sum(1 for r in good if _f(r["pnl_pct"]) > 0) / N
    g_exp = sum(_f(r["pnl_pct"]) for r in good) / N
    groups: Dict[str, List[Dict[str, str]]] = {}
    for r in good:
        groups.setdefault(r["bucket_key"], []).append(r)
    out: Dict[str, BucketStats] = {}
    for key, rs in groups.items():
        n = len(rs)
        pnl = [_f(r["pnl_pct"]) for r in rs]
        wins = sum(1 for p in pnl if p > 0)
        mfe = [m for m in (_f(r.get("mfe_pct")) for r in rs) if m is not None]
        mae = [m for m in (_f(r.get("mae_pct")) for r in rs) if m is not None]
        swr = shrink(wins, n, g_wr, prior_n)
        sexp = shrink(sum(pnl), n, g_exp, prior_n)
        out[key] = BucketStats(
            n=n, wins=wins, win_rate=wins / n, expectancy=sum(pnl) / n,
            avg_mfe=sum(mfe) / len(mfe) if mfe else 0.0,
            avg_mae=sum(mae) / len(mae) if mae else 0.0,
            shrunk_win_rate=swr, shrunk_expectancy=sexp, hist_score=hist_score(swr, sexp),
        )
    return out

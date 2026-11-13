import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.strike_learning import compute_stats, shrink


def row(key, pnl, mfe=10.0, mae=5.0, excl="false"):
    return {"bucket_key": key, "pnl_pct": str(pnl), "mfe_pct": str(mfe), "mae_pct": str(mae),
            "excluded_from_stats": excl}


def test_shrink_formula():
    assert shrink(0, 0, 0.5, 20) == 0.5
    assert abs(shrink(10, 10, 0.0, 20) - 10 / 30) < 1e-9


def test_group_stats_and_exclusion():
    rows = [row("A", 10), row("A", -5), row("A", 20, mfe=30, mae=1), row("B", -10),
            row("B", 99, excl="true"), {"bucket_key": "C", "pnl_pct": "", "excluded_from_stats": "false"}]
    s = compute_stats(rows, prior_n=20)
    assert set(s) == {"A", "B"}
    assert s["A"].n == 3 and s["A"].wins == 2
    assert abs(s["A"].win_rate - 2 / 3) < 1e-9 and abs(s["A"].expectancy - 25 / 3) < 1e-9
    assert abs(s["A"].avg_mfe - 50 / 3) < 1e-9
    assert s["B"].n == 1


def test_small_bucket_shrinks_toward_global():
    rows = [row("BIG", 5 if i % 2 else -5) for i in range(40)] + [row("TINY", 50)]
    s = compute_stats(rows, prior_n=20)
    assert s["TINY"].win_rate == 1.0
    assert s["TINY"].shrunk_win_rate < 0.6          # pulled to ~0.5 global
    assert 45 < s["TINY"].hist_score < 65           # near neutral, not 100
    assert compute_stats([]) == {}

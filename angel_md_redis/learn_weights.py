#!/usr/bin/env python3
"""
learn_weights.py
────────────────
Offline adaptive-learning report (DECISION.md D11). READ-ONLY: prints what
the journal says; never edits weights or config. A human decides what to
change (env vars / constants) after reading it.

Answers the design's questions from closed (paper) trades:
  * Which delta range performed best per market phase / holding time?
  * Did lower theta risk produce better net returns?
  * Did wider bid-ask spreads reduce realized performance?
  * Which probability / trade-quality ranges actually paid (calibration)?
  * Did larger allocations (risk class) improve expectancy?
  * Module 18 exit policy: fixed SL/target/time vs the adaptive trailing stop
    (shadow counterfactual tsl_shadow_pnl), by TSL rule; re-entry results.
  * Module 13 trade ranking (DECISION.md §6 R16): results by ranking score band /
    decision / confidence (in shadow mode: did trades the ranker would have
    rejected lose money?), per-component lift, EV calibration.

Usage:
    python learn_weights.py                    # journal from Redis md:journal
    python learn_weights.py --parquet          # journal from data_lake/stream=md_journal
    python learn_weights.py --min-trades 20    # bucket size needed for a suggestion
"""

from __future__ import annotations

import argparse
import glob
import os
import sys

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
JOURNAL_STREAM = os.getenv("STREAM_JOURNAL", "md:journal")
NUMERIC = ("pnl", "pnl_pct", "mfe", "mae", "holding_minutes", "delta", "theta_per_day", "spread_pct",
           "liquidity_score", "probability", "trade_quality", "expected_value", "executed_lots", "win",
           "entry_premium", "hold_minutes", "tsl_shadow_pnl", "tsl_pct", "reentry_no", "trade_score", "rank")


def load_redis() -> pd.DataFrame:
    import redis
    r = redis.from_url(REDIS_URL, decode_responses=True)
    rows = [f for _id, f in r.xrange(JOURNAL_STREAM, "-", "+")]
    return pd.DataFrame(rows)


def load_parquet() -> pd.DataFrame:
    files = [f for f in glob.glob("data_lake/stream=md_journal/**/*.parquet", recursive=True) if os.path.getsize(f) > 0]
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in NUMERIC:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["abs_delta"] = df["delta"].abs()
    df["delta_bucket"] = pd.cut(df["abs_delta"], [0, 0.35, 0.45, 0.55, 0.65, 0.75, 1.0])
    df["hold_bucket"] = pd.cut(df["holding_minutes"], [0, 15, 30, 60, 120, 400], include_lowest=True)
    df["spread_bucket"] = pd.cut(df["spread_pct"], [0, 0.5, 1.0, 2.0, 3.0, 100], include_lowest=True)
    theta_pct = (df["theta_per_day"].abs() * df["holding_minutes"] / 375.0) / df["entry_premium"] * 100.0
    df["theta_risk_bucket"] = pd.cut(theta_pct, [0, 1, 2, 5, 10, 100], include_lowest=True)
    df["prob_bucket"] = pd.cut(df["probability"], [0, 50, 65, 75, 85, 101], right=False)
    df["quality_bucket"] = pd.cut(df["trade_quality"], [0, 70, 80, 90, 95, 101], right=False)
    if "trade_score" in df:
        df["rank_band"] = pd.cut(df["trade_score"], [0, 50, 60, 70, 80, 90, 101], right=False)
    return df


def summarize(df: pd.DataFrame, by) -> pd.DataFrame:
    g = df.groupby(by, observed=True)
    out = pd.DataFrame({
        "trades": g.size(),
        "win_rate": g["win"].mean().round(3),
        "avg_pnl": g["pnl"].mean().round(2),
        "expectancy_pct": g["pnl_pct"].mean().round(2),
        "avg_mfe": g["mfe"].mean().round(2),
        "avg_mae": g["mae"].mean().round(2),
        "total_pnl": g["pnl"].sum().round(2),
    })
    return out


def exit_policy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fixed exits vs Module 18 on the SAME trades. tsl_pnl = the trailing stop's
    would-be exit (shadow) where it fired, else the realized PnL (the trailing
    stop never fired, so the trade would have ended the same way).
    """
    if "tsl_status" not in df or "tsl_shadow_pnl" not in df:
        return pd.DataFrame()
    t = df[df["tsl_status"].fillna("").astype(str) != ""].copy()
    if t.empty:
        return pd.DataFrame()
    t["tsl_pnl"] = t["tsl_shadow_pnl"].where(t["tsl_shadow_pnl"].notna(), t["pnl"])
    t["tsl_fired"] = t["tsl_shadow_pnl"].notna()
    g = t.groupby(t["tsl_rule"].fillna("").replace("", "NOT_ACTIVATED"), observed=True)
    return pd.DataFrame({
        "trades": g.size(),
        "tsl_fired": g["tsl_fired"].sum(),
        "fixed_total_pnl": g["pnl"].sum().round(2),
        "tsl_total_pnl": g["tsl_pnl"].sum().round(2),
        "fixed_win_rate": g["win"].mean().round(3),
        "tsl_win_rate": (g["tsl_pnl"].apply(lambda s: (s > 0).mean())).round(3),
    })


def ranking_component_lift(df: pd.DataFrame, cut: float = 70.0) -> pd.DataFrame:
    """
    Per Module 13 component: win rate / expectancy when the component was >= cut
    vs below it. A component whose 'high' group does not beat its 'low' group is
    a candidate for a lower weight (human decision, D11).
    """
    if "rank_components" not in df:
        return pd.DataFrame()
    comps = df["rank_components"].map(lambda v: _json_dict(v)).apply(pd.Series)
    if comps.empty:
        return pd.DataFrame()
    rows = []
    for c in comps.columns:
        v = pd.to_numeric(comps[c], errors="coerce")
        hi, lo = df[v >= cut], df[v < cut]
        rows.append({
            "component": c, "n_high": len(hi), "n_low": len(lo),
            "win_high": round(hi["win"].mean(), 3) if len(hi) else None,
            "win_low": round(lo["win"].mean(), 3) if len(lo) else None,
            "exp_pct_high": round(hi["pnl_pct"].mean(), 2) if len(hi) else None,
            "exp_pct_low": round(lo["pnl_pct"].mean(), 2) if len(lo) else None,
        })
    return pd.DataFrame(rows).set_index("component")


def ev_calibration(df: pd.DataFrame) -> pd.DataFrame:
    """ICARE's EV per lot x lots (what was expected) vs realised PnL, by EV source."""
    if "expected_value" not in df or "executed_lots" not in df:
        return pd.DataFrame()
    t = df.dropna(subset=["expected_value"]).copy()
    if t.empty:
        return pd.DataFrame()
    t["expected_pnl"] = t["expected_value"] * t["executed_lots"].fillna(0)
    g = t.groupby(t["ev_source"].fillna("") if "ev_source" in t else pd.Series("", index=t.index), observed=True)
    return pd.DataFrame({
        "trades": g.size(),
        "avg_expected_pnl": g["expected_pnl"].mean().round(2),
        "avg_realised_pnl": g["pnl"].mean().round(2),
        "total_expected": g["expected_pnl"].sum().round(2),
        "total_realised": g["pnl"].sum().round(2),
    })


def load_exec(use_parquet: bool) -> pd.DataFrame:
    """Module 14 final reports (event=REPORT) and missed-move checks from md:exec."""
    if use_parquet:
        files = [f for f in glob.glob("data_lake/stream=md_exec/**/*.parquet", recursive=True) if os.path.getsize(f) > 0]
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True) if files else pd.DataFrame()
    import redis
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
    rows = [f for _id, f in r.xrange(os.getenv("STREAM_EXEC", "md:exec"), "-", "+")]
    return pd.DataFrame(rows)


def execution_report(ex: pd.DataFrame) -> dict:
    """
    DECISION.md §7 E15: slippage / fill ratio / speed by symbol x time-of-day x spread x order size,
    plus the opportunity cost of what was not bought. Training data for Module 19.
    """
    out: dict = {}
    if ex.empty or "event" not in ex:
        return out
    rep = ex[ex["event"] == "REPORT"].copy()
    if not rep.empty:
        for c in ("slippage_pct", "slippage_vs_mid", "filled_lots", "requested_lots", "execution_duration_ms",
                  "total_execution_cost", "implementation_shortfall", "first_spread_pct", "timestamp"):
            if c in rep:
                rep[c] = pd.to_numeric(rep[c], errors="coerce")
        out["by status"] = rep.groupby("execution_status").size().rename("trades").to_frame()
        rep["fill_ratio"] = rep["filled_lots"] / rep["requested_lots"].where(rep["requested_lots"] > 0)
        hhmm = pd.to_datetime(rep["timestamp"], unit="ms", utc=True).dt.tz_convert("Asia/Kolkata")
        rep["time_bucket"] = pd.cut(hhmm.dt.hour * 60 + hhmm.dt.minute, [0, 600, 690, 810, 900, 1440],
                                    labels=["09:15-10:00", "10:00-11:30", "11:30-13:30", "13:30-15:00", "15:00+"])
        rep["spread_bucket"] = pd.cut(rep["first_spread_pct"], [-0.01, 0.25, 0.5, 0.75, 1.0, 100],
                                      labels=["<=0.25%", "0.25-0.5%", "0.5-0.75%", "0.75-1%", ">1%"])
        rep["size_bucket"] = pd.cut(rep["requested_lots"], [0, 1, 3, 6, 1000], labels=["1", "2-3", "4-6", "7+"])
        filled = rep[rep["filled_lots"] > 0]

        def agg(by):
            g = rep.groupby(by, observed=True)
            gf = filled.groupby(by, observed=True)
            return pd.DataFrame({
                "commands": g.size(),
                "fill_ratio": g["fill_ratio"].mean().round(3),
                "avg_slippage_pct": gf["slippage_pct"].mean().round(4),
                "avg_vs_mid": gf["slippage_vs_mid"].mean().round(4),
                "avg_shortfall": gf["implementation_shortfall"].mean().round(2),
                "avg_ms": gf["execution_duration_ms"].mean().round(0),
            })
        out["by symbol"] = agg("symbol")
        out["by time of day"] = agg("time_bucket")
        out["by spread at entry"] = agg("spread_bucket")
        out["by order size (lots)"] = agg("size_bucket")
        reasons = rep["reject_reasons"].fillna("").map(lambda v: ",".join(_json_list(v)) or "-")
        out["rejections / cancel reasons"] = (rep.assign(reason=reasons.where(reasons != "-", rep.get("cancel_reason")))
                                              .groupby("reason").size().rename("trades").to_frame())
    miss = ex[ex["event"] == "MISSED_MOVE"].copy()
    if not miss.empty:
        miss["missed_move_pct"] = pd.to_numeric(miss["missed_move_pct"], errors="coerce")
        out["missed move after no / partial fill (+ = price ran away)"] = miss.groupby("minutes").agg(
            checks=("missed_move_pct", "size"), avg_move_pct=("missed_move_pct", "mean"),
            ran_up_share=("missed_move_pct", lambda s: round(float((s > 0).mean()), 3)))
    return out


def _json_list(v) -> list:
    import json
    if isinstance(v, list):
        return v
    try:
        d = json.loads(v) if isinstance(v, str) and v else []
        return d if isinstance(d, list) else []
    except Exception:
        return []


def _json_dict(v) -> dict:
    import json
    if isinstance(v, dict):
        return v
    try:
        d = json.loads(v) if isinstance(v, str) and v else {}
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def section(title: str, table: pd.DataFrame) -> None:
    print(f"\n── {title} " + "─" * max(0, 70 - len(title)))
    print(table.to_string() if not table.empty else "(no data)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", action="store_true")
    ap.add_argument("--min-trades", type=int, default=30)
    args = ap.parse_args()

    try:
        for title, table in execution_report(load_exec(args.parquet)).items():
            section(f"Module 14 execution: {title}", table)
    except Exception as e:      # the journal report below must not depend on the executor
        print(f"(execution report skipped: {e})")

    raw = load_parquet() if args.parquet else load_redis()
    if raw.empty or "pnl" not in raw:
        print("Journal is empty — let the paper-trading pipeline run first.")
        return 1
    df = prepare(raw)

    print(f"Journal: {len(df)} closed trades | win rate {df['win'].mean():.1%} | "
          f"total PnL ₹{df['pnl'].sum():,.0f} | expectancy {df['pnl_pct'].mean():.2f}% of premium")

    section("Delta range by market phase (SIE delta bands)", summarize(df, ["market_phase", "delta_bucket"]))
    section("Delta range by holding time", summarize(df, ["hold_bucket", "delta_bucket"]))
    section("Theta risk over hold (% of premium)", summarize(df, "theta_risk_bucket"))
    section("Entry bid-ask spread %", summarize(df, "spread_bucket"))
    section("Probability calibration (score band vs observed win rate)", summarize(df, "prob_bucket"))
    section("Trade quality band", summarize(df, "quality_bucket"))
    section("Risk class (allocation size)", summarize(df, "risk_class"))
    section("Exit reason", summarize(df, "exit_reason"))
    section("Module 18 exit policy: fixed vs adaptive trailing stop (by TSL rule)", exit_policy(df))
    if "reentry_no" in df:
        section("Module 18 re-entries (0 = original trade)", summarize(df.fillna({"reentry_no": 0}), "reentry_no"))
    if "rank_decision" in df:
        ranked = df.assign(rank_decision=df["rank_decision"].fillna("").replace("", "NOT_RANKED"),
                           rank_confidence=df.get("rank_confidence", pd.Series("", index=df.index)).fillna("").replace("", "-"))
        section("Module 13 trade ranking: by ranking decision (shadow: would the ranker have taken it?)",
                summarize(ranked, "rank_decision"))
        section("Module 13 trade ranking: by confidence", summarize(ranked, "rank_confidence"))
        if "rank_band" in df:
            section("Module 13 trade ranking: score band vs outcome", summarize(df, "rank_band"))
        section("Module 13 trade ranking: component lift (>= 70 vs < 70)", ranking_component_lift(df))
    section("Expected value calibration (EV per lot x lots vs realised PnL)", ev_calibration(df))

    # Suggestions only: best-expectancy delta bucket per phase with enough trades.
    print(f"\n── Suggestions (buckets with >= {args.min_trades} trades; review before changing anything) ──")
    t = summarize(df, ["market_phase", "delta_bucket"]).reset_index()
    t = t[t["trades"] >= args.min_trades]
    if t.empty:
        print("Not enough trades per bucket yet.")
    else:
        for phase, g in t.groupby("market_phase"):
            best = g.sort_values("expectancy_pct", ascending=False).iloc[0]
            print(f"  {phase}: best |delta| {best['delta_bucket']} "
                  f"(n={best['trades']}, expectancy {best['expectancy_pct']}%, win {best['win_rate']:.0%}) "
                  f"-> compare with app/strike_intel.py DELTA_BANDS['{phase}']")
    return 0


if __name__ == "__main__":
    sys.exit(main())

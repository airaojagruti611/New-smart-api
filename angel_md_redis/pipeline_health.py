#!/usr/bin/env python3
"""
Pipeline Health Check — validates and prints per-layer outputs from the live pipeline.

Maps every implemented module to its spec layer (1.1 through 1.7) from
Option-rider-algo-specs.md, checks Redis streams and latest keys, and prints
sample output from each module.

Usage:
    python pipeline_health.py              # full report
    python pipeline_health.py --watch      # re-run every 15 seconds
    python pipeline_health.py --layer 1.1  # check specific layer only
    python pipeline_health.py --symbol RELIANCE  # show sample data for one symbol
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# ──────────────────────────────────────────────────────────────
# Color helpers (works in Windows Terminal / PowerShell 7+)
# ──────────────────────────────────────────────────────────────
if os.name == "nt":
    os.system("")  # enable ANSI on Windows

GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"

OK   = f"{GREEN}✅ OK{RESET}"
WARN = f"{YELLOW}⚠️  WARN{RESET}"
FAIL = f"{RED}❌ FAIL{RESET}"
SKIP = f"{DIM}⏭️  SKIP{RESET}"
STALE = f"{YELLOW}⏳ STALE{RESET}"

# ──────────────────────────────────────────────────────────────
# Layer/Module definitions — exact Redis keys from the codebase
# ──────────────────────────────────────────────────────────────

LAYERS = [
    {
        "id": "0",
        "name": "Data Ingestion (Infrastructure)",
        "modules": [
            {
                "name": "Producer (WebSocket → Redis)",
                "worker": "run_producer.py",
                "streams": ["md:ticks:eq", "md:ticks:opt"],
                "latest_prefix": None,
                "latest_key": None,
                "hash_pattern": "md:active_expiry",
                "description": "Raw equity + option ticks from Angel One WebSocket",
            },
            {
                "name": "Greeks Poller (REST → Redis)",
                "worker": "run_greeks_only.py",
                "streams": ["md:greeks:snap"],
                "latest_prefix": "md:greeks:latest:*",
                "latest_key": None,
                "description": "Option greeks (delta/gamma/theta/vega/IV) via REST API",
            },
            {
                "name": "Joiner (Ticks + Greeks → Features)",
                "worker": "run_joiner.py",
                "streams": ["md:features:opt"],
                "latest_prefix": None,
                "latest_key": None,
                "description": "Enriched option ticks: tick fields + greeks merged",
            },
        ],
    },
    {
        "id": "1.1",
        "name": "Core Market Analysis",
        "modules": [
            {
                "name": "Module 1a: Supertrend MTF Bias",
                "worker": "run_supertrend_mtf_bias.py",
                "streams": ["md:supertrend:bias"],
                "latest_prefix": "md:supertrend:bias:latest:*",
                "latest_key": None,
                "description": "Multi-timeframe Supertrend → CALL/PUT/NEUTRAL bias",
                "sample_fields": ["bias", "tf_1m", "tf_5m", "tf_10m", "tf_30m"],
            },
            {
                "name": "Module 1b: EMA Cross (Momentum)",
                "worker": "run_ema_cross.py",
                "streams": ["md:ema:cross"],
                "latest_prefix": "md:ema:cross:latest:*",
                "latest_key": None,
                "description": "EMA9/EMA26 momentum state → bullish/bearish",
                "sample_fields": ["state", "ema9", "ema26"],
            },
            {
                "name": "Module 1c: HTF Trend Filter",
                "worker": "run_htf_trend_filter.py",
                "streams": ["md:htf:trend"],
                "latest_prefix": "md:htf:trend:latest:*",
                "latest_key": None,
                "description": "Daily/Weekly/Monthly trend → CALL/PUT/NEUTRAL gate",
                "sample_fields": ["bias"],
            },
            {
                "name": "Module 1d: Pivot Levels",
                "worker": "run_daily_pivots.py",
                "streams": ["md:pivots:prevday"],
                "latest_prefix": "md:pivots:prevday:*",
                "latest_key": None,
                "hash_keys": True,
                "description": "Previous day pivot, R1, R2, S1, S2",
            },
            {
                "name": "Module 1e: Level Entry (Pivot Break)",
                "worker": "run_level_entry.py",
                "streams": ["md:level:entry"],
                "latest_prefix": "md:level:entry:latest:*",
                "latest_key": None,
                "description": "Pivot break detection → BUY CALL / BUY PUT / NEUTRAL",
                "sample_fields": ["signal", "level", "strength"],
            },
            {
                "name": "Module 1f: Momentum Confirm (ST+EMA)",
                "worker": "run_momentum_confirm.py",
                "streams": ["md:momentum:confirm"],
                "latest_prefix": "md:momentum:confirm:latest:*",
                "latest_key": None,
                "extra_prefix": "md:indicator:score:latest:*",
                "description": "Supertrend=Bullish AND EMA bullish → Confirmed",
            },
            {
                "name": "Module 1g: Candles (1m/1d)",
                "worker": "run_candles_publisher.py",
                "streams": ["md:candles:1m", "md:candles:1d"],
                "latest_prefix": None,
                "latest_key": None,
                "description": "1-minute and daily OHLCV candles from ticks",
            },
            {
                "name": "Module 1h: Candles Resampled (5m/10m/30m)",
                "worker": "run_candles_resampler.py",
                "streams": ["md:candles:5m", "md:candles:10m", "md:candles:30m"],
                "latest_prefix": None,
                "latest_key": None,
                "description": "Resampled multi-timeframe candles",
            },
            {
                "name": "Module 2: Volume Analyzer",
                "worker": "run_volume_analyzer.py",
                "streams": ["md:volume:signal"],
                "latest_prefix": None,
                "latest_key": "md:volume:latest",
                "description": "1m OHLCV buyer/seller dominance → Bullish/Bearish/Wrong Entry Volume",
            },
            {
                "name": "Module 3: Market Regime Detector",
                "worker": "run_market_regime.py",
                "streams": ["md:regime"],
                "latest_prefix": None,
                "latest_key": "md:regime:latest",
                "description": "Advance/Decline breadth → Bullish/Bearish/Neutral regime",
            },
            {
                "name": "Module 4a: Bid-Ask Analyzer",
                "worker": "run_bidask_analyzer.py",
                "streams": ["md:bidask:signal"],
                "latest_prefix": "md:bidask:latest:*",
                "latest_key": None,
                "description": "Spread%, 0-100 depth score; options vs 10-day avg spread (1.5x caution / 2x exit)",
                "extra_prefix": "md:bidask:spread_hist:*",
            },
            {
                "name": "Module 4b: Smart Money Detection",
                "worker": "run_smart_money.py",
                "streams": ["md:smartmoney:signal"],
                "latest_prefix": "md:smartmoney:latest:*",
                "latest_key": None,
                "description": "Size anomaly, absorption, sweep, clustering",
            },
            {
                "name": "Module 4c: Order Flow",
                "worker": "run_order_flow.py",
                "streams": ["md:orderflow:signal"],
                "latest_prefix": "md:orderflow:latest:*",
                "latest_key": None,
                "description": "Net delta, cumulative delta, directional bias",
            },
            {
                "name": "Module 4d: Bid-Ask Imbalance",
                "worker": "run_bidask_imbalance.py",
                "streams": ["md:imbalance:signal"],
                "latest_prefix": "md:imbalance:latest:*",
                "latest_key": None,
                "description": "Depth-weighted imbalance (-1 to +1), spoof filter",
            },
            {
                "name": "Module 4e: Stock Entry/Exit Gates",
                "worker": "run_stock_entry_exit.py",
                "streams": ["md:stockflow:signal"],
                "latest_prefix": "md:stockflow:latest:*",
                "latest_key": None,
                "description": "5-condition entry / exit gate from bid-ask data",
            },
            {
                "name": "Module 4f: Composite Score",
                "worker": "run_composite.py",
                "streams": ["md:composite:signal"],
                "latest_prefix": "md:composite:latest:*",
                "latest_key": None,
                "description": "Weighted composite bid-ask score (>+0.60 entry, <-0.40 exit)",
            },
            {
                "name": "Module 5: OI Analysis",
                "worker": "run_oi_analysis.py",
                "streams": ["md:oi:signal", "md:oi:underlying:signal"],
                "latest_prefix": "md:oi:latest:*",
                "latest_key": None,
                "extra_prefix": "md:oi:underlying:latest:*",
                "description": "OI buildup type + positioning + support/resistance",
            },
        ],
    },
    {
        "id": "1.2",
        "name": "Option Microstructure",
        "modules": [
            {
                "name": "Module 6: Greeks Phase Detector",
                "worker": "run_greeks_analyzer.py",
                "streams": ["md:greeks:phase:signal"],
                "latest_prefix": "md:greeks:phase:latest:*",
                "latest_key": None,
                "extra_prefix": "md:greeks:phase:underlying:latest:*",
                "description": "Phase: Accumulation → Markup → Distribution",
            },
            {
                "name": "Module 7a: Liquidity Score",
                "worker": "run_liquidity_score.py",
                "streams": ["md:liquidity:score:signal"],
                "latest_prefix": "md:liquidity:score:latest:*",
                "latest_key": None,
                "description": "Option liquidity 0-100 (Green/Yellow/Orange/Red)",
            },
            {
                "name": "Module 7b: Option Liquidity Exit",
                "worker": "run_option_liquidity_exit.py",
                "streams": ["md:optexit:signal"],
                "latest_prefix": "md:optexit:latest:*",
                "latest_key": None,
                "description": "Staged exit warning when option spread widens",
            },
            {
                "name": "Module 7c: Strike Flow",
                "worker": "run_strike_flow.py",
                "streams": ["md:strikeflow:signal"],
                "latest_prefix": "md:strikeflow:latest:*",
                "latest_key": None,
                "description": "Option order flow: Vol/OI, sweep detection per strike",
            },
        ],
    },
    {
        "id": "1.3",
        "name": "Volatility Intelligence",
        "modules": [
            {
                "name": "Module 8: Expected Move Calculator",
                "worker": "run_expected_move.py",
                "streams": ["md:expected_move:signal"],
                "latest_prefix": "md:expected_move:latest:*",
                "latest_key": None,
                "description": "Predicted move %, target price, confidence 0-100",
            },
            {
                "name": "Module 9: Greeks Change Predictor",
                "worker": "run_greeks_change.py",
                "streams": ["md:greeks_change:signal"],
                "latest_prefix": "md:greeks_change:latest:*",
                "latest_key": None,
                "description": "Scenario grid of predicted Greeks from Expected Move + ATM/strikeflow candidate",
            },
        ],
    },
    {
        "id": "1.4",
        "name": "Trade Decision Layer",
        "modules": [
            {
                "name": "Module 12: Entry Trigger (FINAL GATE)",
                "worker": "run_entry_trigger.py",
                "streams": ["md:entry:trigger"],
                "latest_prefix": "md:entry:trigger:latest:*",
                "latest_key": None,
                "description": "ALL 7 filters align → BUY CALL / BUY PUT / NEUTRAL",
                "sample_fields": ["signal", "strength", "level", "reason"],
            },
            {
                "name": "Module 10: Strike Selector",
                "worker": "run_strike_select.py",
                "streams": ["md:strike:select"],
                "latest_prefix": "md:strike:select:latest:*",
                "latest_key": None,
                "description": "Maps entry signal → concrete option contract (ATM/OTM/OI-target)",
                "sample_fields": ["status", "signal", "side", "strike", "tradingsymbol"],
            },
            {
                "name": "Module 10: Strike Intelligence Engine",
                "worker": "run_strike_intel.py",
                "streams": ["md:strike:intel"],
                "latest_prefix": "md:strike:intel:latest:*",
                "latest_key": None,
                "description": "Ranks ATM±N strikes (liquidity/EM fit/Greeks) → top-3 + confidence",
                "sample_fields": ["status", "side", "market_phase", "tradingsymbol", "strike_score", "confidence"],
            },
            {
                "name": "Module 12: Probability Engine",
                "worker": "run_probability.py",
                "streams": ["md:probability"],
                "latest_prefix": "md:probability:latest:*",
                "latest_key": None,
                "description": "8 weighted inputs + hard filters → probability 0-100, grade, decision",
                "sample_fields": ["probability", "grade", "decision", "reject_reasons", "flags"],
            },
            {
                "name": "Module 11: Lot Sizing (ICARE)",
                "worker": "run_icare.py",
                "streams": ["md:icare"],
                "latest_prefix": "md:icare:latest:*",
                "latest_key": None,
                "description": "Trade quality → risk class → EV → MIN(margin, risk, capital, portfolio, liquidity) lots",
                "sample_fields": ["status", "trade_quality", "risk_class", "expected_value", "recommended_lots", "limiting_factor"],
            },
            {
                "name": "Module 13: Trade Ranking Engine",
                "worker": "run_trade_ranking.py",
                "streams": ["md:ranking", "md:ranking:cycle"],
                "latest_prefix": None,
                "latest_key": "md:ranking:rank",
                "description": "13 weighted scores × direction agreement × reward/risk × data quality, hard gates, "
                               "rank + sector/underlying filter, NO_TRADE allowed (RANK_MODE shadow/active)",
            },
            {
                "name": "Module 14: Order Executor / Trade Entry",
                "worker": "run_order_executor.py",
                "streams": ["md:exec", "md:exec:fill", "md:exec:exit_request", "md:exec:exit_fill"],
                "latest_prefix": "md:exec:latest:*",
                "latest_key": None,
                "description": "Fresh validation, capped limit-order ladder (no market orders), quantity MIN(), "
                               "depth slicing, partial-fill cost check, timeout / kill switch, charges + slippage "
                               "(EXEC_MODE shadow/paper; live disabled — no static IP)",
                "sample_fields": ["execution_status", "filled_lots", "average_fill_price", "slippage", "total_execution_cost"],
            },
            {
                "name": "Module 16: Circuit Breaker / Kill Switch",
                "worker": None,
                "description": "PARTIAL — manual kill switch `md:control:kill_switch`=1 blocks new trades in "
                               "Trade Ranking; circuit-limit check flag-only (no data source)",
            },
        ],
    },
    {
        "id": "1.5",
        "name": "Risk Layer",
        "modules": [
            {"name": "Module 17: Risk Management", "worker": None, "description": "NOT IMPLEMENTED"},
            {
                "name": "Module 18: Adaptive Trailing SL & Re-entry",
                "worker": "run_adaptive_tsl.py",
                "streams": ["md:tsl", "md:tsl:reentry"],
                "latest_prefix": "md:tsl:latest:*",
                "latest_key": None,
                "description": "Volatility + trend → dynamic TSL % (ratchet), confirmed exits, swing-break re-entry (TSL_MODE shadow/active)",
                "sample_fields": ["status", "current_trailing_stop", "trailing_percentage", "tsl_rule", "reentry_state", "event"],
            },
            {"name": "Module 19: Slippage Estimator", "worker": None, "description": "NOT IMPLEMENTED"},
        ],
    },
    {
        "id": "1.6",
        "name": "Capital Layer",
        "modules": [
            {
                "name": "Account snapshot (ICARE input)",
                "worker": "run_account.py",
                "streams": [],
                "latest_prefix": None,
                "latest_key": "md:account:latest",
                "description": "Paper ledger (default) or Angel getRMS — capital, margin, day PnL, open risk",
            },
            {
                "name": "Module 20: Capital Allocation",
                "worker": "run_capital_alloc.py",
                "streams": ["md:capital:alloc"],
                "latest_prefix": "md:capital:alloc:latest:*",
                "latest_key": None,
                "description": "Regime-based capital bias + risk-based position sizing",
            },
        ],
    },
    {
        "id": "1.7",
        "name": "Monitoring Layer",
        "modules": [
            {"name": "Module 21: Portfolio Exposure Monitor", "worker": None, "description": "NOT IMPLEMENTED"},
            {
                "name": "Module 22: Trade Journal Engine (paper)",
                "worker": "run_trade_journal.py",
                "streams": ["md:journal"],
                "latest_prefix": "md:position:open:*",
                "latest_key": "md:journal:stats",
                "description": "Paper entries from ICARE APPROVED, MFE/MAE, SL/target/time/EOD exits, bucket stats",
            },
            {"name": "Module 23: Accumulation Phase Detector", "worker": None, "description": "NOT IMPLEMENTED"},
        ],
    },
]


# ──────────────────────────────────────────────────────────────
# Freshness thresholds (seconds) — newest output older than this → STALE.
# None = event-driven module (only emits on trades), no age check.
# Override per module with "max_age_sec" in LAYERS, globally with
# --max-age / HEALTH_MAX_AGE_SEC, or per worker with
# HEALTH_MAX_AGE_<WORKER> (e.g. HEALTH_MAX_AGE_RUN_PRODUCER=60).
# ──────────────────────────────────────────────────────────────

DEFAULT_MAX_AGE_SEC = 300.0

MAX_AGE_SEC_BY_WORKER = {
    "run_producer.py": 30,
    "run_greeks_only.py": 300,
    "run_joiner.py": 60,
    "run_candles_publisher.py": 150,          # newest of 1m / 1d
    "run_candles_resampler.py": 660,          # newest of 5m / 10m / 30m
    "run_daily_pivots.py": 4 * 86400,         # once per session (weekends)
    "run_htf_trend_filter.py": 3600,
    "run_bidask_analyzer.py": 60,
    "run_bidask_imbalance.py": 60,
    "run_smart_money.py": 120,
    "run_order_flow.py": 120,
    "run_stock_entry_exit.py": 120,
    "run_composite.py": 120,
    "run_liquidity_score.py": 120,
    "run_option_liquidity_exit.py": 120,
    "run_strike_flow.py": 120,
    "run_account.py": 180,
    "run_trade_ranking.py": 600,
    "run_order_executor.py": None,
    "run_adaptive_tsl.py": None,
    "run_trade_journal.py": None,
}

_TS_FIELDS = ("ts_ms", "updated_ms", "timestamp", "ts_recv", "bar_ts_ms", "rank_ts_ms")


def module_max_age(mod: dict, override: Optional[float] = None) -> Optional[float]:
    worker = mod.get("worker") or ""
    if "max_age_sec" in mod:
        base = mod["max_age_sec"]
    else:
        base = MAX_AGE_SEC_BY_WORKER.get(worker, DEFAULT_MAX_AGE_SEC)
    env_key = "HEALTH_MAX_AGE_" + worker.replace(".py", "").upper()
    if os.getenv(env_key):
        try:
            return float(os.getenv(env_key))
        except ValueError:
            pass
    if base is None:
        return None
    if override is None:
        try:
            override = float(os.getenv("HEALTH_MAX_AGE_SEC", "") or 0) or None
        except ValueError:
            override = None
    return float(override) if override else float(base)


def _to_ms(v) -> Optional[int]:
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    return int(n * 1000) if n < 1e11 else int(n)


def doc_ts_ms(doc, stream_id=None) -> Optional[int]:
    """Doc timestamp (ts_ms / updated_ms / … ), else the stream id's ms part."""
    if isinstance(doc, dict):
        for f in _TS_FIELDS:
            ms = _to_ms(doc.get(f))
            if ms:
                return ms
        try:  # session date (pivots): that session's close, 10:00 UTC = 15:30 IST
            d = datetime.strptime(str(doc.get("date") or "")[:10], "%Y-%m-%d")
            return int((d - datetime(1970, 1, 1)).total_seconds() * 1000) + 10 * 3600 * 1000
        except ValueError:
            pass
    if stream_id is not None:
        try:
            return int(str(stream_id).split("-", 1)[0])
        except ValueError:
            return None
    return None


def _newest_latest_ms(r, keys, sample: int = 50) -> Optional[int]:
    newest = None
    for k in sorted(keys)[:sample] if len(keys) > sample else keys:
        try:
            t = r.type(k)
            if t == "string":
                raw = r.get(k)
                val = safe_json(raw)
                if isinstance(val, list):  # md:greeks:latest list form: newest per-item ts_ms (if stamped)
                    ts = max((doc_ts_ms(it) or 0 for it in val if isinstance(it, dict)), default=0) or None
                else:
                    ts = doc_ts_ms(val)
            elif t == "hash":
                ts = doc_ts_ms(r.hgetall(k))
            else:
                ts = None
        except Exception:
            ts = None
        if ts and (newest is None or ts > newest):
            newest = ts
    return newest


def evaluate_module(r, mod: dict, now_ms: Optional[int] = None, keys=None,
                    max_age_override: Optional[float] = None) -> dict:
    """Pure health verdict for one module (no printing).

    status: OK (newest output within max_age) | STALE (data exists, newest too old,
    or no timestamp at all) | FAIL (no data) | NOT_IMPL.  `keys` = optional pre-scanned
    key list (filtered locally with fnmatch instead of one SCAN per prefix).
    """
    import fnmatch

    now_ms = now_ms or int(time.time() * 1000)
    out = {"name": mod["name"], "worker": mod.get("worker"), "status": "NOT_IMPL",
           "max_age_sec": module_max_age(mod, max_age_override), "newest_age_sec": None, "components": []}
    if not mod.get("worker"):
        return out
    newest = None
    has_data = False
    for stream in mod.get("streams", []) or []:
        try:
            n = int(r.xlen(stream) or 0)
        except Exception:
            n = 0
        ts = None
        if n:
            try:
                rows = r.xrevrange(stream, count=1) or []
                if rows:
                    mid, fields = rows[0]
                    ts = doc_ts_ms(fields, mid)
            except Exception:
                ts = None
        has_data = has_data or n > 0
        out["components"].append({"kind": "stream", "name": stream, "count": n,
                                  "age_sec": None if ts is None else (now_ms - ts) / 1000.0})
        if ts and (newest is None or ts > newest):
            newest = ts
    for prefix in (mod.get("latest_prefix"), mod.get("extra_prefix")):
        if not prefix:
            continue
        try:
            if keys is not None:
                ks = [k for k in keys if fnmatch.fnmatchcase(k, prefix)]
            else:
                ks = list(r.scan_iter(match=prefix, count=500))
        except Exception:
            ks = []
        ts = _newest_latest_ms(r, ks) if ks else None
        has_data = has_data or bool(ks)
        out["components"].append({"kind": "keys", "name": prefix, "count": len(ks),
                                  "age_sec": None if ts is None else (now_ms - ts) / 1000.0})
        if ts and (newest is None or ts > newest):
            newest = ts
    for single in (mod.get("latest_key"), mod.get("hash_pattern")):
        if not single:
            continue
        try:
            t = r.type(single)
        except Exception:
            t = "none"
        ts = None
        cnt = 0
        try:
            if t == "string":
                val = safe_json(r.get(single))
                cnt = 1 if val is not None else 0
                ts = doc_ts_ms(val)
            elif t == "hash":
                cnt = len(r.hgetall(single) or {})
                ts = _to_ms(r.get(single + ":ts_ms")) if single == "md:active_expiry" else None
            elif t == "zset":
                cnt = len(r.zrevrange(single, 0, 9) or [])
        except Exception:
            pass
        has_data = has_data or cnt > 0
        out["components"].append({"kind": t, "name": single, "count": cnt,
                                  "age_sec": None if ts is None else (now_ms - ts) / 1000.0})
        if ts and (newest is None or ts > newest):
            newest = ts
    out["newest_age_sec"] = None if newest is None else max(0.0, (now_ms - newest) / 1000.0)
    max_age = out["max_age_sec"]
    if not has_data:
        out["status"] = "FAIL"
    elif max_age is None:
        out["status"] = "OK"
    elif newest is None or out["newest_age_sec"] > max_age:
        out["status"] = "STALE"
    else:
        out["status"] = "OK"
    return out


_BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def worker_pid_names(run_all: Optional[str] = None) -> dict:
    """script -> pid-file name, parsed from run_all.sh `start "name" python3 script.py` lines."""
    import re

    path = run_all or os.path.join(_BASE_DIR, "run_all.sh")
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    except OSError:
        return {}
    return {m.group(2): m.group(1) for m in re.finditer(r'start\s+"([^"]+)"\s+python3?\s+(\S+\.py)', text)}


def worker_alive(worker: Optional[str], names: Optional[dict] = None, log_dir: Optional[str] = None) -> Optional[bool]:
    """True/False from the newest logs/<date>/pids/<name>.pid written by run_all.sh; None = unknown."""
    import glob

    if not worker:
        return None
    name = (names if names is not None else worker_pid_names()).get(worker)
    if not name:
        return None
    log_dir = log_dir or os.getenv("LOG_DIR") or os.path.join(_BASE_DIR, "logs")
    pid_files = glob.glob(os.path.join(log_dir, "*", "pids", f"{name}.pid"))
    if not pid_files:
        return None
    try:
        with open(max(pid_files, key=os.path.getmtime), encoding="utf-8") as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)
    except PermissionError:
        return True
    except (OSError, ValueError):
        return False
    return True


def evaluate_all(r, now_ms: Optional[int] = None, keys=None, max_age_override: Optional[float] = None,
                 filter_layer: Optional[str] = None, check_workers: bool = False) -> list:
    """check_workers: also look at run_all.sh pid files (same host only). A worker
    that is running but has produced nothing is IDLE (e.g. no entry signal yet),
    not FAIL; a worker whose process is gone is DOWN."""
    names = worker_pid_names() if check_workers else {}
    rows = []
    for layer in LAYERS:
        if filter_layer and layer["id"] != filter_layer:
            continue
        for mod in layer["modules"]:
            res = evaluate_module(r, mod, now_ms=now_ms, keys=keys, max_age_override=max_age_override)
            if check_workers and mod.get("worker"):
                alive = worker_alive(mod["worker"], names)
                res["alive"] = alive
                if alive is False:
                    res["status"] = "DOWN"
                elif alive and res["status"] == "FAIL":
                    res["status"] = "IDLE"
            res["layer"] = layer["id"]
            res["layer_name"] = layer["name"]
            rows.append(res)
    return rows


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def safe_json(raw: Optional[str]) -> Optional[dict]:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def fmt_json(obj, max_width=100) -> str:
    """Pretty-print a dict, truncated to max_width."""
    if obj is None:
        return f"{DIM}(empty){RESET}"
    try:
        s = json.dumps(obj, indent=None, separators=(", ", ": "))
    except Exception:
        s = str(obj)
    if len(s) > max_width:
        s = s[: max_width - 3] + "..."
    return s


def age_str(ts_ms_raw) -> str:
    """Human-readable age from a ts_ms value."""
    try:
        ts_ms = int(ts_ms_raw)
        age = time.time() - ts_ms / 1000
        if age < 60:
            return f"{age:.0f}s ago"
        elif age < 3600:
            return f"{age / 60:.1f}m ago"
        else:
            return f"{age / 3600:.1f}h ago"
    except Exception:
        return "?"


# ──────────────────────────────────────────────────────────────
# Core check logic
# ──────────────────────────────────────────────────────────────

def check_module(r: redis.Redis, mod: dict, sample_symbol: Optional[str] = None,
                 max_age_override: Optional[float] = None) -> dict:
    """Check a single module's health. Returns a result dict."""
    result = {
        "name": mod["name"],
        "description": mod.get("description", ""),
        "worker": mod.get("worker"),
        "status": "SKIP",
        "details": [],
        "sample": None,
    }

    if not mod.get("worker"):
        result["status"] = "NOT_IMPL"
        return result

    streams = mod.get("streams", [])
    latest_prefix = mod.get("latest_prefix")
    latest_key = mod.get("latest_key")
    extra_prefix = mod.get("extra_prefix")
    hash_pattern = mod.get("hash_pattern")
    has_any_data = False

    # Check streams
    for stream in streams:
        try:
            length = r.xlen(stream)
            has_any_data = has_any_data or length > 0
            status = OK if length > 0 else FAIL
            result["details"].append(f"Stream {BOLD}{stream}{RESET}: {length:,} msgs {status}")

            # Show latest message from stream
            if length > 0 and sample_symbol:
                msgs = r.xrevrange(stream, count=5)
                for _mid, fields in msgs:
                    sym_val = fields.get("symbol") or fields.get("underlying") or ""
                    if sample_symbol.upper() in sym_val.upper():
                        ts = fields.get("ts_recv") or fields.get("ts_ms")
                        result["sample"] = fields
                        result["details"].append(
                            f"  └─ Sample ({sym_val}): {fmt_json(fields)} [{age_str(ts)}]"
                        )
                        break
            elif length > 0:
                msgs = r.xrevrange(stream, count=1)
                if msgs:
                    _mid, fields = msgs[0]
                    ts = fields.get("ts_recv") or fields.get("ts_ms")
                    sym = fields.get("symbol") or fields.get("underlying") or ""
                    result["details"].append(
                        f"  └─ Latest ({sym}): {fmt_json(fields)} [{age_str(ts)}]"
                    )
        except Exception as e:
            result["details"].append(f"Stream {stream}: {RED}ERROR {e}{RESET}")

    # Check latest keys (pattern scan)
    for prefix in [latest_prefix, extra_prefix]:
        if not prefix:
            continue
        try:
            keys = list(r.scan_iter(match=prefix, count=500))
            count = len(keys)
            has_any_data = has_any_data or count > 0
            status = OK if count > 0 else FAIL
            result["details"].append(f"Keys  {BOLD}{prefix}{RESET}: {count} symbols {status}")

            if count > 0:
                # Pick a key to show sample
                show_key = None
                if sample_symbol:
                    for k in keys:
                        if sample_symbol.upper() in k.upper():
                            show_key = k
                            break
                if not show_key:
                    show_key = sorted(keys)[0]

                if show_key:
                    # Could be string (JSON) or hash
                    key_type = r.type(show_key)
                    if key_type == "string":
                        val = safe_json(r.get(show_key))
                        ts = doc_ts_ms(val) if isinstance(val, dict) else None
                        result["details"].append(
                            f"  └─ {show_key}: {fmt_json(val)} [{age_str(ts)}]"
                        )
                        if not result["sample"]:
                            result["sample"] = val
                    elif key_type == "hash":
                        val = r.hgetall(show_key)
                        result["details"].append(
                            f"  └─ {show_key}: {fmt_json(val)}"
                        )
                        if not result["sample"]:
                            result["sample"] = val
        except Exception as e:
            result["details"].append(f"Keys {prefix}: {RED}ERROR {e}{RESET}")

    # Check single latest key
    if latest_key and r.type(latest_key) == "zset":
        ranked = r.zrevrange(latest_key, 0, 9, withscores=True)
        has_any_data = has_any_data or bool(ranked)
        result["details"].append(f"ZSet  {BOLD}{latest_key}{RESET}: {OK if ranked else FAIL}")
        if ranked:
            result["details"].append("  └─ " + ", ".join(f"{s} {int(v)}" for s, v in ranked))
        latest_key = None
    if latest_key:
        try:
            raw = r.get(latest_key)
            val = safe_json(raw)
            has_any_data = has_any_data or val is not None
            status = OK if val else FAIL
            result["details"].append(f"Key   {BOLD}{latest_key}{RESET}: {status}")
            if val:
                ts = val.get("ts_ms") or val.get("ts_recv")
                # For regime/volume, show a summary
                if "regime" in latest_key:
                    regime = val.get("regime", "?")
                    adv = val.get("advance_pct", "?")
                    ratio = val.get("breadth_ratio", "?")
                    bias = val.get("capital_bias", "?")
                    result["details"].append(
                        f"  └─ Regime={BOLD}{regime}{RESET}, Advance%={adv}, "
                        f"Ratio={ratio}, Bias={bias} [{age_str(ts)}]"
                    )
                elif "volume" in latest_key:
                    vol_data = {k: v for k, v in val.items() if k != "ts_ms"}
                    num_syms = len(vol_data)
                    result["details"].append(
                        f"  └─ {num_syms} symbols with volume data [{age_str(ts)}]"
                    )
                    if sample_symbol and sample_symbol.upper() in val:
                        sym_data = val[sample_symbol.upper()]
                        result["details"].append(
                            f"  └─ {sample_symbol}: {fmt_json(sym_data)}"
                        )
                else:
                    result["details"].append(f"  └─ {fmt_json(val)} [{age_str(ts)}]")
                if not result["sample"]:
                    result["sample"] = val
        except Exception as e:
            result["details"].append(f"Key {latest_key}: {RED}ERROR {e}{RESET}")

    # Check hash pattern
    if hash_pattern:
        try:
            val = r.hgetall(hash_pattern)
            has_any_data = has_any_data or bool(val)
            status = OK if val else FAIL
            result["details"].append(f"Hash  {BOLD}{hash_pattern}{RESET}: {len(val)} entries {status}")
            if val:
                result["details"].append(f"  └─ {fmt_json(val)}")
        except Exception as e:
            result["details"].append(f"Hash {hash_pattern}: {RED}ERROR {e}{RESET}")

    verdict = evaluate_module(r, mod, max_age_override=max_age_override)
    result["status"] = verdict["status"]
    result["newest_age_sec"] = verdict["newest_age_sec"]
    result["max_age_sec"] = verdict["max_age_sec"]
    age = verdict["newest_age_sec"]
    lim = verdict["max_age_sec"]
    age_txt = "no timestamp" if age is None else f"{age:.0f}s"
    lim_txt = "no age check (event-driven)" if lim is None else f"limit {lim:.0f}s"
    if has_any_data:
        colour = RED if result["status"] == "STALE" else GREEN
        result["details"].append(f"Freshness: newest output {colour}{age_txt}{RESET} ({lim_txt})")
    return result


# ──────────────────────────────────────────────────────────────
# Report printer
# ──────────────────────────────────────────────────────────────

def print_report(r: redis.Redis, filter_layer: Optional[str] = None,
                 sample_symbol: Optional[str] = None, max_age_override: Optional[float] = None):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'=' * 80}")
    print(f"{BOLD}{CYAN}  PIPELINE HEALTH CHECK — {now}{RESET}")
    if sample_symbol:
        print(f"  Sample symbol: {BOLD}{sample_symbol.upper()}{RESET}")
    print(f"{'=' * 80}")

    # Quick Redis connectivity check
    try:
        r.ping()
        print(f"\n  Redis: {GREEN}connected{RESET} ({REDIS_URL})")
    except Exception as e:
        print(f"\n  Redis: {RED}CANNOT CONNECT{RESET} — {e}")
        print(f"  Make sure Redis is running: docker compose up -d")
        return

    # Count total streams
    try:
        all_keys = list(r.scan_iter(match="md:*", count=10000))
        print(f"  Total md:* keys in Redis: {BOLD}{len(all_keys)}{RESET}")
    except Exception:
        pass

    total_ok = 0
    total_fail = 0
    total_stale = 0
    total_not_impl = 0

    for layer in LAYERS:
        if filter_layer and layer["id"] != filter_layer:
            continue

        print(f"\n{'─' * 80}")
        print(f"{BOLD}{CYAN}  Layer {layer['id']} — {layer['name']}{RESET}")
        print(f"{'─' * 80}")

        for mod in layer["modules"]:
            result = check_module(r, mod, sample_symbol, max_age_override=max_age_override)

            # Status icon
            if result["status"] == "OK":
                icon = OK
                total_ok += 1
            elif result["status"] == "STALE":
                icon = STALE
                total_stale += 1
            elif result["status"] == "NOT_IMPL":
                icon = f"{DIM}🚧 NOT IMPLEMENTED{RESET}"
                total_not_impl += 1
            else:
                icon = FAIL
                total_fail += 1

            worker_str = f" ({result['worker']})" if result.get("worker") else ""
            print(f"\n  {icon}  {BOLD}{result['name']}{RESET}{worker_str}")
            print(f"       {DIM}{result['description']}{RESET}")

            for detail in result.get("details", []):
                print(f"       {detail}")

    # Summary
    print(f"\n{'=' * 80}")
    print(f"{BOLD}  SUMMARY{RESET}")
    print(f"    {GREEN}✅ Producing data:{RESET}  {total_ok}")
    print(f"    {YELLOW}⏳ Stale data:{RESET}      {total_stale}")
    print(f"    {RED}❌ No data found:{RESET}   {total_fail}")
    print(f"    {DIM}🚧 Not implemented:{RESET} {total_not_impl}")

    if total_fail > 0 and total_ok == 0:
        print(f"\n  {YELLOW}💡 Pipeline appears to not be running.{RESET}")
        print(f"     Start it with: {BOLD}.\\run_all.ps1{RESET}")
        print(f"     Or check if it's market hours (9:15 AM - 3:30 PM IST, Mon-Fri).")
    if total_stale > 0:
        print(f"\n  {YELLOW}💡 {total_stale} module(s) have only old data (worker stopped / market closed / "
              f"upstream stalled). Thresholds: MAX_AGE_SEC_BY_WORKER, --max-age, HEALTH_MAX_AGE_SEC.{RESET}")
    if total_fail > 0 and total_ok > 0:
        print(f"\n  {YELLOW}💡 Some modules have no data. This could mean:{RESET}")
        print(f"     - The worker hasn't processed enough data yet (wait a few minutes)")
        print(f"     - The upstream dependency hasn't produced output yet")
        print(f"     - The worker crashed (check logs/YYYY-MM-DD/*.err.log)")
    print(f"{'=' * 80}\n")


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline Health Check — validate per-layer outputs from the live pipeline"
    )
    parser.add_argument(
        "--watch", action="store_true",
        help="Re-run the health check every 15 seconds (Ctrl+C to stop)"
    )
    parser.add_argument(
        "--layer", type=str, default=None,
        help="Check only a specific layer (e.g. '1.1', '1.2', '1.4')"
    )
    parser.add_argument(
        "--symbol", type=str, default=None,
        help="Show sample data for a specific symbol (e.g. 'RELIANCE')"
    )
    parser.add_argument(
        "--max-age", type=float, default=None,
        help="Override every module's freshness threshold (seconds); event-driven modules stay unchecked"
    )
    parser.add_argument(
        "--interval", type=int, default=15,
        help="Refresh interval in seconds when using --watch (default: 15)"
    )
    args = parser.parse_args()

    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

    if args.watch:
        try:
            while True:
                # Clear screen
                os.system("cls" if os.name == "nt" else "clear")
                print_report(r, filter_layer=args.layer, sample_symbol=args.symbol, max_age_override=args.max_age)
                print(f"  {DIM}Auto-refreshing every {args.interval}s... Press Ctrl+C to stop.{RESET}")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        print_report(r, filter_layer=args.layer, sample_symbol=args.symbol, max_age_override=args.max_age)


if __name__ == "__main__":
    main()

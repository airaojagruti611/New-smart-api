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
                "streams": [],
                "latest_prefix": "md:supertrend:bias:latest:*",
                "latest_key": None,
                "description": "Multi-timeframe Supertrend → CALL/PUT/NEUTRAL bias",
                "sample_fields": ["bias", "tf_1m", "tf_5m", "tf_10m", "tf_30m"],
            },
            {
                "name": "Module 1b: EMA Cross (Momentum)",
                "worker": "run_ema_cross.py",
                "streams": [],
                "latest_prefix": "md:ema:cross:latest:*",
                "latest_key": None,
                "description": "EMA9/EMA26 momentum state → bullish/bearish",
                "sample_fields": ["state", "ema9", "ema26"],
            },
            {
                "name": "Module 1c: HTF Trend Filter",
                "worker": "run_htf_trend_filter.py",
                "streams": [],
                "latest_prefix": "md:htf:trend:latest:*",
                "latest_key": None,
                "description": "Daily/Weekly/Monthly trend → CALL/PUT/NEUTRAL gate",
                "sample_fields": ["bias"],
            },
            {
                "name": "Module 1d: Pivot Levels",
                "worker": "run_daily_pivots.py",
                "streams": [],
                "latest_prefix": "md:pivots:prevday:*",
                "latest_key": None,
                "hash_keys": True,
                "description": "Previous day pivot, R1, R2, S1, S2",
            },
            {
                "name": "Module 1e: Level Entry (Pivot Break)",
                "worker": "run_level_entry.py",
                "streams": [],
                "latest_prefix": "md:level:entry:latest:*",
                "latest_key": None,
                "description": "Pivot break detection → BUY CALL / BUY PUT / NEUTRAL",
                "sample_fields": ["signal", "level", "strength"],
            },
            {
                "name": "Module 1f: Momentum Confirm (ST+EMA)",
                "worker": "run_momentum_confirm.py",
                "streams": [],
                "latest_prefix": "md:momentum:confirm:latest:*",
                "latest_key": None,
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
                "description": "Buyer/seller dominance → Bullish/Bearish/Wrong Entry Volume",
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
                "streams": [],
                "latest_prefix": "md:bidask:latest:*",
                "latest_key": None,
                "description": "Spread%, liquidity score, S/R from depth",
            },
            {
                "name": "Module 4b: Smart Money Detection",
                "worker": "run_smart_money.py",
                "streams": [],
                "latest_prefix": "md:smartmoney:latest:*",
                "latest_key": None,
                "description": "Size anomaly, absorption, sweep, clustering",
            },
            {
                "name": "Module 4c: Order Flow",
                "worker": "run_order_flow.py",
                "streams": [],
                "latest_prefix": "md:orderflow:latest:*",
                "latest_key": None,
                "description": "Net delta, cumulative delta, directional bias",
            },
            {
                "name": "Module 4d: Bid-Ask Imbalance",
                "worker": "run_bidask_imbalance.py",
                "streams": [],
                "latest_prefix": "md:imbalance:latest:*",
                "latest_key": None,
                "description": "Depth-weighted imbalance (-1 to +1), spoof filter",
            },
            {
                "name": "Module 4e: Stock Entry/Exit Gates",
                "worker": "run_stock_entry_exit.py",
                "streams": [],
                "latest_prefix": "md:stockflow:latest:*",
                "latest_key": None,
                "description": "5-condition entry / exit gate from bid-ask data",
            },
            {
                "name": "Module 4f: Composite Score",
                "worker": "run_composite.py",
                "streams": [],
                "latest_prefix": "md:composite:latest:*",
                "latest_key": None,
                "description": "Weighted composite bid-ask score (>+0.60 entry, <-0.40 exit)",
            },
            {
                "name": "Module 5: OI Analysis",
                "worker": "run_oi_analysis.py",
                "streams": [],
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
                "streams": [],
                "latest_prefix": "md:greeks:phase:latest:*",
                "latest_key": None,
                "extra_prefix": "md:greeks:phase:underlying:latest:*",
                "description": "Phase: Accumulation → Markup → Distribution",
            },
            {
                "name": "Module 7a: Liquidity Score",
                "worker": "run_liquidity_score.py",
                "streams": [],
                "latest_prefix": "md:liquidity:score:latest:*",
                "latest_key": None,
                "description": "Option liquidity 0-100 (Green/Yellow/Orange/Red)",
            },
            {
                "name": "Module 7b: Option Liquidity Exit",
                "worker": "run_option_liquidity_exit.py",
                "streams": [],
                "latest_prefix": "md:optexit:latest:*",
                "latest_key": None,
                "description": "Staged exit warning when option spread widens",
            },
            {
                "name": "Module 7c: Strike Flow",
                "worker": "run_strike_flow.py",
                "streams": [],
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
                "streams": [],
                "latest_prefix": "md:expected_move:latest:*",
                "latest_key": None,
                "description": "Predicted move %, target price, confidence 0-100",
            },
            {
                "name": "Module 9: Greeks Change Predictor",
                "worker": "run_greeks_change.py",
                "streams": [],
                "latest_prefix": "md:greeks_change:latest:*",
                "latest_key": None,
                "description": "Per-tick greeks acceleration/deceleration",
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
                "streams": [],
                "latest_prefix": "md:strike:select:latest:*",
                "latest_key": None,
                "description": "Maps entry signal → concrete option contract (ATM/OTM/OI-target)",
                "sample_fields": ["status", "signal", "side", "strike", "tradingsymbol"],
            },
            {
                "name": "Module 11: Lot Sizing",
                "worker": None,
                "description": "NOT IMPLEMENTED — spec backlog",
            },
            {
                "name": "Module 13: Probability Engine",
                "worker": None,
                "description": "NOT IMPLEMENTED — spec backlog (learning layer)",
            },
            {
                "name": "Module 14: Trade Ranking Engine",
                "worker": None,
                "description": "NOT IMPLEMENTED — spec backlog",
            },
            {
                "name": "Module 15: Order Executor",
                "worker": None,
                "description": "NOT IMPLEMENTED — needs Broker Bridge",
            },
            {
                "name": "Module 16: Circuit Breaker / Kill Switch",
                "worker": None,
                "description": "NOT IMPLEMENTED — spec backlog",
            },
        ],
    },
    {
        "id": "1.5",
        "name": "Risk Layer",
        "modules": [
            {"name": "Module 17: Risk Management", "worker": None, "description": "NOT IMPLEMENTED"},
            {"name": "Module 18: Smart Stop Loss", "worker": None, "description": "NOT IMPLEMENTED"},
            {"name": "Module 19: Slippage Estimator", "worker": None, "description": "NOT IMPLEMENTED"},
        ],
    },
    {
        "id": "1.6",
        "name": "Capital Layer",
        "modules": [
            {
                "name": "Module 20: Capital Allocation",
                "worker": "run_capital_alloc.py",
                "streams": [],
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
            {"name": "Module 22: Trade Journal Engine", "worker": None, "description": "NOT IMPLEMENTED"},
            {"name": "Module 23: Accumulation Phase Detector", "worker": None, "description": "NOT IMPLEMENTED"},
        ],
    },
]


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

def check_module(r: redis.Redis, mod: dict, sample_symbol: Optional[str] = None) -> dict:
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
                        ts = (val or {}).get("ts_ms") or (val or {}).get("ts_recv")
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

    result["status"] = "OK" if has_any_data else "FAIL"
    return result


# ──────────────────────────────────────────────────────────────
# Report printer
# ──────────────────────────────────────────────────────────────

def print_report(r: redis.Redis, filter_layer: Optional[str] = None,
                 sample_symbol: Optional[str] = None):
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
    total_not_impl = 0

    for layer in LAYERS:
        if filter_layer and layer["id"] != filter_layer:
            continue

        print(f"\n{'─' * 80}")
        print(f"{BOLD}{CYAN}  Layer {layer['id']} — {layer['name']}{RESET}")
        print(f"{'─' * 80}")

        for mod in layer["modules"]:
            result = check_module(r, mod, sample_symbol)

            # Status icon
            if result["status"] == "OK":
                icon = OK
                total_ok += 1
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
    print(f"    {RED}❌ No data found:{RESET}   {total_fail}")
    print(f"    {DIM}🚧 Not implemented:{RESET} {total_not_impl}")

    if total_fail > 0 and total_ok == 0:
        print(f"\n  {YELLOW}💡 Pipeline appears to not be running.{RESET}")
        print(f"     Start it with: {BOLD}.\\run_all.ps1{RESET}")
        print(f"     Or check if it's market hours (9:15 AM - 3:30 PM IST, Mon-Fri).")
    elif total_fail > 0:
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
                print_report(r, filter_layer=args.layer, sample_symbol=args.symbol)
                print(f"  {DIM}Auto-refreshing every {args.interval}s... Press Ctrl+C to stop.{RESET}")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        print_report(r, filter_layer=args.layer, sample_symbol=args.symbol)


if __name__ == "__main__":
    main()

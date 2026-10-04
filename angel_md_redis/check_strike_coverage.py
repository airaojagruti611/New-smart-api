#!/usr/bin/env python3
"""
check_strike_coverage.py
────────────────────────
P0 pre-flight for the Strike Intelligence Engine (DECISION.md D4).

SIE ranks ATM ± SIE_STRIKES_AROUND strikes per side, so every one of those
contracts needs live per-contract latest keys:

  md:greeks:phase:latest:{TSYM}     Greeks (delta/gamma/theta/vega/iv)
  md:liquidity:score:latest:{TSYM}  liquidity_score 0-100 + spot
  md:bidask:latest:{TSYM}           spread / mid

Reads keys only (no streams consumed). Run while the pipeline is live:

    python check_strike_coverage.py              # ATM±5 (SIE default)
    python check_strike_coverage.py --around 10
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from typing import Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
GREEKS_PREFIX = os.getenv("GREEKS_PHASE_LATEST_PREFIX", "md:greeks:phase:latest:")
LIQUIDITY_PREFIX = os.getenv("LIQUIDITY_SCORE_LATEST_PREFIX", "md:liquidity:score:latest:")
BIDASK_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")
EXPECTED_MOVE_PREFIX = os.getenv("EXPECTED_MOVE_LATEST_PREFIX", "md:expected_move:latest:")
SIE_STRIKES_AROUND = int(os.getenv("SIE_STRIKES_AROUND", "5"))


def _load(r: redis.Redis, key: str) -> Optional[dict]:
    raw = r.get(key)
    if not raw:
        return None
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _f(v) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def _window(strikes: List[float], spot: float, around: int) -> List[float]:
    """ATM (closest to spot) ± `around` strikes from the sorted chain."""
    if not strikes:
        return []
    atm_i = min(range(len(strikes)), key=lambda i: abs(strikes[i] - spot))
    return strikes[max(0, atm_i - around): atm_i + around + 1]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--around", type=int, default=SIE_STRIKES_AROUND)
    args = ap.parse_args()

    r = redis.from_url(REDIS_URL, decode_responses=True)
    try:
        r.ping()
    except Exception as e:
        print(f"Redis not reachable at {REDIS_URL}: {e}")
        return 2

    # (underlying, cp) -> strike -> tradingsymbol, from the greeks keys
    chain: Dict[tuple, Dict[float, str]] = defaultdict(dict)
    for key in r.scan_iter(match=f"{GREEKS_PREFIX}*", count=500):
        doc = _load(r, key)
        if not doc:
            continue
        und = str(doc.get("underlying") or "").upper()
        cp = str(doc.get("cp") or "").upper()
        strike = _f(doc.get("strike"))
        tsym = key[len(GREEKS_PREFIX):]
        if und and cp in ("CE", "PE") and strike is not None:
            chain[(und, cp)][strike] = tsym

    if not chain:
        print("No md:greeks:phase:latest:* keys — is run_greeks_analyzer.py running?")
        return 1

    # spot per underlying from Module 8 (the liquidity payload carries no spot)
    spots: Dict[str, float] = {}
    for und in {u for (u, _cp) in chain}:
        spot = _f((_load(r, f"{EXPECTED_MOVE_PREFIX}{und}") or {}).get("spot_price"))
        if spot:
            spots[und] = spot

    need = 2 * args.around + 1
    ok_all = True
    print(f"Strike coverage, ATM±{args.around} ({need} strikes per side)\n")
    print(f"{'UNDERLYING':<12}{'CP':<4}{'SPOT':>10}{'ATM':>10}{'CHAIN':>7}{'GREEKS':>8}{'LIQ':>6}{'BIDASK':>8}  STATUS")
    for (und, cp) in sorted(chain):
        strikes = sorted(chain[(und, cp)])
        spot = spots.get(und) or strikes[len(strikes) // 2]
        win = _window(strikes, spot, args.around)
        atm = win[len(win) // 2] if win else 0.0
        tsyms = [chain[(und, cp)][s] for s in win]
        n_liq = sum(1 for t in tsyms if r.exists(f"{LIQUIDITY_PREFIX}{t}"))
        n_ba = sum(1 for t in tsyms if r.exists(f"{BIDASK_PREFIX}{t}"))
        ok = len(win) >= need and n_liq == len(win) and n_ba == len(win)
        ok_all &= ok
        spot_s = f"{spots[und]:.2f}" if und in spots else "n/a"
        print(
            f"{und:<12}{cp:<4}{spot_s:>10}{atm:>10.2f}{len(strikes):>7}{len(win):>8}"
            f"{n_liq:>6}{n_ba:>8}  {'OK' if ok else 'GAP'}"
        )

    if not ok_all:
        print("\nGAP: raise STRIKES_AROUND in .env (must be >= --around) and restart the producer,"
              " or wait for liquidity/bidask workers to publish every contract.")
    return 0 if ok_all else 1


if __name__ == "__main__":
    sys.exit(main())

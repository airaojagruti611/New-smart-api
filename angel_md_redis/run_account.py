"""
run_account.py
──────────────
Account snapshot publisher for ICARE (DECISION.md D10).

ACCOUNT_MODE=paper (default): TOTAL_CAPITAL + open paper positions
  (md:position:open:*) + today's realized paper PnL (md:journal:daily:{date}).
ACCOUNT_MODE=live: Angel getRMS via SmartConnect.rmsLimit() (2 req/s limit;
  polled every ACCOUNT_POLL_SEC). Falls back to the paper ledger, flagged,
  if the call fails, so ICARE never sizes off a missing account.

Writes:
  Key : md:account:latest   (JSON AccountSnapshot + "error")
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time
from typing import List

import redis

from app.broker_account import paper_snapshot, parse_rms
from app.logging_setup import setup_logger
from app.option_pricing import IST

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
ACCOUNT_KEY = os.getenv("ACCOUNT_LATEST_KEY", "md:account:latest")
POSITION_OPEN_PREFIX = os.getenv("POSITION_OPEN_PREFIX", "md:position:open:")
JOURNAL_DAILY_PREFIX = os.getenv("JOURNAL_DAILY_PREFIX", "md:journal:daily:")

ACCOUNT_MODE = os.getenv("ACCOUNT_MODE", "paper").strip().lower()
TOTAL_CAPITAL = float(os.getenv("TOTAL_CAPITAL", "100000"))
POLL_SEC = float(os.getenv("ACCOUNT_POLL_SEC", "30"))

log = setup_logger("account")


def load_positions(r: redis.Redis) -> List[dict]:
    out = []
    for key in r.scan_iter(match=f"{POSITION_OPEN_PREFIX}*", count=500):
        raw = r.get(key)
        try:
            doc = json.loads(raw) if raw else None
        except Exception:
            doc = None
        if isinstance(doc, dict):
            out.append(doc)
    return out


def realized_today(r: redis.Redis) -> float:
    raw = r.get(f"{JOURNAL_DAILY_PREFIX}{dt.datetime.now(IST).date().isoformat()}")
    try:
        return float(json.loads(raw).get("realized_pnl") or 0.0) if raw else 0.0
    except Exception:
        return 0.0


def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    smart = None
    if ACCOUNT_MODE == "live":
        from app.angel_auth import login
        smart, _auth, _feed = login()
    log.info("START mode=%s capital=%s poll_sec=%s key=%s", ACCOUNT_MODE, TOTAL_CAPITAL, POLL_SEC, ACCOUNT_KEY)

    while True:
        now_ms = int(time.time() * 1000)
        positions = load_positions(r)
        snap, error = None, ""
        if smart is not None:
            try:
                resp = smart.rmsLimit()
                snap = parse_rms((resp or {}).get("data"), positions, now_ms)
                if snap is None:
                    error = f"rms_empty:{(resp or {}).get('message')}"
            except Exception as e:
                error = f"rms_error:{e}"
        if snap is None:
            snap = paper_snapshot(TOTAL_CAPITAL, positions, realized_today(r), now_ms)

        doc = snap.to_dict()
        doc["error"] = error
        r.set(ACCOUNT_KEY, json.dumps(doc, separators=(",", ":")))
        log.info(
            "ACCOUNT mode=%s capital=%s avail=%s used=%s util=%s%% day_pnl=%s open=%s risk=%s err=%s",
            snap.mode, snap.total_capital, snap.available_margin, snap.used_margin,
            snap.margin_utilization_pct, snap.day_pnl, snap.open_positions, snap.open_risk, error,
        )
        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()

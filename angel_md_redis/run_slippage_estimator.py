"""
run_slippage_estimator.py
─────────────────────────
Module 19 — Slippage Estimator Redis wiring. Advisory: places / blocks nothing.

Consumes md:icare (own consumer group). For every APPROVED sizing, reads the option's
latest quote (md:bidask:latest:{TSYM}: bid / ask / 5-level depth, from run_bidask_analyzer)
and estimates, BEFORE the order is placed, the entry VWAP fill walking the ask depth for
recommended_lots x lot_size, plus the exit side at the bid (round-trip cost vs mid).

  Stream : md:slippage:estimate
  Key    : md:slippage:latest:{TRADINGSYMBOL}   (JSON, TTL)
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, Optional

import redis

from app.logging_setup import setup_logger
from app.order_executor.market import quote_from_bidask
from app.slippage_estimator import estimate_slippage, round_trip_cost

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
IN_STREAM = os.getenv("STREAM_ICARE", "md:icare")
OUT_STREAM = os.getenv("STREAM_SLIPPAGE", "md:slippage:estimate")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_SLIPPAGE", "100000"))
LATEST_PREFIX = os.getenv("SLIPPAGE_LATEST_PREFIX", "md:slippage:latest:")
LATEST_TTL_SEC = int(os.getenv("SLIPPAGE_LATEST_TTL_SEC", "3600"))
BIDASK_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")
MAX_SLIPPAGE_PCT = float(os.getenv("SLIPPAGE_MAX_PCT", os.getenv("EXEC_MAX_SLIPPAGE_PCT", "0.5")))
GROUP = os.getenv("SLIPPAGE_GROUP", "slippage")
CONSUMER = os.getenv("SLIPPAGE_CONSUMER", "slippage-1")

log = setup_logger("slippage_estimator")


def ensure_group(r, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="$", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _f(v) -> Optional[float]:
    try:
        return None if v in (None, "") else float(v)
    except Exception:
        return None


def _load_json(r, key: str) -> dict:
    raw = r.get(key)
    if not raw:
        return {}
    try:
        d = json.loads(raw)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def build_estimate(approval: dict, quote_doc: dict, now_ms: int, max_slippage_pct: float = MAX_SLIPPAGE_PCT) -> dict:
    """Entry (BUY at the ask side) + exit (SELL at the bid side) estimate for one md:icare approval."""
    q = quote_from_bidask(quote_doc)
    lots, lot_size = _f(approval.get("recommended_lots")), _f(approval.get("lot_size"))
    premium = _f(approval.get("premium"))
    entry = estimate_slippage(q, lots or 0, lot_size or 0, "BUY", reference=premium, max_slippage_pct=max_slippage_pct)
    exit_ = estimate_slippage(q, lots or 0, lot_size or 0, "SELL")
    return {
        "ts_ms": now_ms,
        "symbol": str(approval.get("symbol") or ""),
        "tradingsymbol": str(approval.get("tradingsymbol") or ""),
        "side": str(approval.get("side") or ""),
        "trade_quality": approval.get("trade_quality"),
        "icare_ts_ms": approval.get("ts_ms"),
        "quote_ts_ms": q.ts_ms if q else None,
        "quote_age_ms": q.age_ms(now_ms) if q and (q.ts_ms or q.recv_ts_ms) else None,
        "entry": entry,
        "exit": exit_,
        "round_trip_cost": round_trip_cost(entry, exit_),
        "status": entry.get("status"),
        "flags": entry.get("flags", []),
    }


def handle_message(r, fields: dict, now_ms: int) -> Optional[dict]:
    """Estimate one md:icare message; None when skipped (not APPROVED / no contract / no lots)."""
    if str(fields.get("status") or fields.get("execution_status") or "").upper() != "APPROVED":
        return None
    tsym = str(fields.get("tradingsymbol") or "").strip()
    if not tsym or not (_f(fields.get("recommended_lots")) or 0) > 0:
        return None
    est = build_estimate(fields, _load_json(r, f"{BIDASK_PREFIX}{tsym}"), now_ms)
    doc = json.dumps(est, separators=(",", ":"))
    r.xadd(OUT_STREAM, {"ts_ms": str(now_ms), "symbol": est["symbol"], "tradingsymbol": tsym,
                        "status": str(est["status"]), "data": doc}, maxlen=OUT_MAXLEN, approximate=True)
    r.set(f"{LATEST_PREFIX}{tsym}", doc, ex=LATEST_TTL_SEC)
    log.info("LOGIC tsym=%s status=%s slip_pct=%s fill_vwap=%s unfilled=%s flags=%s", tsym, est["status"],
             est["entry"].get("slippage_pct"), est["entry"].get("vwap"), est["entry"].get("unfilled_qty"),
             est["flags"])
    return est


def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_STREAM, GROUP)
    log.info("START reading %s, writing %s + %s*", IN_STREAM, OUT_STREAM, LATEST_PREFIX)
    while True:
        resp = r.xreadgroup(groupname=GROUP, consumername=CONSUMER, streams={IN_STREAM: ">"}, count=200, block=2000)
        if not resp:
            continue
        for stream, msgs in resp:
            ids = []
            for msg_id, fields in msgs:
                ids.append(msg_id)
                try:
                    handle_message(r, fields, int(time.time() * 1000))
                except Exception as e:  # one bad message must not stop the worker
                    log.warning("ERROR id=%s %s", msg_id, e)
            if ids:
                r.xack(stream, GROUP, *ids)


if __name__ == "__main__":
    main()

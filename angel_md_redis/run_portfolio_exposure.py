"""
run_portfolio_exposure.py
─────────────────────────
Module 21 — Portfolio Exposure Monitor Redis wiring (read-only on positions).

Every PORTFOLIO_EXPOSURE_INTERVAL_SEC (default 5) reads
  md:position:open:*            open paper positions (run_trade_journal.py — never written here)
  md:bidask:latest:{TSYM}       live option bid to mark each position
  md:account:latest             account snapshot (run_account.py)
  sectors.json                  underlying -> sector
and limits from the ICARE config (run_icare.CFG), then publishes:

  Key    : md:portfolio:exposure          latest snapshot (JSON, TTL)
  Stream : md:portfolio:exposure:stream   one entry per interval (field `data` = JSON, plus headline fields)
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, List, Optional

import redis

from app.logging_setup import setup_logger
from app.portfolio_exposure import compute_exposure
from run_icare import ACCOUNT_KEY, CFG, POSITION_OPEN_PREFIX, TOTAL_CAPITAL, load_sectors

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
LATEST_KEY = os.getenv("PORTFOLIO_EXPOSURE_KEY", "md:portfolio:exposure")
OUT_STREAM = os.getenv("STREAM_PORTFOLIO_EXPOSURE", "md:portfolio:exposure:stream")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_PORTFOLIO_EXPOSURE", "50000"))
INTERVAL_SEC = float(os.getenv("PORTFOLIO_EXPOSURE_INTERVAL_SEC", "5"))
LATEST_TTL_SEC = int(os.getenv("PORTFOLIO_EXPOSURE_TTL_SEC", "600"))
BIDASK_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")

log = setup_logger("portfolio_exposure")


def _load_json(r, key: str) -> dict:
    raw = r.get(key)
    if not raw:
        return {}
    try:
        d = json.loads(raw)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _f(v) -> Optional[float]:
    try:
        return None if v in (None, "") else float(v)
    except Exception:
        return None


def load_open_positions(r) -> List[dict]:
    out = []
    for key in r.scan_iter(match=f"{POSITION_OPEN_PREFIX}*", count=500):
        doc = _load_json(r, key)
        if doc:
            out.append(doc)
    return out


def load_bids(r, positions: List[dict]) -> Dict[str, Optional[float]]:
    bids: Dict[str, Optional[float]] = {}
    for p in positions:
        tsym = str(p.get("tradingsymbol") or "")
        if tsym and tsym not in bids:
            bids[tsym] = _f(_load_json(r, f"{BIDASK_PREFIX}{tsym}").get("bid"))
    return bids


def run_once(r, sectors: Dict[str, str], now_ms: int) -> dict:
    positions = load_open_positions(r)
    snap = compute_exposure(positions, load_bids(r, positions), _load_json(r, ACCOUNT_KEY), sectors, CFG,
                            TOTAL_CAPITAL, now_ms)
    doc = json.dumps(snap, separators=(",", ":"))
    r.set(LATEST_KEY, doc, ex=LATEST_TTL_SEC)
    r.xadd(OUT_STREAM, {
        "ts_ms": str(now_ms), "open_positions": str(snap["open_positions"]), "deployed": str(snap["deployed"]),
        "utilization_pct": "" if snap["utilization_pct"] is None else str(snap["utilization_pct"]),
        "unrealized_pnl": str(snap["unrealized_pnl"]), "risk_to_sl": str(snap["risk_to_sl"]),
        "flags": json.dumps(snap["flags"]), "data": doc,
    }, maxlen=OUT_MAXLEN, approximate=True)
    return snap


def main():
    sectors = load_sectors()
    r = redis.from_url(REDIS_URL, decode_responses=True)
    log.info("START every %ss -> %s + %s (sectors=%d)", INTERVAL_SEC, LATEST_KEY, OUT_STREAM, len(sectors))
    while True:
        try:
            snap = run_once(r, sectors, int(time.time() * 1000))
            log.info("LOGIC open=%s deployed=%s util=%s upnl=%s risk_to_sl=%s flags=%s", snap["open_positions"],
                     snap["deployed"], snap["utilization_pct"], snap["unrealized_pnl"], snap["risk_to_sl"],
                     snap["flags"])
        except Exception as e:
            log.warning("ERROR %s", e)
        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    main()

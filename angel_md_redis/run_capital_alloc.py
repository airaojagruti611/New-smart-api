"""
run_capital_alloc.py
────────────────────
Consumes md:strike:select OK signals, reads md:regime:latest, and emits sized
capital allocation:
  md:capital:alloc
  md:capital:alloc:latest:{SYMBOL}
"""

from __future__ import annotations

import json
import os
import time

import redis

from app.capital_alloc import capital_alloc
from app.config import load_symbols
from app.logging_setup import setup_logger


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

IN_STREAM = os.getenv("STREAM_STRIKE_SELECT", "md:strike:select")
OUT_STREAM = os.getenv("STREAM_CAPITAL_ALLOC", "md:capital:alloc")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_CAPITAL_ALLOC", "200000"))
LATEST_KEY_PREFIX = os.getenv("CAPITAL_ALLOC_LATEST_PREFIX", "md:capital:alloc:latest:")
REGIME_LATEST_KEY = os.getenv("REGIME_LATEST_KEY", "md:regime:latest")

GROUP = os.getenv("CAPITAL_ALLOC_GROUP", "capital-alloc")
CONSUMER = os.getenv("CAPITAL_ALLOC_CONSUMER", "capital-alloc-1")

TOTAL_CAPITAL = float(os.getenv("TOTAL_CAPITAL", "100000"))
MAX_RISK_PER_TRADE_PCT = float(os.getenv("MAX_RISK_PER_TRADE_PCT", "5"))

log = setup_logger("capital_alloc")


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _load_regime(r: redis.Redis) -> dict:
    raw = r.get(REGIME_LATEST_KEY)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def main():
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_STREAM, GROUP)

    log.info(
        "START reading %s, writing %s (total_capital=%s risk_pct=%s symbols=%d)",
        IN_STREAM,
        OUT_STREAM,
        TOTAL_CAPITAL,
        MAX_RISK_PER_TRADE_PCT,
        len(symbols),
    )

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={IN_STREAM: ">"},
            count=2000,
            block=2000,
        )
        if not resp:
            continue

        now_ms = int(time.time() * 1000)
        ack_ids = []
        regime_snap = _load_regime(r)

        for _stream, msgs in resp:
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)

                sym = str(fields.get("symbol") or "").strip().upper()
                log.debug("MSG_IN id=%s symbol=%s fields=%s", msg_id, sym, fields)

                if not sym or sym not in symbols:
                    log.debug("SKIP unknown_symbol id=%s symbol=%r", msg_id, sym)
                    continue

                status = str(fields.get("status") or "").strip().upper()
                if status != "OK":
                    log.debug("SKIP strike_not_ok symbol=%s status=%s", sym, status)
                    continue

                signal = str(fields.get("signal") or "").strip()
                result = capital_alloc(
                    signal=signal,
                    regime_snapshot=regime_snap,
                    total_capital=TOTAL_CAPITAL,
                    max_risk_per_trade_pct=MAX_RISK_PER_TRADE_PCT,
                )

                payload = {
                    "ts_ms": str(now_ms),
                    "symbol": sym,
                    "signal": result.signal,
                    "status": result.status,
                    "side": result.side,
                    "regime": result.regime,
                    "call_alloc_pct": str(result.call_alloc_pct),
                    "put_alloc_pct": str(result.put_alloc_pct),
                    "side_alloc_pct": str(result.side_alloc_pct),
                    "total_capital": str(result.total_capital),
                    "side_budget": str(result.side_budget),
                    "trade_notional": str(result.trade_notional),
                    "strike": str(fields.get("strike") or ""),
                    "token": str(fields.get("token") or ""),
                    "tradingsymbol": str(fields.get("tradingsymbol") or ""),
                    "exchange": str(fields.get("exchange") or ""),
                    "mode": str(fields.get("mode") or ""),
                    "strength": str(fields.get("strength") or ""),
                    "spot": str(fields.get("spot") or ""),
                    "expiry": str(fields.get("expiry") or ""),
                    "reason": result.reason,
                    "bar_ts_ms": str(fields.get("bar_ts_ms") or now_ms),
                }

                log.info(
                    "LOGIC symbol=%s status=%s regime=%s side=%s "
                    "call=%s put=%s budget=%s notional=%s reason=%s",
                    sym,
                    result.status,
                    result.regime,
                    result.side,
                    result.call_alloc_pct,
                    result.put_alloc_pct,
                    result.side_budget,
                    result.trade_notional,
                    result.reason,
                )

                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(
                    f"{LATEST_KEY_PREFIX}{sym}",
                    json.dumps(payload, separators=(",", ":")),
                    ex=3600,
                )

                if result.status == "OK":
                    log.info("EMIT symbol=%s payload=%s", sym, payload)
                else:
                    log.info("SKIP_RESULT symbol=%s payload=%s", sym, payload)

        if ack_ids:
            r.xack(IN_STREAM, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

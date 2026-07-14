"""
run_level_entry.py
──────────────────
Consumes 1m candles and previous-day classic pivots to emit pivot-break entry
signals into:
  md:level:entry
  md:level:entry:latest:{SYMBOL}
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, Optional

import redis

from app.config import load_symbols
from app.level_entry import level_entry, parse_pivots_payload
from app.logging_setup import setup_logger


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

IN_1M = os.getenv("STREAM_CANDLES_1M", "md:candles:1m")
PIVOTS_KEY_PREFIX = os.getenv("PIVOTS_PREVDAY_PREFIX", "md:pivots:prevday:")

OUT_STREAM = os.getenv("STREAM_LEVEL_ENTRY", "md:level:entry")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_LEVEL_ENTRY", "200000"))
LATEST_KEY_PREFIX = os.getenv("LEVEL_ENTRY_LATEST_PREFIX", "md:level:entry:latest:")

GROUP = os.getenv("LEVEL_ENTRY_GROUP", "level-entry")
CONSUMER = os.getenv("LEVEL_ENTRY_CONSUMER", "level-entry-1")

log = setup_logger("level_entry")


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _safe_float(v) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _safe_int(v) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(float(v))
    except Exception:
        return None


def _load_pivots(r: redis.Redis, symbol: str):
    key = f"{PIVOTS_KEY_PREFIX}{symbol}"
    raw = r.get(key)
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except Exception:
        log.warning("bad_json key=%s", key)
        return None
    if not isinstance(data, dict):
        return None
    return parse_pivots_payload(data)


def main():
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_1M, GROUP)

    # Last closed 1m price per symbol (for break detection).
    prev_close_by_symbol: Dict[str, float] = {}

    log.info(
        "START reading %s + %s{{SYMBOL}}, writing %s (symbols=%d)",
        IN_1M,
        PIVOTS_KEY_PREFIX,
        OUT_STREAM,
        len(symbols),
    )

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={IN_1M: ">"},
            count=2000,
            block=2000,
        )
        if not resp:
            continue

        now_ms = int(time.time() * 1000)
        ack_ids = []

        for _stream, msgs in resp:
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)

                sym = str(fields.get("symbol") or "").strip().upper()
                log.debug("MSG_IN id=%s symbol=%s fields=%s", msg_id, sym, fields)

                if not sym or sym not in symbols:
                    log.debug("SKIP unknown_symbol id=%s symbol=%r", msg_id, sym)
                    continue

                close = _safe_float(fields.get("c"))
                bar_ts = _safe_int(fields.get("ts_ms"))
                if close is None:
                    log.debug("SKIP bad_close symbol=%s c=%r", sym, fields.get("c"))
                    continue

                prev_close = prev_close_by_symbol.get(sym)
                prev_close_by_symbol[sym] = close
                if prev_close is None:
                    log.debug("SKIP seed_prev_close symbol=%s close=%.4f", sym, close)
                    continue

                pivots = _load_pivots(r, sym)
                if pivots is None:
                    log.debug("SKIP no_pivots symbol=%s", sym)
                    continue

                log.debug(
                    "INPUTS symbol=%s prev_close=%.4f close=%.4f pivots=%s",
                    sym,
                    prev_close,
                    close,
                    pivots,
                )

                result = level_entry(prev_close, close, pivots)

                payload = {
                    "ts_ms": str(now_ms),
                    "symbol": sym,
                    "tf": "1m",
                    "signal": result.signal,
                    "level": result.level,
                    "side": result.side,
                    "strength": result.strength,
                    "price": f"{result.price:.2f}",
                    "aligned": "1" if result.signal.startswith("BUY") else "0",
                    "P": f"{pivots.P:.2f}",
                    "R1": f"{pivots.R1:.2f}",
                    "S1": f"{pivots.S1:.2f}",
                    "R2": f"{pivots.R2:.2f}",
                    "S2": f"{pivots.S2:.2f}",
                    "pivot_date": pivots.date,
                    "bar_ts_ms": str(bar_ts or now_ms),
                    "reason": result.reason,
                }

                if result.signal.startswith("BUY"):
                    log.info(
                        "LOGIC symbol=%s prev=%.2f close=%.2f -> signal=%s level=%s "
                        "strength=%s reason=%s",
                        sym,
                        prev_close,
                        close,
                        result.signal,
                        result.level,
                        result.strength,
                        result.reason,
                    )
                else:
                    log.debug(
                        "LOGIC symbol=%s prev=%.2f close=%.2f -> signal=%s reason=%s",
                        sym,
                        prev_close,
                        close,
                        result.signal,
                        result.reason,
                    )

                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(
                    f"{LATEST_KEY_PREFIX}{sym}",
                    json.dumps(payload, separators=(",", ":")),
                    ex=3600,
                )

                if result.signal.startswith("BUY"):
                    log.info("EMIT symbol=%s payload=%s", sym, payload)
                else:
                    log.debug("NEUTRAL_EMIT symbol=%s payload=%s", sym, payload)

        if ack_ids:
            r.xack(IN_1M, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

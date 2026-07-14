"""
run_htf_trend_filter.py
───────────────────────
Chartink-style higher-timeframe trend filter from closed daily candles:

  Daily close  > previous daily
  Weekly close > previous weekly
  Monthly close > previous monthly

Writes:
  md:htf:trend
  md:htf:trend:latest:{SYMBOL}
"""

from __future__ import annotations

import json
import os
import time
from collections import defaultdict, deque
from typing import Deque, Dict, Optional, Tuple

import redis

from app.candle_types import Candle
from app.config import load_symbols
from app.htf_trend_filter import htf_trend_bias
from app.logging_setup import setup_logger


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
IN_1D = os.getenv("STREAM_CANDLES_1D", "md:candles:1d")

OUT_STREAM = os.getenv("STREAM_HTF_TREND", "md:htf:trend")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_HTF_TREND", "200000"))
LATEST_KEY_PREFIX = os.getenv("HTF_TREND_LATEST_PREFIX", "md:htf:trend:latest:")
LATEST_TTL_SEC = int(os.getenv("HTF_TREND_LATEST_TTL_SEC", str(7 * 24 * 3600)))

GROUP = os.getenv("HTF_TREND_GROUP", "htf-trend")
CONSUMER = os.getenv("HTF_TREND_CONSUMER", "htf-trend-1")

# Keep enough dailies to form prior weeks/months.
WINDOW = int(os.getenv("HTF_DAILY_WINDOW", "120"))
BOOTSTRAP_COUNT = int(os.getenv("HTF_BOOTSTRAP_COUNT", "50000"))

log = setup_logger("htf_trend")


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


def _parse_1d(fields: dict) -> Optional[Tuple[str, Candle]]:
    sym = str(fields.get("symbol") or "").strip().upper()
    ts_ms = _safe_int(fields.get("ts_ms"))
    o = _safe_float(fields.get("o"))
    h = _safe_float(fields.get("h"))
    l = _safe_float(fields.get("l"))
    c = _safe_float(fields.get("c"))
    v = _safe_float(fields.get("v")) or 0.0
    if not sym or ts_ms is None or o is None or h is None or l is None or c is None:
        return None
    return sym, Candle(ts_ms=ts_ms, o=o, h=h, l=l, c=c, v=v)


def _append_daily(buf: Deque[Candle], candle: Candle) -> None:
    if buf and buf[-1].ts_ms == candle.ts_ms:
        buf[-1] = candle
        return
    if buf and candle.ts_ms < buf[-1].ts_ms:
        # Out-of-order: rebuild sorted unique by ts_ms.
        by_ts = {x.ts_ms: x for x in buf}
        by_ts[candle.ts_ms] = candle
        ordered = [by_ts[k] for k in sorted(by_ts)]
        buf.clear()
        buf.extend(ordered[-WINDOW:])
        return
    buf.append(candle)
    while len(buf) > WINDOW:
        buf.popleft()


def _publish(r: redis.Redis, sym: str, result, now_ms: int) -> None:
    payload = {
        "ts_ms": str(now_ms),
        "symbol": sym,
        "daily": result.daily,
        "weekly": result.weekly,
        "monthly": result.monthly,
        "bias": result.bias,
        "d_close": f"{result.d_close:.2f}",
        "d_prev": f"{result.d_prev:.2f}",
        "w_close": f"{result.w_close:.2f}",
        "w_prev": f"{result.w_prev:.2f}",
        "m_close": f"{result.m_close:.2f}",
        "m_prev": f"{result.m_prev:.2f}",
    }
    r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
    r.set(
        f"{LATEST_KEY_PREFIX}{sym}",
        json.dumps(payload, separators=(",", ":")),
        ex=LATEST_TTL_SEC,
    )
    log.debug("EMIT symbol=%s payload=%s", sym, payload)


def _bootstrap(r: redis.Redis, symbols: set, daily_by_sym: Dict[str, Deque[Candle]]) -> None:
    """Seed per-symbol daily buffers from existing 1d stream history."""
    try:
        rows = r.xrevrange(IN_1D, max="+", min="-", count=BOOTSTRAP_COUNT)
    except Exception as e:
        log.warning("bootstrap skipped: %s", e)
        return

    # xrevrange is newest-first; reverse to oldest-first.
    for _id, fields in reversed(rows):
        parsed = _parse_1d(fields)
        if not parsed:
            continue
        sym, candle = parsed
        if symbols and sym not in symbols:
            continue
        _append_daily(daily_by_sym[sym], candle)

    now_ms = int(time.time() * 1000)
    n = 0
    for sym, buf in daily_by_sym.items():
        if len(buf) < 2:
            log.debug("bootstrap skip short_history symbol=%s bars=%d", sym, len(buf))
            continue
        result = htf_trend_bias(list(buf))
        _publish(r, sym, result, now_ms)
        log.info(
            "BOOTSTRAP symbol=%s bias=%s D=%s W=%s M=%s bars=%d",
            sym,
            result.bias,
            result.daily,
            result.weekly,
            result.monthly,
            len(buf),
        )
        n += 1
    log.info("bootstrap done: symbols_with_bias=%s", n)


def main():
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_1D, GROUP)

    daily_by_sym: Dict[str, Deque[Candle]] = defaultdict(lambda: deque(maxlen=WINDOW))
    last_bias: Dict[str, str] = {}

    log.info(
        "START reading %s -> writing %s + %s{{SYMBOL}} (window=%s symbols=%d)",
        IN_1D,
        OUT_STREAM,
        LATEST_KEY_PREFIX,
        WINDOW,
        len(symbols),
    )
    _bootstrap(r, symbols, daily_by_sym)

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={IN_1D: ">"},
            count=2000,
            block=5000,
        )
        if not resp:
            continue

        now_ms = int(time.time() * 1000)
        ack_ids = []

        for _stream, msgs in resp:
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)

                parsed = _parse_1d(fields)
                if not parsed:
                    log.debug("SKIP bad_1d id=%s fields=%s", msg_id, fields)
                    continue
                sym, candle = parsed
                log.debug(
                    "MSG_IN id=%s symbol=%s ts_ms=%s c=%.4f",
                    msg_id,
                    sym,
                    candle.ts_ms,
                    candle.c,
                )
                if symbols and sym not in symbols:
                    log.debug("SKIP unknown_symbol symbol=%s", sym)
                    continue

                _append_daily(daily_by_sym[sym], candle)
                result = htf_trend_bias(list(daily_by_sym[sym]))
                _publish(r, sym, result, now_ms)

                log.info(
                    "LOGIC symbol=%s bias=%s D=%s(%.2f/%.2f) W=%s(%.2f/%.2f) M=%s(%.2f/%.2f)",
                    sym,
                    result.bias,
                    result.daily,
                    result.d_close,
                    result.d_prev,
                    result.weekly,
                    result.w_close,
                    result.w_prev,
                    result.monthly,
                    result.m_close,
                    result.m_prev,
                )

                prev = last_bias.get(sym)
                if prev != result.bias:
                    last_bias[sym] = result.bias
                    log.info(
                        "BIAS_CHANGE symbol=%s %s -> %s D=%s W=%s M=%s",
                        sym,
                        prev,
                        result.bias,
                        result.daily,
                        result.weekly,
                        result.monthly,
                    )

        if ack_ids:
            r.xack(IN_1D, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

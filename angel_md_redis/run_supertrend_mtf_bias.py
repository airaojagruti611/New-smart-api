import json
import os
import time
from collections import defaultdict, deque

import redis

from app.candle_io import parse_candle_fields, read_last_candles
from app.candle_types import Candle
from app.config import load_symbols
from app.logging_setup import setup_logger
from app.trend_filter import mtf_supertrend_bias


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

IN_1M = os.getenv("STREAM_CANDLES_1M", "md:candles:1m")
IN_5M = os.getenv("STREAM_CANDLES_5M", "md:candles:5m")
IN_10M = os.getenv("STREAM_CANDLES_10M", "md:candles:10m")
IN_30M = os.getenv("STREAM_CANDLES_30M", "md:candles:30m")

OUT_STREAM = os.getenv("STREAM_SUPERTREND_BIAS", "md:supertrend:bias")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_SUPERTREND_BIAS", "200000"))
LATEST_KEY_PREFIX = os.getenv("SUPERTREND_BIAS_LATEST_PREFIX", "md:supertrend:bias:latest:")

GROUP = os.getenv("SUPERTREND_BIAS_GROUP", "supertrend-bias")
CONSUMER = os.getenv("SUPERTREND_BIAS_CONSUMER", "supertrend-bias-1")

ST_ATR = int(os.getenv("ST_ATR", "7"))
ST_MULT = float(os.getenv("ST_MULT", "1.0"))
ST_MAJORITY = int(os.getenv("ST_MAJORITY", "3"))
WINDOW = int(os.getenv("ST_WINDOW_BARS", "250"))

log = setup_logger("st_bias")


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _parse_candle(fields: dict) -> Candle | None:
    parsed = parse_candle_fields(fields)
    if parsed is None:
        return None
    return parsed[1]


def main():
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)

    for s in (IN_1M, IN_5M, IN_10M, IN_30M):
        ensure_group(r, s, GROUP)

    by_tf = {
        "1m": IN_1M,
        "5m": IN_5M,
        "10m": IN_10M,
        "30m": IN_30M,
    }

    # in-memory rolling windows: tf -> symbol -> deque[Candle]
    windows: dict[str, dict[str, deque[Candle]]] = {
        tf: defaultdict(lambda: deque(maxlen=WINDOW)) for tf in by_tf
    }
    last_bias: dict[str, str] = {}
    warmup_done: set[str] = set()

    log.info(
        "START reading 1m/5m/10m/30m candles, writing %s "
        "(atr=%s, mult=%s, majority=%s, window=%s symbols=%d)",
        OUT_STREAM,
        ST_ATR,
        ST_MULT,
        ST_MAJORITY,
        WINDOW,
        len(symbols),
    )

    now_ms = int(time.time() * 1000)
    for sym in symbols:
        bar_counts = {}
        for tf, stream in by_tf.items():
            bars = read_last_candles(r, stream, sym, WINDOW)
            if bars:
                windows[tf][sym].extend(bars)
            bar_counts[tf] = len(windows[tf][sym])
        candles_by_tf = {tf: list(windows[tf][sym]) for tf in windows}
        res = mtf_supertrend_bias(
            candles_by_tf=candles_by_tf,
            atr_period=ST_ATR,
            multiplier=ST_MULT,
            majority=ST_MAJORITY,
        )
        payload = {
            "ts_ms": str(now_ms),
            "symbol": sym,
            "bias": res.bias,
            "bullish": str(res.bullish),
            "bearish": str(res.bearish),
            "st_30m": res.per_tf.get("30m", "na"),
            "st_10m": res.per_tf.get("10m", "na"),
            "st_5m": res.per_tf.get("5m", "na"),
            "st_1m": res.per_tf.get("1m", "na"),
        }
        log.info(
            "WARMUP symbol=%s bars=%s per_tf=%s bias=%s",
            sym,
            bar_counts,
            res.per_tf,
            res.bias,
        )
        if all(v != "na" for v in res.per_tf.values()):
            warmup_done.add(sym)
        last_bias[sym] = res.bias
        r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
        r.set(f"{LATEST_KEY_PREFIX}{sym}", json.dumps(payload, separators=(",", ":")), ex=3600)

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={IN_1M: ">", IN_5M: ">", IN_10M: ">", IN_30M: ">"},
            count=2000,
            block=2000,
        )
        if not resp:
            continue

        # recompute bias for symbols touched in this batch
        touched: set[str] = set()

        for stream, msgs in resp:
            tf = None
            if stream == IN_1M:
                tf = "1m"
            elif stream == IN_5M:
                tf = "5m"
            elif stream == IN_10M:
                tf = "10m"
            elif stream == IN_30M:
                tf = "30m"

            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)
                c = _parse_candle(fields)
                if c is None:
                    log.debug("SKIP bad_candle stream=%s id=%s", stream, msg_id)
                    continue
                sym = str(fields.get("symbol") or "").strip().upper()
                if sym not in symbols:
                    log.debug("SKIP unknown_symbol stream=%s symbol=%r", stream, sym)
                    continue
                if tf is None:
                    log.debug("SKIP unknown_stream stream=%s", stream)
                    continue
                log.debug(
                    "MSG_IN stream=%s tf=%s id=%s symbol=%s ts_ms=%s c=%.4f",
                    stream,
                    tf,
                    msg_id,
                    sym,
                    c.ts_ms,
                    c.c,
                )
                windows[tf][sym].append(c)
                touched.add(sym)

            if ack_ids:
                r.xack(stream, GROUP, *ack_ids)

        now_ms = int(time.time() * 1000)
        for sym in touched:
            candles_by_tf = {tf: list(windows[tf][sym]) for tf in windows}
            res = mtf_supertrend_bias(
                candles_by_tf=candles_by_tf,
                atr_period=ST_ATR,
                multiplier=ST_MULT,
                majority=ST_MAJORITY,
            )
            payload = {
                "ts_ms": str(now_ms),
                "symbol": sym,
                "bias": res.bias,
                "bullish": str(res.bullish),
                "bearish": str(res.bearish),
                "st_30m": res.per_tf.get("30m", "na"),
                "st_10m": res.per_tf.get("10m", "na"),
                "st_5m": res.per_tf.get("5m", "na"),
                "st_1m": res.per_tf.get("1m", "na"),
            }

            prev = last_bias.get(sym)
            if prev != res.bias:
                last_bias[sym] = res.bias
                log.info(
                    "BIAS_CHANGE symbol=%s %s -> %s bullish=%s bearish=%s "
                    "tf={30m:%s,10m:%s,5m:%s,1m:%s}",
                    sym,
                    prev,
                    res.bias,
                    res.bullish,
                    res.bearish,
                    payload["st_30m"],
                    payload["st_10m"],
                    payload["st_5m"],
                    payload["st_1m"],
                )
            else:
                log.debug(
                    "LOGIC symbol=%s bias=%s bullish=%s bearish=%s per_tf=%s",
                    sym,
                    res.bias,
                    res.bullish,
                    res.bearish,
                    res.per_tf,
                )
            if sym not in warmup_done and all(v != "na" for v in res.per_tf.values()):
                warmup_done.add(sym)
                log.info("WARMUP_READY symbol=%s per_tf=%s bias=%s", sym, res.per_tf, res.bias)

            r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
            r.set(f"{LATEST_KEY_PREFIX}{sym}", json.dumps(payload, separators=(",", ":")), ex=3600)
            log.debug("EMIT symbol=%s payload=%s", sym, payload)


if __name__ == "__main__":
    main()

import json
import os
import time
from collections import defaultdict, deque

import redis

from app.candle_io import parse_candle_fields, read_last_candles
from app.candle_types import Candle
from app.config import load_symbols
from app.ema_cross import last_ema_cross_signal
from app.logging_setup import setup_logger


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

IN_1M = os.getenv("STREAM_CANDLES_1M", "md:candles:1m")

OUT_STREAM = os.getenv("STREAM_EMA_CROSS", "md:ema:cross")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_EMA_CROSS", "200000"))
LATEST_KEY_PREFIX = os.getenv("EMA_CROSS_LATEST_PREFIX", "md:ema:cross:latest:")

GROUP = os.getenv("EMA_CROSS_GROUP", "ema-cross")
CONSUMER = os.getenv("EMA_CROSS_CONSUMER", "ema-cross-1")

EMA_FAST = int(os.getenv("EMA_FAST", "9"))
EMA_SLOW = int(os.getenv("EMA_SLOW", "26"))
WINDOW = int(os.getenv("EMA_WINDOW_BARS", "250"))

log = setup_logger("ema_cross")


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

    ensure_group(r, IN_1M, GROUP)

    # in-memory rolling windows: symbol -> deque[Candle]
    windows: dict[str, deque[Candle]] = defaultdict(lambda: deque(maxlen=WINDOW))
    last_state: dict[str, str] = {}

    log.info(
        "START reading %s, writing %s (fast=%s, slow=%s, window=%s symbols=%d)",
        IN_1M,
        OUT_STREAM,
        EMA_FAST,
        EMA_SLOW,
        WINDOW,
        len(symbols),
    )

    now_ms = int(time.time() * 1000)
    for sym in symbols:
        bars = read_last_candles(r, IN_1M, sym, WINDOW)
        if bars:
            windows[sym].extend(bars)
        pt = last_ema_cross_signal(list(windows[sym]), fast=EMA_FAST, slow=EMA_SLOW)
        if pt is None:
            log.info("WARMUP symbol=%s bars=%d need=%d", sym, len(windows[sym]), EMA_SLOW)
            continue
        last_state[sym] = pt.state
        payload = {
            "ts_ms": str(now_ms),
            "symbol": sym,
            "tf": "1m",
            "signal": pt.signal,
            "state": pt.state,
            "ema9": f"{pt.ema_fast:.6f}",
            "ema26": f"{pt.ema_slow:.6f}",
            "bar_ts_ms": str(pt.ts_ms),
        }
        log.info(
            "WARMUP_READY symbol=%s bars=%d state=%s ema9=%.6f ema26=%.6f",
            sym,
            len(windows[sym]),
            pt.state,
            pt.ema_fast,
            pt.ema_slow,
        )
        r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
        r.set(f"{LATEST_KEY_PREFIX}{sym}", json.dumps(payload, separators=(",", ":")), ex=3600)

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

        touched: set[str] = set()
        ack_ids = []

        for _stream, msgs in resp:
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)
                c = _parse_candle(fields)
                if c is None:
                    log.debug("SKIP bad_candle id=%s fields=%s", msg_id, fields)
                    continue
                sym = str(fields.get("symbol") or "").strip().upper()
                if sym not in symbols:
                    log.debug("SKIP unknown_symbol id=%s symbol=%r", msg_id, sym)
                    continue
                log.debug("MSG_IN id=%s symbol=%s ts_ms=%s c=%.4f", msg_id, sym, c.ts_ms, c.c)
                windows[sym].append(c)
                touched.add(sym)

        if ack_ids:
            r.xack(IN_1M, GROUP, *ack_ids)

        now_ms = int(time.time() * 1000)
        for sym in touched:
            pt = last_ema_cross_signal(
                list(windows[sym]),
                fast=EMA_FAST,
                slow=EMA_SLOW,
            )
            if pt is None:
                if last_state.get(sym) != "warmup":
                    last_state[sym] = "warmup"
                    log.info("WARMUP symbol=%s bars=%d need=%d", sym, len(windows[sym]), EMA_SLOW)
                continue

            payload = {
                "ts_ms": str(now_ms),
                "symbol": sym,
                "tf": "1m",
                "signal": pt.signal,
                "state": pt.state,
                "ema9": f"{pt.ema_fast:.6f}",
                "ema26": f"{pt.ema_slow:.6f}",
                "bar_ts_ms": str(pt.ts_ms),
            }

            prev = last_state.get(sym)
            if prev != pt.state:
                last_state[sym] = pt.state
                log.info(
                    "STATE_CHANGE symbol=%s %s -> %s signal=%s ema9=%.6f ema26=%.6f",
                    sym,
                    prev,
                    pt.state,
                    pt.signal,
                    pt.ema_fast,
                    pt.ema_slow,
                )
            else:
                log.debug(
                    "LOGIC symbol=%s state=%s signal=%s ema9=%.6f ema26=%.6f",
                    sym,
                    pt.state,
                    pt.signal,
                    pt.ema_fast,
                    pt.ema_slow,
                )

            r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
            r.set(f"{LATEST_KEY_PREFIX}{sym}", json.dumps(payload, separators=(",", ":")), ex=3600)
            log.debug("EMIT symbol=%s payload=%s", sym, payload)


if __name__ == "__main__":
    main()

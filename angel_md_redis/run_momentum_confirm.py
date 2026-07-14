import json
import os
import time

import redis

from app.config import load_symbols
from app.logging_setup import setup_logger
from app.momentum_confirm import momentum_confirm


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

IN_ST = os.getenv("STREAM_SUPERTREND_BIAS", "md:supertrend:bias")
IN_EMA = os.getenv("STREAM_EMA_CROSS", "md:ema:cross")

ST_LATEST_PREFIX = os.getenv("SUPERTREND_BIAS_LATEST_PREFIX", "md:supertrend:bias:latest:")
EMA_LATEST_PREFIX = os.getenv("EMA_CROSS_LATEST_PREFIX", "md:ema:cross:latest:")

OUT_STREAM = os.getenv("STREAM_MOMENTUM_CONFIRM", "md:momentum:confirm")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_MOMENTUM_CONFIRM", "200000"))
LATEST_KEY_PREFIX = os.getenv("MOMENTUM_CONFIRM_LATEST_PREFIX", "md:momentum:confirm:latest:")

GROUP = os.getenv("MOMENTUM_CONFIRM_GROUP", "momentum-confirm")
CONSUMER = os.getenv("MOMENTUM_CONFIRM_CONSUMER", "momentum-confirm-1")

MAX_AGE_MS = int(os.getenv("MOMENTUM_MAX_AGE_MS", "120000"))

log = setup_logger("momentum_confirm")


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _safe_int(v):
    try:
        if v is None or v == "":
            return None
        return int(float(v))
    except Exception:
        return None


def _load_latest(r: redis.Redis, key: str) -> dict | None:
    raw = r.get(key)
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except Exception:
        log.warning("bad_json key=%s", key)
        return None
    return data if isinstance(data, dict) else None


def _is_fresh(payload: dict | None, now_ms: int) -> bool:
    if not payload:
        return False
    ts = _safe_int(payload.get("ts_ms"))
    if ts is None:
        return False
    return (now_ms - ts) <= MAX_AGE_MS


def main():
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)

    ensure_group(r, IN_ST, GROUP)
    ensure_group(r, IN_EMA, GROUP)

    log.info(
        "START reading %s + %s, writing %s (max_age_ms=%s symbols=%d)",
        IN_ST,
        IN_EMA,
        OUT_STREAM,
        MAX_AGE_MS,
        len(symbols),
    )

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={IN_ST: ">", IN_EMA: ">"},
            count=2000,
            block=2000,
        )
        if not resp:
            continue

        touched: set[str] = set()

        for stream, msgs in resp:
            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)
                sym = str(fields.get("symbol") or "").strip().upper()
                log.debug("MSG_IN stream=%s id=%s symbol=%s fields=%s", stream, msg_id, sym, fields)
                if not sym or sym not in symbols:
                    log.debug("SKIP unknown_symbol stream=%s symbol=%r", stream, sym)
                    continue
                touched.add(sym)
            if ack_ids:
                r.xack(stream, GROUP, *ack_ids)

        now_ms = int(time.time() * 1000)
        for sym in touched:
            st_key = f"{ST_LATEST_PREFIX}{sym}"
            ema_key = f"{EMA_LATEST_PREFIX}{sym}"
            st = _load_latest(r, st_key)
            ema = _load_latest(r, ema_key)

            fresh_st = _is_fresh(st, now_ms)
            fresh_ema = _is_fresh(ema, now_ms)
            log.debug(
                "INPUTS symbol=%s st=%s fresh_st=%s ema=%s fresh_ema=%s",
                sym,
                st,
                fresh_st,
                ema,
                fresh_ema,
            )

            if not fresh_st or not fresh_ema:
                log.info(
                    "SKIP stale_or_missing symbol=%s fresh_st=%s fresh_ema=%s",
                    sym,
                    fresh_st,
                    fresh_ema,
                )
                continue

            st_bias = str(st.get("bias") or "").strip()
            ema_state = str(ema.get("state") or "").strip()
            signal = momentum_confirm(st_bias, ema_state)

            payload = {
                "ts_ms": str(now_ms),
                "symbol": sym,
                "signal": signal,
                "st_bias": st_bias,
                "ema_state": ema_state,
                "aligned": "1" if signal.startswith("BUY") else "0",
                "ema9": str(ema.get("ema9") or ""),
                "ema26": str(ema.get("ema26") or ""),
                "st_30m": str(st.get("st_30m") or ""),
                "st_10m": str(st.get("st_10m") or ""),
                "st_5m": str(st.get("st_5m") or ""),
                "st_1m": str(st.get("st_1m") or ""),
            }

            log.info(
                "LOGIC symbol=%s st=%s ema=%s -> signal=%s",
                sym,
                st_bias,
                ema_state,
                signal,
            )

            r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
            r.set(
                f"{LATEST_KEY_PREFIX}{sym}",
                json.dumps(payload, separators=(",", ":")),
                ex=3600,
            )

            if signal.startswith("BUY"):
                log.info("EMIT symbol=%s payload=%s", sym, payload)
            else:
                log.debug("NEUTRAL_EMIT symbol=%s payload=%s", sym, payload)


if __name__ == "__main__":
    main()

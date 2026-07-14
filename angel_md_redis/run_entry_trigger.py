"""
run_entry_trigger.py
────────────────────
Combines level break + HTF D/W/M + Supertrend bias + EMA9/26 + volume into final entry signals:
  md:entry:trigger
  md:entry:trigger:latest:{SYMBOL}
"""

from __future__ import annotations

import json
import os
import time

import redis

from app.config import load_symbols
from app.entry_trigger import entry_trigger
from app.logging_setup import setup_logger


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

IN_LEVEL = os.getenv("STREAM_LEVEL_ENTRY", "md:level:entry")

ST_LATEST_PREFIX = os.getenv("SUPERTREND_BIAS_LATEST_PREFIX", "md:supertrend:bias:latest:")
EMA_LATEST_PREFIX = os.getenv("EMA_CROSS_LATEST_PREFIX", "md:ema:cross:latest:")
HTF_LATEST_PREFIX = os.getenv("HTF_TREND_LATEST_PREFIX", "md:htf:trend:latest:")
VOLUME_LATEST_KEY = os.getenv("VOLUME_LATEST_KEY", "md:volume:latest")
OI_UNDERLYING_LATEST_PREFIX = os.getenv("OI_UNDERLYING_LATEST_PREFIX", "md:oi:underlying:latest:")

OUT_STREAM = os.getenv("STREAM_ENTRY_TRIGGER", "md:entry:trigger")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_ENTRY_TRIGGER", "200000"))
LATEST_KEY_PREFIX = os.getenv("ENTRY_TRIGGER_LATEST_PREFIX", "md:entry:trigger:latest:")

GROUP = os.getenv("ENTRY_TRIGGER_GROUP", "entry-trigger")
CONSUMER = os.getenv("ENTRY_TRIGGER_CONSUMER", "entry-trigger-1")

MAX_AGE_MS = int(os.getenv("ENTRY_TRIGGER_MAX_AGE_MS", "120000"))

log = setup_logger("entry_trigger")


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


def _safe_float_local(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _load_latest(r: redis.Redis, key: str) -> dict | None:
    raw = r.get(key)
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except Exception:
        log.warning("bad_json key=%s raw=%r", key, raw[:200] if isinstance(raw, str) else raw)
        return None
    return data if isinstance(data, dict) else None


def _is_fresh(payload: dict | None, now_ms: int) -> bool:
    if not payload:
        return False
    ts = _safe_int(payload.get("ts_ms"))
    if ts is None:
        return False
    return (now_ms - ts) <= MAX_AGE_MS


def _load_volume_for_symbol(r: redis.Redis, symbol: str) -> dict | None:
    """Read per-symbol slice from md:volume:latest multi-symbol blob."""
    blob = _load_latest(r, VOLUME_LATEST_KEY)
    if not blob:
        return None
    entry = blob.get(symbol)
    if not isinstance(entry, dict):
        return None
    # Prefer symbol ts_ms; fall back to blob-level ts_ms.
    if entry.get("ts_ms") in (None, "") and blob.get("ts_ms") not in (None, ""):
        entry = {**entry, "ts_ms": blob.get("ts_ms")}
    return entry


def main():
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_LEVEL, GROUP)

    log.info(
        "START reading %s + HTF/ST/EMA/volume latest, writing %s (max_age_ms=%s symbols=%d)",
        IN_LEVEL,
        OUT_STREAM,
        MAX_AGE_MS,
        len(symbols),
    )

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={IN_LEVEL: ">"},
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

                level_signal = str(fields.get("signal") or "").strip()
                # Only evaluate on actual level breaks.
                if not level_signal.startswith("BUY"):
                    log.debug(
                        "SKIP not_buy_level symbol=%s signal=%s",
                        sym,
                        level_signal,
                    )
                    continue

                st_key = f"{ST_LATEST_PREFIX}{sym}"
                ema_key = f"{EMA_LATEST_PREFIX}{sym}"
                htf_key = f"{HTF_LATEST_PREFIX}{sym}"
                oi_key = f"{OI_UNDERLYING_LATEST_PREFIX}{sym}"
                st = _load_latest(r, st_key)
                ema = _load_latest(r, ema_key)
                htf = _load_latest(r, htf_key)
                oi = _load_latest(r, oi_key)
                vol = _load_volume_for_symbol(r, sym)

                fresh_st = _is_fresh(st, now_ms)
                fresh_ema = _is_fresh(ema, now_ms)
                fresh_vol = _is_fresh(vol, now_ms)
                log.debug(
                    "INPUTS symbol=%s level_signal=%s st_key=%s st=%s fresh_st=%s "
                    "ema_key=%s ema=%s fresh_ema=%s htf_key=%s htf=%s oi_key=%s oi=%s vol=%s fresh_vol=%s",
                    sym,
                    level_signal,
                    st_key,
                    st,
                    fresh_st,
                    ema_key,
                    ema,
                    fresh_ema,
                    htf_key,
                    htf,
                    oi_key,
                    oi,
                    vol,
                    fresh_vol,
                )

                # HTF and OI are slow-moving: require key present, not 120s freshness.
                if not fresh_st or not fresh_ema or not htf or not oi or not fresh_vol:
                    log.info(
                        "SKIP stale_or_missing symbol=%s fresh_st=%s fresh_ema=%s "
                        "htf_present=%s oi_present=%s fresh_vol=%s",
                        sym,
                        fresh_st,
                        fresh_ema,
                        bool(htf),
                        bool(oi),
                        fresh_vol,
                    )
                    continue

                st_bias = str(st.get("bias") or "").strip()
                ema_state = str(ema.get("state") or "").strip()
                htf_bias = str(htf.get("bias") or "").strip()
                volume_signal = str(vol.get("signal") or "").strip()
                oi_positioning = str(oi.get("positioning") or "").strip()
                oi_resistance = _safe_float_local(oi.get("primary_resistance"))
                oi_support = _safe_float_local(oi.get("primary_support"))
                level = str(fields.get("level") or "").strip()
                strength = str(fields.get("strength") or "").strip()

                result = entry_trigger(
                    st_bias=st_bias,
                    ema_state=ema_state,
                    level_signal=level_signal,
                    level=level,
                    strength=strength,
                    htf_bias=htf_bias,
                    volume_signal=volume_signal,
                    oi_positioning=oi_positioning,
                    oi_resistance=oi_resistance,
                    oi_support=oi_support,
                )

                payload = {
                    "ts_ms": str(now_ms),
                    "symbol": sym,
                    "signal": result.signal,
                    "strength": result.strength,
                    "level": result.level or level,
                    "side": str(fields.get("side") or ""),
                    "price": str(fields.get("price") or ""),
                    "st_bias": st_bias,
                    "ema_state": ema_state,
                    "htf_bias": htf_bias,
                    "htf_daily": str(htf.get("daily") or ""),
                    "htf_weekly": str(htf.get("weekly") or ""),
                    "htf_monthly": str(htf.get("monthly") or ""),
                    "volume_signal": volume_signal,
                    "buy_pct": str(vol.get("buy_pct") or ""),
                    "sell_pct": str(vol.get("sell_pct") or ""),
                    "volume_surge": str(vol.get("volume_surge") or ""),
                    "aligned": "1" if result.signal.startswith("BUY") else "0",
                    "ema9": str(ema.get("ema9") or ""),
                    "ema26": str(ema.get("ema26") or ""),
                    "bar_ts_ms": str(fields.get("bar_ts_ms") or now_ms),
                    "reason": result.reason,
                    "oi_positioning": oi_positioning,
                    "oi_target_strike": "" if result.oi_target_strike is None else str(result.oi_target_strike),
                }

                log.info(
                    "LOGIC symbol=%s in=(htf=%s st=%s ema=%s vol=%s lvl_sig=%s lvl=%s) "
                    "out=(signal=%s strength=%s reason=%s)",
                    sym,
                    htf_bias,
                    st_bias,
                    ema_state,
                    volume_signal,
                    level_signal,
                    level,
                    result.signal,
                    result.strength,
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
            r.xack(IN_LEVEL, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

"""
run_strike_select.py
────────────────────
Consumes md:entry:trigger BUY signals and selects ATM / slight-OTM option contracts:
  md:strike:select
  md:strike:select:latest:{SYMBOL}
"""

from __future__ import annotations

import json
import os
import time

import redis

from app.config import load_symbols
from app.logging_setup import setup_logger
from app.scripmaster import load_scripmaster
from app.strike_select import strike_select


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

IN_STREAM = os.getenv("STREAM_ENTRY_TRIGGER", "md:entry:trigger")

OUT_STREAM = os.getenv("STREAM_STRIKE_SELECT", "md:strike:select")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_STRIKE_SELECT", "200000"))
LATEST_KEY_PREFIX = os.getenv("STRIKE_SELECT_LATEST_PREFIX", "md:strike:select:latest:")

GROUP = os.getenv("STRIKE_SELECT_GROUP", "strike-select")
CONSUMER = os.getenv("STRIKE_SELECT_CONSUMER", "strike-select-1")

OFFSET_BASE = int(os.getenv("STRIKE_OFFSET_BASE", "0"))
OFFSET_STRONG = int(os.getenv("STRIKE_OFFSET_STRONG", "1"))
COOLDOWN_MS = int(os.getenv("STRIKE_SELECT_COOLDOWN_MS", "60000"))

log = setup_logger("strike_select")


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _safe_float(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def main():
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_STREAM, GROUP)

    log.info("loading ScripMaster ...")
    df = load_scripmaster()
    log.info(
        "START reading %s, writing %s (offset_base=%s, offset_strong=%s, cooldown_ms=%s symbols=%d)",
        IN_STREAM,
        OUT_STREAM,
        OFFSET_BASE,
        OFFSET_STRONG,
        COOLDOWN_MS,
        len(symbols),
    )

    last_emit_ms: dict[str, int] = {}

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

        for _stream, msgs in resp:
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)

                sym = str(fields.get("symbol") or "").strip().upper()
                log.debug("MSG_IN id=%s symbol=%s fields=%s", msg_id, sym, fields)

                if not sym or sym not in symbols:
                    log.debug("SKIP unknown_symbol id=%s symbol=%r", msg_id, sym)
                    continue

                signal = str(fields.get("signal") or "").strip()
                if not signal.startswith("BUY"):
                    log.debug("SKIP not_buy_signal symbol=%s signal=%s", sym, signal)
                    continue

                prev = last_emit_ms.get(sym)
                if prev is not None and (now_ms - prev) < COOLDOWN_MS:
                    log.info(
                        "SKIP cooldown symbol=%s age_ms=%s cooldown_ms=%s",
                        sym,
                        now_ms - prev,
                        COOLDOWN_MS,
                    )
                    continue

                spot = _safe_float(fields.get("price"))
                if spot is None or spot <= 0:
                    log.info("SKIP invalid_spot symbol=%s price=%r", sym, fields.get("price"))
                    continue

                strength = str(fields.get("strength") or "").strip()
                oi_target_strike = _safe_float(fields.get("oi_target_strike"))
                log.debug(
                    "INPUTS symbol=%s signal=%s strength=%s spot=%s oi_target_strike=%s",
                    sym,
                    signal,
                    strength,
                    spot,
                    oi_target_strike,
                )

                result = strike_select(
                    signal=signal,
                    strength=strength,
                    underlying=sym,
                    spot=spot,
                    df=df,
                    offset_base=OFFSET_BASE,
                    offset_strong=OFFSET_STRONG,
                    oi_target_strike=oi_target_strike,
                )

                payload = {
                    "ts_ms": str(now_ms),
                    "symbol": sym,
                    "signal": result.signal,
                    "status": result.status,
                    "side": result.side,
                    "mode": result.mode,
                    "spot": str(result.spot),
                    "atm": str(result.atm),
                    "strike": str(result.strike),
                    "step": str(result.step),
                    "expiry": result.expiry,
                    "token": result.token,
                    "tradingsymbol": result.tradingsymbol,
                    "exchange": result.exchange,
                    "strength": strength,
                    "level": str(fields.get("level") or ""),
                    "entry_price": str(fields.get("price") or ""),
                    "reason": result.reason,
                    "bar_ts_ms": str(fields.get("bar_ts_ms") or now_ms),
                    "oi_target_strike": "" if oi_target_strike is None else str(oi_target_strike),
                    "oi_positioning": str(fields.get("oi_positioning") or ""),
                }

                log.info(
                    "LOGIC symbol=%s status=%s side=%s mode=%s atm=%s strike=%s "
                    "token=%s tsym=%s reason=%s",
                    sym,
                    result.status,
                    result.side,
                    result.mode,
                    result.atm,
                    result.strike,
                    result.token,
                    result.tradingsymbol,
                    result.reason,
                )

                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(
                    f"{LATEST_KEY_PREFIX}{sym}",
                    json.dumps(payload, separators=(",", ":")),
                    ex=3600,
                )

                if result.status == "OK":
                    last_emit_ms[sym] = now_ms
                    log.info("EMIT symbol=%s payload=%s", sym, payload)
                else:
                    log.info("SKIP_RESULT symbol=%s payload=%s", sym, payload)

        if ack_ids:
            r.xack(IN_STREAM, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

"""
run_smart_money.py
───────────────────────
Smart Money Entry Detection — reads md:ticks:eq / md:ticks:opt (requires
bid/ask + top-5 depth WITH per-level prices, published by ws_producer.py)
and detects wall/iceberg, absorption, sweep, and time-and-sales clustering.

  Stream : md:smartmoney:signal
  Key    : md:smartmoney:latest:{SYMBOL}          (equities)
  Key    : md:smartmoney:latest:{TRADINGSYMBOL}   (options)

Unlike run_bidask_analyzer.py this is NOT throttled — sweep/absorption
detection is inherently tick-level; throttling would hide exactly the
signal being looked for. Runs as its own consumer group, independent of
every other reader on md:ticks:eq / md:ticks:opt.
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, List, Optional

import redis

from app.config import load_symbols
from app.logging_setup import setup_logger
from app.smart_money import DepthLevel, SmartMoneyDetector, SmartMoneySignal

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")
OPT_STREAM = os.getenv("STREAM_OPT", "md:ticks:opt")

OUT_STREAM = os.getenv("STREAM_SMARTMONEY_SIGNAL", "md:smartmoney:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_SMARTMONEY", "200000"))
LATEST_KEY_PREFIX = os.getenv("SMARTMONEY_LATEST_PREFIX", "md:smartmoney:latest:")

GROUP = os.getenv("SMARTMONEY_GROUP", "smartmoney")
CONSUMER = os.getenv("SMARTMONEY_CONSUMER", "smartmoney-1")

LATEST_TTL_SEC = int(os.getenv("SMARTMONEY_LATEST_TTL_SEC", "3600"))

log = setup_logger("smart_money")


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


def _parse_csv_floats(raw: str) -> List[float]:
    if not raw:
        return []
    out: List[float] = []
    for part in raw.split(","):
        v = _safe_float(part)
        if v is not None:
            out.append(v)
    return out


def _build_levels(prices: List[float], sizes: List[float]) -> List[DepthLevel]:
    n = min(len(prices), len(sizes))
    return [DepthLevel(price=prices[i], qty=sizes[i]) for i in range(n)]


def _to_payload(key: str, kind: str, sig: SmartMoneySignal, now_ms: int) -> Dict[str, str]:
    return {
        "ts_ms": str(now_ms),
        "key": key,
        "kind": kind,
        "wall_bid": json.dumps(sig.wall_bid, separators=(",", ":")),
        "wall_ask": json.dumps(sig.wall_ask, separators=(",", ":")),
        "absorption_status": sig.absorption_status,
        "absorption_side": sig.absorption_side,
        "sweep_signal": sig.sweep_signal,
        "sweep_levels": str(sig.sweep_levels),
        "sweep_confirmed": "1" if sig.sweep_confirmed else "0",
        "cluster": json.dumps(sig.cluster, separators=(",", ":")) if sig.cluster else "",
        "composite": sig.composite,
    }


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)

    ensure_group(r, EQ_STREAM, GROUP)
    ensure_group(r, OPT_STREAM, GROUP)

    detectors: Dict[str, SmartMoneyDetector] = {}
    prev_cum_vol: Dict[str, float] = {}
    last_composite: Dict[str, str] = {}

    log.info(
        "START reading %s + %s -> %s + %s{{KEY}} symbols=%d",
        EQ_STREAM, OPT_STREAM, OUT_STREAM, LATEST_KEY_PREFIX, len(symbols),
    )

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={EQ_STREAM: ">", OPT_STREAM: ">"},
            count=2000,
            block=2000,
        )
        if not resp:
            continue

        now_ms = int(time.time() * 1000)

        for stream, msgs in resp:
            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)

                if stream == EQ_STREAM:
                    sym = str(fields.get("symbol") or "").strip().upper()
                    if not sym or sym not in symbols:
                        log.debug("SKIP unknown_symbol stream=eq symbol=%r", sym)
                        continue
                    key, kind = sym, "eq"
                else:
                    tsym = str(fields.get("tradingsymbol") or "").strip().upper()
                    if not tsym:
                        log.debug("SKIP missing_tradingsymbol stream=opt")
                        continue
                    key, kind = tsym, "opt"

                bid_price = _safe_float(fields.get("bid"))
                ask_price = _safe_float(fields.get("ask"))
                bid_size = _safe_float(fields.get("bid_sz"))
                ask_size = _safe_float(fields.get("ask_sz"))
                ltp = _safe_float(fields.get("ltp"))

                if bid_price is None or ask_price is None or bid_size is None or ask_size is None:
                    log.debug("SKIP no_quote key=%s kind=%s", key, kind)
                    continue

                bid_sizes = _parse_csv_floats(fields.get("bid_depth5") or "")
                ask_sizes = _parse_csv_floats(fields.get("ask_depth5") or "")
                bid_prices = _parse_csv_floats(fields.get("bid_depth5_px") or "")
                ask_prices = _parse_csv_floats(fields.get("ask_depth5_px") or "")
                bid_levels = _build_levels(bid_prices, bid_sizes)
                ask_levels = _build_levels(ask_prices, ask_sizes)

                # Tick volume: prefer ltq (last traded quantity) if the feed
                # provides it, else derive from cumulative "vol" delta (same
                # technique as candle_builder.py / run_volume_analyzer.py).
                tick_vol = _safe_float(fields.get("ltq"))
                if tick_vol is None:
                    cum_vol = _safe_float(fields.get("vol"))
                    prev = prev_cum_vol.get(key)
                    if cum_vol is not None:
                        tick_vol = max(0.0, cum_vol - prev) if prev is not None else 0.0
                        prev_cum_vol[key] = cum_vol
                    else:
                        tick_vol = 0.0

                if key not in detectors:
                    detectors[key] = SmartMoneyDetector()

                sig = detectors[key].analyze(
                    ts_ms=now_ms,
                    trade_price=ltp,
                    tick_vol=tick_vol or 0.0,
                    bid_price=bid_price, bid_size=bid_size,
                    ask_price=ask_price, ask_size=ask_size,
                    bid_levels=bid_levels, ask_levels=ask_levels,
                )

                log.debug(
                    "LOGIC key=%s kind=%s wall_bid=%s wall_ask=%s absorb=%s/%s "
                    "sweep=%s(levels=%s,conf=%s) cluster=%s composite=%s",
                    key, kind, sig.wall_bid, sig.wall_ask,
                    sig.absorption_status, sig.absorption_side,
                    sig.sweep_signal, sig.sweep_levels, sig.sweep_confirmed,
                    sig.cluster, sig.composite,
                )

                payload = _to_payload(key, kind, sig, now_ms)
                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(
                    f"{LATEST_KEY_PREFIX}{key}",
                    json.dumps(payload, separators=(",", ":")),
                    ex=LATEST_TTL_SEC,
                )

                prev_composite = last_composite.get(key)
                if sig.composite != "NEUTRAL" or prev_composite not in (None, "NEUTRAL"):
                    log.info("EMIT key=%s kind=%s composite=%s payload=%s", key, kind, sig.composite, payload)
                last_composite[key] = sig.composite

            if ack_ids:
                r.xack(stream, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

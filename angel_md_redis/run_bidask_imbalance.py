"""
run_bidask_imbalance.py
───────────────────────
Bid-Ask Quantity Imbalance — reads md:ticks:eq / md:ticks:opt (bid/ask
top-5 depth with prices, already published by ws_producer.py) and emits:

  Stream : md:imbalance:signal
  Key    : md:imbalance:latest:{SYMBOL}          (equities)
  Key    : md:imbalance:latest:{TRADINGSYMBOL}   (options)

Not throttled — imbalance persistence/flip detection needs every tick to
build its streak correctly. Own consumer group.
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, List, Optional

import redis

from app.bidask_imbalance import DepthLevel, ImbalanceDetector, ImbalanceResult
from app.config import load_symbols
from app.logging_setup import setup_logger

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")
OPT_STREAM = os.getenv("STREAM_OPT", "md:ticks:opt")

OUT_STREAM = os.getenv("STREAM_IMBALANCE_SIGNAL", "md:imbalance:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_IMBALANCE", "200000"))
LATEST_KEY_PREFIX = os.getenv("IMBALANCE_LATEST_PREFIX", "md:imbalance:latest:")

GROUP = os.getenv("IMBALANCE_GROUP", "imbalance")
CONSUMER = os.getenv("IMBALANCE_CONSUMER", "imbalance-1")

LATEST_TTL_SEC = int(os.getenv("IMBALANCE_LATEST_TTL_SEC", "3600"))

log = setup_logger("bidask_imbalance")


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


def _to_payload(key: str, kind: str, res: ImbalanceResult, now_ms: int) -> Dict[str, str]:
    return {
        "ts_ms": str(now_ms),
        "key": key,
        "kind": kind,
        "raw": f"{res.raw:.4f}",
        "weighted_filtered": f"{res.weighted_filtered:.4f}",
        "ema_score": f"{res.ema_score:.4f}",
        "final_score": f"{res.final_score:.4f}",
        "signal": res.signal,
        "streak": str(res.streak),
        "streak_sign": res.streak_sign,
        "flip_detected": "1" if res.flip_detected else "0",
        "layering_bid": "1" if res.layering_bid else "0",
        "layering_ask": "1" if res.layering_ask else "0",
        "spoof_events_bid": json.dumps(res.spoof_events_bid, separators=(",", ":")),
        "spoof_events_ask": json.dumps(res.spoof_events_ask, separators=(",", ":")),
    }


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)

    ensure_group(r, EQ_STREAM, GROUP)
    ensure_group(r, OPT_STREAM, GROUP)

    detectors: Dict[str, ImbalanceDetector] = {}
    last_signal: Dict[str, str] = {}

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

                bid_sizes = _parse_csv_floats(fields.get("bid_depth5") or "")
                ask_sizes = _parse_csv_floats(fields.get("ask_depth5") or "")
                bid_prices = _parse_csv_floats(fields.get("bid_depth5_px") or "")
                ask_prices = _parse_csv_floats(fields.get("ask_depth5_px") or "")
                bid_levels = _build_levels(bid_prices, bid_sizes)
                ask_levels = _build_levels(ask_prices, ask_sizes)

                if not bid_levels or not ask_levels:
                    log.debug("SKIP no_depth key=%s kind=%s", key, kind)
                    continue

                trade_price = _safe_float(fields.get("ltp"))

                if key not in detectors:
                    detectors[key] = ImbalanceDetector()

                res = detectors[key].analyze(bid_levels, ask_levels, trade_price)

                log.debug(
                    "LOGIC key=%s kind=%s raw=%.4f weighted=%.4f ema=%.4f final=%.4f "
                    "signal=%s streak=%s/%s flip=%s layering=%s/%s",
                    key, kind, res.raw, res.weighted_filtered, res.ema_score, res.final_score,
                    res.signal, res.streak, res.streak_sign, res.flip_detected,
                    res.layering_bid, res.layering_ask,
                )

                payload = _to_payload(key, kind, res, now_ms)
                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(
                    f"{LATEST_KEY_PREFIX}{key}",
                    json.dumps(payload, separators=(",", ":")),
                    ex=LATEST_TTL_SEC,
                )

                prev = last_signal.get(key)
                if res.flip_detected or res.signal != prev:
                    log.info("EMIT key=%s kind=%s signal=%s flip=%s payload=%s", key, kind, res.signal, res.flip_detected, payload)
                last_signal[key] = res.signal

            if ack_ids:
                r.xack(stream, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

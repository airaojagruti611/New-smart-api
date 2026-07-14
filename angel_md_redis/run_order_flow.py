"""
run_order_flow.py
───────────────────────
Direction + Support & Resistance from Bid-Ask — reads md:ticks:eq /
md:ticks:opt (uses the same bid/ask + top-5 depth-with-price + ltq fields
published by ws_producer.py for the smart-money module — no producer
changes needed) and emits:

  Stream : md:orderflow:signal
  Key    : md:orderflow:latest:{SYMBOL}          (equities)
  Key    : md:orderflow:latest:{TRADINGSYMBOL}   (options)

Not throttled — direction/S-R/refresh detection is inherently tick-level.
Runs as its own consumer group, independent of every other reader on
md:ticks:eq / md:ticks:opt.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict
from typing import Dict, List, Optional

import redis

from app.config import load_symbols
from app.logging_setup import setup_logger
from app.order_flow import DepthLevel, OrderFlowDetector, OrderFlowSignal

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")
OPT_STREAM = os.getenv("STREAM_OPT", "md:ticks:opt")

OUT_STREAM = os.getenv("STREAM_ORDERFLOW_SIGNAL", "md:orderflow:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_ORDERFLOW", "200000"))
LATEST_KEY_PREFIX = os.getenv("ORDERFLOW_LATEST_PREFIX", "md:orderflow:latest:")

GROUP = os.getenv("ORDERFLOW_GROUP", "orderflow")
CONSUMER = os.getenv("ORDERFLOW_CONSUMER", "orderflow-1")

DIRECTION_WINDOW = int(os.getenv("ORDERFLOW_DIRECTION_WINDOW", "50"))
LATEST_TTL_SEC = int(os.getenv("ORDERFLOW_LATEST_TTL_SEC", "3600"))

log = setup_logger("order_flow")


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


def _to_payload(key: str, kind: str, sig: OrderFlowSignal, now_ms: int) -> Dict[str, str]:
    return {
        "ts_ms": str(now_ms),
        "key": key,
        "kind": kind,
        "buy_pressure": f"{sig.direction.buy_pressure:.0f}",
        "sell_pressure": f"{sig.direction.sell_pressure:.0f}",
        "net_delta": f"{sig.direction.net_delta:.0f}",
        "bias": sig.direction.bias,
        "supports": json.dumps([asdict(x) for x in sig.supports], separators=(",", ":")),
        "resistances": json.dumps([asdict(x) for x in sig.resistances], separators=(",", ":")),
        "support_events": json.dumps(sig.support_events, separators=(",", ":")),
        "resistance_events": json.dumps(sig.resistance_events, separators=(",", ":")),
        "refresh_events": json.dumps(sig.refresh_events, separators=(",", ":")),
    }


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)

    ensure_group(r, EQ_STREAM, GROUP)
    ensure_group(r, OPT_STREAM, GROUP)

    detectors: Dict[str, OrderFlowDetector] = {}
    prev_cum_vol: Dict[str, float] = {}
    last_bias: Dict[str, str] = {}

    log.info(
        "START reading %s + %s -> %s + %s{{KEY}} (direction_window=%s symbols=%d)",
        EQ_STREAM, OPT_STREAM, OUT_STREAM, LATEST_KEY_PREFIX, DIRECTION_WINDOW, len(symbols),
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

                bid = _safe_float(fields.get("bid"))
                ask = _safe_float(fields.get("ask"))
                ltp = _safe_float(fields.get("ltp"))

                bid_sizes = _parse_csv_floats(fields.get("bid_depth5") or "")
                ask_sizes = _parse_csv_floats(fields.get("ask_depth5") or "")
                bid_prices = _parse_csv_floats(fields.get("bid_depth5_px") or "")
                ask_prices = _parse_csv_floats(fields.get("ask_depth5_px") or "")
                bid_levels = _build_levels(bid_prices, bid_sizes)
                ask_levels = _build_levels(ask_prices, ask_sizes)

                if not bid_levels or not ask_levels:
                    log.debug("SKIP no_depth key=%s kind=%s", key, kind)
                    continue

                # Trade qty: prefer ltq, else derive from cumulative "vol" delta
                # (same technique as run_smart_money.py / candle_builder.py).
                trade_qty = _safe_float(fields.get("ltq"))
                if trade_qty is None:
                    cum_vol = _safe_float(fields.get("vol"))
                    prev = prev_cum_vol.get(key)
                    if cum_vol is not None:
                        trade_qty = max(0.0, cum_vol - prev) if prev is not None else 0.0
                        prev_cum_vol[key] = cum_vol
                    else:
                        trade_qty = 0.0

                if key not in detectors:
                    detectors[key] = OrderFlowDetector(direction_window=DIRECTION_WINDOW)

                sig = detectors[key].analyze(
                    ts_ms=now_ms,
                    trade_price=ltp,
                    trade_qty=trade_qty or 0.0,
                    bid=bid, ask=ask,
                    bid_levels=bid_levels, ask_levels=ask_levels,
                )

                log.debug(
                    "LOGIC key=%s kind=%s bias=%s net_delta=%.0f supports=%s resistances=%s "
                    "support_ev=%s resistance_ev=%s refresh_ev=%s",
                    key, kind, sig.direction.bias, sig.direction.net_delta,
                    sig.supports, sig.resistances,
                    sig.support_events, sig.resistance_events, sig.refresh_events,
                )

                payload = _to_payload(key, kind, sig, now_ms)
                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(
                    f"{LATEST_KEY_PREFIX}{key}",
                    json.dumps(payload, separators=(",", ":")),
                    ex=LATEST_TTL_SEC,
                )

                noteworthy = sig.support_events or sig.resistance_events
                prev_bias = last_bias.get(key)
                if noteworthy or sig.direction.bias != prev_bias:
                    log.info("EMIT key=%s kind=%s bias=%s payload=%s", key, kind, sig.direction.bias, payload)
                last_bias[key] = sig.direction.bias

            if ack_ids:
                r.xack(stream, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

"""
run_stock_entry_exit.py
───────────────────────
Entry & Exit Based on Stock Bid-Ask — reads md:ticks:eq (uses the same
bid/ask + top-5 depth-with-price + ltq fields published by ws_producer.py
for the smart-money/order-flow modules — no producer changes needed) and
emits:

  Stream : md:stockflow:signal
  Key    : md:stockflow:latest:{SYMBOL}

Not throttled — entry/exit conditions are tick-level by definition.
Own consumer group, independent of every other reader on md:ticks:eq.

Signal-only: this does NOT track whether a position is open. exit_trigger
reflects live exit-condition state on the tape; a position/PnL layer
(not present in this pipeline yet) would decide whether to act on it.
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, List, Optional

import redis

from app.config import load_symbols
from app.logging_setup import setup_logger
from app.stock_entry_exit import DepthLevel, EntryExitResult, StockEntryExitDetector

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")

OUT_STREAM = os.getenv("STREAM_STOCKFLOW_SIGNAL", "md:stockflow:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_STOCKFLOW", "200000"))
LATEST_KEY_PREFIX = os.getenv("STOCKFLOW_LATEST_PREFIX", "md:stockflow:latest:")

GROUP = os.getenv("STOCKFLOW_GROUP", "stockflow")
CONSUMER = os.getenv("STOCKFLOW_CONSUMER", "stockflow-1")

LATEST_TTL_SEC = int(os.getenv("STOCKFLOW_LATEST_TTL_SEC", "3600"))

log = setup_logger("stock_entry_exit")


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


def _to_payload(sym: str, res: EntryExitResult, now_ms: int) -> Dict[str, str]:
    return {
        "ts_ms": str(now_ms),
        "symbol": sym,
        "spread_pct": f"{res.spread_pct:.4f}",
        "spread_avg": "" if res.spread_avg is None else f"{res.spread_avg:.4f}",
        "spread_ratio": "" if res.spread_ratio is None else f"{res.spread_ratio:.2f}",
        "bid_top3": f"{res.bid_top3:.0f}",
        "ask_top3": f"{res.ask_top3:.0f}",
        "bid_imbalance_pct": "" if res.bid_imbalance_pct is None else f"{res.bid_imbalance_pct:.2f}",
        "ask_imbalance_pct": "" if res.ask_imbalance_pct is None else f"{res.ask_imbalance_pct:.2f}",
        "last_print_side": res.last_print_side,
        "ask_wall_near": "1" if res.ask_wall_near else "0",
        "sweep_sell_confirmed": "1" if res.sweep_sell_confirmed else "0",
        "entry_conditions": json.dumps(res.entry_conditions, separators=(",", ":")),
        "entry_trigger": "1" if res.entry_trigger else "0",
        "exit_conditions": json.dumps(res.exit_conditions, separators=(",", ":")),
        "exit_trigger": "1" if res.exit_trigger else "0",
        "exit_reasons": json.dumps(res.exit_reasons, separators=(",", ":")),
    }


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, EQ_STREAM, GROUP)

    detectors: Dict[str, StockEntryExitDetector] = {}
    prev_cum_vol: Dict[str, float] = {}
    last_entry: Dict[str, bool] = {}

    log.info(
        "START reading %s -> %s + %s{{SYMBOL}} symbols=%d",
        EQ_STREAM, OUT_STREAM, LATEST_KEY_PREFIX, len(symbols),
    )

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={EQ_STREAM: ">"},
            count=2000,
            block=2000,
        )
        if not resp:
            continue

        now_ms = int(time.time() * 1000)

        for _stream, msgs in resp:
            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)

                sym = str(fields.get("symbol") or "").strip().upper()
                if not sym or sym not in symbols:
                    log.debug("SKIP unknown_symbol symbol=%r", sym)
                    continue

                bid = _safe_float(fields.get("bid"))
                ask = _safe_float(fields.get("ask"))
                ltp = _safe_float(fields.get("ltp"))

                bid_sizes = _parse_csv_floats(fields.get("bid_depth5") or "")
                ask_sizes = _parse_csv_floats(fields.get("ask_depth5") or "")
                bid_prices = _parse_csv_floats(fields.get("bid_depth5_px") or "")
                ask_prices = _parse_csv_floats(fields.get("ask_depth5_px") or "")
                bid_levels = _build_levels(bid_prices, bid_sizes)
                ask_levels = _build_levels(ask_prices, ask_sizes)

                if bid is None or ask is None or not bid_levels or not ask_levels:
                    log.debug("SKIP no_quote symbol=%s", sym)
                    continue

                trade_qty = _safe_float(fields.get("ltq"))
                if trade_qty is None:
                    cum_vol = _safe_float(fields.get("vol"))
                    prev = prev_cum_vol.get(sym)
                    if cum_vol is not None:
                        trade_qty = max(0.0, cum_vol - prev) if prev is not None else 0.0
                        prev_cum_vol[sym] = cum_vol
                    else:
                        trade_qty = 0.0

                if sym not in detectors:
                    detectors[sym] = StockEntryExitDetector()

                res = detectors[sym].analyze(
                    trade_price=ltp,
                    trade_qty=trade_qty or 0.0,
                    bid=bid, ask=ask,
                    bid_levels=bid_levels, ask_levels=ask_levels,
                )

                log.debug(
                    "LOGIC symbol=%s spread=%.4f/%s imb_bid=%s imb_ask=%s last=%s "
                    "wall=%s sweep=%s entry=%s(%s) exit=%s(%s)",
                    sym, res.spread_pct, res.spread_avg,
                    res.bid_imbalance_pct, res.ask_imbalance_pct, res.last_print_side,
                    res.ask_wall_near, res.sweep_sell_confirmed,
                    res.entry_trigger, res.entry_conditions,
                    res.exit_trigger, res.exit_reasons,
                )

                payload = _to_payload(sym, res, now_ms)
                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(
                    f"{LATEST_KEY_PREFIX}{sym}",
                    json.dumps(payload, separators=(",", ":")),
                    ex=LATEST_TTL_SEC,
                )

                if res.entry_trigger:
                    log.info("ENTRY symbol=%s payload=%s", sym, payload)
                if res.exit_trigger:
                    log.info("EXIT symbol=%s reasons=%s payload=%s", sym, res.exit_reasons, payload)

                last_entry[sym] = res.entry_trigger

            if ack_ids:
                r.xack(EQ_STREAM, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

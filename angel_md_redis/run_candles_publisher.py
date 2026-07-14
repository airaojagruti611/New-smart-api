"""
run_candles_publisher.py
───────────────────────
Consumes equity ticks from STREAM_EQ (default md:ticks:eq) and publishes:
  - 1-minute candles stream (md:candles:1m)
  - daily candles stream (md:candles:1d)

This is a DATA-FIRST worker to make downstream indicators (Supertrend/EMA/Pivots) possible.
It does not modify or depend on existing regime/volume workers.
"""

from __future__ import annotations

import os
import time
from typing import Dict, Optional

import redis

from app.config import load_symbols
from app.candle_builder import CandleBuilder1d, CandleBuilder1m
from app.candles_store import CandlesStore


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")

OUT_1M_STREAM = os.getenv("STREAM_CANDLES_1M", "md:candles:1m")
OUT_1D_STREAM = os.getenv("STREAM_CANDLES_1D", "md:candles:1d")

OUT_MAXLEN_1M = int(os.getenv("STREAM_MAXLEN_CANDLES_1M", "2000000"))
OUT_MAXLEN_1D = int(os.getenv("STREAM_MAXLEN_CANDLES_1D", "200000"))

GROUP = os.getenv("CANDLES_GROUP", "candles")
CONSUMER = os.getenv("CANDLES_CONSUMER", "candles-1")

MARKET_CLOSE_HHMM = os.getenv("MARKET_CLOSE_HHMM", "15:30")


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


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def main():
    symbols = set(load_symbols())
    print(f"[CANDLES] symbols loaded: {len(symbols)} from symbols.txt")

    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, EQ_STREAM, GROUP)

    store = CandlesStore()

    # Per-symbol candle builders
    cb_1m: Dict[str, CandleBuilder1m] = {}
    cb_1d: Dict[str, CandleBuilder1d] = {}

    # Per-symbol previous cumulative day volume to compute per-tick delta
    prev_cum_vol: Dict[str, float] = {}

    print(
        f"[CANDLES] reading {EQ_STREAM} -> writing 1m:{OUT_1M_STREAM} 1d:{OUT_1D_STREAM} "
        f"(market_close={MARKET_CLOSE_HHMM})"
    )

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={EQ_STREAM: ">"},
            count=1000,
            block=1000,
        )
        if not resp:
            continue

        for _stream, msgs in resp:
            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)

                sym = str(fields.get("symbol") or "").strip().upper()
                if not sym or sym not in symbols:
                    continue

                ltp = _safe_float(fields.get("ltp"))
                if ltp is None:
                    continue

                # Use exchange timestamp if numeric; else ts_recv; else now.
                ts_ms = _safe_int(fields.get("ts_exch")) or _safe_int(fields.get("ts_recv")) or int(time.time() * 1000)

                # Volume is published as "vol" (cumulative day volume) by ws_producer.
                cum_vol = _safe_float(fields.get("vol"))
                if cum_vol is not None and cum_vol >= 0:
                    prev = prev_cum_vol.get(sym, cum_vol)
                    tick_vol = max(0.0, cum_vol - prev)
                    prev_cum_vol[sym] = cum_vol
                else:
                    tick_vol = 0.0

                if sym not in cb_1m:
                    cb_1m[sym] = CandleBuilder1m()
                if sym not in cb_1d:
                    cb_1d[sym] = CandleBuilder1d(market_close_hhmm=MARKET_CLOSE_HHMM)

                closed_1m, _bucket = cb_1m[sym].update_tick(ts_ms=ts_ms, ltp=ltp, vol_delta=tick_vol)
                if closed_1m is not None:
                    store.write_candle(OUT_1M_STREAM, OUT_MAXLEN_1M, sym, "1m", closed_1m)

                closed_1d = cb_1d[sym].update_tick(ts_ms=ts_ms, ltp=ltp, vol_delta=tick_vol)
                if closed_1d is not None:
                    store.write_candle(OUT_1D_STREAM, OUT_MAXLEN_1D, sym, "1d", closed_1d)

            if ack_ids:
                r.xack(EQ_STREAM, GROUP, *ack_ids)


if __name__ == "__main__":
    main()


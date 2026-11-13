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

from app.candle_builder import CandleBuilder1d, CandleBuilder1m, session_date_ist, tick_is_stale
from app.candles_store import CandlesStore
from app.config import load_symbols
from app.freshness import env_ms, stream_id_ms
from app.history_bootstrap import seed_history_if_needed


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")

OUT_1M_STREAM = os.getenv("STREAM_CANDLES_1M", "md:candles:1m")
OUT_1D_STREAM = os.getenv("STREAM_CANDLES_1D", "md:candles:1d")

OUT_MAXLEN_1M = int(os.getenv("STREAM_MAXLEN_CANDLES_1M", "2000000"))
OUT_MAXLEN_1D = int(os.getenv("STREAM_MAXLEN_CANDLES_1D", "200000"))

GROUP = os.getenv("CANDLES_GROUP", "candles")
CONSUMER = os.getenv("CANDLES_CONSUMER", "candles-1")

MARKET_CLOSE_HHMM = os.getenv("MARKET_CLOSE_HHMM", "15:30")

# Emit a bar once the wall clock passes its end + grace (no waiting for a later tick).
FLUSH_GRACE_MS = env_ms("CANDLES_FLUSH_GRACE_SEC", 2.0)
# Ticks whose exchange time lags their XADD time by more than this (or carry
# another IST date, e.g. the connect snapshot with yesterday's timestamp) are ignored.
TICK_MAX_LAG_MS = env_ms("CANDLES_TICK_MAX_LAG_SEC", 300.0)
READ_COUNT = 1000


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

    print("[CANDLES] seeding historical 1d/1m/5m/10m/30m if streams are short...")
    seed_history_if_needed(r, list(symbols))

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

    def _write_1d(sym: str, c) -> None:
        date_str = session_date_ist(c.ts_ms).isoformat()
        store.write_candle(OUT_1D_STREAM, OUT_MAXLEN_1D, sym, "1d", c, date=date_str)

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={EQ_STREAM: ">"},
            count=READ_COUNT,
            block=1000,
        )
        n_read = 0
        for _stream, msgs in resp or []:
            n_read += len(msgs)
            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)
                process_tick(fields, stream_id_ms(msg_id), symbols, cb_1m, cb_1d, prev_cum_vol, store, _write_1d)

            if ack_ids:
                r.xack(EQ_STREAM, GROUP, *ack_ids)

        # Time-based flush only once caught up (a full batch means backlog replay,
        # where wall-clock flushing would cut bars that still have ticks queued).
        if n_read < READ_COUNT:
            flush_all(int(time.time() * 1000), cb_1m, cb_1d, store, _write_1d)


def process_tick(fields, recv_ms, symbols, cb_1m, cb_1d, prev_cum_vol, store, write_1d) -> None:
    sym = str(fields.get("symbol") or "").strip().upper()
    if not sym or sym not in symbols:
        return

    ltp = _safe_float(fields.get("ltp"))
    if ltp is None:
        return

    # Use exchange timestamp if numeric; else ts_recv; else stream time / now.
    ts_ms = (
        _safe_int(fields.get("ts_exch"))
        or _safe_int(fields.get("ts_recv"))
        or recv_ms
        or int(time.time() * 1000)
    )
    if tick_is_stale(ts_ms, recv_ms, TICK_MAX_LAG_MS):
        return

    # Volume is published as "vol" (cumulative day volume) by ws_producer.
    cum_vol = _safe_float(fields.get("vol"))
    if cum_vol is not None and cum_vol >= 0:
        prev = prev_cum_vol.get(sym, cum_vol)
        tick_vol = max(0.0, cum_vol - prev)
        # A lower cum is an out-of-order/duplicate tick: keep the high-water mark,
        # else the next tick re-adds the same delta. A big drop is a new session.
        if cum_vol >= prev or cum_vol < prev * 0.5:
            prev_cum_vol[sym] = cum_vol
    else:
        tick_vol = 0.0

    if sym not in cb_1m:
        cb_1m[sym] = CandleBuilder1m(close_hhmm=MARKET_CLOSE_HHMM)
    if sym not in cb_1d:
        cb_1d[sym] = CandleBuilder1d(market_close_hhmm=MARKET_CLOSE_HHMM)

    closed_1m, _bucket = cb_1m[sym].update_tick(ts_ms=ts_ms, ltp=ltp, vol_delta=tick_vol)
    if closed_1m is not None:
        store.write_candle(OUT_1M_STREAM, OUT_MAXLEN_1M, sym, "1m", closed_1m)

    # Daily bar from the exchange's own day fields ("c" is the PREVIOUS close, not used).
    closed_1d = cb_1d[sym].update_tick(
        ts_ms=ts_ms,
        ltp=ltp,
        vol_delta=tick_vol,
        day_open=_safe_float(fields.get("o")),
        day_high=_safe_float(fields.get("h")),
        day_low=_safe_float(fields.get("l")),
        day_volume=cum_vol,
    )
    if closed_1d is not None:
        write_1d(sym, closed_1d)


def flush_all(now_ms, cb_1m, cb_1d, store, write_1d) -> None:
    for sym, b in cb_1m.items():
        c = b.flush(now_ms, grace_ms=FLUSH_GRACE_MS)
        if c is not None:
            store.write_candle(OUT_1M_STREAM, OUT_MAXLEN_1M, sym, "1m", c)
    for sym, b in cb_1d.items():
        c = b.flush(now_ms, grace_ms=FLUSH_GRACE_MS)
        if c is not None:
            write_1d(sym, c)


if __name__ == "__main__":
    main()


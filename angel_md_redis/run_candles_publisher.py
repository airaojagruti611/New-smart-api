"""
run_candles_publisher.py
───────────────────────
Consumes equity ticks from STREAM_EQ (default md:ticks:eq) and publishes:
  - 1-minute candles stream (md:candles:1m)
  - daily candles stream (md:candles:1d)

This is a DATA-FIRST worker to make downstream indicators (Supertrend/EMA/Pivots) possible.
It does not modify or depend on existing regime/volume workers.

Daily candle:
  Built from Angel's own day fields on each tick (day open/high/low, cumulative
  volume, LTP as close), so a mid-day restart still yields the real day bar.
  Written once per symbol per IST date, at MARKET_CLOSE_HHMM by the wall clock
  (no tick needed after the close), and never if that date already has a bar.

Ticks from any IST date other than today are ignored, so an unconsumed
backlog from an earlier run cannot produce stale candles with today's date.
"""

from __future__ import annotations

import datetime as dt
import os
import time
from typing import Dict, Optional

import redis

from app.candle_builder import CandleBuilder1m
from app.candle_io import read_last_candles
from app.candle_types import Candle
from app.candles_store import CandlesStore
from app.config import load_symbols
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

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


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


def _ist_date(ts_ms: int) -> str:
    return dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=IST).date().isoformat()


def _close_ts_ms(date_str: str) -> int:
    hh, mm = MARKET_CLOSE_HHMM.split(":")
    d = dt.date.fromisoformat(date_str)
    return int(dt.datetime(d.year, d.month, d.day, int(hh), int(mm), tzinfo=IST).timestamp() * 1000)


class DayBar:
    """Running daily bar for one symbol, preferring Angel's day OHLC fields."""

    def __init__(self, date: str):
        self.date = date
        self.o: Optional[float] = None
        self.h: Optional[float] = None
        self.l: Optional[float] = None
        self.c: Optional[float] = None
        self.v: float = 0.0

    def update(self, ltp: float, day_o, day_h, day_l, cum_vol) -> None:
        o, h, l = _safe_float(day_o), _safe_float(day_h), _safe_float(day_l)
        if self.o is None:
            self.o = o if o else ltp
        self.h = max(x for x in (self.h, h, ltp) if x)
        self.l = min(x for x in (self.l, l, ltp) if x)
        self.c = ltp
        if cum_vol is not None and cum_vol > self.v:
            self.v = cum_vol

    def candle(self) -> Optional[Candle]:
        if None in (self.o, self.h, self.l, self.c):
            return None
        return Candle(ts_ms=_close_ts_ms(self.date), o=self.o, h=self.h, l=self.l, c=self.c, v=self.v)


def _has_daily_bar(r: redis.Redis, sym: str, date_str: str) -> bool:
    for c in read_last_candles(r, OUT_1D_STREAM, sym, limit=5, scan=8000):
        if _ist_date(c.ts_ms) == date_str:
            return True
    return False


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
    day_bars: Dict[str, DayBar] = {}
    day_written: set = set()  # (symbol, date) already in md:candles:1d

    # Per-symbol previous cumulative day volume to compute per-tick delta
    prev_cum_vol: Dict[str, float] = {}

    print(
        f"[CANDLES] reading {EQ_STREAM} -> writing 1m:{OUT_1M_STREAM} 1d:{OUT_1D_STREAM} "
        f"(market_close={MARKET_CLOSE_HHMM} IST)"
    )

    def flush_daily_if_closed() -> None:
        now = dt.datetime.now(IST)
        today = now.date().isoformat()
        if now.strftime("%H:%M") < MARKET_CLOSE_HHMM:
            return
        for sym, bar in day_bars.items():
            key = (sym, bar.date)
            if bar.date != today or key in day_written:
                continue
            day_written.add(key)
            if _has_daily_bar(r, sym, bar.date):
                continue
            c = bar.candle()
            if c is not None:
                store.write_candle(OUT_1D_STREAM, OUT_MAXLEN_1D, sym, "1d", c, date=bar.date)
                print(f"[CANDLES] 1d {sym} {bar.date} o={c.o} h={c.h} l={c.l} c={c.c} v={c.v:.0f}")

    skipped_old = 0
    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={EQ_STREAM: ">"},
            count=1000,
            block=1000,
        )
        flush_daily_if_closed()
        if not resp:
            continue

        today = dt.datetime.now(IST).date().isoformat()
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
                tick_date = _ist_date(ts_ms)
                if tick_date != today:
                    skipped_old += 1
                    if skipped_old in (1, 1000) or skipped_old % 100000 == 0:
                        print(f"[CANDLES] skipping ticks not from today ({tick_date}); skipped={skipped_old}")
                    continue

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

                closed_1m, _bucket = cb_1m[sym].update_tick(ts_ms=ts_ms, ltp=ltp, vol_delta=tick_vol)
                if closed_1m is not None:
                    store.write_candle(OUT_1M_STREAM, OUT_MAXLEN_1M, sym, "1m", closed_1m)

                bar = day_bars.get(sym)
                if bar is None or bar.date != tick_date:
                    bar = day_bars[sym] = DayBar(tick_date)
                if (sym, tick_date) not in day_written:
                    bar.update(ltp, fields.get("o"), fields.get("h"), fields.get("l"), cum_vol)

            if ack_ids:
                r.xack(EQ_STREAM, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

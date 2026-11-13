"""
run_candles_resampler.py
───────────────────────
Consumes 1-minute candles from md:candles:1m and publishes higher-timeframe candles:
  - md:candles:5m
  - md:candles:10m
  - md:candles:30m

This keeps resampling logic separate and restart-friendly.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import redis

from app.candle_builder import session_bucket_close_ms, session_bucket_start_ms
from app.candle_io import stream_symbol_ts
from app.candle_types import Candle
from app.candles_store import CandlesStore
from app.freshness import env_ms


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
IN_1M_STREAM = os.getenv("STREAM_CANDLES_1M", "md:candles:1m")

OUT_5M_STREAM = os.getenv("STREAM_CANDLES_5M", "md:candles:5m")
OUT_10M_STREAM = os.getenv("STREAM_CANDLES_10M", "md:candles:10m")
OUT_30M_STREAM = os.getenv("STREAM_CANDLES_30M", "md:candles:30m")

OUT_MAXLEN_5M = int(os.getenv("STREAM_MAXLEN_CANDLES_5M", "800000"))
OUT_MAXLEN_10M = int(os.getenv("STREAM_MAXLEN_CANDLES_10M", "500000"))
OUT_MAXLEN_30M = int(os.getenv("STREAM_MAXLEN_CANDLES_30M", "300000"))

# Close a higher-TF bucket by wall clock once past its end + grace (covers a
# missing last 1m bar, e.g. at 15:30). 1m bars themselves arrive ~2s after end.
FLUSH_GRACE_MS = env_ms("RESAMPLE_FLUSH_GRACE_SEC", 5.0)
READ_COUNT = 2000

GROUP = os.getenv("RESAMPLE_GROUP", "resampler")
CONSUMER = os.getenv("RESAMPLE_CONSUMER", "resampler-1")


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


def _safe_int(v) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(float(v))
    except Exception:
        return None


@dataclass
class Agg:
    bucket: Optional[int] = None
    o: Optional[float] = None
    h: Optional[float] = None
    l: Optional[float] = None
    c: Optional[float] = None
    v: float = 0.0

    def update(self, c: Candle) -> None:
        if self.o is None:
            self.o = c.o
            self.h = c.h
            self.l = c.l
            self.c = c.c
            self.v = c.v
            return
        if self.h is None or c.h > self.h:
            self.h = c.h
        if self.l is None or c.l < self.l:
            self.l = c.l
        self.c = c.c
        self.v += c.v

    def close(self, ts_ms: int) -> Optional[Candle]:
        if self.o is None or self.h is None or self.l is None or self.c is None:
            return None
        return Candle(ts_ms=ts_ms, o=float(self.o), h=float(self.h), l=float(self.l), c=float(self.c), v=float(self.v))

    def reset(self) -> None:
        self.bucket = None
        self.o = None
        self.h = None
        self.l = None
        self.c = None
        self.v = 0.0


class Resampler:
    """Aggregates 1m bars (ts_ms = bar end) into session-anchored buckets.

    Buckets start at 09:15 IST (5m/10m/30m); the last one is truncated at 15:30.
    A bucket closes when (a) the bar for its last minute arrives, (b) a bar of a
    later bucket arrives, or (c) flush(now) is past bucket end + grace.
    Out-of-order bars for an older / already-emitted bucket are dropped.
    """

    def __init__(self, minutes: int):
        self.minutes = minutes
        self.by_symbol: Dict[str, Agg] = {}
        self._last_emitted: Dict[str, int] = {}

    def prime(self, symbol: str, last_bar_end_ms: int) -> None:
        """Mark buckets up to an existing output bar as emitted, so replaying the
        1m history (group starts at id 0) does not write them a second time."""
        b = session_bucket_start_ms(last_bar_end_ms, self.minutes)
        if b is not None and b > self._last_emitted.get(symbol, -1):
            self._last_emitted[symbol] = b

    def _close(self, symbol: str, agg: Agg) -> Optional[Candle]:
        start = agg.bucket
        closed = agg.close(session_bucket_close_ms(start, self.minutes)) if start is not None else None
        if start is not None:
            self._last_emitted[symbol] = start
        agg.reset()
        return closed

    def ingest(self, symbol: str, candle_1m: Candle) -> List[Candle]:
        """Returns the buckets closed by this bar (0, 1 or 2), oldest first."""
        b = session_bucket_start_ms(candle_1m.ts_ms, self.minutes)
        if b is None:
            return []  # outside 09:15-15:30 IST
        last = self._last_emitted.get(symbol)
        if last is not None and b <= last:
            return []  # late bar for an emitted bucket
        agg = self.by_symbol.get(symbol)
        if agg is None:
            agg = Agg()
            self.by_symbol[symbol] = agg
        if agg.bucket is not None and b < agg.bucket:
            return []  # out of order

        out: List[Candle] = []
        if agg.bucket is not None and b != agg.bucket:
            c = self._close(symbol, agg)
            if c is not None:
                out.append(c)
        if agg.bucket is None:
            agg.bucket = b
        agg.update(candle_1m)

        if candle_1m.ts_ms >= session_bucket_close_ms(b, self.minutes):
            # last minute of the bucket arrived -> emit now, not on the next bar
            c = self._close(symbol, agg)
            if c is not None:
                out.append(c)
        return out

    def flush(self, now_ms: int, grace_ms: int = 5000) -> List[Tuple[str, Candle]]:
        out: List[Tuple[str, Candle]] = []
        for sym, agg in self.by_symbol.items():
            if agg.bucket is None:
                continue
            if now_ms < session_bucket_close_ms(agg.bucket, self.minutes) + 1 + grace_ms:
                continue
            c = self._close(sym, agg)
            if c is not None:
                out.append((sym, c))
        return out


def _parse_1m_fields(fields: dict) -> Optional[Tuple[str, Candle]]:
    sym = str(fields.get("symbol") or "").strip().upper()
    ts_ms = _safe_int(fields.get("ts_ms"))
    o = _safe_float(fields.get("o"))
    h = _safe_float(fields.get("h"))
    l = _safe_float(fields.get("l"))
    c = _safe_float(fields.get("c"))
    v = _safe_float(fields.get("v")) or 0.0
    if not sym or ts_ms is None or o is None or h is None or l is None or c is None:
        return None
    return sym, Candle(ts_ms=ts_ms, o=o, h=h, l=l, c=c, v=v)


def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_1M_STREAM, GROUP)
    store = CandlesStore()

    rs5 = Resampler(5)
    rs10 = Resampler(10)
    rs30 = Resampler(30)

    print(f"[RESAMPLE] reading {IN_1M_STREAM} -> writing 5m:{OUT_5M_STREAM} 10m:{OUT_10M_STREAM} 30m:{OUT_30M_STREAM}")

    targets = (
        (rs5, OUT_5M_STREAM, OUT_MAXLEN_5M, "5m"),
        (rs10, OUT_10M_STREAM, OUT_MAXLEN_10M, "10m"),
        (rs30, OUT_30M_STREAM, OUT_MAXLEN_30M, "30m"),
    )
    for rs, stream, _maxlen, tf in targets:
        newest = {sym: max(ts) for sym, ts in stream_symbol_ts(r, stream).items() if ts}
        for sym, ts in newest.items():
            rs.prime(sym, ts)
        print(f"[RESAMPLE] primed {tf} from {stream}: {len(newest)} symbols")

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={IN_1M_STREAM: ">"},
            count=READ_COUNT,
            block=2000,
        )
        n_read = 0
        for _stream, msgs in resp or []:
            n_read += len(msgs)
            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)
                parsed = _parse_1m_fields(fields)
                if not parsed:
                    continue
                sym, c1 = parsed
                for rs, stream, maxlen, tf in targets:
                    for c in rs.ingest(sym, c1):
                        store.write_candle(stream, maxlen, sym, tf, c)

            if ack_ids:
                r.xack(IN_1M_STREAM, GROUP, *ack_ids)

        # Wall-clock close (e.g. 15:00-15:30 bar at 15:30 + grace) once caught up.
        if n_read < READ_COUNT:
            now_ms = int(time.time() * 1000)
            for rs, stream, maxlen, tf in targets:
                for sym, c in rs.flush(now_ms, grace_ms=FLUSH_GRACE_MS):
                    store.write_candle(stream, maxlen, sym, tf, c)


if __name__ == "__main__":
    main()


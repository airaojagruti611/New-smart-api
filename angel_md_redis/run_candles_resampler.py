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
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import redis

from app.candle_types import Candle
from app.candles_store import CandlesStore


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
IN_1M_STREAM = os.getenv("STREAM_CANDLES_1M", "md:candles:1m")

OUT_5M_STREAM = os.getenv("STREAM_CANDLES_5M", "md:candles:5m")
OUT_10M_STREAM = os.getenv("STREAM_CANDLES_10M", "md:candles:10m")
OUT_30M_STREAM = os.getenv("STREAM_CANDLES_30M", "md:candles:30m")

OUT_MAXLEN_5M = int(os.getenv("STREAM_MAXLEN_CANDLES_5M", "800000"))
OUT_MAXLEN_10M = int(os.getenv("STREAM_MAXLEN_CANDLES_10M", "500000"))
OUT_MAXLEN_30M = int(os.getenv("STREAM_MAXLEN_CANDLES_30M", "300000"))

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


def _bucket(ts_ms: int, minutes: int) -> int:
    return ts_ms // (minutes * 60_000)


def _bucket_close_ts_ms(bucket: int, minutes: int) -> int:
    return (bucket + 1) * (minutes * 60_000) - 1


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
    def __init__(self, minutes: int):
        self.minutes = minutes
        self.by_symbol: Dict[str, Agg] = {}

    def ingest(self, symbol: str, candle_1m: Candle) -> Optional[Candle]:
        b = _bucket(candle_1m.ts_ms, self.minutes)
        agg = self.by_symbol.get(symbol)
        if agg is None:
            agg = Agg(bucket=b)
            self.by_symbol[symbol] = agg

        if agg.bucket is None:
            agg.bucket = b

        if b != agg.bucket:
            closed = agg.close(_bucket_close_ts_ms(agg.bucket, self.minutes))
            agg.reset()
            agg.bucket = b
            agg.update(candle_1m)
            return closed

        agg.update(candle_1m)
        return None


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

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={IN_1M_STREAM: ">"},
            count=2000,
            block=2000,
        )
        if not resp:
            continue

        for _stream, msgs in resp:
            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)
                parsed = _parse_1m_fields(fields)
                if not parsed:
                    continue
                sym, c1 = parsed

                c5 = rs5.ingest(sym, c1)
                if c5 is not None:
                    store.write_candle(OUT_5M_STREAM, OUT_MAXLEN_5M, sym, "5m", c5)

                c10 = rs10.ingest(sym, c1)
                if c10 is not None:
                    store.write_candle(OUT_10M_STREAM, OUT_MAXLEN_10M, sym, "10m", c10)

                c30 = rs30.ingest(sym, c1)
                if c30 is not None:
                    store.write_candle(OUT_30M_STREAM, OUT_MAXLEN_30M, sym, "30m", c30)

            if ack_ids:
                r.xack(IN_1M_STREAM, GROUP, *ack_ids)


if __name__ == "__main__":
    main()


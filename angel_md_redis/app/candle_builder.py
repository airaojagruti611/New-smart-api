from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Optional, Tuple

from .candle_types import Candle


def _safe_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        if v == "":
            return None
        return float(v)
    except Exception:
        return None


def _safe_int(v) -> Optional[int]:
    try:
        if v is None:
            return None
        if v == "":
            return None
        return int(float(v))
    except Exception:
        return None


def _minute_bucket(ts_ms: int, minutes: int = 1) -> int:
    return ts_ms // (minutes * 60_000)


def _bucket_close_ts_ms(bucket: int, minutes: int = 1) -> int:
    # last millisecond of the bucket window
    return (bucket + 1) * (minutes * 60_000) - 1


def _date_str_local(ts_ms: int) -> str:
    # Uses system-local timezone. If you run on an IST machine/container, this matches NSE day boundaries.
    return dt.datetime.fromtimestamp(ts_ms / 1000.0).date().isoformat()


@dataclass
class RunningCandle:
    o: Optional[float] = None
    h: Optional[float] = None
    l: Optional[float] = None
    c: Optional[float] = None
    v: float = 0.0
    _has_first: bool = False

    def update(self, price: float, vol_delta: float) -> None:
        if not self._has_first:
            self.o = price
            self.h = price
            self.l = price
            self.c = price
            self._has_first = True
        else:
            if self.h is None or price > self.h:
                self.h = price
            if self.l is None or price < self.l:
                self.l = price
            self.c = price
        if vol_delta > 0:
            self.v += vol_delta

    def ready(self) -> bool:
        return self._has_first and self.o is not None and self.h is not None and self.l is not None and self.c is not None

    def close(self, ts_ms: int) -> Optional[Candle]:
        if not self.ready():
            return None
        return Candle(
            ts_ms=ts_ms,
            o=float(self.o),
            h=float(self.h),
            l=float(self.l),
            c=float(self.c),
            v=float(self.v),
        )

    def reset(self) -> None:
        self.o = None
        self.h = None
        self.l = None
        self.c = None
        self.v = 0.0
        self._has_first = False


class CandleBuilder1m:
    """
    Builds 1-minute candles per symbol from ticks.

    Use update_tick() per tick. It returns (closed_candle, bucket_id) when a minute rolls over,
    otherwise (None, current_bucket).
    """

    def __init__(self):
        self.bucket: Optional[int] = None
        self.rc = RunningCandle()

    def update_tick(self, ts_ms: int, ltp: float, vol_delta: float) -> Tuple[Optional[Candle], int]:
        b = _minute_bucket(ts_ms, minutes=1)
        if self.bucket is None:
            self.bucket = b
            self.rc.update(ltp, vol_delta)
            return None, b

        if b != self.bucket:
            closed = self.rc.close(_bucket_close_ts_ms(self.bucket, minutes=1))
            self.rc.reset()
            self.bucket = b
            self.rc.update(ltp, vol_delta)
            return closed, b

        self.rc.update(ltp, vol_delta)
        return None, b


class CandleBuilder1d:
    """
    Builds a daily candle per symbol from ticks, using local calendar day boundary by default.
    Also supports an optional "market close" HH:MM local time to flush the candle without waiting
    for date rollover.
    """

    def __init__(self, market_close_hhmm: str = "15:30"):
        self.day: Optional[str] = None
        self.rc = RunningCandle()
        self._market_close_hhmm = market_close_hhmm
        self._closed_for_day: Optional[str] = None

    def _should_force_close(self, ts_ms: int) -> bool:
        # force close once per day after market_close_hhmm, if candle is open and not yet closed for that day
        if not self.day or self._closed_for_day == self.day:
            return False
        try:
            hh, mm = self._market_close_hhmm.split(":")
            close_min = int(hh) * 60 + int(mm)
        except Exception:
            return False

        t = dt.datetime.fromtimestamp(ts_ms / 1000.0)
        now_min = t.hour * 60 + t.minute
        return now_min >= close_min

    def update_tick(self, ts_ms: int, ltp: float, vol_delta: float) -> Optional[Candle]:
        d = _date_str_local(ts_ms)
        if self.day is None:
            self.day = d
            self.rc.update(ltp, vol_delta)
            return None

        # Day rollover
        if d != self.day:
            closed = self.rc.close(ts_ms=ts_ms)
            self.rc.reset()
            self._closed_for_day = self.day
            self.day = d
            self.rc.update(ltp, vol_delta)
            return closed

        # Force close after market close time (once)
        if self._should_force_close(ts_ms) and self.rc.ready():
            closed = self.rc.close(ts_ms=ts_ms)
            self._closed_for_day = self.day
            # keep candle open state reset so we don't keep adding after close
            self.rc.reset()
            return closed

        self.rc.update(ltp, vol_delta)
        return None


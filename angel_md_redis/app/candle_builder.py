from __future__ import annotations

import datetime as dt
import os
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

from .candle_types import Candle

try:
    from zoneinfo import ZoneInfo

    IST = ZoneInfo("Asia/Kolkata")
except Exception:  # pragma: no cover
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

# NSE cash/F&O continuous session (IST). Bars are anchored to the open and the
# last bucket of a timeframe is truncated at the close.
SESSION_OPEN_HHMM = os.getenv("SESSION_OPEN_HHMM", "09:15")
SESSION_CLOSE_HHMM = os.getenv("MARKET_CLOSE_HHMM", "15:30")


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


def _hhmm(s: str, default: Tuple[int, int]) -> Tuple[int, int]:
    try:
        hh, mm = str(s).split(":")
        return int(hh), int(mm)
    except Exception:
        return default


def _now_ms() -> int:
    return int(time.time() * 1000)


# ── IST session helpers ────────────────────────────────────────────────────

def session_date_ist(ts_ms: int) -> dt.date:
    """IST calendar date of an epoch-ms timestamp (independent of host TZ)."""
    return dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=IST).date()


def session_bounds_ms(
    day: dt.date,
    open_hhmm: Optional[str] = None,
    close_hhmm: Optional[str] = None,
) -> Tuple[int, int]:
    """(open_ms, close_ms) of the NSE session on IST date `day`; close is exclusive."""
    oh, om = _hhmm(open_hhmm or SESSION_OPEN_HHMM, (9, 15))
    ch, cm = _hhmm(close_hhmm or SESSION_CLOSE_HHMM, (15, 30))
    o = dt.datetime(day.year, day.month, day.day, oh, om, tzinfo=IST)
    c = dt.datetime(day.year, day.month, day.day, ch, cm, tzinfo=IST)
    return int(o.timestamp() * 1000), int(c.timestamp() * 1000)


def in_session(ts_ms: int, close_hhmm: Optional[str] = None) -> bool:
    o, c = session_bounds_ms(session_date_ist(ts_ms), close_hhmm=close_hhmm)
    return o <= ts_ms < c


def session_close_bar_ts_ms(day: dt.date, close_hhmm: Optional[str] = None) -> int:
    """Canonical ts_ms of a 1d bar: last ms of the session (15:29:59.999 IST)."""
    return session_bounds_ms(day, close_hhmm=close_hhmm)[1] - 1


def session_bucket_start_ms(ts_ms: int, minutes: int, close_hhmm: Optional[str] = None) -> Optional[int]:
    """Start ms of the `minutes` bucket anchored at the 09:15 IST open; None outside the session."""
    o, c = session_bounds_ms(session_date_ist(ts_ms), close_hhmm=close_hhmm)
    if not (o <= ts_ms < c):
        return None
    step = max(1, int(minutes)) * 60_000
    return o + ((ts_ms - o) // step) * step


def session_bucket_close_ms(start_ms: int, minutes: int, close_hhmm: Optional[str] = None) -> int:
    """Inclusive last ms of a session bucket; the final bucket is truncated at the close."""
    _o, c = session_bounds_ms(session_date_ist(start_ms), close_hhmm=close_hhmm)
    return min(start_ms + max(1, int(minutes)) * 60_000, c) - 1


def tick_is_stale(ts_ms: int, recv_ms: Optional[int], max_lag_ms: int = 300_000) -> bool:
    """A tick whose exchange time is from another IST date than its arrival
    (e.g. the WS connect snapshot carrying yesterday's last timestamp), or lags
    arrival by more than `max_lag_ms`, must not build bars."""
    if recv_ms is None:
        return False
    if session_date_ist(ts_ms) != session_date_ist(recv_ms):
        return True
    return max_lag_ms > 0 and (recv_ms - ts_ms) > max_lag_ms


def session_completed(day: dt.date, now_ms: int, close_hhmm: Optional[str] = None) -> bool:
    return now_ms >= session_bounds_ms(day, close_hhmm=close_hhmm)[1]


def is_degenerate_daily(c: Candle) -> bool:
    return c.h is None or c.l is None or not (c.h > c.l)


# Epoch-anchored helpers kept for history_yahoo / history_moneycontrol (1m is
# identical to session anchoring; use session_bucket_* for >1m).
def _minute_bucket(ts_ms: int, minutes: int = 1) -> int:
    return ts_ms // (minutes * 60_000)


def _bucket_close_ts_ms(bucket: int, minutes: int = 1) -> int:
    # last millisecond of the bucket window
    return (bucket + 1) * (minutes * 60_000) - 1


def resample_candles(candles: List[Candle], minutes: int, now_ms: Optional[int] = None) -> List[Candle]:
    """Aggregate 1m bars (ts_ms = bar end) into closed `minutes` candles, oldest-first.

    Buckets are anchored at the 09:15 IST session open (09:15-09:25 for 10m,
    09:15-09:45 for 30m); the last bucket is truncated at 15:30. Bars outside
    the session are ignored and the still-open bucket is not emitted.
    """
    if minutes <= 1 or not candles:
        return list(candles)
    now_ms = _now_ms() if now_ms is None else int(now_ms)
    out: List[Candle] = []
    cur: Optional[int] = None
    o = h = l = c = None
    v = 0.0

    def _flush(start: int) -> None:
        if o is None or h is None or l is None or c is None:
            return
        close_ts = session_bucket_close_ms(start, minutes)
        if close_ts > now_ms:
            return
        out.append(Candle(ts_ms=close_ts, o=float(o), h=float(h), l=float(l), c=float(c), v=float(v)))

    for bar in sorted(candles, key=lambda x: x.ts_ms):
        b = session_bucket_start_ms(bar.ts_ms, minutes)
        if b is None:
            continue
        if cur is None or b != cur:
            if cur is not None:
                _flush(cur)
            cur = b
            o, h, l, c, v = bar.o, bar.h, bar.l, bar.c, bar.v
        else:
            h = max(h, bar.h)
            l = min(l, bar.l)
            c = bar.c
            v += bar.v
    if cur is not None:
        _flush(cur)
    return out


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
    Builds 1-minute candles per symbol from ticks (ts_ms of a bar = its last ms).

    update_tick() returns (closed_candle, bucket_id) when a minute rolls over,
    otherwise (None, current_bucket). flush(now_ms) closes the open bar once the
    wall clock is past its end + grace, so the 15:29 bar is emitted at ~15:30:02
    instead of on the next morning's first tick.

    Ticks outside 09:15-15:30 IST and out-of-order ticks for an older (or
    already flushed) minute are dropped.
    """

    def __init__(self, close_hhmm: Optional[str] = None):
        self.bucket: Optional[int] = None
        self.rc = RunningCandle()
        self._close_hhmm = close_hhmm
        self._last_closed: Optional[int] = None

    def _close_current(self) -> Optional[Candle]:
        if self.bucket is None:
            return None
        closed = self.rc.close(_bucket_close_ts_ms(self.bucket, minutes=1))
        self._last_closed = self.bucket
        self.rc.reset()
        self.bucket = None
        return closed

    def update_tick(self, ts_ms: int, ltp: float, vol_delta: float) -> Tuple[Optional[Candle], Optional[int]]:
        if not in_session(ts_ms, close_hhmm=self._close_hhmm):
            return None, self.bucket
        b = _minute_bucket(ts_ms, minutes=1)
        if self._last_closed is not None and b <= self._last_closed:
            return None, self.bucket  # late tick for a bar already emitted
        if self.bucket is not None and b < self.bucket:
            return None, self.bucket  # out-of-order tick from an older minute

        if self.bucket is None:
            self.bucket = b
            self.rc.update(ltp, vol_delta)
            return None, b

        if b != self.bucket:
            closed = self._close_current()
            self.bucket = b
            self.rc.update(ltp, vol_delta)
            return closed, b

        self.rc.update(ltp, vol_delta)
        return None, b

    def flush(self, now_ms: int, grace_ms: int = 2000) -> Optional[Candle]:
        """Emit the open bar when `now_ms` >= bar end + grace."""
        if self.bucket is None:
            return None
        if now_ms < _bucket_close_ts_ms(self.bucket, minutes=1) + 1 + grace_ms:
            return None
        return self._close_current()


class CandleBuilder1d:
    """
    Builds the daily candle per symbol for the NSE session (09:15-15:30 IST).

    - Only in-session ticks count; ticks after the close never start a new bar.
    - When the exchange day fields (open/high/low/cumulative volume of the day)
      are passed, they override what was seen since start, so a midday restart
      still yields the true session H/L/V. Close = last in-session LTP.
    - The bar is emitted once: by flush(now) after close + grace, or on the
      first tick of a later session if no flush happened. Its ts_ms is the
      session's last ms (15:29:59.999 IST) so its IST date is the session date.
    """

    def __init__(self, market_close_hhmm: str = "15:30"):
        self.day: Optional[dt.date] = None
        self.rc = RunningCandle()
        self._market_close_hhmm = market_close_hhmm
        self._closed_for_day: Optional[dt.date] = None

    def _emit(self) -> Optional[Candle]:
        if self.day is None or self._closed_for_day == self.day:
            return None
        day = self.day
        self._closed_for_day = day
        closed = None
        if self.rc.ready():
            c = self.rc.close(ts_ms=session_close_bar_ts_ms(day, close_hhmm=self._market_close_hhmm))
            if c is not None and c.h >= c.l:
                closed = c
        self.rc.reset()
        return closed

    def update_tick(
        self,
        ts_ms: int,
        ltp: float,
        vol_delta: float,
        day_open: Optional[float] = None,
        day_high: Optional[float] = None,
        day_low: Optional[float] = None,
        day_volume: Optional[float] = None,
    ) -> Optional[Candle]:
        d = session_date_ist(ts_ms)
        closed: Optional[Candle] = None

        if self.day is not None and d < self.day:
            return None  # out-of-order tick from an older session
        if self.day is not None and d > self.day:
            closed = self._emit()  # rollover without a flush at close
            self.day = None

        if not in_session(ts_ms, close_hhmm=self._market_close_hhmm):
            _o, close_ms = session_bounds_ms(d, close_hhmm=self._market_close_hhmm)
            if self.day == d and ts_ms >= close_ms:
                after = self._emit()  # after-close tick: close once, never reopen
                return closed or after
            return closed

        if self._closed_for_day == d:
            return closed
        if self.day is None:
            self.day = d

        self.rc.update(ltp, vol_delta)
        if day_open is not None and day_open > 0:
            self.rc.o = float(day_open)
        if day_high is not None and day_high > 0 and (self.rc.h is None or day_high > self.rc.h):
            self.rc.h = float(day_high)
        if day_low is not None and day_low > 0 and (self.rc.l is None or day_low < self.rc.l):
            self.rc.l = float(day_low)
        if day_volume is not None and day_volume >= 0:
            self.rc.v = float(day_volume)
        return closed

    def flush(self, now_ms: int, grace_ms: int = 2000) -> Optional[Candle]:
        """Emit the session bar once the wall clock passes 15:30 IST + grace."""
        if self.day is None or self._closed_for_day == self.day:
            return None
        _o, close_ms = session_bounds_ms(self.day, close_hhmm=self._market_close_hhmm)
        if now_ms < close_ms + grace_ms:
            return None
        return self._emit()

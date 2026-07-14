from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
try:
    from zoneinfo import ZoneInfo

    IST = ZoneInfo("Asia/Kolkata")
except Exception:  # pragma: no cover - Windows without tzdata
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

from .candle_types import Candle


@dataclass(frozen=True)
class HtfTrendResult:
    daily: str  # "bullish" / "bearish" / "na"
    weekly: str
    monthly: str
    bias: str  # "CALL" / "PUT" / "NEUTRAL"
    d_close: float = 0.0
    d_prev: float = 0.0
    w_close: float = 0.0
    w_prev: float = 0.0
    m_close: float = 0.0
    m_prev: float = 0.0


def _candle_date_ist(c: Candle) -> dt.date:
    return dt.datetime.fromtimestamp(c.ts_ms / 1000.0, tz=IST).date()


def _week_key(d: dt.date) -> Tuple[int, int]:
    iso = d.isocalendar()
    return int(iso.year), int(iso.week)


def _month_key(d: dt.date) -> Tuple[int, int]:
    return d.year, d.month


def _merge_ohlcv(candles: List[Candle]) -> Optional[Candle]:
    if not candles:
        return None
    return Candle(
        ts_ms=int(candles[-1].ts_ms),
        o=float(candles[0].o),
        h=float(max(x.h for x in candles)),
        l=float(min(x.l for x in candles)),
        c=float(candles[-1].c),
        v=float(sum(x.v for x in candles)),
    )


def aggregate_period_candles(
    daily: List[Candle],
    key_fn,
) -> List[Candle]:
    """
    Aggregate sorted daily candles into period candles (week or month).
    Periods are ordered by first appearance; each period close = last daily close in group.
    """
    if not daily:
        return []

    ordered = sorted(daily, key=lambda x: x.ts_ms)
    groups: Dict[Tuple[int, int], List[Candle]] = {}
    order: List[Tuple[int, int]] = []

    for c in ordered:
        k = key_fn(_candle_date_ist(c))
        if k not in groups:
            groups[k] = []
            order.append(k)
        groups[k].append(c)

    out: List[Candle] = []
    for k in order:
        merged = _merge_ohlcv(groups[k])
        if merged is not None:
            out.append(merged)
    return out


def weekly_candles_from_daily(daily: List[Candle]) -> List[Candle]:
    return aggregate_period_candles(daily, _week_key)


def monthly_candles_from_daily(daily: List[Candle]) -> List[Candle]:
    return aggregate_period_candles(daily, _month_key)


def closed_period_candles(
    period_candles: List[Candle],
    *,
    drop_current: bool,
) -> List[Candle]:
    """
    For week/month, drop the in-progress period (current calendar bucket).
    Daily candles from md:candles:1d are already closed — do not drop.
    """
    if not drop_current:
        return list(period_candles)
    if len(period_candles) <= 1:
        return []
    return list(period_candles[:-1])


def _direction_from_closes(prev_close: float, close: float) -> str:
    if close > prev_close:
        return "bullish"
    if close < prev_close:
        return "bearish"
    return "na"


def _tf_direction(closed: List[Candle]) -> Tuple[str, float, float]:
    if len(closed) < 2:
        return "na", 0.0, 0.0
    prev, last = closed[-2], closed[-1]
    return _direction_from_closes(prev.c, last.c), float(last.c), float(prev.c)


def htf_trend_bias(daily: List[Candle]) -> HtfTrendResult:
    """
    Chartink-style higher-timeframe trend:

      Daily close  > previous daily close
      Weekly close > previous weekly close
      Monthly close > previous monthly close

    All three bullish -> CALL (only allow calls)
    All three bearish -> PUT (only allow puts)
    Else -> NEUTRAL
    """
    ordered = sorted(daily, key=lambda x: x.ts_ms)

    d_dir, d_close, d_prev = _tf_direction(ordered)

    weeks = closed_period_candles(weekly_candles_from_daily(ordered), drop_current=True)
    months = closed_period_candles(monthly_candles_from_daily(ordered), drop_current=True)

    w_dir, w_close, w_prev = _tf_direction(weeks)
    m_dir, m_close, m_prev = _tf_direction(months)

    if d_dir == "bullish" and w_dir == "bullish" and m_dir == "bullish":
        bias = "CALL"
    elif d_dir == "bearish" and w_dir == "bearish" and m_dir == "bearish":
        bias = "PUT"
    else:
        bias = "NEUTRAL"

    return HtfTrendResult(
        daily=d_dir,
        weekly=w_dir,
        monthly=m_dir,
        bias=bias,
        d_close=d_close,
        d_prev=d_prev,
        w_close=w_close,
        w_prev=w_prev,
        m_close=m_close,
        m_prev=m_prev,
    )

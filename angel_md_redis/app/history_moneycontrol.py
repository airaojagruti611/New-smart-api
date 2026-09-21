"""Moneycontrol TradingView-style NSE equity history (OHLCV)."""

from __future__ import annotations

import time
from typing import List, Optional

import requests

from .candle_builder import _bucket_close_ts_ms, _minute_bucket
from .candle_types import Candle

MC_URL = "https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history"
_UA = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
}

_RES = {
    "1m": ("1", 12 * 86400, 5000),
    "5m": ("5", 40 * 86400, 5000),
    "30m": ("30", 80 * 86400, 4000),
    "1d": ("1D", 400 * 86400, 400),
}


def _as_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def fetch_moneycontrol_nse_candles(symbol: str, tf: str, timeout: int = 25) -> List[Candle]:
    spec = _RES.get(tf)
    if spec is None:
        raise ValueError(f"unsupported moneycontrol tf={tf}")
    resolution, lookback_sec, countback = spec
    now = int(time.time())
    last_err: Optional[Exception] = None
    for attempt in range(4):
        try:
            r = requests.get(
                MC_URL,
                params={
                    "symbol": str(symbol).strip().upper(),
                    "resolution": resolution,
                    "from": now - lookback_sec,
                    "to": now,
                    "countback": countback,
                    "currencyCode": "INR",
                },
                headers=_UA,
                timeout=timeout,
            )
            if r.status_code >= 500:
                last_err = RuntimeError(f"http {r.status_code}")
                time.sleep(0.8 * (attempt + 1))
                continue
            r.raise_for_status()
            body = r.json() if r.content else {}
            if (body.get("s") or "").lower() != "ok":
                raise RuntimeError(f"moneycontrol s={body.get('s')}")
            ts_list = body.get("t") or []
            opens = body.get("o") or []
            highs = body.get("h") or []
            lows = body.get("l") or []
            closes = body.get("c") or []
            vols = body.get("v") or []
            minutes = {"1m": 1, "5m": 5, "30m": 30, "1d": 0}[tf]
            out: List[Candle] = []
            n = min(len(ts_list), len(opens), len(highs), len(lows), len(closes))
            for i in range(n):
                o = _as_float(opens[i])
                h = _as_float(highs[i])
                l = _as_float(lows[i])
                c = _as_float(closes[i])
                if o is None or h is None or l is None or c is None:
                    continue
                raw_ms = int(float(ts_list[i]) * 1000)
                if minutes > 0:
                    ts_ms = _bucket_close_ts_ms(_minute_bucket(raw_ms, minutes), minutes)
                else:
                    ts_ms = raw_ms
                v = _as_float(vols[i] if i < len(vols) else None) or 0.0
                out.append(Candle(ts_ms=ts_ms, o=o, h=h, l=l, c=c, v=v))
            by_ts = {c.ts_ms: c for c in out}
            return [by_ts[k] for k in sorted(by_ts)]
        except Exception as e:
            last_err = e
            time.sleep(0.6 * (attempt + 1))
    raise RuntimeError(f"moneycontrol failed symbol={symbol} tf={tf}: {last_err!r}")

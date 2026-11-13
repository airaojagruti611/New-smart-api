"""
NSE equity OHLCV from Yahoo chart API.

Angel historical REST (getCandleData) often returns AG8004 / empty data
on this deployment while the websocket still works. Yahoo `.NS` bars are
the same NSE session used by Supertrend / EMA / pivots warmup.
"""

from __future__ import annotations

import time
from typing import List, Optional

import requests

from .candle_builder import _bucket_close_ts_ms, _minute_bucket
from .candle_types import Candle

_HOSTS = (
    "https://query1.finance.yahoo.com",
    "https://query2.finance.yahoo.com",
)
_UA = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
}

_INTERVAL_RANGE = {
    "1m": ("1m", "7d"),
    "5m": ("5m", "1mo"),
    "15m": ("15m", "1mo"),
    "30m": ("30m", "1mo"),
    "1d": ("1d", "1y"),
}

_SESSION: Optional[requests.Session] = None
_CRUMB: str = ""


def _as_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _session() -> requests.Session:
    global _SESSION, _CRUMB
    if _SESSION is not None:
        return _SESSION
    s = requests.Session()
    s.headers.update(_UA)
    try:
        s.get("https://fc.yahoo.com", timeout=10)
    except Exception:
        pass
    try:
        crumb_r = s.get("https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=10)
        if crumb_r.ok:
            text = (crumb_r.text or "").strip().strip('"')
            if text and "<" not in text:
                _CRUMB = text
    except Exception:
        pass
    _SESSION = s
    return s


def _chart_get(symbol: str, interval: str, rng: str, timeout: int) -> dict:
    sess = _session()
    last_err: Optional[Exception] = None
    params = {"interval": interval, "range": rng, "includePrePost": "false"}
    if _CRUMB:
        params["crumb"] = _CRUMB
    for attempt in range(5):
        host = _HOSTS[attempt % len(_HOSTS)]
        url = f"{host}/v8/finance/chart/{symbol}.NS"
        try:
            r = sess.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                wait = 2.0 * (attempt + 1)
                print(f"[HISTORY] yahoo 429 {symbol} {interval}; sleep {wait:.0f}s")
                time.sleep(wait)
                last_err = requests.HTTPError(f"429 {url}")
                continue
            r.raise_for_status()
            return r.json() if r.content else {}
        except Exception as e:
            last_err = e
            time.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"yahoo chart failed symbol={symbol} interval={interval}: {last_err!r}")


def fetch_yahoo_nse_candles(symbol: str, tf: str, timeout: int = 30) -> List[Candle]:
    """
    Return oldest-first closed candles for an NSE equity symbol.
    `tf` is 1m / 5m / 30m / 1d. Unknown tfs raise ValueError.
    """
    spec = _INTERVAL_RANGE.get(tf)
    if spec is None:
        raise ValueError(f"unsupported yahoo tf={tf}")
    interval, rng = spec
    body = _chart_get(str(symbol).strip().upper(), interval, rng, timeout)
    result = ((body.get("chart") or {}).get("result") or [None])[0]
    if not result:
        err = (body.get("chart") or {}).get("error")
        raise RuntimeError(f"yahoo empty result symbol={symbol} tf={tf} err={err}")

    ts_list = result.get("timestamp") or []
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    vols = quote.get("volume") or []

    minutes = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1d": 0}[tf]
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

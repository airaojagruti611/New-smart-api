"""
Seed md:candles:* from Angel One historical API so Indicator Signals
(Supertrend / EMA / HTF / pivots) can compute on a cold midday start.
"""

from __future__ import annotations

import datetime as dt
import os
import time
from typing import Dict, List, Optional, Sequence, Tuple

import redis

from .angel_auth import login
from .angel_rest import fetch_candle_data
from .candle_io import count_symbol_candles, existing_ts_set
from .candle_types import Candle
from .candles_store import CandlesStore
from .config import load_symbols
from .pivots import classic_pivots
from .scripmaster import load_scripmaster, resolve_eq_tokens

try:
    from zoneinfo import ZoneInfo

    IST = ZoneInfo("Asia/Kolkata")
except Exception:  # pragma: no cover
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

STREAM_1M = os.getenv("STREAM_CANDLES_1M", "md:candles:1m")
STREAM_5M = os.getenv("STREAM_CANDLES_5M", "md:candles:5m")
STREAM_10M = os.getenv("STREAM_CANDLES_10M", "md:candles:10m")
STREAM_30M = os.getenv("STREAM_CANDLES_30M", "md:candles:30m")
STREAM_1D = os.getenv("STREAM_CANDLES_1D", "md:candles:1d")
STREAM_PIVOTS = os.getenv("STREAM_PIVOTS_PREVDAY", "md:pivots:prevday")
PIVOTS_KEY_PREFIX = os.getenv("PIVOTS_PREVDAY_PREFIX", "md:pivots:prevday:")

OUT_MAXLEN_1M = int(os.getenv("STREAM_MAXLEN_CANDLES_1M", "2000000"))
OUT_MAXLEN_5M = int(os.getenv("STREAM_MAXLEN_CANDLES_5M", "800000"))
OUT_MAXLEN_10M = int(os.getenv("STREAM_MAXLEN_CANDLES_10M", "500000"))
OUT_MAXLEN_30M = int(os.getenv("STREAM_MAXLEN_CANDLES_30M", "300000"))
OUT_MAXLEN_1D = int(os.getenv("STREAM_MAXLEN_CANDLES_1D", "200000"))
OUT_MAXLEN_PIVOTS = int(os.getenv("STREAM_MAXLEN_PIVOTS_PREVDAY", "200000"))

SLEEP_SEC = float(os.getenv("HISTORY_SLEEP_SEC", "0.40"))

# interval, stream, maxlen, lookback_days, min_bars_to_skip_fetch, skip_today
INTERVALS: List[Tuple[str, str, str, int, int, int, bool]] = [
    ("ONE_DAY", "1d", STREAM_1D, OUT_MAXLEN_1D, 220, 2, True),
    ("ONE_MINUTE", "1m", STREAM_1M, OUT_MAXLEN_1M, 3, 26, False),
    ("FIVE_MINUTE", "5m", STREAM_5M, OUT_MAXLEN_5M, 10, 8, False),
    ("TEN_MINUTE", "10m", STREAM_10M, OUT_MAXLEN_10M, 15, 8, False),
    ("THIRTY_MINUTE", "30m", STREAM_30M, OUT_MAXLEN_30M, 25, 8, False),
]


def _now_ist() -> dt.datetime:
    return dt.datetime.now(IST)


def _fmt(ts: dt.datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M")


def _parse_row_ts(value) -> Optional[dt.datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 1e12:
            raw = raw / 1000.0
        return dt.datetime.fromtimestamp(raw, tz=IST)
    s = str(value).strip()
    if not s:
        return None
    try:
        parsed = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                parsed = dt.datetime.strptime(s[:19], fmt)
                break
            except ValueError:
                parsed = None
        if parsed is None:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=IST)
    return parsed.astimezone(IST)


def _row_to_candle(row) -> Optional[Candle]:
    if not isinstance(row, (list, tuple)) or len(row) < 6:
        return None
    when = _parse_row_ts(row[0])
    if when is None:
        return None
    try:
        o, h, l, c = float(row[1]), float(row[2]), float(row[3]), float(row[4])
        v = float(row[5] or 0)
    except (TypeError, ValueError):
        return None
    return Candle(ts_ms=int(when.timestamp() * 1000), o=o, h=h, l=l, c=c, v=v)


def _range_for(interval: str, lookback_days: int) -> Tuple[str, str]:
    now = _now_ist()
    if interval == "ONE_DAY":
        start = (now - dt.timedelta(days=lookback_days)).replace(
            hour=9, minute=15, second=0, microsecond=0
        )
        end = now.replace(hour=15, minute=30, second=0, microsecond=0)
    else:
        start = (now - dt.timedelta(days=lookback_days)).replace(
            hour=9, minute=15, second=0, microsecond=0
        )
        end = now
    return _fmt(start), _fmt(end)


def _needs_seed(r: redis.Redis, symbols: Sequence[str]) -> bool:
    checks = [
        (STREAM_1D, 2),
        (STREAM_1M, 26),
        (STREAM_5M, 8),
        (STREAM_10M, 8),
        (STREAM_30M, 8),
    ]
    for stream, need in checks:
        counts = count_symbol_candles(r, stream, list(symbols), per_symbol_limit=need)
        if any(counts.get(s.upper(), 0) < need for s in symbols):
            return True
    return False


def seed_pivots_from_daily(r: redis.Redis, symbols: Sequence[str], store: Optional[CandlesStore] = None) -> int:
    """Write today's prev-day pivots from the latest closed 1d bar per symbol."""
    from .candle_io import read_last_candles

    store = store or CandlesStore()
    written = 0
    for sym in symbols:
        bars = read_last_candles(r, STREAM_1D, sym, limit=2, scan=8000)
        if not bars:
            continue
        src = bars[-1]
        date_str = dt.datetime.fromtimestamp(src.ts_ms / 1000.0, tz=IST).date().isoformat()
        p = classic_pivots(src, date=date_str)
        store.write_pivots_prevday(f"{PIVOTS_KEY_PREFIX}{sym.upper()}", p)
        r.xadd(
            STREAM_PIVOTS,
            {
                "ts_ms": str(int(src.ts_ms)),
                "symbol": sym.upper(),
                "date": p.date,
                "P": f"{p.P:.2f}",
                "R1": f"{p.R1:.2f}",
                "S1": f"{p.S1:.2f}",
                "R2": f"{p.R2:.2f}",
                "S2": f"{p.S2:.2f}",
            },
            maxlen=OUT_MAXLEN_PIVOTS,
            approximate=True,
        )
        written += 1
        print(f"[HISTORY] pivots {sym} from {p.date} P={p.P:.2f} R1={p.R1:.2f} S1={p.S1:.2f}")
    return written


def seed_history(
    r: Optional[redis.Redis] = None,
    symbols: Optional[Sequence[str]] = None,
    force: bool = False,
) -> Dict[str, int]:
    """
    Fetch Angel history into candle streams. Skips an interval/symbol that
    already has enough bars unless force=True.
    """
    symbols = [s.upper() for s in (symbols or load_symbols())]
    if not symbols:
        print("[HISTORY] no symbols")
        return {}

    if r is None:
        r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)

    if not force and not _needs_seed(r, symbols):
        n = seed_pivots_from_daily(r, symbols)
        print(f"[HISTORY] skip fetch: daily candles already present; pivots_written={n}")
        return {"skipped": 1, "pivots": n}

    print(f"[HISTORY] login + ScripMaster for {len(symbols)} symbols")
    _obj, auth_token, _feed = login()
    df = load_scripmaster()
    tokens = resolve_eq_tokens(df, list(symbols))
    store = CandlesStore()
    today = _now_ist().date()
    written: Dict[str, int] = {}

    for interval, tf, stream, maxlen, lookback_days, min_bars, skip_today in INTERVALS:
        fromdate, todate = _range_for(interval, lookback_days)
        for sym in symbols:
            info = tokens.get(sym)
            if not info:
                print(f"[HISTORY] SKIP no_eq_token symbol={sym}")
                continue
            have = len(existing_ts_set(r, stream, sym, scan=max(4000, min_bars * 20)))
            if not force and have >= min_bars:
                print(f"[HISTORY] skip {sym} {tf}: already {have} bars")
                continue

            body = fetch_candle_data(
                auth_token,
                exchange=info["exchange"],
                symboltoken=info["token"],
                interval=interval,
                fromdate=fromdate,
                todate=todate,
            )
            time.sleep(SLEEP_SEC)
            if not body or body.get("status") is False:
                print(
                    f"[HISTORY] FAIL {sym} {tf}: {body.get('message') if body else 'empty'} "
                    f"code={body.get('errorcode') if body else ''}"
                )
                continue

            rows = body.get("data") or []
            existing = existing_ts_set(r, stream, sym, scan=12000)
            n = 0
            for row in rows:
                candle = _row_to_candle(row)
                if candle is None:
                    continue
                if skip_today:
                    d = dt.datetime.fromtimestamp(candle.ts_ms / 1000.0, tz=IST).date()
                    if d >= today:
                        continue
                if candle.ts_ms in existing:
                    continue
                extra_date = ""
                if tf == "1d":
                    extra_date = dt.datetime.fromtimestamp(
                        candle.ts_ms / 1000.0, tz=IST
                    ).date().isoformat()
                store.write_candle(stream, maxlen, sym, tf, candle, date=extra_date)
                existing.add(candle.ts_ms)
                n += 1
            key = f"{sym}:{tf}"
            written[key] = n
            print(f"[HISTORY] wrote {n} {tf} bars for {sym} (had={have})")

    written["pivots"] = seed_pivots_from_daily(r, symbols, store=store)
    return written


def seed_history_if_needed(r: redis.Redis, symbols: Sequence[str]) -> Dict[str, int]:
    try:
        return seed_history(r=r, symbols=symbols, force=False)
    except Exception as e:
        print(f"[HISTORY] seed failed: {e!r}")
        return {"error": 1}

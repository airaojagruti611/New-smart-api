"""
Seed md:candles:* from Angel One historical API so Indicator Signals
(Supertrend / EMA / HTF / pivots) can compute on a cold midday start.
"""

from __future__ import annotations

import datetime as dt
import os
import time
from typing import Dict, List, Optional, Sequence, Set, Tuple

import redis

from .angel_auth import login
from .angel_rest import api_error_text, api_failed, candle_rows, fetch_candle_data
from .candle_builder import (
    in_session,
    is_degenerate_daily,
    resample_candles,
    session_close_bar_ts_ms,
    session_completed,
    session_date_ist,
)
from .candle_io import existing_ts_set, read_last_candles_multi, read_symbol_candles, stream_symbol_ts
from .candle_types import Candle
from .candles_store import CandlesStore
from .config import load_symbols
from .history_moneycontrol import fetch_moneycontrol_nse_candles
from .history_yahoo import fetch_yahoo_nse_candles
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

# Remote fetch only 1d + 1m. 5m/10m/30m are resampled from 1m so we
# do not hammer Yahoo / Angel with extra interval calls.
FETCH_INTERVALS: List[Tuple[str, str, str, int, int, int, bool]] = [
    ("ONE_DAY", "1d", STREAM_1D, OUT_MAXLEN_1D, 220, 2, True),
    ("ONE_MINUTE", "1m", STREAM_1M, OUT_MAXLEN_1M, 3, 26, False),
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
    if s.isdigit() or (s.replace(".", "", 1).isdigit() and s.count(".") < 2):
        try:
            raw = float(s)
            if raw > 1e12:
                raw = raw / 1000.0
            if raw > 1e9:
                return dt.datetime.fromtimestamp(raw, tz=IST)
        except ValueError:
            pass
    try:
        parsed = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        parsed = None
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%d-%m-%Y %H:%M:%S",
            "%d %b %Y %H:%M:%S",
        ):
            try:
                parsed = dt.datetime.strptime(s[:19] if len(s) >= 19 else s, fmt)
                break
            except ValueError:
                continue
        if parsed is None:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=IST)
    return parsed.astimezone(IST)


def _row_to_candle(row) -> Optional[Candle]:
    if isinstance(row, dict):
        when = _parse_row_ts(
            row.get("timestamp") or row.get("time") or row.get("datetime") or row.get(0)
        )
        try:
            o = float(row.get("open") if row.get("open") is not None else row.get("o"))
            h = float(row.get("high") if row.get("high") is not None else row.get("h"))
            l = float(row.get("low") if row.get("low") is not None else row.get("l"))
            c = float(row.get("close") if row.get("close") is not None else row.get("c"))
            v = float(row.get("volume") or row.get("v") or 0)
        except (TypeError, ValueError):
            return None
        if when is None:
            return None
        return Candle(ts_ms=int(when.timestamp() * 1000), o=o, h=h, l=l, c=c, v=v)
    if not isinstance(row, (list, tuple)) or len(row) < 5:
        return None
    when = _parse_row_ts(row[0])
    if when is None:
        return None
    try:
        o, h, l, c = float(row[1]), float(row[2]), float(row[3]), float(row[4])
        v = float(row[5] or 0) if len(row) > 5 else 0.0
    except (TypeError, ValueError):
        return None
    return Candle(ts_ms=int(when.timestamp() * 1000), o=o, h=h, l=l, c=c, v=v)


def normalize_bar_ts(candle: Candle, tf: str, source: str = "") -> Candle:
    """Stamp a history bar with the pipeline convention (ts_ms = bar END).

    - 1d: last ms of the IST session (15:29:59.999) for every source, so Angel
      (00:00 / 09:15 start stamps), Yahoo/Moneycontrol and the live builder agree.
    - intraday from Angel getCandleData: rows are stamped at bar START
      (11:15:00 for 11:15-11:16) -> start + tf - 1ms (11:15:59.999), the same as
      live / Yahoo / Moneycontrol bars.
    """
    if tf == "1d":
        ts = session_close_bar_ts_ms(session_date_ist(candle.ts_ms))
    elif source == "angel":
        minutes = _TF_MINUTES.get(tf, 0)
        if minutes <= 0:
            return candle
        ts = candle.ts_ms + minutes * 60_000 - 1
    else:
        return candle
    if ts == candle.ts_ms:
        return candle
    return Candle(ts_ms=ts, o=candle.o, h=candle.h, l=candle.l, c=candle.c, v=candle.v)


_TF_MINUTES = {"1m": 1, "5m": 5, "10m": 10, "15m": 15, "30m": 30}
_INTERVAL_TF = {"ONE_DAY": "1d", "ONE_MINUTE": "1m", "FIVE_MINUTE": "5m", "TEN_MINUTE": "10m",
                "FIFTEEN_MINUTE": "15m", "THIRTY_MINUTE": "30m"}


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


def _ts_index(r: redis.Redis, stream: str, cache: Dict[str, Dict[str, Set[int]]]) -> Dict[str, Set[int]]:
    """Per-stream symbol -> bar ts set, built once per seed run (full stream pass)."""
    if stream not in cache:
        cache[stream] = stream_symbol_ts(r, stream)
    return cache[stream]


def _needs_seed(
    r: redis.Redis,
    symbols: Sequence[str],
    cache: Optional[Dict[str, Dict[str, Set[int]]]] = None,
) -> bool:
    cache = {} if cache is None else cache
    checks = [
        (STREAM_1D, 2),
        (STREAM_1M, 26),
        (STREAM_5M, 8),
        (STREAM_10M, 8),
        (STREAM_30M, 8),
    ]
    for stream, need in checks:
        idx = _ts_index(r, stream, cache)
        if any(len(idx.get(s.upper(), ())) < need for s in symbols):
            return True
    return False


def pick_pivot_source(bars: Sequence[Candle], now_ms: int) -> Optional[Candle]:
    """Latest daily bar of a COMPLETED session that is not degenerate (H > L)."""
    for c in sorted(bars, key=lambda b: b.ts_ms, reverse=True):
        if is_degenerate_daily(c):
            continue
        if not session_completed(session_date_ist(c.ts_ms), now_ms):
            continue
        return c
    return None


def seed_pivots_from_daily(r: redis.Redis, symbols: Sequence[str], store: Optional[CandlesStore] = None) -> int:
    """Write today's prev-day pivots from the latest closed 1d bar per symbol."""
    store = store or CandlesStore()
    written = 0
    last_bars = read_last_candles_multi(r, STREAM_1D, list(symbols), 5)
    for sym in symbols:
        bars = last_bars.get(sym.upper(), [])
        src = pick_pivot_source(bars, int(time.time() * 1000))
        if src is None:
            continue
        date_str = session_date_ist(src.ts_ms).isoformat()
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


def _write_candles(
    r: redis.Redis,
    store: CandlesStore,
    stream: str,
    maxlen: int,
    sym: str,
    tf: str,
    candles: Sequence[Candle],
    skip_today: bool,
    existing: Optional[Set[int]] = None,
) -> int:
    """`existing` (from _ts_index) is updated in place with written bars."""
    now_ms = int(time.time() * 1000)
    if existing is None:
        existing = existing_ts_set(r, stream, sym, scan=20000)
    n = 0
    skip_d = 0
    skip_fut = 0
    for candle in candles:
        if candle is None:
            continue
        # ts_ms is the bar END: a bar still in progress has end > now.
        if candle.ts_ms > now_ms:
            skip_fut += 1
            continue
        if tf != "1d" and not in_session(candle.ts_ms):
            continue
        if skip_today:
            # a daily bar is final only once its session has closed
            if not session_completed(session_date_ist(candle.ts_ms), now_ms):
                skip_d += 1
                continue
        if candle.ts_ms in existing:
            continue
        extra_date = ""
        if tf == "1d":
            if is_degenerate_daily(candle):
                continue
            extra_date = session_date_ist(candle.ts_ms).isoformat()
        store.write_candle(stream, maxlen, sym, tf, candle, date=extra_date)
        existing.add(candle.ts_ms)
        n += 1
    if skip_d or skip_fut:
        print(f"[HISTORY] {sym} {tf}: skipped_today={skip_d} skipped_future={skip_fut}")
    return n


def _angel_candles(
    auth_token: str,
    info: dict,
    interval: str,
    fromdate: str,
    todate: str,
) -> Tuple[List[Candle], str]:
    body = fetch_candle_data(
        auth_token,
        exchange=info["exchange"],
        symboltoken=info["token"],
        interval=interval,
        fromdate=fromdate,
        todate=todate,
    )
    time.sleep(SLEEP_SEC)
    if api_failed(body):
        return [], api_error_text(body)
    rows = candle_rows(body)
    if not rows:
        return [], f"empty data ({api_error_text(body)})"
    tf = _INTERVAL_TF.get(interval, "")
    ok = [normalize_bar_ts(c, tf, source="angel") for c in (_row_to_candle(row) for row in rows) if c is not None]
    if not ok:
        sample = rows[0] if rows else None
        return [], f"unparsed rows={len(rows)} sample={repr(sample)[:200]}"
    return ok, ""


def _fallback_candles(sym: str, tf: str) -> Tuple[List[Candle], str]:
    if tf not in ("1d", "1m", "5m", "30m"):
        return [], f"no public mapping for {tf}"
    try:
        bars = fetch_moneycontrol_nse_candles(sym, tf)
        if bars:
            time.sleep(0.25)
            return bars, "moneycontrol"
    except Exception as e:
        print(f"[HISTORY] Moneycontrol FAIL {sym} {tf}: {e!r}")
    try:
        bars = fetch_yahoo_nse_candles(sym, tf)
        if bars:
            time.sleep(0.8)
            return bars, "yahoo"
    except Exception as e:
        return [], f"yahoo {e!r}"
    return [], "public empty"


def _topup_daily(
    r: redis.Redis,
    symbols: Sequence[str],
    cache: Dict[str, Dict[str, Set[int]]],
) -> int:
    """
    Add missing completed daily bars from the public sources. The live
    publisher only writes a day's bar if it is still running at the close
    (+ grace), and the full seed only runs when a stream is short, so a day
    missed by stopping at 15:30 would otherwise stay missing and pivots would
    come from an older session. One request per symbol that is behind; dates
    that already have a bar are left alone.
    """
    now = int(time.time() * 1000)
    last_day = _now_ist().date()
    while not (last_day.weekday() < 5 and session_completed(last_day, now)):
        last_day -= dt.timedelta(days=1)
    idx = _ts_index(r, STREAM_1D, cache)
    store = CandlesStore()
    added = 0
    for sym in symbols:
        existing = idx.setdefault(sym.upper(), set())
        days = {session_date_ist(t) for t in existing}
        if days and max(days) >= last_day:
            continue  # up to date (holidays just fetch and add nothing)
        try:
            candles, src = _fallback_candles(sym, "1d")
        except Exception as e:
            print(f"[HISTORY] daily top-up {sym} failed: {e!r}")
            continue
        if not candles:
            print(f"[HISTORY] daily top-up {sym}: no data ({src})")
            continue
        candles = [normalize_bar_ts(c, "1d", source=src) for c in candles]
        candles = [c for c in candles if session_date_ist(c.ts_ms) not in days]
        n = _write_candles(r, store, STREAM_1D, OUT_MAXLEN_1D, sym, "1d", candles,
                           skip_today=True, existing=existing)
        if n:
            print(f"[HISTORY] daily top-up {sym}: added {n} bars via {src}")
        added += n
    return added


def _seed_htf_from_1m(
    r: redis.Redis,
    store: CandlesStore,
    symbols: Sequence[str],
    written: Dict[str, int],
    cache: Dict[str, Dict[str, Set[int]]],
) -> None:
    """Fill 5m/10m/30m from seeded 1m so Supertrend MTF has ATR history."""

    targets = (
        (5, STREAM_5M, OUT_MAXLEN_5M, "5m"),
        (10, STREAM_10M, OUT_MAXLEN_10M, "10m"),
        (30, STREAM_30M, OUT_MAXLEN_30M, "30m"),
    )
    all_1m = read_symbol_candles(r, STREAM_1M, set(symbols), limit=4000)
    for sym in symbols:
        bars_1m = all_1m.get(sym.upper(), [])
        if len(bars_1m) < 8:
            print(f"[HISTORY] resample skip {sym}: only {len(bars_1m)} 1m bars")
            continue
        for minutes, stream, maxlen, tf in targets:
            existing = _ts_index(r, stream, cache).setdefault(sym.upper(), set())
            have = len(existing)
            ht = resample_candles(bars_1m, minutes)
            n = _write_candles(r, store, stream, maxlen, sym, tf, ht, skip_today=False, existing=existing)
            written[f"{sym}:{tf}:resample"] = n
            print(
                f"[HISTORY] resampled {n} {tf} bars for {sym} "
                f"(from {len(bars_1m)} 1m, already={have})"
            )


def seed_history(
    r: Optional[redis.Redis] = None,
    symbols: Optional[Sequence[str]] = None,
    force: bool = False,
) -> Dict[str, int]:
    """
    Fetch candle history into Redis streams. Tries Angel first; on AG8004 /
    empty / parse failure falls back to Yahoo NSE so Indicator Signals can
    warm Supertrend, EMA, HTF and pivots on a midday start.
    """
    symbols = [s.upper() for s in (symbols or load_symbols())]
    if not symbols:
        print("[HISTORY] no symbols")
        return {}

    if r is None:
        r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)

    cache: Dict[str, Dict[str, Set[int]]] = {}
    if not force and not _needs_seed(r, symbols, cache):
        added = _topup_daily(r, symbols, cache)
        n = seed_pivots_from_daily(r, symbols)
        print(f"[HISTORY] skip full fetch: candles already present; daily_topup={added} pivots_written={n}")
        return {"skipped": 1, "daily_topup": added, "pivots": n}

    auth_token = None
    tokens: Dict[str, dict] = {}
    try:
        print(f"[HISTORY] Angel login + ScripMaster for {len(symbols)} symbols")
        _obj, auth_token, _feed = login(retries=1, delay_sec=2.0)
        df = load_scripmaster()
        tokens = resolve_eq_tokens(df, list(symbols))
    except Exception as e:
        print(f"[HISTORY] Angel login skipped: {e!r}; Yahoo NSE fallback for all TFs")
        auth_token = None

    store = CandlesStore()
    written: Dict[str, int] = {}
    angel_usable = auth_token is not None

    idx_1d = _ts_index(r, STREAM_1D, cache)
    dailies_ready = all(len(idx_1d.get(s, ())) >= 2 for s in symbols)

    for interval, tf, stream, maxlen, lookback_days, min_bars, skip_today in FETCH_INTERVALS:
        fromdate, todate = _range_for(interval, lookback_days)
        for sym in symbols:
            existing = _ts_index(r, stream, cache).setdefault(sym, set())
            have = len(existing)
            if not force and dailies_ready and have >= min_bars:
                print(f"[HISTORY] skip {sym} {tf}: already {have} bars")
                continue

            candles: List[Candle] = []
            source = ""
            if angel_usable:
                info = tokens.get(sym)
                if not info:
                    print(f"[HISTORY] SKIP no_eq_token symbol={sym}")
                else:
                    candles, err = _angel_candles(auth_token, info, interval, fromdate, todate)
                    if candles:
                        source = "angel"
                    else:
                        print(f"[HISTORY] Angel FAIL {sym} {tf}: {err}")
                        if "AG8004" in err or "Invalid API Key" in err:
                            angel_usable = False
                            print("[HISTORY] disabling further Angel candle calls this run")

            if not candles:
                candles, err = _fallback_candles(sym, tf)
                if candles:
                    source = err
                elif tf == "10m":
                    written[f"{sym}:{tf}"] = 0
                    continue
                else:
                    print(f"[HISTORY] public FAIL {sym} {tf}: {err}")
                    written[f"{sym}:{tf}"] = 0
                    continue

            if source != "angel":  # Angel rows are normalized in _angel_candles
                candles = [normalize_bar_ts(c, tf, source=source) for c in candles]
            n = _write_candles(r, store, stream, maxlen, sym, tf, candles, skip_today, existing=existing)
            written[f"{sym}:{tf}"] = n
            print(f"[HISTORY] wrote {n} {tf} bars for {sym} via {source} (had={have})")

    _seed_htf_from_1m(r, store, symbols, written, cache)
    written["pivots"] = seed_pivots_from_daily(r, symbols, store=store)
    return written


def seed_history_if_needed(r: redis.Redis, symbols: Sequence[str]) -> Dict[str, int]:
    try:
        return seed_history(r=r, symbols=symbols, force=False)
    except Exception as e:
        print(f"[HISTORY] seed failed: {e!r}")
        return {"error": 1}

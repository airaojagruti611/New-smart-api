"""Parse Redis candle stream payloads and read last-N bars per symbol."""

from __future__ import annotations

from typing import Deque, Dict, List, Optional, Set, Tuple

import redis

from .candle_types import Candle


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


def parse_candle_fields(fields: dict) -> Optional[Tuple[str, Candle]]:
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


def sort_unique_candles(candles: List[Candle], limit: int = 0) -> List[Candle]:
    """Oldest-first, last write wins on duplicate ts_ms. Optionally keep last `limit`."""
    by_ts: Dict[int, Candle] = {}
    for c in candles:
        by_ts[int(c.ts_ms)] = c
    ordered = [by_ts[k] for k in sorted(by_ts)]
    if limit and limit > 0:
        return ordered[-limit:]
    return ordered


def upsert_candle_window(window: Deque[Candle], candle: Candle) -> None:
    """Keep an in-memory rolling window in timestamp order (seed can arrive after live)."""
    if not window:
        window.append(candle)
        return
    last = window[-1]
    if candle.ts_ms == last.ts_ms:
        window[-1] = candle
        return
    if candle.ts_ms > last.ts_ms:
        window.append(candle)
        return
    by_ts = {c.ts_ms: c for c in window}
    by_ts[candle.ts_ms] = candle
    ordered = [by_ts[k] for k in sorted(by_ts)]
    maxlen = window.maxlen
    if maxlen:
        ordered = ordered[-maxlen:]
    window.clear()
    window.extend(ordered)


def read_last_candles(
    r: redis.Redis,
    stream: str,
    symbol: str,
    limit: int,
    scan: int = 0,
) -> List[Candle]:
    """
    Return oldest-first unique candles for `symbol` (by ts_ms).

    scan > 0: look only at the newest `scan` stream entries (cheap, may find
    fewer than `limit`). scan == 0: walk back until `limit` candles are found
    or the stream ends; the stream interleaves every symbol, so a fixed window
    holds only a few bars per symbol when many symbols are collected.
    """
    if limit <= 0:
        return []
    if scan <= 0:
        return read_last_candles_multi(r, stream, [symbol], limit).get(symbol.upper(), [])
    resp = r.xrevrange(stream, max="+", min="-", count=scan)
    found: List[Candle] = []
    want = symbol.upper()
    for _msg_id, fields in resp:
        parsed = parse_candle_fields(fields)
        if parsed is None:
            continue
        sym, candle = parsed
        if sym != want:
            continue
        found.append(candle)
    return sort_unique_candles(found, limit=limit)


def _iter_stream(r: redis.Redis, stream: str, page: int = 10000):
    """Yield every (id, fields) oldest-first, paging with exclusive start ids."""
    start = "-"
    while True:
        resp = r.xrange(stream, min=start, max="+", count=page)
        if not resp:
            return
        yield from resp
        if len(resp) < page:
            return
        start = "(" + resp[-1][0]


def stream_symbol_ts(r: redis.Redis, stream: str) -> Dict[str, Set[int]]:
    """Full pass: symbol -> set of bar ts_ms.

    All symbols share one stream, so a tail scan (existing_ts_set) misses every
    symbol written earlier in a seed loop and reports it as empty.
    """
    out: Dict[str, Set[int]] = {}
    for _msg_id, fields in _iter_stream(r, stream):
        sym = str(fields.get("symbol") or "").strip().upper()
        ts = _safe_int(fields.get("ts_ms"))
        if sym and ts is not None:
            out.setdefault(sym, set()).add(ts)
    return out


def read_symbol_candles(
    r: redis.Redis,
    stream: str,
    symbols: Set[str],
    limit: int = 0,
) -> Dict[str, List[Candle]]:
    """Full pass: symbol -> oldest-first unique candles (last `limit` if > 0)."""
    found: Dict[str, List[Candle]] = {s.upper(): [] for s in symbols}
    for _msg_id, fields in _iter_stream(r, stream):
        parsed = parse_candle_fields(fields)
        if parsed is None or parsed[0] not in found:
            continue
        found[parsed[0]].append(parsed[1])
    return {s: sort_unique_candles(c, limit=limit) for s, c in found.items()}



def read_last_candles_multi(
    r: redis.Redis,
    stream: str,
    symbols: List[str],
    limit: int,
    chunk: int = 20000,
    max_scan: int = 0,
) -> Dict[str, List[Candle]]:
    """
    Newest `limit` candles per symbol (oldest-first), in one backwards walk
    over `stream` shared by all symbols. Stops when every symbol has `limit`
    candles, the stream ends, or `max_scan` entries were read (0 = no cap).
    """
    want = {s.upper() for s in symbols}
    found: Dict[str, Dict[int, Candle]] = {s: {} for s in want}
    if limit <= 0 or not want:
        return {s: [] for s in want}
    need = set(want)
    max_id = "+"
    scanned = 0
    while need:
        rows = r.xrevrange(stream, max=max_id, min="-", count=chunk)
        if not rows:
            break
        for _msg_id, fields in rows:
            parsed = parse_candle_fields(fields)
            if parsed is None:
                continue
            sym, candle = parsed
            if sym not in need:
                continue
            bucket = found[sym]
            bucket[int(candle.ts_ms)] = bucket.get(int(candle.ts_ms), candle)  # newest write wins
            if len(bucket) >= limit:
                need.discard(sym)
        scanned += len(rows)
        if len(rows) < chunk or (max_scan and scanned >= max_scan):
            break
        max_id = f"({rows[-1][0]}"
    return {s: sort_unique_candles(list(found[s].values()), limit=limit) for s in want}


def existing_ts_set(
    r: redis.Redis,
    stream: str,
    symbol: str,
    scan: int = 8000,
) -> Set[int]:
    candles = read_last_candles(r, stream, symbol, limit=scan, scan=scan)
    return {int(c.ts_ms) for c in candles}


def count_symbol_candles(
    r: redis.Redis,
    stream: str,
    symbols: List[str],
    per_symbol_limit: int = 8,
) -> Dict[str, int]:
    out = {s.upper(): 0 for s in symbols}
    if not symbols:
        return out
    scan = max(2000, per_symbol_limit * max(1, len(symbols)) * 4)
    resp = r.xrevrange(stream, max="+", min="-", count=scan)
    remaining = set(out)
    for _msg_id, fields in resp:
        parsed = parse_candle_fields(fields)
        if parsed is None:
            continue
        sym, _candle = parsed
        if sym not in remaining:
            continue
        out[sym] += 1
        if out[sym] >= per_symbol_limit:
            remaining.discard(sym)
            if not remaining:
                break
    return out

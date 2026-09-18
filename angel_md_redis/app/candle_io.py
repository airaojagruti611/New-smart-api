"""Parse Redis candle stream payloads and read last-N bars per symbol."""

from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple

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


def read_last_candles(
    r: redis.Redis,
    stream: str,
    symbol: str,
    limit: int,
    scan: int = 0,
) -> List[Candle]:
    """Newest-first scan of `stream`, return oldest-first candles for `symbol`."""
    if limit <= 0:
        return []
    count = scan if scan > 0 else max(2000, limit * 8)
    resp = r.xrevrange(stream, max="+", min="-", count=count)
    out: List[Candle] = []
    want = symbol.upper()
    for _msg_id, fields in resp:
        parsed = parse_candle_fields(fields)
        if parsed is None:
            continue
        sym, candle = parsed
        if sym != want:
            continue
        out.append(candle)
        if len(out) >= limit:
            break
    return list(reversed(out))


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

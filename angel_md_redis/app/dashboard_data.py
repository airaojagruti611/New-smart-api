"""Live Redis snapshot helpers for the Streamlit dashboard."""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

import redis

from app.config import REDIS_URL, load_symbols


def connect() -> redis.Redis:
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


def load_json(r: redis.Redis, key: str) -> Optional[Any]:
    raw = r.get(key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def pick_stream(r: redis.Redis, stream: str, symbol: str, n: int = 120) -> dict:
    want = symbol.upper()
    try:
        rows = r.xrevrange(stream, count=n)
    except Exception:
        return {}
    for _mid, fields in rows:
        for k in ("symbol", "underlying", "key"):
            v = str(fields.get(k) or "").upper()
            if v == want or v.startswith(want + ":"):
                return dict(fields)
    return {}


def last_candles(r: redis.Redis, symbol: str, stream: str = "md:candles:1m", n: int = 80) -> List[dict]:
    want = symbol.upper()
    out: List[dict] = []
    try:
        rows = r.xrevrange(stream, count=max(n * 8, 200))
    except Exception:
        return []
    for _mid, fields in reversed(rows):
        if str(fields.get("symbol") or "").upper() != want:
            continue
        out.append(dict(fields))
        if len(out) >= n:
            break
    return out


def fnum(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def age_sec(doc: Any) -> Optional[float]:
    if not isinstance(doc, dict):
        return None
    ts = doc.get("ts_ms") or doc.get("ts_recv") or doc.get("bar_ts_ms")
    try:
        return max(0.0, time.time() - int(ts) / 1000.0)
    except Exception:
        return None


def collect_symbol(r: redis.Redis, sym: str) -> Dict[str, Any]:
    s = sym.upper()
    vol_blob = load_json(r, "md:volume:latest") or {}
    vol = vol_blob.get(s) if isinstance(vol_blob, dict) else None
    if not isinstance(vol, dict):
        vol = {}
    expiry = r.hgetall("md:active_expiry") or {}
    strike = load_json(r, f"md:strike:select:latest:{s}") or {}
    greeks_ce = load_json(r, f"md:greeks:phase:underlying:latest:{s}:CE") or {}
    tsym = str(strike.get("tradingsymbol") or greeks_ce.get("tradingsymbol") or "")
    liq = load_json(r, f"md:liquidity:score:latest:{tsym}") if tsym else None
    gchg = load_json(r, f"md:greeks_change:latest:{tsym}") if tsym else None

    return {
        "tick": pick_stream(r, "md:ticks:eq", s, 150),
        "c1m": pick_stream(r, "md:candles:1m", s, 40),
        "c5m": pick_stream(r, "md:candles:5m", s, 20),
        "c10m": pick_stream(r, "md:candles:10m", s, 20),
        "c30m": pick_stream(r, "md:candles:30m", s, 20),
        "expiry": expiry.get(s, ""),
        "st": load_json(r, f"md:supertrend:bias:latest:{s}") or {},
        "ema": load_json(r, f"md:ema:cross:latest:{s}") or {},
        "htf": load_json(r, f"md:htf:trend:latest:{s}") or {},
        "pivots": load_json(r, f"md:pivots:prevday:{s}") or {},
        "level": load_json(r, f"md:level:entry:latest:{s}") or {},
        "momentum": load_json(r, f"md:momentum:confirm:latest:{s}") or {},
        "volume": vol,
        "regime": load_json(r, "md:regime:latest") or {},
        "bidask": load_json(r, f"md:bidask:latest:{s}") or {},
        "smartmoney": load_json(r, f"md:smartmoney:latest:{s}") or {},
        "orderflow": load_json(r, f"md:orderflow:latest:{s}") or {},
        "imbalance": load_json(r, f"md:imbalance:latest:{s}") or {},
        "stockflow": load_json(r, f"md:stockflow:latest:{s}") or {},
        "composite": load_json(r, f"md:composite:latest:{s}") or {},
        "oi_und": load_json(r, f"md:oi:underlying:latest:{s}") or {},
        "greeks_ce": greeks_ce,
        "greeks_pe": load_json(r, f"md:greeks:phase:underlying:latest:{s}:PE") or {},
        "strikeflow": load_json(r, f"md:strikeflow:latest:{s}") or {},
        "expected": load_json(r, f"md:expected_move:latest:{s}") or {},
        "entry": load_json(r, f"md:entry:trigger:latest:{s}") or {},
        "strike": strike,
        "capital": load_json(r, f"md:capital:alloc:latest:{s}") or {},
        "liquidity": liq or {},
        "greeks_change": gchg or {},
        "candles_1m": last_candles(r, s, "md:candles:1m", 60),
    }


def redis_health(r: redis.Redis) -> Dict[str, Any]:
    try:
        r.ping()
        return {
            "ok": True,
            "eq": int(r.xlen("md:ticks:eq") or 0),
            "opt": int(r.xlen("md:ticks:opt") or 0),
            "c1m": int(r.xlen("md:candles:1m") or 0),
            "greeks": int(r.xlen("md:greeks:snap") or 0),
            "keys": int(r.dbsize() or 0),
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "eq": 0, "opt": 0, "c1m": 0, "greeks": 0, "keys": 0}


def universe(r: redis.Redis) -> List[str]:
    try:
        return [s.upper() for s in load_symbols()]
    except Exception:
        return []

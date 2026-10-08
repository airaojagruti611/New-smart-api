"""Live Redis snapshot helpers for the Streamlit dashboard."""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

import redis

from app.config import REDIS_URL, load_symbols

# Everything this module reads, for run_cloud_mirror.py (keeps a hosted copy of
# the dashboard's inputs in a small cloud Redis). Update both together.
MIRROR_KEY_PATTERNS = [
    "md:supertrend:bias:latest:*",
    "md:ema:cross:latest:*",
    "md:htf:trend:latest:*",
    "md:pivots:prevday:*",
    "md:level:entry:latest:*",
    "md:momentum:confirm:latest:*",
    "md:regime:latest",
    "md:volume:latest",
    "md:bidask:latest:*",
    "md:smartmoney:latest:*",
    "md:orderflow:latest:*",
    "md:imbalance:latest:*",
    "md:stockflow:latest:*",
    "md:composite:latest:*",
    "md:oi:underlying:latest:*",
    "md:greeks:phase:underlying:latest:*",
    "md:strikeflow:latest:*",
    "md:expected_move:latest:*",
    "md:entry:trigger:latest:*",
    "md:capital:alloc:latest:*",
    "md:strike:select:latest:*",
    "md:liquidity:score:latest:*",
    "md:greeks_change:latest:*",
]
MIRROR_HASHES = ["md:active_expiry"]
MIRROR_STREAMS = ["md:ticks:eq", "md:candles:1m", "md:candles:5m", "md:candles:10m", "md:candles:30m"]
# The mirror stores the source's real stream sizes here (the copy is trimmed).
MIRROR_HEALTH_KEY = "md:mirror:health"


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
    """Last `n` entries for `symbol`, oldest-first (walks back past other symbols' rows)."""
    want = symbol.upper()
    newest_first: List[dict] = []
    max_id, scanned = "+", 0
    while len(newest_first) < n and scanned < 200000:
        try:
            rows = r.xrevrange(stream, max=max_id, count=5000)
        except Exception:
            break
        if not rows:
            break
        for _mid, fields in rows:
            if str(fields.get("symbol") or "").upper() == want:
                newest_first.append(dict(fields))
                if len(newest_first) >= n:
                    break
        scanned += len(rows)
        max_id = f"({rows[-1][0]}"
    return list(reversed(newest_first))


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


# Per-symbol snapshot keys: output field -> key template.
_SYMBOL_KEYS = {
    "st": "md:supertrend:bias:latest:{s}",
    "ema": "md:ema:cross:latest:{s}",
    "htf": "md:htf:trend:latest:{s}",
    "pivots": "md:pivots:prevday:{s}",
    "level": "md:level:entry:latest:{s}",
    "momentum": "md:momentum:confirm:latest:{s}",
    "bidask": "md:bidask:latest:{s}",
    "smartmoney": "md:smartmoney:latest:{s}",
    "orderflow": "md:orderflow:latest:{s}",
    "imbalance": "md:imbalance:latest:{s}",
    "stockflow": "md:stockflow:latest:{s}",
    "composite": "md:composite:latest:{s}",
    "oi_und": "md:oi:underlying:latest:{s}",
    "greeks_ce": "md:greeks:phase:underlying:latest:{s}:CE",
    "greeks_pe": "md:greeks:phase:underlying:latest:{s}:PE",
    "strikeflow": "md:strikeflow:latest:{s}",
    "expected": "md:expected_move:latest:{s}",
    "entry": "md:entry:trigger:latest:{s}",
    "strike": "md:strike:select:latest:{s}",
    "capital": "md:capital:alloc:latest:{s}",
    "greeks_change": "md:greeks_change:latest:{s}",
}


def _json_or_empty(raw: Optional[str]) -> Any:
    if not raw:
        return {}
    try:
        return json.loads(raw) or {}
    except Exception:
        return {}


def latest_per_symbol(r: redis.Redis, stream: str, symbols: List[str], max_scan: int = 30000) -> Dict[str, dict]:
    """Newest entry per symbol in one backwards walk (stops when all are found or at max_scan)."""
    want = {s.upper() for s in symbols}
    out: Dict[str, dict] = {}
    max_id, scanned = "+", 0
    while want - set(out) and scanned < max_scan:
        try:
            rows = r.xrevrange(stream, max=max_id, count=5000)
        except Exception:
            break
        if not rows:
            break
        for _mid, fields in rows:
            for k in ("symbol", "underlying", "key"):
                v = str(fields.get(k) or "").upper()
                base = v.split(":", 1)[0]
                if base in want and base not in out:
                    out[base] = dict(fields)
                    break
        scanned += len(rows)
        max_id = f"({rows[-1][0]}"
    return out


def collect_all(r: redis.Redis, symbols: List[str], detail: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
    """
    Snapshot for many symbols with few round trips: one pipeline for every
    latest key, one shared walk per stream. `detail` symbols also get the
    5m/10m/30m bars and the 1m candle history (the per-symbol views).
    """
    syms = [s.upper() for s in symbols]
    detail = [s.upper() for s in (detail or [])]

    pipe = r.pipeline()
    pipe.get("md:volume:latest")
    pipe.hgetall("md:active_expiry")
    pipe.get("md:regime:latest")
    for s in syms:
        for tmpl in _SYMBOL_KEYS.values():
            pipe.get(tmpl.format(s=s))
    res = pipe.execute()
    vol_blob = _json_or_empty(res[0])
    expiry = res[1] or {}
    regime = _json_or_empty(res[2])

    out: Dict[str, Dict[str, Any]] = {}
    fields = list(_SYMBOL_KEYS)
    i = 3
    for s in syms:
        d: Dict[str, Any] = {}
        for f in fields:
            d[f] = _json_or_empty(res[i])
            i += 1
        vol = vol_blob.get(s) if isinstance(vol_blob, dict) else None
        d["volume"] = vol if isinstance(vol, dict) else {}
        d["regime"] = regime
        d["expiry"] = expiry.get(s, "")
        out[s] = d

    # Option-level keys depend on the selected contract.
    pipe = r.pipeline()
    lookups = []
    for s in syms:
        d = out[s]
        tsym = str(d["strike"].get("tradingsymbol") or d["greeks_ce"].get("tradingsymbol") or "")
        if tsym:
            pipe.get(f"md:liquidity:score:latest:{tsym}")
            lookups.append((s, "liquidity"))
            if not d["greeks_change"]:
                pipe.get(f"md:greeks_change:latest:{tsym}")
                lookups.append((s, "greeks_change"))
    for (s, f), raw in zip(lookups, pipe.execute() if lookups else []):
        out[s][f] = _json_or_empty(raw)
    for s in syms:
        out[s].setdefault("liquidity", {})

    ticks = latest_per_symbol(r, "md:ticks:eq", syms)
    c1m = latest_per_symbol(r, "md:candles:1m", syms, max_scan=20000)
    for s in syms:
        out[s]["tick"] = ticks.get(s, {})
        out[s]["c1m"] = c1m.get(s, {})
        out[s]["c5m"] = out[s]["c10m"] = out[s]["c30m"] = {}
        out[s]["candles_1m"] = []
    if detail:
        for stream, f in (("md:candles:5m", "c5m"), ("md:candles:10m", "c10m"), ("md:candles:30m", "c30m")):
            got = latest_per_symbol(r, stream, detail, max_scan=20000)
            for s in detail:
                if s in out:
                    out[s][f] = got.get(s, {})
        for s in detail:
            if s in out:
                out[s]["candles_1m"] = last_candles(r, s, "md:candles:1m", 60)
    return out


def collect_symbol(r: redis.Redis, sym: str) -> Dict[str, Any]:
    return collect_all(r, [sym], detail=[sym])[sym.upper()]


def redis_health(r: redis.Redis) -> Dict[str, Any]:
    try:
        r.ping()
        mirrored = load_json(r, MIRROR_HEALTH_KEY)
        if isinstance(mirrored, dict):
            # Reading a cloud copy: report the PC pipeline's real counts.
            return {"ok": True, **{k: mirrored.get(k, 0) for k in ("eq", "opt", "c1m", "greeks", "keys")},
                    "mirror_age_s": age_sec(mirrored)}
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

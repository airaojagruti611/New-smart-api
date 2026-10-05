"""Live Redis snapshot helpers for the Streamlit dashboard.

Every function here is READ-ONLY (get / hgetall / hget / type / ttl / scan_iter /
xrevrange / xlen / zrevrange / smembers / lrange) and takes a redis client as its
first argument, so the whole module can be exercised with an in-memory fake.

Key / stream names use the same env vars and defaults as the writers.
"""

from __future__ import annotations

import fnmatch
import importlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

try:  # redis is only needed for the real client factory
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    from app.config import REDIS_URL, load_symbols
except Exception:  # pragma: no cover
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    def load_symbols() -> List[str]:  # type: ignore
        return []


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


# ─────────────────────────────────────────────────────────────────────
# Client factory (overridable for tests: set_client_factory() or env
# DASHBOARD_REDIS_FACTORY="package.module:callable")
# ─────────────────────────────────────────────────────────────────────

_CLIENT_FACTORY: Optional[Callable[[], Any]] = None


def set_client_factory(fn: Optional[Callable[[], Any]]) -> None:
    global _CLIENT_FACTORY
    _CLIENT_FACTORY = fn


def connect():
    if _CLIENT_FACTORY is not None:
        return _CLIENT_FACTORY()
    spec = os.getenv("DASHBOARD_REDIS_FACTORY", "").strip()
    if spec:
        mod_name, _, attr = spec.partition(":")
        fn = getattr(importlib.import_module(mod_name), attr or "make_client")
        return fn()
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


# ─────────────────────────────────────────────────────────────────────
# Key / stream names (same env vars + defaults as the writers)
# ─────────────────────────────────────────────────────────────────────

K = {
    # ingestion
    "ticks_eq": _env("STREAM_EQ", "md:ticks:eq"),
    "ticks_opt": _env("STREAM_OPT", "md:ticks:opt"),
    "active_expiry": _env("ACTIVE_EXPIRY_HASH", "md:active_expiry"),
    "greeks_snap": "md:greeks:snap",
    "greeks_latest": "md:greeks:latest:",
    "features_opt": _env("STREAM_OPT_FEATURES", "md:features:opt"),
    "greeks_phase_stream": _env("STREAM_GREEKS_PHASE", "md:greeks:phase:signal"),
    "greeks_phase_latest": _env("GREEKS_PHASE_LATEST_PREFIX", "md:greeks:phase:latest:"),
    "greeks_phase_und": _env("GREEKS_PHASE_UNDERLYING_PREFIX", "md:greeks:phase:underlying:latest:"),
    # candles
    "c1m": "md:candles:1m", "c5m": "md:candles:5m", "c10m": "md:candles:10m",
    "c30m": "md:candles:30m", "c1d": "md:candles:1d",
    "pivots_stream": _env("STREAM_PIVOTS_PREVDAY", "md:pivots:prevday"),
    "pivots_prefix": _env("PIVOTS_PREVDAY_PREFIX", "md:pivots:prevday:"),
    # decision
    "probability_rank": _env("PROBABILITY_RANK_KEY", "md:probability:rank"),
    "ranking_rank": _env("RANKING_RANK_KEY", "md:ranking:rank"),
    "ranking_book": _env("RANKING_BOOK_KEY", "md:ranking:book"),
    "ranking_cycle_latest": _env("RANKING_CYCLE_LATEST_KEY", "md:ranking:cycle:latest"),
    "icare_origin": _env("ICARE_ORIGIN_PREFIX", "md:icare:origin:"),
    "account": _env("ACCOUNT_LATEST_KEY", "md:account:latest"),
    "volume_latest": _env("VOLUME_LATEST_KEY", "md:volume:latest"),
    "regime_latest": _env("REGIME_LATEST_KEY", "md:regime:latest"),
    # execution
    "exec": _env("STREAM_EXEC", "md:exec"),
    "exec_fill": _env("STREAM_EXEC_FILL", "md:exec:fill"),
    "exec_latest": _env("EXEC_LATEST_PREFIX", "md:exec:latest:"),
    "exec_state": _env("EXEC_STATE_PREFIX", "md:exec:state:"),
    "exec_active": _env("EXEC_ACTIVE_KEY", "md:exec:active"),
    "exec_missed": _env("EXEC_MISSED_KEY", "md:exec:missed"),
    "exec_exit_request": _env("STREAM_EXEC_EXIT_REQUEST", "md:exec:exit_request"),
    "exec_exit_fill": _env("STREAM_EXEC_EXIT_FILL", "md:exec:exit_fill"),
    "exec_exit_state": _env("EXEC_EXIT_STATE_PREFIX", "md:exec:exit_state:"),
    "exec_exit_active": "md:exec:exit:active",
    "kill_switch": _env("KILL_SWITCH_KEY", "md:control:kill_switch"),
    # TSL
    "tsl": _env("STREAM_TSL", "md:tsl"),
    "tsl_latest": _env("TSL_LATEST_PREFIX", "md:tsl:latest:"),
    "tsl_state": _env("TSL_STATE_PREFIX", "md:tsl:state:"),
    "tsl_reentry": _env("STREAM_TSL_REENTRY", "md:tsl:reentry"),
    "tsl_chain": _env("TSL_CHAIN_PREFIX", "md:tsl:chain:"),
    "tsl_chains": _env("TSL_CHAINS_SET", "md:tsl:chains"),
    "tsl_trades": _env("TSL_TRADES_SET", "md:tsl:trades"),
    "tsl_virtual": _env("TSL_VIRTUAL_SET", "md:tsl:virtual"),
    "tsl_block": _env("TSL_BLOCK_PREFIX", "md:tsl:block:"),
    "tsl_origin": _env("TSL_ORIGIN_PREFIX", "md:tsl:origin:"),
    # journal
    "position_open": _env("POSITION_OPEN_PREFIX", "md:position:open:"),
    "journal": _env("STREAM_JOURNAL", "md:journal"),
    "journal_stats": _env("JOURNAL_STATS_KEY", "md:journal:stats"),
    "journal_daily": _env("JOURNAL_DAILY_PREFIX", "md:journal:daily:"),
    "journal_closed": _env("JOURNAL_CLOSED_PREFIX", "md:journal:closed:"),
}


@dataclass(frozen=True)
class Layer:
    """One pipeline output. `latest` is a key prefix (or full key for keyed=SINGLE)."""

    id: str
    group: str
    name: str
    stream: Optional[str] = None
    latest: Optional[str] = None
    keyed: str = "SYM"          # SYM | TSYM | ANY (SYM or TSYM) | SYM_ISO | SYM_CP | SYMSIDE | SINGLE | BLOB | TID
    stale_sec: Optional[float] = 300.0   # None = no freshness check (state that is legitimately old)


def _L(*a, **kw) -> Layer:
    return Layer(*a, **kw)


LAYERS: List[Layer] = [
    # Ingestion
    _L("ticks_eq", "Ingestion", "EQ ticks", stream=K["ticks_eq"], stale_sec=30),
    _L("ticks_opt", "Ingestion", "Option ticks", stream=K["ticks_opt"], stale_sec=30),
    _L("greeks", "Ingestion", "Greeks snapshot", stream=K["greeks_snap"], latest=K["greeks_latest"], keyed="SYM_ISO", stale_sec=300),
    _L("features", "Ingestion", "Option features (joiner)", stream=K["features_opt"], stale_sec=60),
    _L("greeks_phase", "Options", "Greeks phase (contract)", stream=K["greeks_phase_stream"], latest=K["greeks_phase_latest"], keyed="TSYM", stale_sec=300),
    _L("greeks_phase_und", "Options", "Greeks phase (underlying ATM)", latest=K["greeks_phase_und"], keyed="SYM_CP", stale_sec=300),
    # Candles
    _L("c1m", "Candles", "Candles 1m", stream=K["c1m"], stale_sec=150),
    _L("c5m", "Candles", "Candles 5m", stream=K["c5m"], stale_sec=660),
    _L("c10m", "Candles", "Candles 10m", stream=K["c10m"], stale_sec=1260),
    _L("c30m", "Candles", "Candles 30m", stream=K["c30m"], stale_sec=3660),
    _L("c1d", "Candles", "Candles 1d", stream=K["c1d"], stale_sec=4 * 86400),
    _L("pivots", "Candles", "Prev-day pivots", stream=K["pivots_stream"], latest=K["pivots_prefix"], stale_sec=4 * 86400),
    # Module 1-3
    _L("ema", "Signals", "EMA 9/26 cross", stream=_env("STREAM_EMA_CROSS", "md:ema:cross"), latest=_env("EMA_CROSS_LATEST_PREFIX", "md:ema:cross:latest:")),
    _L("supertrend", "Signals", "Supertrend MTF bias", stream=_env("STREAM_SUPERTREND_BIAS", "md:supertrend:bias"), latest=_env("SUPERTREND_BIAS_LATEST_PREFIX", "md:supertrend:bias:latest:")),
    _L("htf", "Signals", "HTF trend", stream=_env("STREAM_HTF_TREND", "md:htf:trend"), latest=_env("HTF_TREND_LATEST_PREFIX", "md:htf:trend:latest:"), stale_sec=3600),
    _L("level", "Signals", "Level entry (pivot break)", stream=_env("STREAM_LEVEL_ENTRY", "md:level:entry"), latest=_env("LEVEL_ENTRY_LATEST_PREFIX", "md:level:entry:latest:")),
    _L("momentum", "Signals", "Momentum confirm", stream=_env("STREAM_MOMENTUM_CONFIRM", "md:momentum:confirm"), latest=_env("MOMENTUM_CONFIRM_LATEST_PREFIX", "md:momentum:confirm:latest:")),
    _L("indicator_score", "Signals", "Indicator score", latest=_env("INDICATOR_SCORE_LATEST_PREFIX", "md:indicator:score:latest:")),
    _L("volume", "Signals", "Volume analyzer", stream=_env("STREAM_VOLUME_SIGNAL", "md:volume:signal"), latest=K["volume_latest"], keyed="BLOB"),
    _L("regime", "Signals", "Market regime", stream=_env("STREAM_REGIME", "md:regime"), latest=K["regime_latest"], keyed="SINGLE"),
    _L("entry", "Signals", "Entry trigger", stream=_env("STREAM_ENTRY_TRIGGER", "md:entry:trigger"), latest=_env("ENTRY_TRIGGER_LATEST_PREFIX", "md:entry:trigger:latest:")),
    # Microstructure
    _L("bidask", "Microstructure", "Bid-ask", stream=_env("STREAM_BIDASK_SIGNAL", "md:bidask:signal"), latest=_env("BIDASK_LATEST_PREFIX", "md:bidask:latest:"), keyed="ANY", stale_sec=60),
    _L("imbalance", "Microstructure", "Bid-ask imbalance", stream=_env("STREAM_IMBALANCE_SIGNAL", "md:imbalance:signal"), latest=_env("IMBALANCE_LATEST_PREFIX", "md:imbalance:latest:"), keyed="ANY", stale_sec=60),
    _L("smartmoney", "Microstructure", "Smart money", stream=_env("STREAM_SMARTMONEY_SIGNAL", "md:smartmoney:signal"), latest=_env("SMARTMONEY_LATEST_PREFIX", "md:smartmoney:latest:"), keyed="ANY", stale_sec=120),
    _L("orderflow", "Microstructure", "Order flow", stream=_env("STREAM_ORDERFLOW_SIGNAL", "md:orderflow:signal"), latest=_env("ORDERFLOW_LATEST_PREFIX", "md:orderflow:latest:"), keyed="ANY", stale_sec=120),
    _L("strikeflow", "Microstructure", "Strike flow", stream=_env("STREAM_STRIKEFLOW_SIGNAL", "md:strikeflow:signal"), latest=_env("STRIKEFLOW_LATEST_PREFIX", "md:strikeflow:latest:"), keyed="ANY", stale_sec=120),
    _L("oi", "Microstructure", "OI (contract)", stream=_env("STREAM_OI_SIGNAL", "md:oi:signal"), latest=_env("OI_LATEST_PREFIX", "md:oi:latest:"), keyed="TSYM", stale_sec=300),
    _L("oi_und", "Microstructure", "OI (underlying)", stream=_env("STREAM_OI_UNDERLYING_SIGNAL", "md:oi:underlying:signal"), latest=_env("OI_UNDERLYING_LATEST_PREFIX", "md:oi:underlying:latest:"), stale_sec=300),
    _L("liquidity", "Microstructure", "Liquidity score", stream=_env("STREAM_LIQUIDITY_SCORE", "md:liquidity:score:signal"), latest=_env("LIQUIDITY_SCORE_LATEST_PREFIX", "md:liquidity:score:latest:"), keyed="TSYM", stale_sec=120),
    _L("optexit", "Microstructure", "Option liquidity exit", stream=_env("STREAM_OPTEXIT_SIGNAL", "md:optexit:signal"), latest=_env("OPTEXIT_LATEST_PREFIX", "md:optexit:latest:"), keyed="TSYM", stale_sec=120),
    _L("stockflow", "Microstructure", "Stock entry/exit", stream=_env("STREAM_STOCKFLOW_SIGNAL", "md:stockflow:signal"), latest=_env("STOCKFLOW_LATEST_PREFIX", "md:stockflow:latest:"), keyed="ANY", stale_sec=120),
    _L("composite", "Microstructure", "Composite score", stream=_env("STREAM_COMPOSITE_SIGNAL", "md:composite:signal"), latest=_env("COMPOSITE_LATEST_PREFIX", "md:composite:latest:"), keyed="ANY", stale_sec=120),
    # Volatility / strike
    _L("expected", "Strike", "Expected move", stream=_env("STREAM_EXPECTED_MOVE", "md:expected_move:signal"), latest=_env("EXPECTED_MOVE_LATEST_PREFIX", "md:expected_move:latest:")),
    _L("greeks_change", "Strike", "Greeks change", stream=_env("STREAM_GREEKS_CHANGE", "md:greeks_change:signal"), latest=_env("GREEKS_CHANGE_LATEST_PREFIX", "md:greeks_change:latest:"), keyed="ANY"),
    _L("strike", "Strike", "Strike select", stream=_env("STREAM_STRIKE_SELECT", "md:strike:select"), latest=_env("STRIKE_SELECT_LATEST_PREFIX", "md:strike:select:latest:")),
    _L("strike_intel", "Strike", "Strike intelligence", stream=_env("STREAM_STRIKE_INTEL", "md:strike:intel"), latest=_env("STRIKE_INTEL_LATEST_PREFIX", "md:strike:intel:latest:")),
    _L("capital", "Strike", "Capital allocation", stream=_env("STREAM_CAPITAL_ALLOC", "md:capital:alloc"), latest=_env("CAPITAL_ALLOC_LATEST_PREFIX", "md:capital:alloc:latest:")),
    # Decision
    _L("probability", "Decision", "Probability", stream=_env("STREAM_PROBABILITY", "md:probability"), latest=_env("PROBABILITY_LATEST_PREFIX", "md:probability:latest:")),
    _L("ranking", "Decision", "Trade ranking", stream=_env("STREAM_RANKING", "md:ranking"), latest=_env("RANKING_LATEST_PREFIX", "md:ranking:latest:"), keyed="SYMSIDE"),
    _L("ranking_cycle", "Decision", "Ranking cycle", stream=_env("STREAM_RANKING_CYCLE", "md:ranking:cycle"), latest=K["ranking_cycle_latest"], keyed="SINGLE", stale_sec=600),
    _L("icare", "Decision", "ICARE", stream=_env("STREAM_ICARE", "md:icare"), latest=_env("ICARE_LATEST_PREFIX", "md:icare:latest:")),
    _L("icare_origin", "Decision", "ICARE origin (approved)", latest=K["icare_origin"], keyed="TSYM", stale_sec=None),
    _L("account", "Decision", "Account", latest=K["account"], keyed="SINGLE", stale_sec=180),
    # Execution
    _L("exec", "Execution", "Exec events", stream=K["exec"], stale_sec=None),
    _L("exec_fill", "Execution", "Exec fills", stream=K["exec_fill"], stale_sec=None),
    _L("exec_exit_request", "Execution", "Exit requests", stream=K["exec_exit_request"], stale_sec=None),
    _L("exec_exit_fill", "Execution", "Exit fills", stream=K["exec_exit_fill"], stale_sec=None),
    _L("exec_latest", "Execution", "Exec report (by contract)", latest=K["exec_latest"] + "tsym:", keyed="TSYM", stale_sec=None),
    # TSL
    _L("tsl", "TSL", "Adaptive TSL", stream=K["tsl"], latest=K["tsl_latest"], keyed="TSYM", stale_sec=120),
    _L("tsl_reentry", "TSL", "TSL re-entry", stream=K["tsl_reentry"], stale_sec=None),
    # Journal
    _L("position", "Journal", "Open position", latest=K["position_open"], keyed="TSYM", stale_sec=None),
    _L("journal", "Journal", "Journal (closed trades)", stream=K["journal"], stale_sec=None),
]

LAYER_BY_ID = {l.id: l for l in LAYERS}

# Field names that commonly hold JSON-in-a-string inside stream entries / flat payloads.
JSON_FIELDS = ("top", "ranked", "reasons", "data_json", "ranking_json", "rank_components",
               "reject_reasons", "flags", "charges", "orders", "context", "rank_reject_reasons")

TS_FIELDS = ("ts_ms", "updated_ms", "timestamp", "ts_recv", "bar_ts_ms", "rank_ts_ms",
             "exit_ts_ms", "signal_ts_ms", "entry_ts_ms", "arrived_ms")


# ─────────────────────────────────────────────────────────────────────
# Small helpers
# ─────────────────────────────────────────────────────────────────────

def now_ms() -> int:
    return int(time.time() * 1000)


def fnum(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _s(v: Any) -> str:
    if isinstance(v, bytes):
        return v.decode("utf-8", "replace")
    return "" if v is None else str(v)


def parse_jsonish(v: Any) -> Any:
    """JSON-decode strings that look like JSON objects/lists; everything else unchanged."""
    if isinstance(v, bytes):
        v = v.decode("utf-8", "replace")
    if isinstance(v, str):
        t = v.strip()
        if t[:1] in ("{", "[") and t[-1:] in ("}", "]"):
            try:
                return json.loads(t)
            except Exception:
                return v
    return v


def expand_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    return {_s(k): parse_jsonish(v) for k, v in (fields or {}).items()}


def load_json(r, key: str) -> Optional[Any]:
    try:
        raw = r.get(key)
    except Exception:
        return None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def read_doc(r, key: str) -> Optional[Any]:
    """Value of a 'latest' key: JSON string → object; hash → dict; plain string → str."""
    try:
        raw = r.get(key)
    except Exception:
        raw = None
        try:
            h = r.hgetall(key)
        except Exception:
            h = None
        return expand_fields(h) if h else None
    if raw is None:
        try:
            if _s(r.type(key)) == "hash":
                h = r.hgetall(key)
                return expand_fields(h) if h else None
        except Exception:
            pass
        return None
    raw = _s(raw)
    try:
        return json.loads(raw)
    except Exception:
        return raw


def _norm_ms(v: Any) -> Optional[int]:
    n = fnum(_s(v) if isinstance(v, bytes) else v)
    if n is None or n <= 0:
        return None
    if n < 1e11:      # seconds
        n *= 1000.0
    return int(n)


def _date_ms(v: Any) -> Optional[int]:
    """'YYYY-MM-DD' (session date, e.g. pivots) → that session's close (10:00 UTC = 15:30 IST)."""
    try:
        import datetime as _dt

        d = _dt.date.fromisoformat(_s(v)[:10])
        return int(_dt.datetime(d.year, d.month, d.day, 10, 0, tzinfo=_dt.timezone.utc).timestamp() * 1000)
    except Exception:
        return None


def stream_id_ms(msg_id: Any) -> Optional[int]:
    try:
        return int(_s(msg_id).split("-", 1)[0])
    except Exception:
        return None


def doc_ts_ms(doc: Any, stream_id: Any = None) -> Optional[int]:
    """Timestamp of a doc: first known ts field, else the stream id's ms part."""
    if isinstance(doc, dict):
        for f in TS_FIELDS:
            ms = _norm_ms(doc.get(f))
            if ms:
                return ms
        ms = _date_ms(doc.get("date"))
        if ms:
            return ms
    if stream_id is not None:
        return stream_id_ms(stream_id)
    return None


def age_sec(doc: Any, now: Optional[int] = None, stream_id: Any = None) -> Optional[float]:
    ts = doc_ts_ms(doc, stream_id)
    if ts is None:
        return None
    return max(0.0, ((now or now_ms()) - ts) / 1000.0)


def fmt_age(a: Optional[float]) -> str:
    if a is None:
        return "no ts"
    if a < 90:
        return f"{a:.0f}s"
    if a < 3600:
        return f"{a / 60:.1f}m"
    if a < 86400 * 2:
        return f"{a / 3600:.1f}h"
    return f"{a / 86400:.1f}d"


def freshness(age: Optional[float], stale_sec: Optional[float]) -> str:
    """FRESH | STALE | NO_TS | N/A (no threshold configured)."""
    if stale_sec is None:
        return "N/A"
    if age is None:
        return "NO_TS"
    return "STALE" if age > stale_sec else "FRESH"


def effective_threshold(layer: Layer, override_sec: Optional[float] = None) -> Optional[float]:
    if layer.stale_sec is None:
        return None
    if override_sec and override_sec > 0:
        return float(override_sec)
    return float(layer.stale_sec)


def doc_flags(doc: Any) -> str:
    """CROSSED / STALE / … flags carried in a payload (field 'flags' or boolean markers)."""
    if not isinstance(doc, dict):
        return ""
    out: List[str] = []
    fl = parse_jsonish(doc.get("flags"))
    if isinstance(fl, (list, tuple)):
        out += [str(x) for x in fl]
    elif fl:
        out += [x.strip() for x in str(fl).split(",") if x.strip()]
    for k in ("crossed", "stale", "is_stale", "is_crossed"):
        v = doc.get(k)
        if str(v).lower() in ("1", "true", "yes"):
            out.append(k.replace("is_", "").upper())
    seen, uniq = set(), []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return ", ".join(uniq)


# ─────────────────────────────────────────────────────────────────────
# Key index (one capped SCAN, filtered locally)
# ─────────────────────────────────────────────────────────────────────

def scan_keys(r, pattern: str = "*", cap: int = 2000, count: int = 1000) -> List[str]:
    out: List[str] = []
    try:
        for k in r.scan_iter(match=pattern, count=count):
            out.append(_s(k))
            if len(out) >= cap:
                break
    except Exception:
        return out
    return sorted(out)


def key_index(r, cap: int = 50000) -> List[str]:
    return scan_keys(r, "md:*", cap=cap, count=2000)


def _suffix_matches(suffix: str, sym: str, keyed: str) -> Optional[str]:
    """'symbol' | 'contract' when the key suffix belongs to `sym`, else None."""
    s = sym.upper()
    u = suffix.upper()
    is_sym = u == s
    is_contract = u.startswith(s) and len(u) > len(s) and u[len(s)].isdigit()
    is_sub = u.startswith(s + ":")
    if keyed == "SYM":
        return "symbol" if (is_sym or is_sub) else None
    if keyed == "TSYM":
        return "contract" if is_contract else None
    if keyed == "ANY":
        return "symbol" if (is_sym or is_sub) else ("contract" if is_contract else None)
    if keyed in ("SYM_ISO", "SYM_CP", "SYMSIDE"):
        return "symbol" if is_sub else None
    return None


def contract_of(suffix: str, keyed: str, kind: str) -> str:
    if kind == "contract":
        return suffix
    if keyed in ("SYM_ISO", "SYM_CP", "SYMSIDE") and ":" in suffix:
        return suffix.split(":", 1)[1]
    return ""


def parse_greeks_latest(raw: Any) -> Dict[str, Any]:
    """md:greeks:latest:{SYM}:{ISO} — old form: JSON list; new form: {ts_ms, data|items|contracts|data_json}."""
    v = parse_jsonish(raw) if not isinstance(raw, (list, dict)) else raw
    if isinstance(v, list):
        # list form; newer pollers stamp ts_ms on every item → newest item wins
        ts = max((_norm_ms(it.get("ts_ms")) or 0 for it in v if isinstance(it, dict)), default=0) or None
        return {"ts_ms": ts, "items": v, "n": len(v), "form": "list"}
    if isinstance(v, dict):
        items = None
        for f in ("data", "items", "contracts", "greeks", "data_json"):
            if f in v:
                items = parse_jsonish(v.get(f))
                break
        items = items if isinstance(items, list) else []
        return {"ts_ms": _norm_ms(v.get("ts_ms") or v.get("ts_recv")), "items": items, "n": len(items),
                "form": "dict", **{k: x for k, x in v.items() if k not in ("data", "items", "contracts", "greeks", "data_json", "ts_ms")}}
    return {"ts_ms": None, "items": [], "n": 0, "form": "missing"}


def symbol_latest_docs(r, sym: str, keys: Optional[List[str]] = None, now: Optional[int] = None,
                       override_sec: Optional[float] = None, per_layer_cap: int = 60) -> List[Dict[str, Any]]:
    """Every latest-key doc for `sym` and its option contracts, across all layers.

    Returns rows {layer, group, name, key, scope (symbol|contract), contract, ts_ms, age_s, age, fresh, flags, doc}.
    """
    s = sym.upper()
    now = now or now_ms()
    if keys is None:
        keys = key_index(r)
    rows: List[Dict[str, Any]] = []

    def _row(layer: Layer, key: str, scope: str, contract: str, doc: Any, ts: Optional[int]) -> None:
        a = None if ts is None else max(0.0, (now - ts) / 1000.0)
        rows.append({
            "layer": layer.id, "group": layer.group, "name": layer.name, "key": key, "scope": scope,
            "contract": contract, "ts_ms": ts, "age_s": a, "age": fmt_age(a),
            "fresh": freshness(a, effective_threshold(layer, override_sec)),
            "flags": doc_flags(doc), "doc": doc,
        })

    for layer in LAYERS:
        if not layer.latest:
            continue
        if layer.keyed == "SINGLE":
            doc = read_doc(r, layer.latest)
            if doc is not None:
                _row(layer, layer.latest, "global", "", doc, doc_ts_ms(doc))
            continue
        if layer.keyed == "BLOB":
            blob = load_json(r, layer.latest)
            if isinstance(blob, dict) and isinstance(blob.get(s), dict):
                doc = blob[s]
                _row(layer, f"{layer.latest}[{s}]", "symbol", "", doc, doc_ts_ms(doc) or _norm_ms(blob.get("ts_ms")))
            continue
        n = 0
        prefix = layer.latest
        for k in keys:
            if not k.startswith(prefix):
                continue
            suffix = k[len(prefix):]
            # md:exec:latest:tsym:* lives under md:exec:latest:, md:tsl:state:x:exit_context etc.
            scope = _suffix_matches(suffix, s, layer.keyed)
            if not scope:
                continue
            if layer.keyed == "SYM_ISO":
                raw = _safe_get(r, k)
                g = parse_greeks_latest(_s(raw) if raw is not None else None)
                doc = {kk: vv for kk, vv in g.items() if kk != "items"}
                doc["sample"] = g["items"][:3]
                _row(layer, k, scope, contract_of(suffix, layer.keyed, scope), doc, g.get("ts_ms"))
            else:
                doc = read_doc(r, k)
                if doc is None:
                    continue
                _row(layer, k, scope, contract_of(suffix, layer.keyed, scope), doc, doc_ts_ms(doc))
            n += 1
            if n >= per_layer_cap:
                break
    return rows


def _safe_get(r, k: str):
    try:
        return r.get(k)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────
# Streams
# ─────────────────────────────────────────────────────────────────────

def _match_symbol(fields: Dict[str, Any], sym: str) -> bool:
    want = sym.upper()
    for k in ("symbol", "underlying", "key", "tradingsymbol", "option_symbol"):
        v = _s(fields.get(k)).upper()
        if not v:
            continue
        if v == want or v.startswith(want + ":") or (v.startswith(want) and len(v) > len(want) and v[len(want)].isdigit()):
            return True
    return False


def stream_tail(r, stream: str, n: int = 50, symbol: Optional[str] = None, scan: int = 0,
                now: Optional[int] = None, stale_sec: Optional[float] = None) -> List[Dict[str, Any]]:
    """Last `n` entries (newest first). With `symbol`, scans up to `scan` (default 20×n) entries."""
    now = now or now_ms()
    count = n if not symbol else max(scan or n * 20, n)
    try:
        raw = r.xrevrange(stream, count=count) or []
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for mid, fields in raw:
        f = {_s(k): _s(v) for k, v in (fields or {}).items()}
        if symbol and not _match_symbol(f, symbol):
            continue
        ts = doc_ts_ms(f, mid)
        id_ms = stream_id_ms(mid)
        a = None if ts is None else max(0.0, (now - ts) / 1000.0)
        row = {"_id": _s(mid), "_age": fmt_age(a), "_age_s": a, "_fresh": freshness(a, stale_sec),
               "_id_age_s": None if id_ms is None else max(0.0, (now - id_ms) / 1000.0)}
        row.update(f)
        out.append(row)
        if len(out) >= n:
            break
    return out


def stream_len(r, stream: str) -> int:
    try:
        return int(r.xlen(stream) or 0)
    except Exception:
        return 0


def stream_newest_ms(r, stream: str) -> Optional[int]:
    try:
        rows = r.xrevrange(stream, count=1) or []
    except Exception:
        return None
    if not rows:
        return None
    mid, fields = rows[0]
    return doc_ts_ms({_s(k): _s(v) for k, v in (fields or {}).items()}, mid)


def streams_overview(r, now: Optional[int] = None, override_sec: Optional[float] = None) -> List[Dict[str, Any]]:
    now = now or now_ms()
    out = []
    for layer in LAYERS:
        if not layer.stream:
            continue
        ts = stream_newest_ms(r, layer.stream)
        a = None if ts is None else max(0.0, (now - ts) / 1000.0)
        n = stream_len(r, layer.stream)
        out.append({"group": layer.group, "layer": layer.name, "stream": layer.stream, "length": n,
                    "newest_age": fmt_age(a) if n else "empty", "age_s": a,
                    "fresh": freshness(a, effective_threshold(layer, override_sec)) if n else "EMPTY"})
    return out


def pick_stream(r, stream: str, symbol: str, n: int = 120) -> dict:
    rows = stream_tail(r, stream, n=1, symbol=symbol, scan=n)
    if not rows:
        return {}
    return {k: v for k, v in rows[0].items() if not k.startswith("_")}


def last_candles(r, symbol: str, stream: str = "md:candles:1m", n: int = 80) -> List[dict]:
    rows = stream_tail(r, stream, n=n, symbol=symbol, scan=max(n * 8, 200))
    return [{k: v for k, v in x.items() if not k.startswith("_")} for x in reversed(rows)
            if _s(x.get("symbol")).upper() == symbol.upper()]


# ─────────────────────────────────────────────────────────────────────
# Per-symbol snapshot (used by the top KPIs / gate view)
# ─────────────────────────────────────────────────────────────────────

# Higher-TF streams can hold a long single-symbol run (history seeding writes one
# symbol at a time), so scan deep enough to find every symbol's newest bar.
CANDLE_SCAN = int(os.getenv("DASHBOARD_CANDLE_SCAN", "1500"))


def _latest(r, layer_id: str, suffix: str) -> dict:
    layer = LAYER_BY_ID[layer_id]
    doc = read_doc(r, f"{layer.latest}{suffix}")
    return doc if isinstance(doc, dict) else {}


def collect_symbol(r, sym: str) -> Dict[str, Any]:
    s = sym.upper()
    vol_blob = load_json(r, K["volume_latest"]) or {}
    vol = vol_blob.get(s) if isinstance(vol_blob, dict) else None
    if not isinstance(vol, dict):
        vol = {}
    try:
        expiry = r.hgetall(K["active_expiry"]) or {}
    except Exception:
        expiry = {}
    strike = _latest(r, "strike", s)
    strikeflow = _latest(r, "strikeflow", s)
    entry = _latest(r, "entry", s)
    momentum = _latest(r, "momentum", s)
    greeks_ce = _latest(r, "greeks_phase_und", f"{s}:CE")
    greeks_pe = _latest(r, "greeks_phase_und", f"{s}:PE")
    # Contract the per-contract layers are shown for: selected strike, else the
    # strike-flow pick, else the ATM leg on the side the signals lean to.
    side = str(entry.get("signal") or momentum.get("signal") or "").upper()
    atm_leg = greeks_pe if "PUT" in side else greeks_ce
    tsym = str(strike.get("tradingsymbol") or strikeflow.get("chosen_tradingsymbol")
               or atm_leg.get("tradingsymbol") or "")
    liq = _latest(r, "liquidity", tsym) if tsym else {}
    gchg = _latest(r, "greeks_change", s) or (_latest(r, "greeks_change", tsym) if tsym else {})

    return {
        "tick": pick_stream(r, K["ticks_eq"], s, 150),
        "c1m": pick_stream(r, K["c1m"], s, CANDLE_SCAN),
        "c5m": pick_stream(r, K["c5m"], s, CANDLE_SCAN),
        "c10m": pick_stream(r, K["c10m"], s, CANDLE_SCAN),
        "c30m": pick_stream(r, K["c30m"], s, CANDLE_SCAN),
        "expiry": _s(expiry.get(s, "")),
        "st": _latest(r, "supertrend", s),
        "ema": _latest(r, "ema", s),
        "htf": _latest(r, "htf", s),
        "pivots": _latest(r, "pivots", s),
        "level": _latest(r, "level", s),
        "momentum": momentum,
        "indicator_score": _latest(r, "indicator_score", s),
        "volume": vol,
        "regime": read_doc(r, K["regime_latest"]) or {},
        "bidask": _latest(r, "bidask", s),
        "smartmoney": _latest(r, "smartmoney", s),
        "orderflow": _latest(r, "orderflow", s),
        "imbalance": _latest(r, "imbalance", s),
        "stockflow": _latest(r, "stockflow", s),
        "composite": _latest(r, "composite", s),
        "oi_und": _latest(r, "oi_und", s),
        "greeks_ce": greeks_ce,
        "greeks_pe": greeks_pe,
        "strikeflow": strikeflow,
        "expected": _latest(r, "expected", s),
        "entry": entry,
        "strike": strike,
        "capital": _latest(r, "capital", s),
        "strike_intel": _latest(r, "strike_intel", s),
        "probability": _latest(r, "probability", s),
        "icare": _latest(r, "icare", s),
        "contract": tsym,
        "liquidity": liq or {},
        "optexit": _latest(r, "optexit", tsym) if tsym else {},
        "oi_contract": _latest(r, "oi", tsym) if tsym else {},
        "greeks_contract": _latest(r, "greeks_phase", tsym) if tsym else {},
        "greeks_change": gchg or {},
        "candles_1m": last_candles(r, s, K["c1m"], 60),
    }


# ─────────────────────────────────────────────────────────────────────
# Decision chain: level break → entry → strike → SIE → probability → ranking → ICARE → exec → TSL → journal
# ─────────────────────────────────────────────────────────────────────

CHAIN_FIELDS = {
    "level": ("signal", "level", "side", "strength", "price", "reason", "P", "R1", "S1"),
    "entry": ("signal", "strength", "level", "reason", "signal_ts_ms"),
    "strike": ("status", "side", "tradingsymbol", "strike", "reason"),
    "strike_intel": ("status", "side", "tradingsymbol", "strike_score", "confidence", "market_phase", "reason"),
    "probability": ("probability", "grade", "decision", "tradingsymbol", "reject_reasons", "flags", "signal_ts_ms"),
    "ranking": ("rank", "trade_score", "rank_decision", "rank_confidence", "rank_emit", "rank_reject_reasons", "tradingsymbol"),
    "icare": ("status", "tradingsymbol", "recommended_lots", "expected_value", "gross_ev", "charges", "net_ev",
              "risk_class", "limiting_factor", "exec_mode", "flags", "signal_ts_ms"),
    "icare_origin": ("probability", "decision", "tradingsymbol"),
    "exec": ("execution_status", "filled_lots", "requested_lots", "average_fill_price", "slippage", "mode",
             "signal_ts_ms", "reject_reasons", "cancel_reason"),
    "tsl": ("status", "current_trailing_stop", "trailing_percentage", "tsl_rule", "reentry_state", "event", "mode"),
    "position": ("trade_id", "lots", "entry_premium", "last_premium", "sl_premium", "target_premium"),
    "journal": ("trade_id", "exit_reason", "pnl", "pnl_pct", "exit_premium", "exec_mode"),
}


def _pick(doc: Any, fields: Iterable[str]) -> Dict[str, Any]:
    if not isinstance(doc, dict):
        return {}
    return {f: doc.get(f) for f in fields if doc.get(f) not in (None, "")}


# Stages a trade must pass, in order. The rest (ICARE origin, TSL, position,
# journal) only exist once a trade is live, so they are never "the blocker".
CHAIN_GATES = ("level", "entry", "strike", "strike_intel", "probability", "ranking", "icare", "exec")
PROB_PASS = frozenset({"SMALL_POSITION", "TRADE", "HIGH_CONVICTION"})


def _u(doc: Dict[str, Any], f: str) -> str:
    return str(doc.get(f) or "").strip().upper()


def stage_verdict(sid: str, doc: Any) -> str:
    """PASS | REJECTED | NO_SIGNAL | MISSING | INFO (stage is not a gate)."""
    if not isinstance(doc, dict) or not doc:
        return "MISSING"
    if sid == "level":
        return "PASS" if _u(doc, "signal").startswith("BUY") else "NO_SIGNAL"
    if sid == "entry":
        return "PASS" if _u(doc, "signal").startswith("BUY") else "REJECTED"
    if sid in ("strike", "strike_intel"):
        return "PASS" if _u(doc, "status") == "OK" else "REJECTED"
    if sid == "probability":
        return "PASS" if _u(doc, "decision") in PROB_PASS else "REJECTED"
    if sid == "ranking":
        return "PASS" if _u(doc, "rank_decision") == "TAKE_TRADE" else "REJECTED"
    if sid == "icare":
        return "PASS" if _u(doc, "status") == "APPROVED" else "REJECTED"
    if sid == "exec":
        st_ = _u(doc, "execution_status")
        ok = st_ == "FILLED" or (st_.startswith("PARTIAL") and (fnum(doc.get("filled_lots"), 0) or 0) > 0)
        return "PASS" if ok else "REJECTED"
    return "INFO"


def _stage_reason(sid: str, doc: Dict[str, Any]) -> str:
    if sid == "level":
        why = str(doc.get("reason") or "no_break")
        px, lv = doc.get("price"), "  ".join(f"{k} {doc.get(k)}" for k in ("S1", "P", "R1") if doc.get(k) not in (None, ""))
        return f"{why} · price {px} vs {lv}" if px not in (None, "") and lv else why
    for f in ("reason", "reject_reasons", "rank_reject_reasons", "cancel_reason", "limiting_factor"):
        v = doc.get(f)
        if v not in (None, "", [], "[]"):
            return ", ".join(map(str, v)) if isinstance(v, list) else str(v)
    for f in ("signal", "status", "decision", "rank_decision", "execution_status"):
        if doc.get(f) not in (None, ""):
            return f"{f}={doc.get(f)}"
    return ""


def chain_blocker(chain: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """First gate the trade did not pass (None when every gate passed)."""
    by = {c["stage"]: c for c in chain}
    for i, sid in enumerate(CHAIN_GATES):
        c = by.get(sid)
        if not c:
            continue
        if c["verdict"] == "MISSING":
            # An expired upstream doc is not the blocker when a later stage has output.
            if any((by.get(g) or {}).get("present") for g in CHAIN_GATES[i + 1:]):
                continue
            return {"stage": sid, "name": c["name"], "verdict": "MISSING",
                    "reason": f"no {c['name'].lower()} output yet (`{c['key'] or '—'}`)"}
        if c["verdict"] != "PASS":
            return {"stage": sid, "name": c["name"], "verdict": c["verdict"], "reason": _stage_reason(sid, c["doc"])}
        if c["fresh"] == "STALE":
            return {"stage": sid, "name": c["name"], "verdict": "STALE",
                    "reason": f"last {c['name'].lower()} output is {c['age']} old — replay, or the worker stopped"}
    return None


def decision_chain(r, sym: str, now: Optional[int] = None, keys: Optional[List[str]] = None,
                   override_sec: Optional[float] = None, journal_scan: int = 500) -> List[Dict[str, Any]]:
    s = sym.upper()
    now = now or now_ms()
    if keys is None:
        keys = key_index(r)
    stages: List[Tuple[str, str, Optional[str], Any]] = []

    level = _latest(r, "level", s)
    entry = _latest(r, "entry", s)
    strike = _latest(r, "strike", s)
    sie = _latest(r, "strike_intel", s)
    prob = _latest(r, "probability", s)
    stages += [("level", "Level break (pivot)", f"{LAYER_BY_ID['level'].latest}{s}", level),
               ("entry", "Entry trigger", f"{LAYER_BY_ID['entry'].latest}{s}", entry),
               ("strike", "Strike select", f"{LAYER_BY_ID['strike'].latest}{s}", strike),
               ("strike_intel", "Strike intelligence", f"{LAYER_BY_ID['strike_intel'].latest}{s}", sie),
               ("probability", "Probability", f"{LAYER_BY_ID['probability'].latest}{s}", prob)]
    rk_prefix = LAYER_BY_ID["ranking"].latest + s + ":"
    rk_keys = [k for k in keys if k.startswith(rk_prefix)]
    rk_docs = [(k, read_doc(r, k)) for k in rk_keys]
    rk_docs = [(k, d) for k, d in rk_docs if isinstance(d, dict)]
    rk_docs.sort(key=lambda kd: doc_ts_ms(kd[1]) or 0, reverse=True)
    if rk_docs:
        stages.append(("ranking", "Trade ranking", rk_docs[0][0], rk_docs[0][1]))
    else:
        stages.append(("ranking", "Trade ranking", f"{rk_prefix}*", {}))
    ic = _latest(r, "icare", s)
    stages.append(("icare", "ICARE", f"{LAYER_BY_ID['icare'].latest}{s}", ic))
    tsym = str(ic.get("tradingsymbol") or prob.get("tradingsymbol") or sie.get("tradingsymbol")
               or strike.get("tradingsymbol") or "")
    if tsym:
        stages.append(("icare_origin", "ICARE origin", f"{K['icare_origin']}{tsym}", read_doc(r, f"{K['icare_origin']}{tsym}") or {}))
        stages.append(("exec", "Execution report", f"{K['exec_latest']}tsym:{tsym}", read_doc(r, f"{K['exec_latest']}tsym:{tsym}") or {}))
        stages.append(("tsl", "Trailing stop", f"{K['tsl_latest']}{tsym}", read_doc(r, f"{K['tsl_latest']}{tsym}") or {}))
        stages.append(("position", "Open position", f"{K['position_open']}{tsym}", read_doc(r, f"{K['position_open']}{tsym}") or {}))
    else:
        for sid, name in (("icare_origin", "ICARE origin"), ("exec", "Execution report"), ("tsl", "Trailing stop"), ("position", "Open position")):
            stages.append((sid, name, None, {}))
    jr = stream_tail(r, K["journal"], n=1, symbol=s, scan=journal_scan, now=now)
    stages.append(("journal", "Journal (last closed)", K["journal"], ({k: v for k, v in jr[0].items() if not k.startswith("_")} if jr else {})))

    out = []
    for sid, name, key, doc in stages:
        lay = LAYER_BY_ID.get(sid)
        ts = doc_ts_ms(doc) if doc else None
        if sid == "journal" and jr:
            ts = doc_ts_ms(doc, jr[0]["_id"])
        a = None if ts is None else max(0.0, (now - ts) / 1000.0)
        thr = effective_threshold(lay, override_sec) if lay else None
        out.append({"stage": sid, "name": name, "key": key, "present": bool(doc),
                    "age_s": a, "age": fmt_age(a) if doc else "—",
                    "fresh": freshness(a, thr) if doc else "MISSING",
                    "verdict": stage_verdict(sid, doc),
                    "summary": _pick(doc, CHAIN_FIELDS.get(sid, ())), "flags": doc_flags(doc), "doc": doc})

    # Explain empty stages: what they are waiting for, and why a key is unknown.
    blk = chain_blocker(out)
    for c in out:
        if c["present"]:
            c["note"] = _stage_reason(c["stage"], c["doc"]) if c["verdict"] in ("REJECTED", "NO_SIGNAL") else ""
        elif blk and c["stage"] != blk["stage"]:
            c["note"] = f"waiting — chain stopped at {blk['name']}"
        else:
            c["note"] = "no output yet"
        if c["key"] is None:
            c["note"] += " · key unknown until a contract is picked (strike select / SIE / probability / ICARE)"
    return out


# ─────────────────────────────────────────────────────────────────────
# Execution / TSL / journal / ranking / account
# ─────────────────────────────────────────────────────────────────────

def _hash_json(r, key: str) -> Dict[str, Any]:
    try:
        h = r.hgetall(key) or {}
    except Exception:
        return {}
    return {_s(k): parse_jsonish(v) for k, v in h.items()}


def _zset(r, key: str, n: int = 50) -> List[Tuple[str, float]]:
    try:
        return [(_s(m), float(sc)) for m, sc in (r.zrevrange(key, 0, n - 1, withscores=True) or [])]
    except Exception:
        return []


def _smembers(r, key: str) -> List[str]:
    try:
        return sorted(_s(x) for x in (r.smembers(key) or []))
    except Exception:
        return []


def _docs_by_prefix(r, keys: List[str], prefix: str, exclude: Iterable[str] = (), cap: int = 300) -> List[Tuple[str, Any]]:
    ex = tuple(exclude)
    out = []
    for k in keys:
        if not k.startswith(prefix) or (ex and any(k.startswith(e) for e in ex)):
            continue
        d = read_doc(r, k)
        if d is not None:
            out.append((k, d))
        if len(out) >= cap:
            break
    return out


def kill_switch_state(r) -> Dict[str, Any]:
    raw = _safe_get(r, K["kill_switch"])
    v = _s(raw)
    return {"key": K["kill_switch"], "raw": v, "on": v.strip() == "1"}


def exec_overview(r, n: int = 50, keys: Optional[List[str]] = None, now: Optional[int] = None) -> Dict[str, Any]:
    now = now or now_ms()
    keys = key_index(r) if keys is None else keys
    pre = K["exec_latest"]
    reports = []
    for k, d in _docs_by_prefix(r, keys, pre, exclude=(pre + "tsym:",)):
        if isinstance(d, dict):
            reports.append(dict(d, _key=k, _age_s=age_sec(d, now)))
    reports.sort(key=lambda x: doc_ts_ms(x) or 0, reverse=True)
    by_tsym = []
    for k, d in _docs_by_prefix(r, keys, pre + "tsym:"):
        if isinstance(d, dict):
            by_tsym.append(dict(d, _key=k, _age_s=age_sec(d, now)))
    states = []
    for k, d in _docs_by_prefix(r, keys, K["exec_state"]):
        if isinstance(d, dict):
            states.append(dict(d, _key=k, _age_s=age_sec(d, now)))
    exit_states = []
    for k, d in _docs_by_prefix(r, keys, K["exec_exit_state"]):
        if isinstance(d, dict):
            exit_states.append(dict(d, _key=k, _age_s=age_sec(d, now)))
    exit_states.sort(key=lambda x: doc_ts_ms(x) or 0, reverse=True)
    return {
        "kill_switch": kill_switch_state(r),
        "active": _hash_json(r, K["exec_active"]),
        "missed": _hash_json(r, K["exec_missed"]),
        "reports": reports[:n],
        "reports_by_tsym": by_tsym,
        "states": states,
        "exit_states": exit_states,
        "exit_active": _hash_json(r, K["exec_exit_active"]),
        "events": stream_tail(r, K["exec"], n=n, now=now),
        "fills": stream_tail(r, K["exec_fill"], n=n, now=now),
        "exit_requests": stream_tail(r, K["exec_exit_request"], n=n, now=now),
        "exit_request_exists": stream_len(r, K["exec_exit_request"]) > 0,
        "exit_fills": stream_tail(r, K["exec_exit_fill"], n=n, now=now),
    }


def tsl_overview(r, n: int = 50, keys: Optional[List[str]] = None, now: Optional[int] = None) -> Dict[str, Any]:
    now = now or now_ms()
    keys = key_index(r) if keys is None else keys
    thr = LAYER_BY_ID["tsl"].stale_sec

    def _rows(prefix, exclude=()):
        out = []
        for k, d in _docs_by_prefix(r, keys, prefix, exclude=exclude):
            if isinstance(d, dict):
                a = age_sec(d, now)
                out.append(dict(d, _key=k, _age_s=a, _fresh=freshness(a, thr)))
        out.sort(key=lambda x: doc_ts_ms(x) or 0, reverse=True)
        return out

    states = [x for x in _rows(K["tsl_state"]) if not x["_key"].endswith(":exit_context")]
    return {
        "latest": _rows(K["tsl_latest"]),
        "states": states,
        "exit_contexts": [x for x in _rows(K["tsl_state"]) if x["_key"].endswith(":exit_context")],
        "chains": _rows(K["tsl_chain"]),
        "blocks": _rows(K["tsl_block"]),
        "origins": _rows(K["tsl_origin"]),
        "sets": {"chains": _smembers(r, K["tsl_chains"]), "trades": _smembers(r, K["tsl_trades"]),
                 "virtual": _smembers(r, K["tsl_virtual"])},
        "events": stream_tail(r, K["tsl"], n=n, now=now, stale_sec=thr),
        "reentry": stream_tail(r, K["tsl_reentry"], n=n, now=now),
    }


def journal_overview(r, n: int = 50, keys: Optional[List[str]] = None, now: Optional[int] = None) -> Dict[str, Any]:
    now = now or now_ms()
    keys = key_index(r) if keys is None else keys
    positions = []
    for k, d in _docs_by_prefix(r, keys, K["position_open"]):
        if isinstance(d, dict):
            last = fnum(d.get("last_premium"))
            entry = fnum(d.get("entry_premium"))
            qty = fnum(d.get("qty")) or ((fnum(d.get("lots")) or 0) * (fnum(d.get("lot_size")) or 0))
            upnl = None if last is None or entry is None or not qty else round((last - entry) * qty, 2)
            positions.append(dict(d, _key=k, unrealized_pnl=upnl, _age_s=age_sec(d, now)))
    stats = load_json(r, K["journal_stats"]) or {}
    stats_rows = []
    if isinstance(stats, dict):
        for b, v in stats.items():
            if not isinstance(v, dict):
                continue
            nn = int(fnum(v.get("samples"), 0) or 0)
            w = int(fnum(v.get("wins"), 0) or 0)
            stats_rows.append({"bucket": b, "trades": nn, "wins": w, "win_rate": round(w / nn, 3) if nn else None,
                               "pnl": v.get("pnl"),
                               "avg_win_pct": round(v.get("sum_win_pct", 0.0) / w, 3) if w else None,
                               "avg_loss_pct": round(v.get("sum_loss_pct", 0.0) / (nn - w), 3) if nn - w > 0 else None})
    daily = []
    for k, d in _docs_by_prefix(r, keys, K["journal_daily"]):
        if isinstance(d, dict):
            daily.append(dict(d, date=k[len(K["journal_daily"]):]))
    daily.sort(key=lambda x: x["date"], reverse=True)
    closed = []
    for k, d in _docs_by_prefix(r, keys, K["journal_closed"]):
        if isinstance(d, dict):
            closed.append(dict(d, _key=k))
    closed.sort(key=lambda x: doc_ts_ms(x) or 0, reverse=True)
    return {"positions": positions, "stats": stats_rows, "daily": daily, "closed": closed[:n],
            "stream": stream_tail(r, K["journal"], n=n, now=now)}


def ranking_overview(r, keys: Optional[List[str]] = None, now: Optional[int] = None, n: int = 50) -> Dict[str, Any]:
    now = now or now_ms()
    keys = key_index(r) if keys is None else keys
    latest = []
    for k, d in _docs_by_prefix(r, keys, LAYER_BY_ID["ranking"].latest):
        if isinstance(d, dict):
            latest.append(dict(d, _key=k, _age_s=age_sec(d, now)))
    latest.sort(key=lambda x: (int(fnum(x.get("rank"), 999) or 999), str(x.get("tradingsymbol") or "")))
    book = _hash_json(r, K["ranking_book"])
    cycle = read_doc(r, K["ranking_cycle_latest"]) or {}
    return {
        "latest": latest,
        "rank_zset": _zset(r, K["ranking_rank"], n),
        "probability_zset": _zset(r, K["probability_rank"], n),
        "book": book,
        "cycle": cycle,
        "cycle_age_s": age_sec(cycle, now) if cycle else None,
        "cycles": stream_tail(r, LAYER_BY_ID["ranking_cycle"].stream, n=n, now=now),
    }


def account_snapshot(r, now: Optional[int] = None) -> Dict[str, Any]:
    doc = read_doc(r, K["account"])
    doc = doc if isinstance(doc, dict) else {}
    a = age_sec(doc, now) if doc else None
    return {"doc": doc, "age_s": a, "fresh": freshness(a, LAYER_BY_ID["account"].stale_sec) if doc else "MISSING"}


def active_expiry(r) -> Dict[str, str]:
    try:
        h = r.hgetall(K["active_expiry"]) or {}
    except Exception:
        h = {}
    out = {_s(k): _s(v) for k, v in h.items()}
    ts = _safe_get(r, K["active_expiry"] + ":ts_ms")
    if ts:
        out["_ts_ms"] = _s(ts)
    return out


def contracts_for_symbol(r, sym: str, cap: int = 3000) -> List[Dict[str, str]]:
    """Subscribed option contracts (meta:opt:{tok} hashes) for an underlying — capped scan."""
    s = sym.upper()
    out = []
    for k in scan_keys(r, "meta:opt:*", cap=cap):
        try:
            h = {_s(a): _s(b) for a, b in (r.hgetall(k) or {}).items()}
        except Exception:
            continue
        if h.get("underlying", "").upper() == s:
            out.append(dict(h, token=k.split(":")[-1]))
    out.sort(key=lambda x: (x.get("expiry", ""), fnum(x.get("strike"), 0) or 0, x.get("cp", "")))
    return out


def eq_meta(r, sym: str, cap: int = 500) -> Dict[str, str]:
    s = sym.upper()
    for k in scan_keys(r, "meta:eq:*", cap=cap):
        try:
            h = {_s(a): _s(b) for a, b in (r.hgetall(k) or {}).items()}
        except Exception:
            continue
        if h.get("symbol", "").upper() == s:
            return dict(h, token=k.split(":")[-1])
    return {}


# ─────────────────────────────────────────────────────────────────────
# Raw explorer (read-only)
# ─────────────────────────────────────────────────────────────────────

def read_key(r, key: str, n: int = 20) -> Dict[str, Any]:
    """type, TTL and a bounded view of the value for any key."""
    try:
        t = _s(r.type(key))
    except Exception as e:
        return {"key": key, "type": "error", "ttl": None, "value": str(e)}
    try:
        ttl = int(r.ttl(key))
    except Exception:
        ttl = None
    val: Any = None
    try:
        if t == "string":
            val = parse_jsonish(_s(r.get(key)))
            if isinstance(val, str):
                val = parse_jsonish(val)
        elif t == "hash":
            val = expand_fields(r.hgetall(key) or {})
        elif t == "stream":
            val = [{"id": _s(mid), **expand_fields(f)} for mid, f in (r.xrevrange(key, count=n) or [])]
        elif t == "zset":
            val = [{"member": _s(m), "score": float(sc)} for m, sc in (r.zrevrange(key, 0, n - 1, withscores=True) or [])]
        elif t == "set":
            val = sorted(_s(x) for x in (r.smembers(key) or []))[:n]
        elif t == "list":
            val = [parse_jsonish(_s(x)) for x in (r.lrange(key, 0, n - 1) or [])]
        elif t == "none":
            val = None
    except Exception as e:
        val = f"error: {e}"
    return {"key": key, "type": t, "ttl": ttl, "value": val}


def explore(r, pattern: str, cap: int = 200) -> List[Dict[str, Any]]:
    out = []
    for k in scan_keys(r, pattern or "*", cap=cap):
        try:
            t = _s(r.type(k))
        except Exception:
            t = "?"
        try:
            ttl = int(r.ttl(k))
        except Exception:
            ttl = None
        out.append({"key": k, "type": t, "ttl": ttl})
    return out


# ─────────────────────────────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────────────────────────────

def redis_health(r) -> Dict[str, Any]:
    try:
        r.ping()
        return {
            "ok": True,
            "eq": stream_len(r, K["ticks_eq"]),
            "opt": stream_len(r, K["ticks_opt"]),
            "c1m": stream_len(r, K["c1m"]),
            "greeks": stream_len(r, K["greeks_snap"]),
            "keys": int(r.dbsize() or 0),
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "eq": 0, "opt": 0, "c1m": 0, "greeks": 0, "keys": 0}


def universe(r=None) -> List[str]:
    syms: List[str] = []
    try:
        syms = [s.upper() for s in load_symbols()]
    except Exception:
        syms = []
    if r is not None:
        for s in active_expiry(r):
            if not s.startswith("_") and s.upper() not in syms:
                syms.append(s.upper())
    return syms


def layer_freshness_table(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for x in rows:
        out[x.get("fresh", "?")] = out.get(x.get("fresh", "?"), 0) + 1
    return out

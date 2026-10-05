#!/usr/bin/env python3
"""Option Rider — live production dashboard (Streamlit)."""

from __future__ import annotations

import datetime as dt
import json
import os
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

from app import dashboard_data as dd
from app.dashboard_data import (
    age_sec,
    connect,
    fnum,
    load_json,
    redis_health,
    universe,
)

# Read-only dashboard: nothing in this file writes to Redis.
CACHE_TTL = float(os.getenv("DASHBOARD_CACHE_TTL_SEC", "3"))
DEFAULT_REFRESH = int(os.getenv("DASHBOARD_REFRESH_SEC", "5"))

try:
    from zoneinfo import ZoneInfo

    IST = ZoneInfo("Asia/Kolkata")
except Exception:
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

st.set_page_config(
    page_title="Option Rider",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.block-container { padding-top: 1.2rem; padding-bottom: 2rem; max-width: 1400px; }
#MainMenu, footer, header { visibility: hidden; }

.hero {
  background: linear-gradient(135deg, #0b1220 0%, #132337 50%, #0e4d6b 100%);
  border-radius: 16px; padding: 1.15rem 1.4rem; color: #e8f1f8;
  margin-bottom: 1rem; border: 1px solid #1e3a4c;
}
.hero h1 { margin: 0; font-size: 1.55rem; letter-spacing: -0.03em; }
.hero p { margin: 0.25rem 0 0; opacity: 0.78; font-size: 0.92rem; }

.kpi {
  background: #ffffff; border: 1px solid #e6edf2; border-radius: 14px;
  padding: 0.85rem 1rem; min-height: 92px;
  box-shadow: 0 1px 2px rgba(16,24,40,0.04);
}
.kpi .lbl { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.06em; color: #667085; font-weight: 600; }
.kpi .val { font-size: 1.35rem; font-weight: 700; color: #101828; margin-top: 0.15rem; line-height: 1.2; }
.kpi .sub { font-size: 0.75rem; color: #667085; margin-top: 0.15rem; }

.pill { display: inline-block; padding: 0.22rem 0.7rem; border-radius: 999px;
  font-weight: 700; font-size: 0.85rem; letter-spacing: 0.02em; }
.pill-call { background: #dcfae6; color: #087443; }
.pill-put { background: #fde8e8; color: #b42318; }
.pill-neu { background: #f2f4f7; color: #344054; }
.pill-ok { background: #dcfae6; color: #087443; }
.pill-empty { background: #fff4e5; color: #b54708; }
.pill-bad { background: #fde8e8; color: #b42318; }

.gate { border: 1px solid #e6edf2; border-radius: 12px; padding: 0.7rem 0.85rem; margin-bottom: 0.45rem; }
.gate-ok { background: #f3fbf6; border-color: #abefc6; }
.gate-no { background: #fffaf5; border-color: #f9dbaf; }
.gate b { font-size: 0.92rem; }
.gate span { color: #667085; font-size: 0.8rem; }

.reason { background: #0b1220; color: #d0d5dd; border-radius: 10px; padding: 0.75rem 1rem;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.82rem; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)


def _now() -> str:
    return dt.datetime.now(IST).strftime("%H:%M:%S IST")


def _fmt(v: Any, nd: int = 2) -> str:
    n = fnum(v)
    if n is None:
        return "—"
    if abs(n) >= 1000:
        return f"{n:,.{nd}f}"
    return f"{n:.{nd}f}"


def _age(doc: Any) -> str:
    a = age_sec(doc)
    if a is None:
        return ""
    return dd.fmt_age(a) + " ago"


def _clock(doc: Any) -> str:
    ts = dd.doc_ts_ms(doc) if doc else None
    return dt.datetime.fromtimestamp(ts / 1000, IST).strftime("%d %b %H:%M") if ts else "—"


FRESH_COLOURS = {
    "FRESH": "background-color: #dcfae6; color: #087443",
    "STALE": "background-color: #fde8e8; color: #b42318",
    "NO_TS": "background-color: #fff4e5; color: #b54708",
    "MISSING": "background-color: #f2f4f7; color: #667085",
    "EMPTY": "background-color: #f2f4f7; color: #667085",
    "OK": "background-color: #dcfae6; color: #087443",
    "FAIL": "background-color: #fde8e8; color: #b42318",
    "NOT_IMPL": "color: #98a2b3",
    "IDLE": "background-color: #f2f4f7; color: #344054",
    "DOWN": "background-color: #fde8e8; color: #b42318",
}


def _cell(v: Any) -> Any:
    if isinstance(v, (dict, list, tuple)):
        try:
            return json.dumps(v, default=str)[:400]
        except Exception:
            return str(v)[:400]
    return v


def _df(rows: List[dict], cols: Optional[List[str]] = None) -> pd.DataFrame:
    """Arrow-safe frame: nested values → JSON text, mixed object columns → str."""
    if not rows:
        return pd.DataFrame(columns=cols or [])
    df = pd.DataFrame([{k: _cell(v) for k, v in r.items()} for r in rows])
    if cols:
        df = df[[c for c in cols if c in df.columns]]
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].map(lambda x: "" if x is None else str(x))
    return df


def show_table(rows: List[dict], cols: Optional[List[str]] = None, fresh_col: Optional[str] = None,
               height: Optional[int] = None, empty: str = "No data.") -> None:
    if not rows:
        st.caption(empty)
        return
    df = _df(rows, cols)
    kw: Dict[str, Any] = {"width": "stretch", "hide_index": True}
    if height:
        kw["height"] = height
    if fresh_col and fresh_col in df.columns:
        st.dataframe(df.style.map(lambda v: FRESH_COLOURS.get(str(v), ""), subset=[fresh_col]), **kw)
    else:
        st.dataframe(df, **kw)


def fresh_pill(state: str, age: str = "") -> str:
    kind = {"FRESH": "ok", "OK": "ok", "STALE": "bad", "FAIL": "bad", "DOWN": "bad", "NO_TS": "empty"}.get(state, "neu")
    return _pill(f"{state} {age}".strip(), kind)


def _pill(text: str, kind: str) -> str:
    return f'<span class="pill pill-{kind}">{text}</span>'


def decision_kind(text: str) -> str:
    t = (text or "").upper()
    if "CALL" in t and "PUT" not in t:
        return "call"
    if "PUT" in t:
        return "put"
    return "neu"


def ltp_of(d: dict) -> Optional[float]:
    return fnum((d.get("tick") or {}).get("ltp")) or fnum((d.get("oi_und") or {}).get("spot")) or fnum(
        (d.get("c1m") or {}).get("c")
    )


def gate_row(ok: bool, title: str, detail: str) -> None:
    cls = "gate gate-ok" if ok else "gate gate-no"
    mark = "PASS" if ok else "WAIT"
    st.markdown(
        f'<div class="{cls}"><b>{mark} · {title}</b><br><span>{detail or "no data yet"}</span></div>',
        unsafe_allow_html=True,
    )


def entry_gates(d: dict) -> List[tuple]:
    entry = d.get("entry") or {}
    st_b = str((d.get("st") or {}).get("bias") or entry.get("st_bias") or "").upper()
    ema_s = str((d.get("ema") or {}).get("state") or entry.get("ema_state") or "").lower()
    htf = str((d.get("htf") or {}).get("bias") or entry.get("htf_bias") or "").upper()
    vol = str((d.get("volume") or {}).get("signal") or entry.get("volume_signal") or "")
    oi = str((d.get("oi_und") or {}).get("positioning") or entry.get("oi_positioning") or "").upper()
    lvl = d.get("level") or {}
    lvl_sig = str(lvl.get("signal") or "").upper()
    lvl_lv = str(lvl.get("level") or entry.get("level") or "")
    gp_ce = str((d.get("greeks_ce") or {}).get("phase") or "").upper()
    gp_pe = str((d.get("greeks_pe") or {}).get("phase") or "").upper()

    call_side = htf == "CALL" and st_b == "CALL" and ema_s == "bullish"
    put_side = htf == "PUT" and st_b == "PUT" and ema_s == "bearish"
    vol_call = vol in ("Bullish Volume", "Strong Bullish Volume") or "Bullish" in vol
    vol_put = vol in ("Bearish Volume", "Strong Bearish Volume") or "Bearish" in vol
    oi_call = oi in ("BULLISH_POSITIONING", "BULLISH")
    oi_put = oi in ("BEARISH_POSITIONING", "BEARISH")
    lvl_call = lvl_sig == "BUY CALL" and lvl_lv in ("P", "R1", "")
    lvl_put = lvl_sig == "BUY PUT" and lvl_lv in ("P", "S1", "")

    return [
        (htf in ("CALL", "PUT"), "Higher timeframe (D/W/M)", f"bias={htf or 'EMPTY'}"),
        (st_b in ("CALL", "PUT"), "Supertrend majority", f"bias={st_b or 'EMPTY'}  1m={(d.get('st') or {}).get('st_1m')}  5m={(d.get('st') or {}).get('st_5m')}  10m={(d.get('st') or {}).get('st_10m')}  30m={(d.get('st') or {}).get('st_30m')}"),
        (ema_s in ("bullish", "bearish"), "EMA 9/26 momentum", f"state={ema_s or 'EMPTY'}  ema9={(d.get('ema') or {}).get('ema9')}  ema26={(d.get('ema') or {}).get('ema26')}"),
        (lvl_call or lvl_put, "Pivot / R1 / S1 break", f"{lvl_sig or 'NEUTRAL'}  level={lvl_lv or '—'}  {(lvl.get('reason') or '')}"),
        (vol_call or vol_put, "Volume imbalance", f"{vol or 'EMPTY'}  buy%={(d.get('volume') or {}).get('buy_pct')}  sell%={(d.get('volume') or {}).get('sell_pct')}"),
        (oi_call or oi_put, "OI positioning", f"{oi or 'EMPTY'}  ATM={(d.get('oi_und') or {}).get('atm')}  max pain={(d.get('oi_und') or {}).get('max_pain')}"),
        (gp_ce == "MARKUP" or gp_pe == "MARKUP", "Greeks phase MARKUP", f"CE={gp_ce or 'EMPTY'}  PE={gp_pe or 'EMPTY'}"),
        (call_side or put_side, "Side alignment (HTF+ST+EMA)", f"{'CALL stack' if call_side else ('PUT stack' if put_side else 'not aligned')}"),
    ]


def kpi(label: str, value: str, sub: str = "") -> None:
    st.markdown(
        f'<div class="kpi"><div class="lbl">{label}</div><div class="val">{value}</div>'
        f'<div class="sub">{sub}</div></div>',
        unsafe_allow_html=True,
    )


def filled(doc: Any) -> bool:
    return bool(doc) and doc != {}


# ── Cached, read-only loaders (short TTL; `_r` is not hashed) ─────────
@st.cache_resource(show_spinner=False)
def _client():
    return connect()


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def c_keys(_r, cap: int) -> List[str]:
    return dd.key_index(_r, cap=cap)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def c_symbol(_r, sym: str) -> Dict[str, Any]:
    return dd.collect_symbol(_r, sym)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def c_symbol_docs(_r, sym: str, cap: int, override: float) -> List[dict]:
    return dd.symbol_latest_docs(_r, sym, keys=c_keys(_r, cap), override_sec=override or None)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def c_chain(_r, sym: str, cap: int, override: float) -> List[dict]:
    return dd.decision_chain(_r, sym, keys=c_keys(_r, cap), override_sec=override or None)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def c_exec(_r, n: int, cap: int) -> Dict[str, Any]:
    return dd.exec_overview(_r, n=n, keys=c_keys(_r, cap))


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def c_tsl(_r, n: int, cap: int) -> Dict[str, Any]:
    return dd.tsl_overview(_r, n=n, keys=c_keys(_r, cap))


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def c_journal(_r, n: int, cap: int) -> Dict[str, Any]:
    return dd.journal_overview(_r, n=n, keys=c_keys(_r, cap))


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def c_ranking(_r, n: int, cap: int) -> Dict[str, Any]:
    return dd.ranking_overview(_r, keys=c_keys(_r, cap), n=n)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def c_streams(_r, override: float) -> List[dict]:
    return dd.streams_overview(_r, override_sec=override or None)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def c_tail(_r, stream: str, n: int, sym: str) -> List[dict]:
    return dd.stream_tail(_r, stream, n=n, symbol=sym or None)


@st.cache_data(ttl=max(CACHE_TTL, 10), show_spinner=False)
def c_health(_r, cap: int, override: float) -> List[dict]:
    import pipeline_health as ph

    return ph.evaluate_all(_r, keys=c_keys(_r, cap), max_age_override=override or None, check_workers=True)


@st.cache_data(ttl=60, show_spinner=False)
def c_contracts(_r, sym: str) -> List[dict]:
    return dd.contracts_for_symbol(_r, sym)


# ── Sidebar ──────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Option Rider")
    st.caption("Live layers from Redis · NSE F&O · read-only")
    refresh_opts = [0, 2, 5, 10, 15, 30]
    refresh_s = st.select_slider("Auto-refresh (seconds)", options=refresh_opts,
                                 value=DEFAULT_REFRESH if DEFAULT_REFRESH in refresh_opts else 5)
    st.caption("0 = pause live updates")

try:
    r = _client()
    health = redis_health(r)
except Exception as e:
    health = {"ok": False, "error": str(e), "eq": 0, "opt": 0, "c1m": 0, "greeks": 0, "keys": 0}
    r = None

symbols = universe(r) if r is not None else []
if not symbols:
    symbols = ["RELIANCE", "TCS", "INFY"]

with st.sidebar:
    symbol = st.selectbox("Symbol", symbols, index=0)
    n_rows = st.select_slider("Stream rows (N)", options=[10, 25, 50, 100, 200, 500], value=50)
    stale_override = float(st.number_input(
        "Stale threshold override (s)", min_value=0, max_value=86400 * 7, value=0, step=30,
        help="0 = per-layer defaults (ticks 30s, bid-ask 60s, signals 300s, candles per timeframe …)"))
    key_cap = int(st.select_slider("Key scan cap", options=[5000, 20000, 50000, 100000], value=50000,
                                   help="SCAN md:* is capped at this many keys"))
    if st.button("Refresh now", width="stretch"):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    if health.get("ok"):
        st.markdown(_pill("REDIS LIVE", "ok"), unsafe_allow_html=True)
        st.caption(f"EQ ticks {health['eq']:,} · OPT {health['opt']:,} · 1m candles {health['c1m']:,}")
        st.caption(f"Greeks snap {health['greeks']:,} · keys {health['keys']:,}")
    else:
        st.markdown(_pill("REDIS DOWN", "bad"), unsafe_allow_html=True)
        st.caption(str(health.get("error") or "Cannot reach Redis"))
    st.divider()
    st.caption("Export client Excel")
    st.code("./export_client_excel.sh", language="bash")

if not health.get("ok") or r is None:
    st.error("Cannot connect to Redis. Start the pipeline with `./run_all.sh` (Redis on localhost:6379).")
    st.stop()

d = c_symbol(r, symbol)
all_data = {s: c_symbol(r, s) for s in symbols}

entry = d.get("entry") or {}
# Decision = the final entry gate only. Momentum (ST + EMA) is a pre-gate input and
# must not be shown as the decision when entry_trigger has not fired.
signal = str(entry.get("signal") or "NO ENTRY")
momentum_signal = str((d.get("momentum") or {}).get("signal") or "—")
kind = decision_kind(signal)
ltp = ltp_of(d)
regime = d.get("regime") or {}
regime_name = str(regime.get("regime") or regime.get("Regime") or regime.get("market_regime") or "—")
if regime_name == "—":
    for k, v in regime.items():
        if "regime" in k.lower() or k.lower() == "signal":
            regime_name = str(v)
            break

# ── Hero ─────────────────────────────────────────────────────────────
st.markdown(
    f"""<div class="hero">
    <h1>Option Rider · {symbol}</h1>
    <p>Production live view of every layer — indicators, microstructure, expected move, and the final CALL/PUT gate. Snapshot {_now()}</p>
    </div>""",
    unsafe_allow_html=True,
)

c1, c2, c3, c4, c5, c6 = st.columns(6)
with c1:
    kpi("Last price", _fmt(ltp, 2), _age(d.get("tick") or d.get("c1m")))
with c2:
    kpi("Decision", signal, str(entry.get("reason") or entry.get("strength") or f"momentum (pre-gate): {momentum_signal}")[:48])
with c3:
    kpi("Supertrend", str((d.get("st") or {}).get("bias") or "—"), _age(d.get("st")))
with c4:
    ema = d.get("ema") or {}
    kpi("EMA 9/26", str(ema.get("state") or "—"), f"9={_fmt(ema.get('ema9'), 2)}  26={_fmt(ema.get('ema26'), 2)}")
with c5:
    comp = d.get("composite") or {}
    kpi("Composite", _fmt(comp.get("score"), 3), str(comp.get("status") or ""))
with c6:
    em = d.get("expected") or {}
    pct = fnum(em.get("expected_move_pct"))
    move_label = f"{pct * 100:.2f}%" if pct is not None else _fmt(em.get("final_expected_move") or em.get("expected_move"), 2)
    kpi(
        "Expected move",
        move_label,
        f"tgt={_fmt(em.get('target_price'), 2)}  {em.get('direction') or ''}  conf={em.get('confidence') or '—'}",
    )

st.markdown(
    f"**Gate** {_pill(signal, kind)} &nbsp; **HTF** {_pill(str((d.get('htf') or {}).get('bias') or 'EMPTY'), decision_kind(str((d.get('htf') or {}).get('bias'))))} &nbsp; "
    f"**Regime** {_pill(str(regime_name), decision_kind(str(regime_name)))} &nbsp; "
    f"**Expiry** `{d.get('expiry') or '—'}`",
    unsafe_allow_html=True,
)

# ── Universe table ───────────────────────────────────────────────────
rows = []
for s, sd in all_data.items():
    e = sd.get("entry") or {}
    rows.append(
        {
            "Symbol": s,
            "LTP": _fmt(ltp_of(sd)),
            "Decision": e.get("signal") or "NO ENTRY",
            "Momentum": (sd.get("momentum") or {}).get("signal") or "—",
            "Reason": (e.get("reason") or "")[:80],
            "ST": (sd.get("st") or {}).get("bias") or "—",
            "EMA": (sd.get("ema") or {}).get("state") or "—",
            "HTF": (sd.get("htf") or {}).get("bias") or "—",
            "Volume": (sd.get("volume") or {}).get("signal") or "—",
            "OI": (sd.get("oi_und") or {}).get("positioning") or "—",
            "Greeks CE": (sd.get("greeks_ce") or {}).get("phase") or "—",
            "Composite": _fmt((sd.get("composite") or {}).get("score"), 3),
            "Strike": (sd.get("strike") or {}).get("tradingsymbol") or sd.get("contract") or "—",
        }
    )
st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True, height=148)

TAB_NAMES = [
    "Decision gate",
    "Decision chain",
    "Symbol data",
    "Indicators",
    "Volume & regime",
    "Bid-ask / flow",
    "Options / OI / Greeks",
    "Expected move",
    "Strike · Probability · ICARE",
    "Execution",
    "TSL",
    "Positions & journal",
    "Streams",
    "Pipeline health",
    "Raw Redis explorer",
    "Raw payloads",
]
T = dict(zip(TAB_NAMES, st.tabs(TAB_NAMES)))

# Decision
with T["Decision gate"]:
    left, right = st.columns([1.15, 1])
    with left:
        st.subheader("Entry gate filters")
        st.caption("A BUY fires only when these align. WAIT is expected most of the session.")
        for ok, title, detail in entry_gates(d):
            gate_row(ok, title, detail)
    with right:
        st.subheader("Final output")
        st.markdown(
            f"<div class='reason'>signal={signal}<br>strength={entry.get('strength') or '—'}<br>"
            f"level={entry.get('level') or '—'}<br>aligned={entry.get('aligned') or '—'}<br>"
            f"reason={entry.get('reason') or 'no entry_trigger payload yet — it publishes only on a live P/R1/S1 break'}<br>"
            f"momentum (pre-gate)={momentum_signal}</div>",
            unsafe_allow_html=True,
        )
        st.write("")
        strike = d.get("strike") or {}
        cap = d.get("capital") or {}
        st.markdown("**Strike select**")
        st.write(
            {
                "status": strike.get("status") or "EMPTY",
                "tradingsymbol": strike.get("tradingsymbol") or "—",
                "strike": strike.get("strike") or "—",
                "atm": strike.get("atm") or "—",
                "expiry": strike.get("expiry") or d.get("expiry") or "—",
                "reason": strike.get("reason") or "—",
            }
        )
        st.markdown("**Capital**")
        st.write(
            {
                "status": cap.get("status") or "EMPTY",
                "side": cap.get("side") or "—",
                "notional": cap.get("trade_notional") or "—",
                "regime": cap.get("regime") or regime_name,
                "reason": cap.get("reason") or "—",
            }
        )

# Indicators
with T["Indicators"]:
    a, b, c = st.columns(3)
    with a:
        st.markdown("**Supertrend (ATR 7 × 1)**")
        st.json(d.get("st") or {"status": "EMPTY — need more MTF candles"})
    with b:
        st.markdown("**EMA 9 / 26**")
        st.json(d.get("ema") or {"status": "EMPTY — ~26 one-minute bars required"})
    with c:
        st.markdown("**HTF daily / weekly / monthly**")
        st.json(d.get("htf") or {"status": "EMPTY — needs prior daily candles"})
    p1, p2, p3 = st.columns(3)
    with p1:
        st.markdown("**Prev-day pivots**")
        st.json(d.get("pivots") or {"status": "EMPTY — first session / no 1d close yet"})
    with p2:
        st.markdown("**Level entry (break)**")
        st.json(d.get("level") or {"status": "EMPTY"})
    with p3:
        st.markdown("**Momentum confirm**")
        st.json(d.get("momentum") or {"status": "EMPTY"})
    q1, q2 = st.columns([1, 2])
    with q1:
        st.markdown("**Indicator score**")
        st.json(d.get("indicator_score") or {"status": "EMPTY"})
    with q2:
        st.markdown("**Last closed bar per timeframe**")
        show_table([{"TF": tf, "Bar end": _clock(d.get(k)),
                     "O": (d.get(k) or {}).get("o"), "H": (d.get(k) or {}).get("h"), "L": (d.get(k) or {}).get("l"),
                     "C": (d.get(k) or {}).get("c"), "V": (d.get(k) or {}).get("v"), "Age": _age(d.get(k)) or "—"}
                    for tf, k in (("1m", "c1m"), ("5m", "c5m"), ("10m", "c10m"), ("30m", "c30m"))],
                   empty="No candles.")

    candles = d.get("candles_1m") or []
    if candles:
        cdf = pd.DataFrame(candles)
        for col in ("o", "h", "l", "c", "v"):
            if col in cdf.columns:
                cdf[col] = pd.to_numeric(cdf[col], errors="coerce")
        if "ts_ms" in cdf.columns:
            cdf["time"] = pd.to_datetime(pd.to_numeric(cdf["ts_ms"], errors="coerce"), unit="ms", utc=True)
            cdf["time"] = cdf["time"].dt.tz_convert("Asia/Kolkata")
            cdf = cdf.set_index("time")
        if "c" in cdf.columns:
            st.markdown("**1-minute close**")
            st.line_chart(cdf["c"], height=220)

# Volume
with T["Volume & regime"]:
    v1, v2 = st.columns(2)
    with v1:
        st.markdown("**Volume analyzer**")
        vol = d.get("volume") or {}
        buy = fnum(vol.get("buy_pct"))
        sell = fnum(vol.get("sell_pct"))
        if buy is not None and sell is not None:
            st.progress(min(max(buy / 100.0, 0.0), 1.0), text=f"Buy {buy:.1f}%  /  Sell {sell:.1f}%")
        st.json(vol or {"status": "EMPTY"})
        st.caption("Check: buy% ≈ (close − low) / (high − low) × 100. Bullish if buy% ≥ 60.")
    with v2:
        st.markdown("**Market regime (this universe only)**")
        if len(symbols) < 50:
            st.warning(f"Breadth of this universe only ({len(symbols)} symbols: {', '.join(symbols[:10])}) — not NSE breadth.")
        st.json(d.get("regime") or {"status": "EMPTY"})

# Bid-ask
with T["Bid-ask / flow"]:
    st.caption(f"Contract-level blocks use `{d.get('contract') or '—'}` (selected strike → strike-flow pick → ATM leg on the signal side).")
    r1 = st.columns(3)
    r2 = st.columns(3)
    r3 = st.columns(3)
    blocks = [
        (r1[0], "Bid-ask / spread", "bidask"),
        (r1[1], "Imbalance (−1…+1)", "imbalance"),
        (r1[2], "Composite score", "composite"),
        (r2[0], "Order flow", "orderflow"),
        (r2[1], "Smart money", "smartmoney"),
        (r2[2], "Stock entry/exit", "stockflow"),
        (r3[0], "Option liquidity exit (contract)", "optexit"),
    ]
    for col, title, key in blocks:
        with col:
            st.markdown(f"**{title}**")
            doc = d.get(key) or {}
            st.markdown(_pill("OK", "ok") if filled(doc) else _pill("EMPTY", "empty"), unsafe_allow_html=True)
            st.json(doc or {"status": "EMPTY"})

# Options
with T["Options / OI / Greeks"]:
    o1, o2, o3 = st.columns(3)
    with o1:
        st.markdown("**Open interest (underlying)**")
        st.json(d.get("oi_und") or {"status": "EMPTY"})
    with o2:
        st.markdown("**Greeks phase ATM CE**")
        st.json(d.get("greeks_ce") or {"status": "EMPTY"})
        st.markdown("**Greeks phase ATM PE**")
        st.json(d.get("greeks_pe") or {"status": "EMPTY"})
    with o3:
        st.markdown(f"**Liquidity · {d.get('contract') or 'no contract'}**")
        st.json(d.get("liquidity") or {"status": "EMPTY"})
        st.markdown("**Strike flow**")
        st.json(d.get("strikeflow") or {"status": "EMPTY"})
    c1_, c2_ = st.columns(2)
    with c1_:
        st.markdown(f"**OI · {d.get('contract') or 'no contract'}**")
        st.json(d.get("oi_contract") or {"status": "EMPTY"}, expanded=False)
    with c2_:
        st.markdown(f"**Greeks phase · {d.get('contract') or 'no contract'}**")
        st.json(d.get("greeks_contract") or {"status": "EMPTY"}, expanded=False)

# Expected move
with T["Expected move"]:
    e1, e2 = st.columns(2)
    with e1:
        st.markdown("**Expected move (prediction only — no strike)**")
        st.json(d.get("expected") or {"status": "EMPTY"})
        st.caption("Score × Greeks/liquidity multiplier × 2%. target_price = spot + expected_move. indicator_score is Supertrend + EMA + pivot strength.")
    with e2:
        st.markdown("**Greeks change (Expected Move × candidate strike)**")
        st.json(d.get("greeks_change") or {"status": "EMPTY until expected_move + ATM/strikeflow"})
        st.markdown("**Last 1m candle**")
        st.json(d.get("c1m") or {"status": "EMPTY"})

# Strike Intelligence -> Probability -> ICARE (shadow pipeline, paper trades only)
with T["Strike · Probability · ICARE"]:
    import json as _json

    def _jl(v, default):
        try:
            return _json.loads(v) if isinstance(v, str) and v else (v or default)
        except Exception:
            return default

    sie = d.get("strike_intel") or {}
    prob = d.get("probability") or {}
    ic = d.get("icare") or {}
    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        kpi("SIE best strike", str(sie.get("tradingsymbol") or "—"), f"{sie.get('market_phase') or ''} Δ {sie.get('delta_band') or ''}")
    with k2:
        kpi("Strike score", _fmt(sie.get("strike_score"), 1), f"confidence {_fmt(sie.get('confidence'), 1)}")
    with k3:
        kpi("Probability", f"{prob.get('probability') or '—'} {prob.get('grade') or ''}", str(prob.get("decision") or ""))
    with k4:
        kpi("ICARE", str(ic.get("status") or "—"), f"quality {_fmt(ic.get('trade_quality'), 1)} · class {ic.get('risk_class') or '—'}")
    with k5:
        kpi("Lots", str(ic.get("recommended_lots") or "0"), f"limit: {ic.get('limiting_factor') or '—'}")

    s1, s2 = st.columns([1.2, 1])
    with s1:
        st.markdown("**Top ranked strikes (SIE)**")
        top = _jl(sie.get("top"), [])
        if top:
            st.dataframe(pd.DataFrame([{
                "Rank": t.get("rank"), "Contract": t.get("tradingsymbol"), "Score": t.get("strike_score"),
                "Conf": t.get("confidence"), "Delta": t.get("delta"), "Theta risk %": t.get("theta_risk_pct"),
                "Liquidity": t.get("liquidity_score"), "Exec Q": t.get("execution_quality"),
                "Proj gain": (t.get("projection") or {}).get("premium_gain"),
            } for t in top]), width="stretch", hide_index=True)
            for line in _jl(sie.get("reasons"), []):
                st.markdown(f"- {line}")
        else:
            st.info(f"No SIE output yet ({sie.get('reason') or 'waiting for a BUY entry trigger'}).")
        st.markdown("**Probability components**")
        comps = {k[2:]: prob.get(k) for k in prob if k.startswith("p_")}
        st.json(comps or {"status": "EMPTY"})
        st.caption(f"Rejects: {prob.get('reject_reasons') or '[]'} · Flags: {prob.get('flags') or '[]'} · history {prob.get('history_samples') or 0} trades")
    with s2:
        st.markdown("**ICARE execution report**")
        if ic:
            st.dataframe(pd.DataFrame([
                ("Trade", f"BUY {ic.get('tradingsymbol')}"), ("Status", ic.get("status")),
                ("Probability", ic.get("probability")), ("Expected value / lot", f"₹{_fmt(ic.get('expected_value'))} ({ic.get('ev_source')})"),
                ("Risk class", ic.get("risk_class")), ("Allocation", f"{ic.get('allocation_pct')}% = ₹{_fmt(ic.get('max_capital'))}"),
                ("Available margin", f"₹{_fmt(ic.get('available_margin'))}"), ("Margin / lot", f"₹{_fmt(ic.get('margin_per_lot'))}"),
                ("Lots by margin / risk / capital / portfolio / liquidity",
                 f"{ic.get('lots_by_margin')} / {ic.get('lots_by_risk')} / {ic.get('lots_by_capital')} / {ic.get('lots_by_portfolio')} / {ic.get('lots_by_liquidity') or '—'}"),
                ("Recommended lots", ic.get("recommended_lots")), ("Entry / SL / Target", f"{ic.get('premium')} / {ic.get('stop_loss_premium')} / {ic.get('target_premium')}"),
                ("Expected max loss", f"₹{_fmt(ic.get('expected_max_loss'))}"), ("Expected reward", f"₹{_fmt(ic.get('expected_reward'))}"),
                ("Reward : Risk", ic.get("reward_risk")), ("Reasons", ic.get("reasons")), ("Flags", ic.get("flags")),
            ], columns=["Field", "Value"]).astype(str), width="stretch", hide_index=True)
        else:
            st.info("No ICARE report yet.")

    st.markdown("**ICARE economics / flags**")
    econ = {k: ic.get(k) for k in ("gross_ev", "charges", "charges_per_lot", "net_ev", "expected_value", "ev_source",
                                    "exec_mode", "signal_ts_ms", "flags") if ic.get(k) not in (None, "")}
    if econ:
        st.json(econ, expanded=False)
    else:
        st.caption("No gross / charges / net EV fields on the ICARE payload yet.")

    rk = c_ranking(r, n_rows, key_cap)
    st.markdown("**Trade ranking (Module 13)**")
    cyc = rk.get("cycle") or {}
    t1, t2 = st.columns([3, 1])
    with t1:
        if cyc:
            st.caption(
                f"Last cycle {_age(cyc)} · **{cyc.get('outcome')}** · scanned {cyc.get('scanned')} · "
                f"insufficient {cyc.get('data_insufficient')} · rejected {cyc.get('rejected')} · watch {cyc.get('watch')} · "
                f"eligible {cyc.get('eligible')} · taken {cyc.get('taken')} · profile {cyc.get('profile')} · mode {cyc.get('mode')}"
            )
        else:
            st.caption("No ranking cycle yet (run_trade_ranking.py).")
    with t2:
        ks = dd.kill_switch_state(r)
        st.markdown(f"**Kill switch** {_pill('ON', 'bad') if ks['on'] else _pill('OFF', 'ok')}", unsafe_allow_html=True)
        st.caption(f"read-only · `redis-cli SET {ks['key']} 1` to block new trades")
    rk_rows = rk.get("latest") or []
    show_table([{
        "Rank": x.get("rank"), "Contract": x.get("tradingsymbol"), "Score": x.get("trade_score"),
        "Decision": x.get("rank_decision"), "Confidence": x.get("rank_confidence"), "Prob": x.get("probability"),
        "Agree": x.get("rank_agreement"), "EV": x.get("rank_expected_value"), "EV/risk": x.get("rank_ev_per_risk"),
        "RR": x.get("rank_reward_risk"), "Lots": x.get("rank_feasible_lots"), "SL %": x.get("rank_initial_stop_loss_pct"),
        "TSL %": x.get("rank_trailing_stop_pct"), "Why": x.get("rank_reject_reasons"), "Emitted": x.get("rank_emit"),
        "Age": dd.fmt_age(x.get("_age_s")), "Fresh": dd.freshness(x.get("_age_s"), dd.effective_threshold(dd.LAYER_BY_ID["ranking"], stale_override)),
    } for x in rk_rows], fresh_col="Fresh", empty="No ranking rows yet.")
    z1, z2, z3 = st.columns(3)
    with z1:
        st.markdown("**md:ranking:rank (zset)**")
        show_table([{"member": m, "score": sc} for m, sc in rk.get("rank_zset") or []], empty="empty")
    with z2:
        st.markdown("**md:probability:rank (zset)**")
        show_table([{"member": m, "score": sc} for m, sc in rk.get("probability_zset") or []], empty="empty")
    with z3:
        st.markdown("**md:ranking:book (candidates)**")
        book = rk.get("book") or {}
        show_table([{"field": f, "arrived": dd.fmt_age(dd.age_sec({"ts_ms": (v or {}).get("arrived_ms")})) if isinstance(v, dict) else "",
                     "emitted": (v or {}).get("emitted") if isinstance(v, dict) else "",
                     "last_decision": (v or {}).get("last_decision") if isinstance(v, dict) else v}
                    for f, v in book.items()], empty="empty")
    with st.expander("Ranking cycles (stream md:ranking:cycle)"):
        show_table(rk.get("cycles") or [], fresh_col="_fresh", empty="No cycles.")

    st.markdown("**Account**")
    acct_s = dd.account_snapshot(r)
    acct = acct_s["doc"]
    st.markdown(fresh_pill(acct_s["fresh"], dd.fmt_age(acct_s["age_s"]) if acct else ""), unsafe_allow_html=True)
    st.json(acct if acct else {"status": "run_account.py not running"}, expanded=False)


# ── Decision chain ───────────────────────────────────────────────────
VERDICT_COLOURS = {
    "PASS": "background-color: #dcfae6; color: #087443",
    "REJECTED": "background-color: #fff4e5; color: #b54708",
    "NO_SIGNAL": "background-color: #f2f4f7; color: #667085",
    "MISSING": "background-color: #f2f4f7; color: #98a2b3",
    "INFO": "color: #667085",
}


def chain_pill(c: dict) -> str:
    # A rejected / no-signal doc is fresh data but not a live trigger: grey it out.
    if c["verdict"] in ("REJECTED", "NO_SIGNAL"):
        return _pill(f"{c['verdict']} {c['age']}".strip(), "empty")
    return fresh_pill(c["fresh"], c["age"] if c["present"] else "")


with T["Decision chain"]:
    st.subheader(f"Decision chain · {symbol}")
    st.caption("level break → entry trigger → strike select → strike intel → probability → ranking → ICARE → exec → TSL → position → journal. "
               "Verdict = did the stage pass the trade on; Fresh = age of its output. "
               "A FRESH stage downstream of a STALE one usually means a replay.")
    chain = c_chain(r, symbol, key_cap, stale_override)
    blk = dd.chain_blocker(chain)
    if blk is None:
        st.success("Every gate passed — trade went through to execution.")
    elif blk["verdict"] in ("NO_SIGNAL", "REJECTED"):
        st.info(f"**Blocked at {blk['name']}** ({blk['verdict']}): {blk['reason']}. "
                "Later stages stay empty until this stage passes — that is expected, not missing data.")
    else:
        st.warning(f"**Blocked at {blk['name']}** ({blk['verdict']}): {blk['reason']}")
    rows_c = [{"Stage": c["name"], "Verdict": c["verdict"], "Fresh": c["fresh"], "Age": c["age"],
               "Note": c.get("note") or "", "Flags": c["flags"], "Summary": c["summary"], "Key": c["key"] or "—"}
              for c in chain]
    st.dataframe(_df(rows_c).style.map(lambda v: VERDICT_COLOURS.get(str(v), ""), subset=["Verdict"])
                 .map(lambda v: FRESH_COLOURS.get(str(v), ""), subset=["Fresh"]),
                 width="stretch", hide_index=True)
    cols = st.columns(3)
    for i, c in enumerate(chain):
        with cols[i % 3]:
            st.markdown(f"**{c['name']}** {chain_pill(c)}", unsafe_allow_html=True)
            st.json(c["doc"] or {"status": "MISSING", "key": c["key"], "note": c.get("note")}, expanded=False)
    st.markdown("**Chain across the universe**")
    uni = []
    for s_ in symbols:
        ch_list = c_chain(r, s_, key_cap, stale_override)
        b_ = dd.chain_blocker(ch_list)
        row = {"Symbol": s_, "Blocked at": b_["name"] if b_ else "— (traded)", "Why": (b_ or {}).get("reason", "")}
        for c in ch_list:
            row[c["stage"]] = (c["verdict"] if c["verdict"] not in ("PASS", "INFO") or not c["present"]
                               else f"{c['fresh']} {c['age']}").strip()
        uni.append(row)
    stage_cols = [c["stage"] for c in chain]
    if uni:
        st.dataframe(_df(uni).style.map(lambda v: VERDICT_COLOURS.get(str(v), FRESH_COLOURS.get(str(v).split(" ")[0], "")),
                                        subset=stage_cols),
                     width="stretch", hide_index=True)

# ── Symbol data (every latest key) ───────────────────────────────────
with T["Symbol data"]:
    st.subheader(f"Every latest-key doc · {symbol} and its option contracts")
    docs = c_symbol_docs(r, symbol, key_cap, stale_override)
    counts = dd.layer_freshness_table(docs)
    st.caption(" · ".join(f"{k}: {v}" for k, v in sorted(counts.items())) or "no docs")
    f1, f2, f3 = st.columns([1, 1, 2])
    with f1:
        groups = sorted({x["group"] for x in docs})
        g_sel = st.multiselect("Layer group", groups, default=groups)
    with f2:
        scope_sel = st.multiselect("Scope", ["symbol", "contract", "global"], default=["symbol", "contract", "global"])
    with f3:
        only_stale = st.checkbox("Only stale / no-ts", value=False)
    view = [x for x in docs if x["group"] in g_sel and x["scope"] in scope_sel
            and (not only_stale or x["fresh"] in ("STALE", "NO_TS"))]
    show_table([{"Group": x["group"], "Layer": x["name"], "Scope": x["scope"], "Contract": x["contract"],
                 "Fresh": x["fresh"], "Age": x["age"], "Flags": x["flags"], "Key": x["key"]} for x in view],
               fresh_col="Fresh", height=420, empty="No latest keys for this symbol.")
    for g in [g for g in groups if g in g_sel]:
        with st.expander(f"{g} — payloads ({sum(1 for x in view if x['group'] == g)})"):
            for x in [x for x in view if x["group"] == g]:
                st.markdown(f"`{x['key']}` {fresh_pill(x['fresh'], x['age'])}", unsafe_allow_html=True)
                st.json(x["doc"], expanded=False)
    with st.expander("Instrument metadata (meta:eq / meta:opt, md:active_expiry)"):
        st.json({"active_expiry": dd.active_expiry(r), "meta_eq": dd.eq_meta(r, symbol)}, expanded=False)
        if st.checkbox("Load subscribed option contracts (scans meta:opt:*, capped)", value=False):
            show_table(c_contracts(r, symbol), empty="No meta:opt contracts for this symbol.")

# ── Execution ────────────────────────────────────────────────────────
with T["Execution"]:
    ex = c_exec(r, n_rows, key_cap)
    ks = ex["kill_switch"]
    e1, e2, e3, e4 = st.columns(4)
    with e1:
        kpi("Kill switch", "ON" if ks["on"] else "OFF", f"{ks['key']} = {ks['raw'] or '(unset)'}")
    with e2:
        kpi("Executing now", str(len(ex["active"])), ", ".join(f"{t} ({v})" for t, v in ex["active"].items())[:60])
    with e3:
        kpi("Missed-move checks", str(len(ex["missed"])), "md:exec:missed")
    with e4:
        mode = (ex["reports"][0].get("mode") if ex["reports"] else "") or "—"
        kpi("Exec mode", str(mode), "from latest report")
    st.markdown("**Final reports (md:exec:latest:{trade_id})**")
    show_table([{
        "Trade": x.get("trade_id"), "Contract": x.get("option_symbol"), "Status": x.get("execution_status"),
        "Mode": x.get("mode"), "Lots": f"{x.get('filled_lots')}/{x.get('requested_lots')}", "Avg fill": x.get("average_fill_price"),
        "Ask (ref)": x.get("reference_price"), "Cap": x.get("price_cap"), "Slippage": x.get("slippage"),
        "Slip %": x.get("slippage_pct"), "Charges": (x.get("charges") or {}).get("total") if isinstance(x.get("charges"), dict) else x.get("charges"),
        "Exec cost": x.get("total_execution_cost"), "Orders": x.get("orders_used"), "ms": x.get("execution_duration_ms"),
        "Signal age at exec (s)": (round((fnum(x.get("received_ms"), 0) - fnum(x.get("signal_ts_ms"), 0)) / 1000.0, 2)
                                    if fnum(x.get("signal_ts_ms")) and fnum(x.get("received_ms")) else ""),
        "Why": ", ".join(x.get("reject_reasons") or []) if isinstance(x.get("reject_reasons"), list) else (x.get("reject_reasons") or x.get("cancel_reason")),
        "Age": dd.fmt_age(x.get("_age_s")),
    } for x in ex["reports"]], empty="No executions yet — run_order_executor.py executes ICARE approvals.")
    tabs_ex = st.tabs(["Events (md:exec)", "Fills (md:exec:fill)", "Exit requests", "Exit orders", "By contract",
                       "State machines", "Active / missed"])
    with tabs_ex[0]:
        show_table(ex["events"], empty="No exec events.")
    with tabs_ex[1]:
        show_table(ex["fills"], empty="No fills.")
    with tabs_ex[2]:
        if not ex["exit_request_exists"]:
            st.caption(f"`{dd.K['exec_exit_request']}` does not exist yet (exit-request stream not produced).")
        show_table(ex["exit_requests"], empty="No exit requests.")
        st.markdown(f"**Exit fills (`{dd.K['exec_exit_fill']}`)**")
        show_table(ex.get("exit_fills") or [], empty="No exit fills.")
    with tabs_ex[3]:
        st.markdown(f"**Exits in progress (`{dd.K['exec_exit_active']}`)** — position trade_id → exit_id")
        show_table([{"position": k, "exit_id": v} for k, v in ex["exit_active"].items()], empty="No active exits.")
        st.markdown(f"**Exit state machines (`{dd.K['exec_exit_state']}*`)**")
        show_table([{k: v for k, v in x.items() if k not in ("orders",)} for x in ex["exit_states"]],
                   empty="No exit state machines.")
    with tabs_ex[4]:
        show_table([{k: v for k, v in x.items() if k not in ("orders",)} for x in ex["reports_by_tsym"]], empty="No md:exec:latest:tsym:* keys.")
    with tabs_ex[5]:
        show_table(ex["states"], empty="No md:exec:state:* keys.")
    with tabs_ex[6]:
        st.json({"md:exec:active": ex["active"], "md:exec:missed": ex["missed"],
                 "md:exec:exit:active": ex["exit_active"]}, expanded=False)

# ── TSL ──────────────────────────────────────────────────────────────
with T["TSL"]:
    ts_ = c_tsl(r, n_rows, key_cap)
    tsl_rows = ts_["latest"]
    if tsl_rows:
        st.caption(f"Mode: {tsl_rows[0].get('mode') or '—'} (shadow = observes only; active = closes trades and re-enters via ICARE)")
    st.markdown("**Latest per contract (md:tsl:latest:{TSYM})**")
    show_table([{
        "Contract": t.get("tradingsymbol"), "Status": t.get("status"), "Event": t.get("event"), "Bid": t.get("current_price"),
        "High": t.get("highest_price"), "Stop": t.get("current_trailing_stop"), "TSL %": t.get("trailing_percentage"),
        "Rule": t.get("tsl_rule"), "Trend": t.get("trend_strength"), "Vol": t.get("volatility_score"),
        "Activated": t.get("activated"), "Re-entry": t.get("reentry_state"), "Watch": t.get("watch_price"),
        "Why waiting": t.get("wait_reason"), "Virtual": t.get("virtual"), "Age": dd.fmt_age(t.get("_age_s")), "Fresh": t.get("_fresh"),
    } for t in tsl_rows], fresh_col="Fresh", empty="No trailing-stop state yet — appears once a position is open (run_adaptive_tsl.py).")
    tt = st.tabs(["States", "Re-entry chains", "Blocks", "Events (md:tsl)", "Re-entry (md:tsl:reentry)", "Sets / origins / exit context"])
    with tt[0]:
        show_table(ts_["states"], empty="No md:tsl:state:* keys.")
    with tt[1]:
        show_table(ts_["chains"], empty="No md:tsl:chain:* keys.")
    with tt[2]:
        show_table(ts_["blocks"], empty="No md:tsl:block:* keys (no symbol/side blocked).")
    with tt[3]:
        show_table(ts_["events"], fresh_col="_fresh", empty="No md:tsl events.")
    with tt[4]:
        show_table(ts_["reentry"], empty="No re-entries.")
    with tt[5]:
        st.json({"sets": ts_["sets"], "origins": ts_["origins"], "exit_contexts": ts_["exit_contexts"]}, expanded=False)

# ── Positions & journal ──────────────────────────────────────────────
with T["Positions & journal"]:
    jo = c_journal(r, n_rows, key_cap)
    pos = jo["positions"]
    today = dt.datetime.now(IST).date().isoformat()
    td = next((x for x in jo["daily"] if x.get("date") == today), {})
    j1, j2, j3, j4 = st.columns(4)
    with j1:
        kpi("Open positions", str(len(pos)), "md:position:open:*")
    with j2:
        upnl = sum(fnum(p.get("unrealized_pnl"), 0) or 0 for p in pos)
        kpi("Unrealized PnL", _fmt(upnl), "mark at last_premium")
    with j3:
        kpi("Realized today", _fmt(td.get("realized_pnl")), f"{td.get('trades') or 0} trades · {td.get('wins') or 0} wins")
    with j4:
        allb = next((x for x in jo["stats"] if x["bucket"] == "ALL"), {})
        kpi("All-time win rate", "—" if allb.get("win_rate") is None else f"{allb['win_rate'] * 100:.1f}%",
            f"{allb.get('trades') or 0} trades · PnL {_fmt(allb.get('pnl'))}")
    st.markdown("**Open positions**")
    show_table([{
        "Trade": p.get("trade_id"), "Contract": p.get("tradingsymbol"), "Side": p.get("side"), "Lots": p.get("lots"),
        "Entry": p.get("entry_premium"), "Last": p.get("last_premium"), "Max": p.get("max_premium"), "Min": p.get("min_premium"),
        "SL": p.get("sl_premium"), "Target": p.get("target_premium"), "Unrealized": p.get("unrealized_pnl"),
        "Held": dd.fmt_age(p.get("_age_s")),
    } for p in pos], empty="No open positions.")
    jj = st.tabs(["Closed trades (md:journal)", "Closed records", "Bucket stats", "Daily"])
    with jj[0]:
        show_table(jo["stream"], empty="Journal empty — closed trades appear here.")
    with jj[1]:
        show_table(jo["closed"], empty="No md:journal:closed:* keys.")
    with jj[2]:
        show_table(jo["stats"], empty="No md:journal:stats yet.")
    with jj[3]:
        show_table(jo["daily"], empty="No md:journal:daily:* keys.")

# ── Streams ──────────────────────────────────────────────────────────
with T["Streams"]:
    ov = c_streams(r, stale_override)
    st.markdown("**All pipeline streams — length and newest-entry age**")
    show_table(ov, cols=["group", "layer", "stream", "length", "newest_age", "fresh"], fresh_col="fresh", height=420)
    s1, s2 = st.columns([2, 1])
    with s1:
        names = [x["stream"] for x in ov]
        pick = st.selectbox("Stream", names, index=0)
    with s2:
        filt = st.checkbox(f"Only {symbol}", value=False)
    layer = next((l for l in dd.LAYERS if l.stream == pick), None)
    rows_ = c_tail(r, pick, n_rows, symbol if filt else "")
    thr = dd.effective_threshold(layer, stale_override) if layer else None
    for x in rows_:
        x["_fresh"] = dd.freshness(x.get("_age_s"), thr)
    show_table(rows_, fresh_col="_fresh", height=480, empty="Stream empty.")

# ── Pipeline health ──────────────────────────────────────────────────
with T["Pipeline health"]:
    st.caption("Same checks as `python pipeline_health.py`: OK = newest output younger than the module's limit, "
               "STALE = data exists but is old, IDLE = worker running but nothing to emit yet (e.g. no entry signal), "
               "DOWN = worker process not running, FAIL = nothing in Redis and worker state unknown.")
    try:
        hrows = c_health(r, key_cap, stale_override)
    except Exception as e:  # pragma: no cover - defensive
        hrows = []
        st.error(f"health check failed: {e}")
    hc = {}
    for h in hrows:
        hc[h["status"]] = hc.get(h["status"], 0) + 1
    st.markdown(" ".join(fresh_pill(k, str(v)) for k, v in sorted(hc.items())), unsafe_allow_html=True)
    show_table([{
        "Layer": f"{h['layer']} {h['layer_name']}", "Module": h["name"], "Worker": h.get("worker") or "—",
        "Process": {True: "running", False: "not running"}.get(h.get("alive"), "—"),
        "Status": h["status"], "Newest age": dd.fmt_age(h.get("newest_age_sec")) if h["status"] != "NOT_IMPL" else "",
        "Limit (s)": "" if h.get("max_age_sec") is None else int(h["max_age_sec"]),
        "Components": "; ".join(f"{c['name']}={c['count']}" + ("" if c.get("age_sec") is None else f" @{dd.fmt_age(c['age_sec'])}")
                                 for c in h.get("components") or []),
    } for h in hrows], fresh_col="Status", height=600)

# ── Raw Redis explorer (read-only) ───────────────────────────────────
with T["Raw Redis explorer"]:
    st.caption("Read-only SCAN (never KEYS). Pattern examples: `md:*:latest:RELIANCE*`, `md:exec:*`, `meta:opt:*`.")
    x1, x2, x3 = st.columns([3, 1, 1])
    with x1:
        pattern = st.text_input("Key pattern", value=f"md:*{symbol}*")
    with x2:
        cap = int(st.number_input("Max keys", min_value=10, max_value=5000, value=200, step=50))
    with x3:
        n_items = int(st.number_input("Items per value", min_value=1, max_value=500, value=20, step=5))
    found = dd.explore(r, pattern, cap=cap)
    st.caption(f"{len(found)} key(s){' (capped)' if len(found) >= cap else ''}")
    show_table(found, height=300, empty="No keys match.")
    if found:
        sel = st.selectbox("Inspect key", [f["key"] for f in found])
        info = dd.read_key(r, sel, n=n_items)
        st.markdown(f"`{info['key']}` · type **{info['type']}** · TTL {info['ttl']}")
        v = info["value"]
        if isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
            show_table(v)
            with st.expander("JSON"):
                st.json(v, expanded=False)
        elif isinstance(v, (dict, list)):
            st.json(v)
        else:
            st.code(str(v))

# Raw
with T["Raw payloads"]:
    st.caption("Full Redis snapshot for this symbol — for debugging / client questions.")
    pretty = {k: v for k, v in d.items() if k != "candles_1m"}
    st.json(pretty)

if refresh_s and refresh_s > 0:
    import time as _t

    _t.sleep(int(refresh_s))
    st.rerun()

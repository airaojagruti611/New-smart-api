#!/usr/bin/env python3
"""Option Rider — live production dashboard (Streamlit)."""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

from app.dashboard_data import (
    age_sec,
    collect_symbol,
    connect,
    fnum,
    redis_health,
    universe,
)

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
    if a < 90:
        return f"{a:.0f}s ago"
    if a < 3600:
        return f"{a / 60:.1f}m ago"
    return f"{a / 3600:.1f}h ago"


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


# ── Sidebar ──────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Option Rider")
    st.caption("Live layers from Redis · NSE F&O")
    refresh_s = st.select_slider("Auto-refresh (seconds)", options=[0, 2, 5, 10, 15, 30], value=5)
    st.caption("0 = pause live updates")

try:
    r = connect()
    health = redis_health(r)
except Exception as e:
    health = {"ok": False, "error": str(e), "eq": 0, "opt": 0, "c1m": 0, "greeks": 0, "keys": 0}
    r = None

symbols = universe(r) if r is not None else []
if not symbols:
    symbols = ["RELIANCE", "TCS", "INFY"]

with st.sidebar:
    symbol = st.selectbox("Symbol", symbols, index=0)
    if st.button("Refresh now", use_container_width=True):
        st.rerun()
    st.divider()
    if health.get("ok"):
        st.markdown(_pill("REDIS LIVE", "ok"), unsafe_allow_html=True)
        st.caption(f"EQ ticks {health['eq']:,} · OPT {health['opt']:,} · 1m candles {health['c1m']:,}")
        st.caption(f"Greeks snap {health['greeks']:,}")
    else:
        st.markdown(_pill("REDIS DOWN", "bad"), unsafe_allow_html=True)
        st.caption(str(health.get("error") or "Cannot reach Redis"))
    st.divider()
    st.caption("Export client Excel")
    st.code("./export_client_excel.sh", language="bash")

if not health.get("ok") or r is None:
    st.error("Cannot connect to Redis. Start the pipeline with `./run_all.sh` (Redis on localhost:6379).")
    st.stop()

d = collect_symbol(r, symbol)
all_data = {s: collect_symbol(r, s) for s in symbols}

entry = d.get("entry") or {}
signal = str(entry.get("signal") or (d.get("momentum") or {}).get("signal") or "NEUTRAL")
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
    kpi("Decision", signal, str(entry.get("reason") or entry.get("strength") or "")[:48])
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
    move = em.get("final_expected_move") or em.get("expected_move_pct") or em.get("expected_move")
    kpi("Expected move", _fmt(move, 2), str(em.get("direction") or ""))

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
            "Decision": e.get("signal") or (sd.get("momentum") or {}).get("signal") or "—",
            "Reason": (e.get("reason") or "")[:80],
            "ST": (sd.get("st") or {}).get("bias") or "—",
            "EMA": (sd.get("ema") or {}).get("state") or "—",
            "HTF": (sd.get("htf") or {}).get("bias") or "—",
            "Volume": (sd.get("volume") or {}).get("signal") or "—",
            "OI": (sd.get("oi_und") or {}).get("positioning") or "—",
            "Greeks CE": (sd.get("greeks_ce") or {}).get("phase") or "—",
            "Composite": _fmt((sd.get("composite") or {}).get("score"), 3),
            "Strike": (sd.get("strike") or {}).get("tradingsymbol") or "—",
        }
    )
st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True, height=148)

tabs = st.tabs(
    [
        "Decision gate",
        "Indicators",
        "Volume & regime",
        "Bid-ask / flow",
        "Options / OI / Greeks",
        "Expected move",
        "Raw payloads",
    ]
)

# Decision
with tabs[0]:
    left, right = st.columns([1.15, 1])
    with left:
        st.subheader("Seven-filter entry gate")
        st.caption("A BUY fires only when these align. WAIT is expected most of the session.")
        for ok, title, detail in entry_gates(d):
            gate_row(ok, title, detail)
    with right:
        st.subheader("Final output")
        st.markdown(
            f"<div class='reason'>signal={signal}<br>strength={entry.get('strength') or '—'}<br>"
            f"level={entry.get('level') or '—'}<br>aligned={entry.get('aligned') or '—'}<br>"
            f"reason={entry.get('reason') or 'no entry_trigger payload yet'}</div>",
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
with tabs[1]:
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
with tabs[2]:
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
        st.warning("With 3 symbols this is not NSE breadth — only RELIANCE / TCS / INFY.")
        st.json(d.get("regime") or {"status": "EMPTY"})

# Bid-ask
with tabs[3]:
    r1 = st.columns(3)
    r2 = st.columns(3)
    blocks = [
        (r1[0], "Bid-ask / spread", "bidask"),
        (r1[1], "Imbalance (−1…+1)", "imbalance"),
        (r1[2], "Composite score", "composite"),
        (r2[0], "Order flow", "orderflow"),
        (r2[1], "Smart money", "smartmoney"),
        (r2[2], "Stock entry/exit", "stockflow"),
    ]
    for col, title, key in blocks:
        with col:
            st.markdown(f"**{title}**")
            doc = d.get(key) or {}
            st.markdown(_pill("OK", "ok") if filled(doc) else _pill("EMPTY", "empty"), unsafe_allow_html=True)
            st.json(doc or {"status": "EMPTY"})

# Options
with tabs[4]:
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
        st.markdown("**Liquidity (selected/ATM CE)**")
        st.json(d.get("liquidity") or {"status": "EMPTY"})
        st.markdown("**Strike flow**")
        st.json(d.get("strikeflow") or {"status": "EMPTY"})

# Expected move
with tabs[5]:
    e1, e2 = st.columns(2)
    with e1:
        st.markdown("**Expected move (prediction only — no strike)**")
        st.json(d.get("expected") or {"status": "EMPTY"})
        st.caption("Empty IV means REST greeks have not populated yet. indicator_score comes from Supertrend + EMA + pivot strength.")
    with e2:
        st.markdown("**Greeks change (needs a selected option)**")
        st.json(d.get("greeks_change") or {"status": "EMPTY until strike_select OK"})
        st.markdown("**Last 1m candle**")
        st.json(d.get("c1m") or {"status": "EMPTY"})

# Raw
with tabs[6]:
    st.caption("Full Redis snapshot for this symbol — for debugging / client questions.")
    pretty = {k: v for k, v in d.items() if k != "candles_1m"}
    st.json(pretty)

if refresh_s and refresh_s > 0:
    import time as _t

    _t.sleep(int(refresh_s))
    st.rerun()

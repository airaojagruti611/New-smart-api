"""
run_adaptive_tsl.py
───────────────────
Module 18 — Adaptive Trailing Stop Loss & Re-entry Engine, Redis wiring
(DECISION.md §5). Pure logic lives in app/adaptive_tsl.py.

Every TSL_LOOP_SEC:
  1. each paper position md:position:open:{TSYM} (written ONLY by the journal)
     -> manage() -> md:tsl:state:{trade_id}
  2. positions that disappeared while still ACTIVE (journal closed them on
     SL / TARGET / TIME / EOD) -> read md:journal:closed:{trade_id} -> chain update
  3. shadow re-entry trades simulated by the engine (TSL_MODE=shadow only)
  4. re-entry chains md:tsl:chain:{chain_id}: watch -> REENTER -> md:tsl:reentry
     (ICARE sizes it; the journal opens it) / pending timeout / BLOCKED
  5. block maintenance: md:tsl:block:{SYM}:{SIDE} cleared once Supertrend flips

Reads (None when missing or older than TSL_MAX_INPUT_AGE_SEC):
  md:bidask:latest:{TSYM|SYM}  md:greeks:phase:latest:{TSYM}  md:imbalance:latest:{TSYM}
  md:smartmoney:latest:{TSYM}  md:liquidity:score:latest:{TSYM}  md:expected_move:latest:{SYM}
  md:supertrend:bias:latest:{SYM}  md:ema:cross:latest:{SYM}  md:volume:latest
  md:oi:underlying:latest:{SYM}  md:htf:trend:latest:{SYM}  md:pivots:prevday:{SYM}
  md:candles:1m (ATR, swings, fib)  md:regime:latest  md:composite:latest:{SYM}
  md:icare:origin:{TSYM} (probability input of the approved trade, written by ICARE)

Emits:
  Stream : md:tsl            events (stop moved, activation, exit, re-entry, chain changes)
                             + a heartbeat per trade every TSL_HEARTBEAT_SEC
  Key    : md:tsl:latest:{TSYM}
  Stream : md:tsl:reentry    REENTER signals (probability-shaped payload for ICARE)

TSL_MODE=shadow (default): nothing changes the real trades; the engine runs its
own lifecycle (virtual exit, simulated re-entries) and the journal stores the
counterfactual. TSL_MODE=active: the journal closes on a validated TSL exit and
ICARE processes re-entries.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time
from dataclasses import replace
from typing import Dict, List, Optional, Tuple

import redis

from app.adaptive_tsl import (
    BLOCKED,
    DONE,
    EXPIRED,
    IN_TRADE,
    PENDING,
    WAITING,
    Chain,
    Snapshot,
    TradeState,
    TSLConfig,
    band_floor,
    evaluate_reentry,
    exit_output,
    manage,
    new_trade,
    on_reentry_opened,
    on_trade_closed,
    pending_timed_out,
    record_ema_gap,
    revert_pending,
)
from app.candle_io import read_last_candles
from app.logging_setup import setup_logger
from app.market_structure import atr_pct, fib_levels, last_swing_high, last_swing_low
from app.option_pricing import IST
from app.probability_engine import compute_probability
from run_probability import FILTERS, build_inputs, build_payload, is_sideways, live_sie_fields

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MODE = os.getenv("TSL_MODE", "shadow").strip().lower()
LOOP_SEC = float(os.getenv("TSL_LOOP_SEC", "2"))
HEARTBEAT_SEC = float(os.getenv("TSL_HEARTBEAT_SEC", "60"))
MAX_INPUT_AGE_MS = int(float(os.getenv("TSL_MAX_INPUT_AGE_SEC", "180")) * 1000)
CANDLE_REFRESH_SEC = float(os.getenv("TSL_CANDLE_REFRESH_SEC", "30"))
CANDLE_LIMIT = int(os.getenv("TSL_CANDLE_LIMIT", "120"))
EOD_HHMM = os.getenv("JOURNAL_EOD_HHMM", "15:20")
STATE_TTL = int(os.getenv("TSL_STATE_TTL_SEC", str(3 * 86400)))

OUT_STREAM = os.getenv("STREAM_TSL", "md:tsl")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_TSL", "200000"))
REENTRY_STREAM = os.getenv("STREAM_TSL_REENTRY", "md:tsl:reentry")
LATEST_PREFIX = os.getenv("TSL_LATEST_PREFIX", "md:tsl:latest:")
STATE_PREFIX = os.getenv("TSL_STATE_PREFIX", "md:tsl:state:")
CHAIN_PREFIX = os.getenv("TSL_CHAIN_PREFIX", "md:tsl:chain:")
ORIGIN_PREFIX = os.getenv("TSL_ORIGIN_PREFIX", "md:tsl:origin:")
BLOCK_PREFIX = os.getenv("TSL_BLOCK_PREFIX", "md:tsl:block:")
TRADES_SET = os.getenv("TSL_TRADES_SET", "md:tsl:trades")          # real trade ids being managed
VIRTUAL_SET = os.getenv("TSL_VIRTUAL_SET", "md:tsl:virtual")       # shadow re-entry trade ids
CHAINS_SET = os.getenv("TSL_CHAINS_SET", "md:tsl:chains")          # open chain ids

POSITION_OPEN_PREFIX = os.getenv("POSITION_OPEN_PREFIX", "md:position:open:")
JOURNAL_CLOSED_PREFIX = os.getenv("JOURNAL_CLOSED_PREFIX", "md:journal:closed:")
ICARE_ORIGIN_PREFIX = os.getenv("ICARE_ORIGIN_PREFIX", "md:icare:origin:")
BIDASK_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")
GREEKS_PREFIX = os.getenv("GREEKS_PHASE_LATEST_PREFIX", "md:greeks:phase:latest:")
IMBALANCE_PREFIX = os.getenv("IMBALANCE_LATEST_PREFIX", "md:imbalance:latest:")
SMARTMONEY_PREFIX = os.getenv("SMARTMONEY_LATEST_PREFIX", "md:smartmoney:latest:")
LIQUIDITY_PREFIX = os.getenv("LIQUIDITY_SCORE_LATEST_PREFIX", "md:liquidity:score:latest:")
EM_PREFIX = os.getenv("EXPECTED_MOVE_LATEST_PREFIX", "md:expected_move:latest:")
ST_PREFIX = os.getenv("SUPERTREND_BIAS_LATEST_PREFIX", "md:supertrend:bias:latest:")
EMA_PREFIX = os.getenv("EMA_CROSS_LATEST_PREFIX", "md:ema:cross:latest:")
VOLUME_KEY = os.getenv("VOLUME_LATEST_KEY", "md:volume:latest")
OI_UND_PREFIX = os.getenv("OI_UNDERLYING_LATEST_PREFIX", "md:oi:underlying:latest:")
HTF_PREFIX = os.getenv("HTF_TREND_LATEST_PREFIX", "md:htf:trend:latest:")
PIVOTS_PREFIX = os.getenv("PIVOTS_PREFIX", "md:pivots:prevday:")
COMPOSITE_PREFIX = os.getenv("COMPOSITE_LATEST_PREFIX", "md:composite:latest:")
REGIME_KEY = os.getenv("REGIME_LATEST_KEY", "md:regime:latest")
JOURNAL_STATS_KEY = os.getenv("JOURNAL_STATS_KEY", "md:journal:stats")
CANDLES_1M = os.getenv("STREAM_CANDLES_1M", "md:candles:1m")


def _env_range(name: str, default: Tuple[float, float]) -> Tuple[float, float]:
    raw = os.getenv(name, "")
    try:
        lo, hi = (float(x) for x in raw.split(","))
        return lo, hi
    except Exception:
        return default


_D = TSLConfig()
CFG = TSLConfig(
    range_gamma=_env_range("TSL_RANGE_GAMMA", _D.range_gamma),
    range_expiry=_env_range("TSL_RANGE_EXPIRY", _D.range_expiry),
    range_strong_low_vol=_env_range("TSL_RANGE_STRONG_LOW_VOL", _D.range_strong_low_vol),
    range_strong_high_vol=_env_range("TSL_RANGE_STRONG_HIGH_VOL", _D.range_strong_high_vol),
    range_moderate=_env_range("TSL_RANGE_MODERATE", _D.range_moderate),
    range_weak=_env_range("TSL_RANGE_WEAK", _D.range_weak),
    hard_breach_pct=float(os.getenv("TSL_HARD_BREACH_PCT", str(_D.hard_breach_pct))),
    confirm_max_sec=float(os.getenv("TSL_CONFIRM_MAX_SEC", str(_D.confirm_max_sec))),
    min_liquidity=float(os.getenv("SIE_MIN_LIQUIDITY", str(_D.min_liquidity))),
    max_spread_pct=float(os.getenv("PROB_MAX_SPREAD_PCT", str(_D.max_spread_pct))),
    max_reentries=int(os.getenv("TSL_MAX_REENTRIES", str(_D.max_reentries))),
    cooldown_min=float(os.getenv("TSL_COOLDOWN_MIN", str(_D.cooldown_min))),
    watch_min=float(os.getenv("TSL_REENTRY_WATCH_MIN", str(_D.watch_min))),
    pending_ack_sec=float(os.getenv("TSL_REENTRY_ACK_SEC", str(_D.pending_ack_sec))),
    retry_sec=float(os.getenv("TSL_REENTRY_RETRY_SEC", str(_D.retry_sec))),
)

log = setup_logger("adaptive_tsl")


# ── small helpers ──────────────────────────────────────────────────────

def _f(v) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _load_json(r: redis.Redis, key: str) -> dict:
    raw = r.get(key)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def fresh(doc: dict, now_ms: int, max_age_ms: int = MAX_INPUT_AGE_MS) -> dict:
    """The doc if its ts_ms is recent, else {} (treated as missing)."""
    ts = _f(doc.get("ts_ms")) if doc else None
    if ts is None or now_ms - ts > max_age_ms:
        return {}
    return doc


def is_eod(now: dt.datetime, hhmm: str = EOD_HHMM) -> bool:
    hh, mm = (int(x) for x in hhmm.split(":"))
    return (now.hour, now.minute) >= (hh, mm)


def days_to_expiry(expiry: str, today: dt.date) -> Optional[int]:
    try:
        return (dt.date.fromisoformat(str(expiry)[:10]) - today).days
    except Exception:
        return None


def st_counts(doc: dict) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    if not doc:
        return None, None, None
    total = sum(1 for k in ("st_1m", "st_5m", "st_10m", "st_30m") if str(doc.get(k) or "na") not in ("na", ""))
    bull, bear = _f(doc.get("bullish")), _f(doc.get("bearish"))
    return (None if bull is None else int(bull)), (None if bear is None else int(bear)), (total or None)


def flat(d: dict) -> Dict[str, str]:
    out = {}
    for k, v in d.items():
        if isinstance(v, (dict, list, tuple)):
            out[k] = json.dumps(v, separators=(",", ":"))
        elif isinstance(v, bool):
            out[k] = "1" if v else "0"
        else:
            out[k] = "" if v is None else str(v)
    return out


# ── market snapshot ────────────────────────────────────────────────────

class Inputs:
    """Per-loop cache of Redis reads; candles are refreshed every CANDLE_REFRESH_SEC."""

    candle_cache: Dict[str, Tuple[float, list]] = {}

    def __init__(self, r: redis.Redis, now_ms: int):
        self.r = r
        self.now_ms = now_ms
        self.now = dt.datetime.fromtimestamp(now_ms / 1000.0, IST)
        self.regime = _load_json(r, REGIME_KEY)
        self.volume = _load_json(r, VOLUME_KEY)
        self._docs: Dict[str, dict] = {}

    def doc(self, key: str, check_age: bool = True) -> dict:
        if key not in self._docs:
            d = _load_json(self.r, key)
            self._docs[key] = fresh(d, self.now_ms) if check_age else d
        return self._docs[key]

    def candles(self, sym: str) -> list:
        ts, cs = self.candle_cache.get(sym, (0.0, []))
        if time.time() - ts >= CANDLE_REFRESH_SEC:
            cs = read_last_candles(self.r, CANDLES_1M, sym, CANDLE_LIMIT)
            self.candle_cache[sym] = (time.time(), cs)
        return cs

    def volume_entry(self, sym: str) -> dict:
        e = self.volume.get(sym) if isinstance(self.volume, dict) else None
        return fresh(e, self.now_ms) if isinstance(e, dict) else {}


def build_snapshot(inp: Inputs, sym: str, tsym: str, expiry: str = "", probability: Optional[float] = None) -> Snapshot:
    now_ms = inp.now_ms
    ba = inp.doc(f"{BIDASK_PREFIX}{tsym}")
    eq = inp.doc(f"{BIDASK_PREFIX}{sym}")
    greeks = inp.doc(f"{GREEKS_PREFIX}{tsym}")
    em = inp.doc(f"{EM_PREFIX}{sym}")
    st = inp.doc(f"{ST_PREFIX}{sym}")
    ema = inp.doc(f"{EMA_PREFIX}{sym}")
    vol = inp.volume_entry(sym)
    oi = inp.doc(f"{OI_UND_PREFIX}{sym}")
    liq = inp.doc(f"{LIQUIDITY_PREFIX}{tsym}")
    imb = inp.doc(f"{IMBALANCE_PREFIX}{tsym}")
    sm = inp.doc(f"{SMARTMONEY_PREFIX}{tsym}")
    pivots = inp.doc(f"{PIVOTS_PREFIX}{sym}", check_age=False)

    cs = inp.candles(sym)
    today = [c for c in cs if dt.datetime.fromtimestamp(c.ts_ms / 1000.0, IST).date() == inp.now.date()]
    live_cs = bool(cs) and now_ms - cs[-1].ts_ms <= MAX_INPUT_AGE_MS
    spot = _f(eq.get("mid")) or (cs[-1].c if live_cs else None)
    em_pct = _f(em.get("expected_move_pct"))
    bull, bear, total = st_counts(st)
    vol_sig = str(vol.get("signal") or "") or None
    return Snapshot(
        now_ms=now_ms,
        bid=_f(ba.get("bid")),
        ask=_f(ba.get("ask")),
        spot=spot,
        atr_pct=atr_pct(cs) if live_cs else None,
        em_pct=None if em_pct is None else em_pct * 100.0,
        em_direction=str(em.get("direction") or "") or None,
        em_confidence=_f(em.get("confidence")),
        direction_score=_f(em.get("direction_score")),
        iv_change_pct=_f(greeks.get("iv_pct")),
        gamma=_f(greeks.get("gamma")),
        delta=_f(greeks.get("delta")),
        dte=days_to_expiry(expiry, inp.now.date()) if expiry else None,
        st_bias=str(st.get("bias") or "") or None,
        st_bullish=bull,
        st_bearish=bear,
        st_total=total,
        ema9=_f(ema.get("ema9")),
        ema26=_f(ema.get("ema26")),
        volume_signal=vol_sig,
        volume_surge=bool(vol_sig and vol_sig.lower().startswith("strong")),
        amd_phase=str(greeks.get("phase") or "") or None,
        contract_imbalance=str(imb.get("signal") or "") or None,
        smart_money=str(sm.get("composite") or "") or None,
        liquidity=_f(liq.get("liquidity_score")),
        spread_pct=_f(ba.get("spread_pct")),
        oi_buildup=str(oi.get("dominant_buildup") or "") or None,
        sideways=is_sideways(str(em.get("direction") or ""), inp.regime),
        und_close=cs[-1].c if live_cs else None,
        und_swing_high=last_swing_high(cs) if live_cs else None,
        und_swing_low=last_swing_low(cs) if live_cs else None,
        pivots={k: v for k, v in ((k, _f(pivots.get(k))) for k in ("P", "R1", "R2", "S1", "S2")) if v is not None},
        fib=fib_levels(max((c.h for c in today), default=None), min((c.l for c in today), default=None)),
        probability=probability,
        eod=is_eod(inp.now),
    )


def recompute_probability(inp: Inputs, origin: dict, sym: str, tsym: str) -> Tuple[Optional[float], dict]:
    """Probability for re-entry: original input + live votes (run_probability mapping)."""
    if not origin:
        return None, {}
    sie = live_sie_fields(
        origin,
        htf=inp.doc(f"{HTF_PREFIX}{sym}", check_age=False),
        st=inp.doc(f"{ST_PREFIX}{sym}"),
        ema=inp.doc(f"{EMA_PREFIX}{sym}"),
        volume=inp.volume_entry(sym),
        oi_und=inp.doc(f"{OI_UND_PREFIX}{sym}"),
        em=inp.doc(f"{EM_PREFIX}{sym}"),
        liq=inp.doc(f"{LIQUIDITY_PREFIX}{tsym}"),
        bidask=inp.doc(f"{BIDASK_PREFIX}{tsym}"),
    )
    pin, bucket, summary = build_inputs(
        sie, inp.doc(f"{COMPOSITE_PREFIX}{sym}"), inp.doc(f"{GREEKS_PREFIX}{tsym}"), inp.regime,
        _load_json(inp.r, JOURNAL_STATS_KEY), FILTERS.min_historical_samples,
    )
    res = compute_probability(pin, FILTERS)
    payload = build_payload(res, sie, bucket, summary, inp.now_ms)
    # A hard-filter rejection fails the probability check (None = fail).
    return (None if res.reject_reasons else res.probability), payload


# ── persistence ────────────────────────────────────────────────────────

def save_state(r: redis.Redis, st: TradeState, now_ms: int) -> None:
    doc = st.to_dict()
    doc["updated_ms"] = now_ms
    doc["mode"] = MODE
    r.set(f"{STATE_PREFIX}{st.trade_id}", json.dumps(doc, separators=(",", ":")), ex=STATE_TTL)


def load_state(r: redis.Redis, trade_id: str) -> Optional[TradeState]:
    d = _load_json(r, f"{STATE_PREFIX}{trade_id}")
    return TradeState.from_dict(d) if d.get("trade_id") else None


def save_chain(r: redis.Redis, c: Chain) -> None:
    r.set(f"{CHAIN_PREFIX}{c.chain_id}", json.dumps(c.to_dict(), separators=(",", ":")), ex=STATE_TTL)


def load_chain(r: redis.Redis, chain_id: str) -> Optional[Chain]:
    d = _load_json(r, f"{CHAIN_PREFIX}{chain_id}")
    return Chain.from_dict(d) if d.get("chain_id") else None


def publish(r: redis.Redis, out: dict, now_ms: int, event: str, latest: bool = True) -> None:
    payload = flat(dict(out, ts_ms=now_ms, event=event, mode=MODE))
    if event:
        r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
    if latest and out.get("tradingsymbol"):
        r.set(f"{LATEST_PREFIX}{out['tradingsymbol']}", json.dumps(payload, separators=(",", ":")), ex=STATE_TTL)


def trade_event(prev: Optional[TradeState], st: TradeState) -> str:
    if prev is None:
        return "TRACK"
    if st.status != prev.status:
        return st.status
    if st.activated and not prev.activated:
        return "ACTIVATED"
    if st.stop != prev.stop:
        return "STOP_MOVED"
    if (st.breach_since_ms is None) != (prev.breach_since_ms is None):
        return "BREACH" if st.breach_since_ms is not None else "RECOVERED"
    return ""


# ── engine loop pieces ─────────────────────────────────────────────────

_last_beat: Dict[str, float] = {}


def _beat_due(trade_id: str) -> bool:
    if time.time() - _last_beat.get(trade_id, 0.0) >= HEARTBEAT_SEC:
        _last_beat[trade_id] = time.time()
        return True
    return False


def ensure_chain(r: redis.Redis, pos: dict, st: TradeState) -> Chain:
    c = load_chain(r, st.chain_id)
    if c is not None:
        # WAITING too: the pending window can time out just before ICARE + journal open it.
        if c.state in (PENDING, WAITING) and st.reentry_no >= 1 and st.trade_id != c.active_trade_id:
            c = replace(on_reentry_opened(c, st.trade_id), reentries_used=max(c.reentries_used, st.reentry_no))
            save_chain(r, c)
            log.info("REENTRY_OPENED chain=%s trade=%s no=%s", c.chain_id, st.trade_id, st.reentry_no)
        return c
    origin = _load_json(r, f"{ICARE_ORIGIN_PREFIX}{st.tradingsymbol}")
    if origin:
        r.set(f"{ORIGIN_PREFIX}{st.chain_id}", json.dumps(origin, separators=(",", ":")), ex=STATE_TTL)
    entry = st.entry_premium or 0.0
    c = Chain(
        chain_id=st.chain_id, symbol=st.symbol, tradingsymbol=st.tradingsymbol, side=st.side, qty=st.qty,
        sl_pct=round(st.risk_pts / entry, 6) if entry else 0.3,
        hold_ms=max(int((_f(pos.get("time_stop_ms")) or 0) - (_f(pos.get("entry_ts_ms")) or 0)), 0),
        band_floor=band_floor(origin.get("decision")), original_lots=int(_f(pos.get("lots")) or 0),
        state=IN_TRADE, active_trade_id=st.trade_id,
    )
    save_chain(r, c)
    r.sadd(CHAINS_SET, c.chain_id)
    log.info("CHAIN_NEW chain=%s tsym=%s band_floor=%s origin=%s", c.chain_id, c.tradingsymbol, c.band_floor, bool(origin))
    return c


def handle_tick_result(r: redis.Redis, prev: Optional[TradeState], res, chain: Optional[Chain], now_ms: int) -> Optional[Chain]:
    st = res.state
    save_state(r, st, now_ms)
    ev = trade_event(prev, st)
    if not ev and _beat_due(st.trade_id):
        ev = "HEARTBEAT"
    # A finished trade must not hide its chain's live view (watch / re-entry) in md:tsl:latest.
    chain_live = chain is not None and chain.state in (WAITING, PENDING) or (
        chain is not None and chain.state == IN_TRADE and chain.active_trade_id not in ("", st.trade_id))
    publish(r, res.output, now_ms, ev, latest=st.status == "ACTIVE" or ev in ("EXIT", "CLOSED") or not chain_live)
    if ev and ev != "HEARTBEAT":
        log.info("TSL %s trade=%s tsym=%s bid=%s high=%s stop=%s pct=%s rule=%s act=%s checks=%s",
                 ev, st.trade_id, st.tradingsymbol, res.output.get("current_price"), st.highest, st.stop,
                 st.tsl_pct, st.tsl_rule, st.activated, res.output.get("checks"))
    if chain is None:
        return None
    if res.exit_context is not None or res.closed_reason:
        reason = "TRAILING_STOP" if res.exit_context is not None else res.closed_reason
        pnl = ((st.exit_price or st.entry_premium) - st.entry_premium) * st.qty
        chain = on_trade_closed(chain, st.reentry_no, reason, pnl, res.exit_context, now_ms, CFG)
        save_chain(r, chain)
        if res.exit_context is not None:
            r.set(f"{STATE_PREFIX}{st.trade_id}:exit_context", json.dumps(res.exit_context, separators=(",", ":")), ex=STATE_TTL)
            publish(r, dict(exit_output(chain), tradingsymbol=st.tradingsymbol, trade_id=st.trade_id,
                            chain_id=chain.chain_id, exit_trigger=st.exit_trigger), now_ms, "CHAIN_" + chain.state)
        log.info("CHAIN trade_closed chain=%s trade=%s reason=%s pnl=%.2f -> %s watch=%s",
                 chain.chain_id, st.trade_id, reason, pnl, chain.state, chain.watch_price)
    return chain


def manage_positions(r: redis.Redis, inp: Inputs) -> set:
    now_ms = inp.now_ms
    seen = set()
    for key in list(r.scan_iter(match=f"{POSITION_OPEN_PREFIX}*", count=500)):
        pos = _load_json(r, key)
        if not pos or "trade_id" not in pos:
            continue        # not a paper position
        tid = str(pos["trade_id"])
        seen.add(tid)
        ctx = pos.get("context") or {}
        prev = load_state(r, tid)
        sym, tsym = str(pos.get("symbol") or "").upper(), str(pos.get("tradingsymbol") or "")
        em = inp.doc(f"{EM_PREFIX}{sym}")
        st = prev or new_trade(
            tid, str(ctx.get("chain_id") or "") or tid, int(_f(ctx.get("reentry_no")) or 0), sym, tsym,
            str(pos.get("side") or ""), _f(pos.get("qty")) or 0.0, _f(pos.get("entry_premium")) or 0.0,
            _f(pos.get("sl_premium")) or 0.0, int(_f(pos.get("entry_ts_ms")) or now_ms),
            time_stop_ms=int(_f(pos.get("time_stop_ms")) or 0), entry_direction_score=_f(em.get("direction_score")),
        )
        if prev is None:
            r.sadd(TRADES_SET, tid)
        chain = ensure_chain(r, pos, st)
        origin = _load_json(r, f"{ORIGIN_PREFIX}{st.chain_id}")
        snap = build_snapshot(inp, sym, tsym, str(origin.get("expiry") or ""))
        ema = inp.doc(f"{EMA_PREFIX}{sym}")
        if ema:
            st = replace(st, ema_gaps=record_ema_gap(st.ema_gaps, int(_f(ema.get("bar_ts_ms")) or 0) or None,
                                                     (_f(ema.get("ema9")) or 0.0) - (_f(ema.get("ema26")) or 0.0)))
        snap = replace(snap, ema_gaps=tuple(g[1] for g in st.ema_gaps))
        res = manage(st, snap, CFG)
        handle_tick_result(r, prev, res, chain, now_ms)
    return seen


def reconcile_closed(r: redis.Redis, seen: set, now_ms: int) -> None:
    """Real trades the journal closed while the engine still had them ACTIVE."""
    for tid in list(r.smembers(TRADES_SET)):
        if tid in seen:
            continue
        rec = _load_json(r, f"{JOURNAL_CLOSED_PREFIX}{tid}")
        st = load_state(r, tid)
        if not rec and st is not None and st.status == "ACTIVE":
            continue        # journal not finished yet (or key expired) — retry next loop
        r.srem(TRADES_SET, tid)
        if st is None or st.status != "ACTIVE":
            continue        # already handled by a TSL exit
        reason = str(rec.get("exit_reason") or "UNKNOWN")
        pnl = _f(rec.get("pnl"))
        st = replace(st, status="CLOSED", exit_reason=reason, exit_price=_f(rec.get("exit_premium")), exit_ts_ms=now_ms)
        save_state(r, st, now_ms)
        chain = load_chain(r, st.chain_id)
        if chain is not None and chain.active_trade_id in ("", tid):
            chain = on_trade_closed(chain, st.reentry_no, reason, pnl, None, now_ms, CFG)
            save_chain(r, chain)
            log.info("CHAIN journal_closed chain=%s trade=%s reason=%s pnl=%s -> %s",
                     chain.chain_id, tid, reason, pnl, chain.state)
        publish(r, {"symbol": st.symbol, "tradingsymbol": st.tradingsymbol, "trade_id": tid, "chain_id": st.chain_id,
                    "reentry_no": st.reentry_no, "status": "CLOSED", "exit_reason": reason, "pnl": pnl,
                    "reentry_state": chain.state if chain is not None else ""}, now_ms, "CLOSED")


def manage_virtual(r: redis.Redis, inp: Inputs) -> None:
    for tid in list(r.smembers(VIRTUAL_SET)):
        prev = load_state(r, tid)
        if prev is None or prev.status != "ACTIVE":
            r.srem(VIRTUAL_SET, tid)
            continue
        origin = _load_json(r, f"{ORIGIN_PREFIX}{prev.chain_id}")
        snap = build_snapshot(inp, prev.symbol, prev.tradingsymbol, str(origin.get("expiry") or ""))
        res = manage(prev, snap, CFG)
        handle_tick_result(r, prev, res, load_chain(r, prev.chain_id), inp.now_ms)
        if res.state.status != "ACTIVE":
            r.srem(VIRTUAL_SET, tid)


def reentry_payload(prob_payload: dict, signal: dict, snap: Snapshot, origin: dict) -> Dict[str, str]:
    """Probability-shaped message for ICARE (md:tsl:reentry)."""
    out = dict(prob_payload)
    mid = (snap.bid + snap.ask) / 2.0 if snap.bid and snap.ask else (snap.ask or snap.bid)
    old = _f(origin.get("premium"))
    if mid and old:
        ratio = mid / old
        for k in ("projected_premium_gain", "projected_premium_change_adverse"):
            v = _f(origin.get(k))
            if v is not None:
                out[k] = f"{v * ratio:.4f}"
    out["premium"] = "" if mid is None else f"{mid:.4f}"
    out.update(flat({k: signal[k] for k in ("chain_id", "parent_trade_id", "reentry_no", "max_lots",
                                             "watch_price", "confidence", "reason")}))
    out["reentry"] = "1"
    out["tsl_mode"] = MODE
    out["entry_signal"] = "REENTER"
    return out


def manage_chains(r: redis.Redis, inp: Inputs) -> None:
    now_ms = inp.now_ms
    for cid in list(r.smembers(CHAINS_SET)):
        c = load_chain(r, cid)
        if c is None:
            r.srem(CHAINS_SET, cid)
            continue
        if c.state in (DONE, EXPIRED, BLOCKED):
            if c.state == BLOCKED:
                set_block(r, c, inp)
            r.srem(CHAINS_SET, cid)
            publish(r, {"symbol": c.symbol, "tradingsymbol": c.tradingsymbol, "chain_id": cid,
                        "reentry_state": c.state, "end_reason": c.end_reason, "status": "CHAIN_END"},
                    now_ms, "CHAIN_" + c.state, latest=False)
            log.info("CHAIN_END chain=%s state=%s reason=%s used=%s failed=%s",
                     cid, c.state, c.end_reason, c.reentries_used, c.failed_reentries)
            continue
        if c.state == PENDING:
            if pending_timed_out(c, now_ms, CFG):
                c = revert_pending(c, now_ms, CFG)
                save_chain(r, c)
                log.info("REENTRY_NOT_OPENED chain=%s (ICARE did not approve) -> retry after %s", cid, c.retry_after_ms)
            continue
        if c.state != WAITING:
            continue
        origin = _load_json(r, f"{ORIGIN_PREFIX}{cid}")
        prob, prob_payload = recompute_probability(inp, origin, c.symbol, c.tradingsymbol)
        snap = build_snapshot(inp, c.symbol, c.tradingsymbol, str(origin.get("expiry") or ""), prob)
        prev_state = c.state
        c, signal, checks, why = evaluate_reentry(c, snap, CFG)
        save_chain(r, c)
        out = dict(exit_output(c), tradingsymbol=c.tradingsymbol, chain_id=cid, checks=checks, wait_reason=why,
                   probability=prob, current_price=snap.bid, reentries_used=c.reentries_used)
        if signal is None:
            ev = "CHAIN_" + c.state if c.state != prev_state else ("WATCH" if _beat_due(cid) else "")
            publish(r, out, now_ms, ev)
            log.debug("WATCH chain=%s bid=%s watch=%s why=%s", cid, snap.bid, c.watch_price, why)
            continue
        publish(r, dict(signal, tradingsymbol=c.tradingsymbol), now_ms, "REENTER")
        r.xadd(REENTRY_STREAM, reentry_payload(prob_payload, signal, snap, origin), maxlen=OUT_MAXLEN, approximate=True)
        log.info("REENTER chain=%s no=%s entry=%s watch=%s prob=%s mode=%s",
                 cid, signal["reentry_no"], signal["entry_price"], c.watch_price, prob, MODE)
        if MODE != "active":
            start_virtual(r, c, signal, now_ms)


def start_virtual(r: redis.Redis, c: Chain, signal: dict, now_ms: int) -> None:
    """Shadow mode: simulate the re-entry at the ask with the original stop distance and hold."""
    entry = _f(signal.get("entry_price"))
    if not entry:
        return
    tid = f"{c.chain_id}-R{signal['reentry_no']}"
    st = new_trade(tid, c.chain_id, int(signal["reentry_no"]), c.symbol, c.tradingsymbol, c.side, c.qty, entry,
                   round(entry * (1.0 - c.sl_pct), 4), now_ms, time_stop_ms=now_ms + c.hold_ms if c.hold_ms else 0,
                   virtual=True)
    save_state(r, st, now_ms)
    r.sadd(VIRTUAL_SET, tid)
    c = on_reentry_opened(c, tid)
    save_chain(r, c)
    log.info("VIRTUAL_OPEN trade=%s entry=%s sl=%s", tid, st.entry_premium, st.initial_sl)


def set_block(r: redis.Redis, c: Chain, inp: Inputs) -> None:
    """Both re-entries failed -> block (symbol, side) until Supertrend flips or end of day (T11)."""
    if MODE != "active":
        log.info("BLOCK_SHADOW chain=%s symbol=%s side=%s (not enforced in shadow mode)", c.chain_id, c.symbol, c.side)
        return
    eod = inp.now.replace(hour=23, minute=59, second=0, microsecond=0)
    ttl = max(int((eod - inp.now).total_seconds()), 60)
    doc = {"chain_id": c.chain_id, "ts_ms": inp.now_ms, "side": c.side, "flipped": False}
    r.set(f"{BLOCK_PREFIX}{c.symbol}:{c.side}", json.dumps(doc), ex=ttl)
    log.info("BLOCK symbol=%s side=%s chain=%s ttl=%ss", c.symbol, c.side, c.chain_id, ttl)


def maintain_blocks(r: redis.Redis, inp: Inputs) -> None:
    """A block clears once the underlying Supertrend has flipped against the blocked side."""
    for key in list(r.scan_iter(match=f"{BLOCK_PREFIX}*", count=200)):
        sym, side = key[len(BLOCK_PREFIX):].rsplit(":", 1)
        bias = str(inp.doc(f"{ST_PREFIX}{sym}").get("bias") or "").upper()
        if bias == ("PUT" if side == "CE" else "CALL"):
            r.delete(key)
            log.info("UNBLOCK symbol=%s side=%s supertrend_flipped bias=%s", sym, side, bias)


def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    log.info("START mode=%s loop=%ss writing %s / %s (cfg=%s)", MODE, LOOP_SEC, OUT_STREAM, REENTRY_STREAM, CFG)
    while True:
        t0 = time.time()
        try:
            inp = Inputs(r, int(t0 * 1000))
            seen = manage_positions(r, inp)
            reconcile_closed(r, seen, inp.now_ms)
            manage_virtual(r, inp)
            manage_chains(r, inp)
            maintain_blocks(r, inp)
        except redis.exceptions.ConnectionError as e:
            log.warning("REDIS_DOWN %s", e)
        time.sleep(max(LOOP_SEC - (time.time() - t0), 0.1))


if __name__ == "__main__":
    main()

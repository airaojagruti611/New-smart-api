"""
run_strike_intel.py
───────────────────
Module 10 — Strike Intelligence Engine (SIE) Redis wiring (DECISION.md D2/D3).

Consumes md:entry:trigger BUY signals (in parallel with legacy
run_strike_select.py, which keeps running) and ranks the trade-side chain
ATM ± SIE_STRIKES_AROUND using already-published latest: keys:

  md:greeks:phase:latest:{TSYM}      delta/gamma/theta/vega/iv (+ greeks_source)
  md:bidask:latest:{TSYM}            mid premium, spread_pct, depth
  md:liquidity:score:latest:{TSYM}   liquidity_score, liquidity_band, lot_size
  md:expected_move:latest:{SYMBOL}   final_expected_move, direction, iv_trend

Chain membership + expiry come from ScripMaster (same as strike_select).

Emits:
  Stream : md:strike:intel
  Key    : md:strike:intel:latest:{SYMBOL}

The payload echoes the entry-trigger context so the Probability runner
(P3) has everything for one decision in one message.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time
from typing import Dict, Optional

import redis

from app.config import GREEKS_DIVIDEND_YIELD, RISK_FREE_RATE, load_symbols
from app.expected_move import as_annualized_decimal
from app.logging_setup import setup_logger
from app.option_pricing import IST, time_to_expiry_years
from app.probability_engine import normalize_side
from app.scripmaster import build_atm_option_tokens, load_scripmaster
from app.strike_intel import SIEConfig, SIEContext, SIEResult, StrikeCandidate, rank_strikes, select_window


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

IN_STREAM = os.getenv("STREAM_ENTRY_TRIGGER", "md:entry:trigger")
OUT_STREAM = os.getenv("STREAM_STRIKE_INTEL", "md:strike:intel")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_STRIKE_INTEL", "200000"))
LATEST_KEY_PREFIX = os.getenv("STRIKE_INTEL_LATEST_PREFIX", "md:strike:intel:latest:")

GREEKS_PREFIX = os.getenv("GREEKS_PHASE_LATEST_PREFIX", "md:greeks:phase:latest:")
BIDASK_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")
LIQUIDITY_PREFIX = os.getenv("LIQUIDITY_SCORE_LATEST_PREFIX", "md:liquidity:score:latest:")
EXPECTED_MOVE_PREFIX = os.getenv("EXPECTED_MOVE_LATEST_PREFIX", "md:expected_move:latest:")

GROUP = os.getenv("STRIKE_INTEL_GROUP", "strike-intel")
CONSUMER = os.getenv("STRIKE_INTEL_CONSUMER", "strike-intel-1")

STRIKES_AROUND = int(os.getenv("SIE_STRIKES_AROUND", "5"))
HOLD_MINUTES = float(os.getenv("SIE_HOLD_MINUTES", "60"))
MIN_LIQUIDITY = float(os.getenv("SIE_MIN_LIQUIDITY", "70"))
MAX_SPREAD_PCT = float(os.getenv("SIE_MAX_SPREAD_PCT", "3.0"))
TOP_N = int(os.getenv("SIE_TOP_N", "3"))
COOLDOWN_MS = int(os.getenv("STRIKE_INTEL_COOLDOWN_MS", "60000"))

# Entry-trigger fields echoed downstream for the Probability runner.
ECHO_FIELDS = (
    "signal", "strength", "level", "price", "bar_ts_ms", "aligned", "htf_bias",
    "st_bias", "ema_state", "volume_signal", "volume_surge", "oi_positioning",
    "oi_target_strike", "buy_pct", "sell_pct",
)

log = setup_logger("strike_intel")


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _safe_float(v) -> Optional[float]:
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


def _s(v) -> str:
    return "" if v is None else str(v)


def build_lot_sizes(df) -> Dict[str, float]:
    """tradingsymbol -> lot size for NFO options (fallback when liquidity has none)."""
    d = df[(df["exch_seg"] == "NFO") & (df["instrumenttype"].str.contains("OPT", na=False))]
    out: Dict[str, float] = {}
    for sym, lot in zip(d["symbol"], d["lotsize"]):
        v = _safe_float(lot)
        if v and v > 0:
            out[str(sym)] = v
    return out


def trend_aligned(fields: dict, side: str) -> Optional[bool]:
    want = "CALL" if side == "CE" else "PUT"
    htf = str(fields.get("htf_bias") or "").strip().upper()
    st = str(fields.get("st_bias") or "").strip().upper()
    if not htf or not st:
        return None
    return htf == want and st == want


def load_candidate(r: redis.Redis, contract: dict, lot_sizes: Dict[str, float]) -> StrikeCandidate:
    tsym = str(contract.get("tradingsymbol") or "")
    g = _load_json(r, f"{GREEKS_PREFIX}{tsym}")
    b = _load_json(r, f"{BIDASK_PREFIX}{tsym}")
    lq = _load_json(r, f"{LIQUIDITY_PREFIX}{tsym}")

    mid = _safe_float(b.get("mid"))
    return StrikeCandidate(
        tradingsymbol=tsym,
        strike=float(contract.get("strike") or 0.0),
        cp=str(contract.get("cp") or "").upper(),
        token=str(contract.get("token") or ""),
        premium=mid if mid and mid > 0 else None,
        bid=_safe_float(b.get("bid")),
        ask=_safe_float(b.get("ask")),
        spread_pct=_safe_float(b.get("spread_pct")) if mid else None,
        depth=_safe_float(b.get("depth")),
        delta=_safe_float(g.get("delta")),
        gamma=_safe_float(g.get("gamma")),
        theta_per_day=_safe_float(g.get("theta")),
        vega_per_point=_safe_float(g.get("vega")),
        iv=as_annualized_decimal(_safe_float(g.get("iv"))),
        greeks_source=str(g.get("greeks_source") or ""),
        liquidity_score=_safe_float(lq.get("liquidity_score")),
        liquidity_band=str(lq.get("liquidity_band") or ""),
        lot_size=_safe_float(lq.get("lot_size")) or lot_sizes.get(tsym),
    )


def build_payload(res: SIEResult, fields: dict, expiry: str, now_ms: int, em_doc: dict) -> Dict[str, str]:
    best = res.best
    proj = best.projection if best else None
    payload = {
        "ts_ms": str(now_ms),
        "symbol": res.symbol,
        "status": res.status,
        "reason": res.reason,
        "side": res.side,
        "spot": _s(res.spot),
        "expiry": expiry,
        "market_phase": res.market_phase,
        "delta_band": f"{res.delta_band[0]:.2f}-{res.delta_band[1]:.2f}",
        "choppy": "1" if res.choppy else "0",
        "expected_move": _s(res.expected_move),
        "em_direction": str(em_doc.get("direction") or ""),
        "em_direction_score": str(em_doc.get("direction_score") or ""),
        "em_confidence": str(em_doc.get("confidence") or ""),
        "em_conflict": "1" if res.em_conflict else "0",
        "lower_range": _s(res.lower_range),
        "upper_range": _s(res.upper_range),
        "hold_minutes": _s(res.hold_minutes),
        "candidates": str(len(res.ranked)),
        "rejected": str(sum(1 for x in res.ranked if x.status != "OK")),
        # best strike, flat (legacy strike_select-compatible names)
        "strike": _s(best.strike if best else ""),
        "token": best.token if best else "",
        "tradingsymbol": best.tradingsymbol if best else "",
        "exchange": "NFO",
        "strike_score": _s(best.strike_score if best else ""),
        "confidence": _s(best.confidence if best else ""),
        "greeks_score": _s(best.greeks_score if best else ""),
        "execution_quality": _s(best.execution_quality if best else ""),
        "premium": _s(best.premium if best else ""),
        "delta": _s(best.delta if best else ""),
        "gamma": _s(best.gamma if best else ""),
        "theta_per_day": _s(best.theta_per_day if best else ""),
        "vega_per_point": _s(best.vega_per_point if best else ""),
        "iv": _s(best.iv if best else ""),
        "greeks_source": best.greeks_source if best else "",
        "theta_risk_pct": _s(best.theta_risk_pct if best else ""),
        "liquidity_score": _s(best.liquidity_score if best else ""),
        "liquidity_band": best.liquidity_band if best else "",
        "spread_pct": _s(best.spread_pct if best else ""),
        "lot_size": _s(best.lot_size if best else ""),
        "projected_premium": _s(proj.premium if proj else ""),
        "projected_premium_gain": _s(proj.premium_gain if proj else ""),
        "projected_premium_gain_iv_down": _s(proj.premium_gain_iv_down if proj else ""),
        "projected_delta": _s(proj.delta if proj else ""),
        "projected_premium_change_adverse": _s(proj.premium_change_adverse if proj else ""),
        "reasons": json.dumps(best.reasons if best else [], ensure_ascii=False),
        "top": json.dumps([x.to_dict() for x in res.top], separators=(",", ":"), ensure_ascii=False),
        "ranked": json.dumps(
            [
                {"rank": x.rank, "tsym": x.tradingsymbol, "strike": x.strike, "status": x.status,
                 "score": x.strike_score, "reject": x.reject_reasons}
                for x in res.ranked
            ],
            separators=(",", ":"),
        ),
    }
    for k in ECHO_FIELDS:
        payload[f"entry_{k}"] = str(fields.get(k) or "")
    return payload


def main():
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_STREAM, GROUP)

    log.info("loading ScripMaster ...")
    df = load_scripmaster()
    lot_sizes = build_lot_sizes(df)
    cfg = SIEConfig(min_liquidity=MIN_LIQUIDITY, max_spread_pct=MAX_SPREAD_PCT, top_n=TOP_N)
    log.info(
        "START reading %s, writing %s (around=%s hold_min=%s min_liq=%s max_spread=%s top_n=%s symbols=%d)",
        IN_STREAM, OUT_STREAM, STRIKES_AROUND, HOLD_MINUTES, MIN_LIQUIDITY, MAX_SPREAD_PCT, TOP_N, len(symbols),
    )

    last_emit_ms: dict[str, int] = {}

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={IN_STREAM: ">"},
            count=2000,
            block=2000,
        )
        if not resp:
            continue

        now_ms = int(time.time() * 1000)
        ack_ids = []

        for _stream, msgs in resp:
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)

                sym = str(fields.get("symbol") or "").strip().upper()
                log.debug("MSG_IN id=%s symbol=%s fields=%s", msg_id, sym, fields)
                if not sym or sym not in symbols:
                    log.debug("SKIP unknown_symbol id=%s symbol=%r", msg_id, sym)
                    continue

                signal = str(fields.get("signal") or "").strip().upper()
                side = normalize_side(signal)
                if not side:
                    log.debug("SKIP not_buy_signal symbol=%s signal=%s", sym, signal)
                    continue

                prev = last_emit_ms.get(sym)
                if prev is not None and (now_ms - prev) < COOLDOWN_MS:
                    log.info("SKIP cooldown symbol=%s age_ms=%s", sym, now_ms - prev)
                    continue

                spot = _safe_float(fields.get("price"))
                if spot is None or spot <= 0:
                    log.info("SKIP invalid_spot symbol=%s price=%r", sym, fields.get("price"))
                    continue

                contracts, expiry = build_atm_option_tokens(df, sym, spot, STRIKES_AROUND)
                if not contracts or not expiry:
                    log.info("SKIP no_option_chain symbol=%s", sym)
                    continue

                em = _load_json(r, f"{EXPECTED_MOVE_PREFIX}{sym}")
                em_pct = _safe_float(em.get("expected_move_pct"))
                ctx = SIEContext(
                    symbol=sym,
                    side=side,
                    spot=spot,
                    expected_move=_safe_float(em.get("final_expected_move")),
                    em_direction=str(em.get("direction") or ""),
                    em_pct=None if em_pct is None else abs(em_pct) * 100.0,
                    iv_trend=str(em.get("iv_trend") or ""),
                    trend_aligned=trend_aligned(fields, side),
                    time_to_expiry_years=time_to_expiry_years(expiry),
                    expiry_iso=str(expiry),
                    today_iso=dt.datetime.now(IST).date().isoformat(),
                    hold_minutes=HOLD_MINUTES,
                    risk_free_rate=RISK_FREE_RATE,
                    dividend_or_carry=GREEKS_DIVIDEND_YIELD,
                )
                cands = [load_candidate(r, c, lot_sizes) for c in contracts]
                window = select_window(cands, side, spot, STRIKES_AROUND)
                log.debug("LOGIC_IN symbol=%s ctx=%s candidates=%d", sym, ctx, len(window))

                res = rank_strikes(window, ctx, cfg)
                payload = build_payload(res, fields, str(expiry), now_ms, em)

                log.info(
                    "LOGIC symbol=%s status=%s side=%s phase=%s band=%s em=%s best=%s score=%s conf=%s "
                    "rejected=%s/%s reason=%s",
                    sym, res.status, side, res.market_phase, payload["delta_band"], res.expected_move,
                    payload["tradingsymbol"], payload["strike_score"], payload["confidence"],
                    payload["rejected"], payload["candidates"], res.reason,
                )

                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(f"{LATEST_KEY_PREFIX}{sym}", json.dumps(payload, separators=(",", ":")), ex=3600)

                if res.status == "OK":
                    last_emit_ms[sym] = now_ms
                    log.info("EMIT symbol=%s top=%s", sym, [x.tradingsymbol for x in res.top])
                else:
                    log.info("SKIP_RESULT symbol=%s reason=%s ranked=%s", sym, res.reason, payload["ranked"])

        if ack_ids:
            r.xack(IN_STREAM, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

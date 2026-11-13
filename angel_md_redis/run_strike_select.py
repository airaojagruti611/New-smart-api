"""
run_strike_select.py
────────────────────
Consumes md:entry:trigger BUY signals and selects ATM / slight-OTM option contracts:
  md:strike:select
  md:strike:select:latest:{SYMBOL}
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time
from typing import Optional

import redis

from app.config import load_symbols
from app.learning_keys import LEARNING_STATS_KEY
from app.logging_setup import setup_logger
from app.scripmaster import build_atm_option_tokens, load_scripmaster
from app.strike_select import StrikeConfig, strike_select, top3_json


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

IN_STREAM = os.getenv("STREAM_ENTRY_TRIGGER", "md:entry:trigger")

OUT_STREAM = os.getenv("STREAM_STRIKE_SELECT", "md:strike:select")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_STRIKE_SELECT", "200000"))
LATEST_KEY_PREFIX = os.getenv("STRIKE_SELECT_LATEST_PREFIX", "md:strike:select:latest:")

GROUP = os.getenv("STRIKE_SELECT_GROUP", "strike-select")
CONSUMER = os.getenv("STRIKE_SELECT_CONSUMER", "strike-select-1")

OFFSET_BASE = int(os.getenv("STRIKE_OFFSET_BASE", "0"))
OFFSET_STRONG = int(os.getenv("STRIKE_OFFSET_STRONG", "1"))
COOLDOWN_MS = int(os.getenv("STRIKE_SELECT_COOLDOWN_MS", "60000"))

# Strike Intelligence Engine (DECISION.MD A). STRIKE_CANDIDATES_AROUND: default 3, max 10.
# Only the signal side is scored, so this adds no WebSocket tokens (SmartAPI subscription
# limit): non-ATM strikes use the Greeks-chain cache, plus live bid-ask where streamed.
CANDIDATES_AROUND = max(1, min(10, int(os.getenv("STRIKE_CANDIDATES_AROUND", "3"))))
USE_LEARNING = os.getenv("STRIKE_USE_LEARNING", "0") == "1"
LEARN_MIN_SAMPLES = int(os.getenv("LEARN_MIN_SAMPLES", "50"))
IV_VIEW = os.getenv("STRIKE_IV_VIEW", "")  # "", CONTRACT or EXPAND
EXPIRY_CLOSE_HHMM = os.getenv("EXPIRY_CLOSE_HHMM", "15:30")
GREEKS_LATEST_PREFIX = os.getenv("GREEKS_LATEST_PREFIX", "md:greeks:latest:")
LIQ_LATEST_PREFIX = os.getenv("LIQUIDITY_SCORE_LATEST_PREFIX", "md:liquidity:score:latest:")
BIDASK_LATEST_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")
EM_LATEST_PREFIX = os.getenv("EXPECTED_MOVE_LATEST_PREFIX", "md:expected_move:latest:")
REGIME_LATEST_KEY = os.getenv("REGIME_LATEST_KEY", "md:regime:latest")
LEARN_CACHE_SEC = float(os.getenv("STRIKE_LEARN_CACHE_SEC", "30"))

_IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

log = setup_logger("strike_select")


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _safe_float(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _load_json(r, key):
    try:
        raw = r.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


def _years_to_expiry(expiry_iso: str) -> Optional[float]:
    try:
        y, m, d = (int(x) for x in expiry_iso.split("-"))
        hh, mm = (int(x) for x in EXPIRY_CLOSE_HHMM.split(":"))
        end = dt.datetime(y, m, d, hh, mm, tzinfo=_IST)
        return (end - dt.datetime.now(tz=_IST)).total_seconds() / (365.0 * 86400.0)
    except Exception:
        return None


def _age_sec(doc, now_ms: int) -> float:
    ts = _safe_float((doc or {}).get("ts_ms"))
    return 0.0 if ts is None else max(0.0, (now_ms - ts) / 1000.0)


def _gather_quotes(r, sym, spot, df, cfg: StrikeConfig, now_ms: int):
    """Per-token inputs from Redis. Stale live docs are dropped (fallback), never invented."""
    contracts, expiry = build_atm_option_tokens(df, sym, float(spot), CANDIDATES_AROUND)
    if not contracts or not expiry:
        return {}, expiry
    chain = _load_json(r, f"{GREEKS_LATEST_PREFIX}{sym}:{expiry}") or []
    by_key = {}
    for row in chain if isinstance(chain, list) else []:
        if not isinstance(row, dict):
            continue
        row = {str(k).lower(): v for k, v in row.items()}  # same normalisation as app/joiner.py
        st = _safe_float(row.get("strikeprice") or row.get("strike"))
        ot = str(row.get("optiontype") or row.get("cp") or "").strip().upper()
        ot = {"C": "CE", "CALL": "CE", "P": "PE", "PUT": "PE"}.get(ot, ot)
        if st is not None and ot:
            by_key[(round(st, 2), ot)] = row
    quotes = {}
    for c in contracts:
        tok = str(c.get("token") or "")
        tsym = str(c.get("tradingsymbol") or "")
        g = by_key.get((round(float(c["strike"]), 2), str(c.get("cp") or "").upper()), {})
        q = {
            "delta": _safe_float(g.get("delta")), "gamma": _safe_float(g.get("gamma")),
            "theta": _safe_float(g.get("theta")), "vega": _safe_float(g.get("vega")),
            "iv": _safe_float(g.get("impliedvolatility")),
            "volume": _safe_float(g.get("tradevolume")),
        }
        if q["iv"] is not None and q["iv"] > 1.0:
            q["iv"] = q["iv"] / 100.0
        liq = _load_json(r, f"{LIQ_LATEST_PREFIX}{tsym}")
        if liq and _age_sec(liq, now_ms) <= cfg.max_age_sec:
            q["liquidity"] = _safe_float(liq.get("liquidity_score"))
            q["oi"] = _safe_float(liq.get("oi"))
        ba = _load_json(r, f"{BIDASK_LATEST_PREFIX}{tsym}")
        if ba and _age_sec(ba, now_ms) <= cfg.max_age_sec:
            q["premium"] = _safe_float(ba.get("mid"))
            q["spread_pct"] = _safe_float(ba.get("spread_pct"))
            q["live_bidask"] = q["premium"] is not None and q["spread_pct"] is not None
        quotes[tok] = q
    return quotes, expiry


def main():
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_STREAM, GROUP)

    log.info("loading ScripMaster ...")
    df = load_scripmaster()
    log.info(
        "START reading %s, writing %s (offset_base=%s, offset_strong=%s, cooldown_ms=%s symbols=%d)",
        IN_STREAM,
        OUT_STREAM,
        OFFSET_BASE,
        OFFSET_STRONG,
        COOLDOWN_MS,
        len(symbols),
    )

    last_emit_ms: dict[str, int] = {}
    cfg = StrikeConfig.from_env()
    learn_cache = {"ts": 0.0, "stats": None}

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

                signal = str(fields.get("signal") or "").strip()
                if not signal.startswith("BUY"):
                    log.debug("SKIP not_buy_signal symbol=%s signal=%s", sym, signal)
                    continue

                prev = last_emit_ms.get(sym)
                if prev is not None and (now_ms - prev) < COOLDOWN_MS:
                    log.info(
                        "SKIP cooldown symbol=%s age_ms=%s cooldown_ms=%s",
                        sym,
                        now_ms - prev,
                        COOLDOWN_MS,
                    )
                    continue

                spot = _safe_float(fields.get("price"))
                if spot is None or spot <= 0:
                    log.info("SKIP invalid_spot symbol=%s price=%r", sym, fields.get("price"))
                    continue

                strength = str(fields.get("strength") or "").strip()
                oi_target_strike = _safe_float(fields.get("oi_target_strike"))
                log.debug(
                    "INPUTS symbol=%s signal=%s strength=%s spot=%s oi_target_strike=%s",
                    sym,
                    signal,
                    strength,
                    spot,
                    oi_target_strike,
                )

                try:
                    quotes, expiry_iso = _gather_quotes(r, sym, spot, df, cfg, now_ms)
                    regime_doc = _load_json(r, REGIME_LATEST_KEY) or {}
                    em_doc = _load_json(r, f"{EM_LATEST_PREFIX}{sym}") or {}
                    if USE_LEARNING and time.time() - learn_cache["ts"] > LEARN_CACHE_SEC:
                        learn_cache["stats"] = _load_json(r, LEARNING_STATS_KEY)
                        learn_cache["ts"] = time.time()
                    extra = {
                        "regime": str(regime_doc.get("regime") or "").upper(),
                        "em": _safe_float(em_doc.get("final_expected_move")),
                        "em_lower": _safe_float(em_doc.get("lower_range")),
                        "em_upper": _safe_float(em_doc.get("upper_range")),
                        "years_to_expiry": _years_to_expiry(expiry_iso) if expiry_iso else None,
                        "iv_view": IV_VIEW,
                        "learning_stats": learn_cache["stats"] if USE_LEARNING else None,
                        "use_learning": USE_LEARNING,
                        "learn_min_samples": LEARN_MIN_SAMPLES,
                    }
                    result = strike_select(
                        signal=signal, strength=strength, underlying=sym, spot=spot, df=df,
                        offset_base=OFFSET_BASE, offset_strong=OFFSET_STRONG,
                        oi_target_strike=oi_target_strike,
                        quotes=quotes, ctx_extra=extra, cfg=cfg,
                        candidates_around=CANDIDATES_AROUND,
                    )
                except Exception:
                    # Never lose a signal to an engine bug: fall back to legacy selection.
                    log.exception("scored selection failed symbol=%s; using legacy", sym)
                    result = strike_select(
                        signal=signal, strength=strength, underlying=sym, spot=spot, df=df,
                        offset_base=OFFSET_BASE, offset_strong=OFFSET_STRONG,
                        oi_target_strike=oi_target_strike,
                    )

                payload = {
                    "ts_ms": str(now_ms),
                    "symbol": sym,
                    "signal": result.signal,
                    "status": result.status,
                    "side": result.side,
                    "mode": result.mode,
                    "spot": str(result.spot),
                    "atm": str(result.atm),
                    "strike": str(result.strike),
                    "step": str(result.step),
                    "expiry": result.expiry,
                    "token": result.token,
                    "tradingsymbol": result.tradingsymbol,
                    "exchange": result.exchange,
                    "strength": strength,
                    "level": str(fields.get("level") or ""),
                    "entry_price": str(fields.get("price") or ""),
                    "reason": result.reason,
                    "bar_ts_ms": str(fields.get("bar_ts_ms") or now_ms),
                    "oi_target_strike": "" if oi_target_strike is None else str(oi_target_strike),
                    "oi_positioning": str(fields.get("oi_positioning") or ""),
                }

                if result.best is not None:
                    b = result.best

                    def _s(v):
                        return "" if v is None else str(v)

                    payload.update({
                        "regime": result.regime, "regime_class": result.regime_class,
                        "em_pct": _s(result.em_pct),
                        "em_bucket": result.em_bucket, "hold_min": _s(result.hold_min),
                        "strike_bucket": result.strike_bucket, "delta_band": result.delta_band,
                        "top3": top3_json(result),
                    })
                    if result.status == "OK":
                        payload.update({
                            "strike_score": _s(result.strike_score),
                            "confidence": _s(result.confidence),
                            "reasons": json.dumps(list(result.reasons)),
                            "delta": _s(b.delta), "gamma": _s(b.gamma), "theta": _s(b.theta),
                            "vega": _s(b.vega), "iv": _s(b.iv), "premium": _s(b.premium),
                            "lot_size": _s(b.lot_size), "freeze_qty": _s(b.freeze_qty),
                        })

                log.info(
                    "LOGIC symbol=%s status=%s side=%s mode=%s atm=%s strike=%s "
                    "token=%s tsym=%s reason=%s",
                    sym,
                    result.status,
                    result.side,
                    result.mode,
                    result.atm,
                    result.strike,
                    result.token,
                    result.tradingsymbol,
                    result.reason,
                )

                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(
                    f"{LATEST_KEY_PREFIX}{sym}",
                    json.dumps(payload, separators=(",", ":")),
                    ex=3600,
                )

                if result.status == "OK":
                    last_emit_ms[sym] = now_ms
                    log.info("EMIT symbol=%s payload=%s", sym, payload)
                else:
                    log.info("SKIP_RESULT symbol=%s payload=%s", sym, payload)

        if ack_ids:
            r.xack(IN_STREAM, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

"""
run_expected_move.py
───────────────────────
Module 8 — Expected Move Calculator (Word spec: Expected Move Engine).

Reads live spot from md:ticks:eq plus already-published latest: keys:

  md:greeks:phase:underlying:latest:{SYM}:CE / :PE  (run_greeks_analyzer.py)
      -> ATM delta, gamma_pct / iv_pct (momentum multiplier), IV diagnostic
  md:bidask:latest:{tradingsymbol}                  (run_bidask_analyzer.py)
      -> atm_call_mid / atm_put_mid (diagnostic straddle only)
  md:imbalance:latest:{SYMBOL}                      (run_bidask_imbalance.py)
      -> bidask_score (final_score is already -1..+1)
  md:volume:latest                                  (run_volume_analyzer.py)
      -> volume_score (adapted from the string signal)
  md:oi:underlying:latest:{SYM}                      (run_oi_analysis.py)
      -> oi_score + primary_resistance (strong_resistance_nearby)
  md:indicator:score:latest:{SYM}                    (run_momentum_confirm.py)
      -> indicator_score (-2..+2 from Supertrend + EMA + pivot strength)

vacuum_zone has no upstream publisher and is passed as None (flagged).
realized_volatility has no source and is passed as None.

Emits:
  Stream : md:expected_move:signal
  Key    : md:expected_move:latest:{SYMBOL}

Evaluated on a periodic cycle since it synthesizes several independently-
updating cached signals rather than reacting to a single stream.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict
from typing import Dict, Optional

import redis

from app.config import load_symbols
from app.expected_move import (
    RESISTANCE_NEAR_PCT,
    classify_pct_trend,
    clip_bidask_score,
    combine_trend,
    compute_expected_move,
    iv_from_percent,
    GAMMA_TREND_UP_PCT,
    IV_TREND_UP_PCT,
    normalize_oi_signal,
    normalize_volume_signal,
    pick_atm_delta,
    resistance_is_nearby,
)
from app.logging_setup import setup_logger

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")

GREEKS_PHASE_UNDERLYING_PREFIX = os.getenv("GREEKS_PHASE_UNDERLYING_PREFIX", "md:greeks:phase:underlying:latest:")
BIDASK_LATEST_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")
IMBALANCE_LATEST_PREFIX = os.getenv("IMBALANCE_LATEST_PREFIX", "md:imbalance:latest:")
VOLUME_LATEST_KEY = os.getenv("VOLUME_LATEST_KEY", "md:volume:latest")
OI_UNDERLYING_LATEST_PREFIX = os.getenv("OI_UNDERLYING_LATEST_PREFIX", "md:oi:underlying:latest:")
INDICATOR_SCORE_LATEST_PREFIX = os.getenv("INDICATOR_SCORE_LATEST_PREFIX", "md:indicator:score:latest:")

OUT_STREAM = os.getenv("STREAM_EXPECTED_MOVE", "md:expected_move:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_EXPECTED_MOVE", "50000"))
LATEST_KEY_PREFIX = os.getenv("EXPECTED_MOVE_LATEST_PREFIX", "md:expected_move:latest:")

GROUP = os.getenv("EXPECTED_MOVE_GROUP", "expected-move")
CONSUMER = os.getenv("EXPECTED_MOVE_CONSUMER", "expected-move-1")

EVAL_INTERVAL_SEC = float(os.getenv("EXPECTED_MOVE_EVAL_INTERVAL_SEC", "5.0"))
LATEST_TTL_SEC = int(os.getenv("EXPECTED_MOVE_LATEST_TTL_SEC", "3600"))

HORIZON_MINUTES = float(os.getenv("EXPECTED_MOVE_HORIZON_MINUTES", "60"))
TRADING_MINUTES_PER_DAY = float(os.getenv("EXPECTED_MOVE_TRADING_MINUTES_PER_DAY", "375"))

log = setup_logger("expected_move")


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


def _load_json(r: redis.Redis, key: str) -> Optional[dict]:
    raw = r.get(key)
    if not raw:
        return None
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _load_volume_signal_for_symbol(r: redis.Redis, symbol: str) -> Optional[str]:
    """md:volume:latest is a multi-symbol blob; extract this symbol's signal string."""
    blob = _load_json(r, VOLUME_LATEST_KEY)
    if not blob:
        return None
    entry = blob.get(symbol)
    if not isinstance(entry, dict):
        return None
    return entry.get("signal")


def _payload_to_dict(result) -> Dict[str, str]:
    d = asdict(result)
    out: Dict[str, str] = {}
    for k, v in d.items():
        if k == "data_quality_flags":
            out[k] = json.dumps(v, separators=(",", ":"))
        elif v is None:
            out[k] = ""
        else:
            out[k] = str(v)
    return out


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, EQ_STREAM, GROUP)

    spot_by_sym: Dict[str, float] = {}
    next_eval = time.time() + EVAL_INTERVAL_SEC

    log.info(
        "START reading %s + greeks-phase/bidask/imbalance/volume/oi/indicator latest -> %s + %s{{SYMBOL}} "
        "(eval_interval=%ss spec_engine=score*multiplier*0.02 symbols=%d)",
        EQ_STREAM, OUT_STREAM, LATEST_KEY_PREFIX, EVAL_INTERVAL_SEC, len(symbols),
    )

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={EQ_STREAM: ">"},
            count=2000,
            block=2000,
        )
        if resp:
            for stream, msgs in resp:
                ack_ids = []
                for msg_id, fields in msgs:
                    ack_ids.append(msg_id)
                    sym = str(fields.get("symbol") or "").strip().upper()
                    if not sym or sym not in symbols:
                        continue
                    ltp = _safe_float(fields.get("ltp"))
                    if ltp:
                        spot_by_sym[sym] = ltp
                if ack_ids:
                    r.xack(EQ_STREAM, GROUP, *ack_ids)

        now = time.time()
        if now < next_eval:
            continue
        next_eval = now + EVAL_INTERVAL_SEC
        now_ms = int(now * 1000)

        for sym in symbols:
            spot = spot_by_sym.get(sym)
            if not spot:
                continue

            gp_ce = _load_json(r, f"{GREEKS_PHASE_UNDERLYING_PREFIX}{sym}:CE") or {}
            gp_pe = _load_json(r, f"{GREEKS_PHASE_UNDERLYING_PREFIX}{sym}:PE") or {}

            # Greeks-phase latest `iv` is PERCENT (Angel / joiner contract).
            # Convert by declared unit; implausible readings drop to None.
            iv_ce = iv_from_percent(_safe_float(gp_ce.get("iv")))
            iv_pe = iv_from_percent(_safe_float(gp_pe.get("iv")))
            iv_vals = [v for v in (iv_ce, iv_pe) if v is not None]
            implied_volatility = round(sum(iv_vals) / len(iv_vals), 6) if iv_vals else None

            gamma_trend = combine_trend(
                classify_pct_trend(_safe_float(gp_ce.get("gamma_pct")), GAMMA_TREND_UP_PCT),
                classify_pct_trend(_safe_float(gp_pe.get("gamma_pct")), GAMMA_TREND_UP_PCT),
            )
            iv_trend = combine_trend(
                classify_pct_trend(_safe_float(gp_ce.get("iv_pct")), IV_TREND_UP_PCT),
                classify_pct_trend(_safe_float(gp_pe.get("iv_pct")), IV_TREND_UP_PCT),
            )
            # gamma_pct/iv_pct of 0.0 is a real reading ("flat"), not missing.
            if gamma_trend is None and (gp_ce or gp_pe):
                if _safe_float(gp_ce.get("gamma_pct")) is not None or _safe_float(gp_pe.get("gamma_pct")) is not None:
                    gamma_trend = "flat"
            if iv_trend is None and (gp_ce or gp_pe):
                if _safe_float(gp_ce.get("iv_pct")) is not None or _safe_float(gp_pe.get("iv_pct")) is not None:
                    iv_trend = "flat"

            delta = pick_atm_delta(_safe_float(gp_ce.get("delta")), _safe_float(gp_pe.get("delta")))

            atm_call_mid = None
            atm_put_mid = None
            tsym_ce = str(gp_ce.get("tradingsymbol") or "").strip().upper()
            tsym_pe = str(gp_pe.get("tradingsymbol") or "").strip().upper()
            if tsym_ce:
                ba_ce = _load_json(r, f"{BIDASK_LATEST_PREFIX}{tsym_ce}") or {}
                atm_call_mid = _safe_float(ba_ce.get("mid"))
            if tsym_pe:
                ba_pe = _load_json(r, f"{BIDASK_LATEST_PREFIX}{tsym_pe}") or {}
                atm_put_mid = _safe_float(ba_pe.get("mid"))

            imb_doc = _load_json(r, f"{IMBALANCE_LATEST_PREFIX}{sym}") or {}
            bidask_score = clip_bidask_score(_safe_float(imb_doc.get("final_score")))

            volume_signal = _load_volume_signal_for_symbol(r, sym)
            volume_score = normalize_volume_signal(volume_signal) if volume_signal is not None else None

            oi_doc = _load_json(r, f"{OI_UNDERLYING_LATEST_PREFIX}{sym}")
            oi_score = None
            strong_resistance_nearby: Optional[bool] = None
            if oi_doc:
                oi_positioning = oi_doc.get("positioning")
                oi_score = normalize_oi_signal(oi_positioning) if oi_positioning is not None else None
                resistance = _safe_float(oi_doc.get("primary_resistance"))
                nearby = resistance_is_nearby(spot, resistance, RESISTANCE_NEAR_PCT)
                strong_resistance_nearby = False if nearby is None else nearby

            ind_doc = _load_json(r, f"{INDICATOR_SCORE_LATEST_PREFIX}{sym}") or {}
            indicator_score = _safe_float(ind_doc.get("score"))
            realized_volatility = None
            vacuum_zone = None  # no vacuum-zone publisher in this pipeline

            result = compute_expected_move(
                spot_price=spot,
                implied_volatility=implied_volatility,
                horizon_minutes=HORIZON_MINUTES,
                atm_call_mid=atm_call_mid,
                atm_put_mid=atm_put_mid,
                realized_volatility=realized_volatility,
                indicator_score=indicator_score,
                volume_score=volume_score,
                bidask_score=bidask_score,
                oi_score=oi_score,
                gamma_trend=gamma_trend,
                iv_trend=iv_trend,
                delta=delta,
                vacuum_zone=vacuum_zone,
                strong_resistance_nearby=strong_resistance_nearby,
                trading_minutes_per_day=TRADING_MINUTES_PER_DAY,
            )

            log.debug(
                "LOGIC symbol=%s spot=%s ind=%s vol=%s ba=%s oi=%s gamma=%s iv_tr=%s delta=%s res=%s "
                "-> pct=%s move=%s target=%s conf=%s dir=%s(%.4f) mult=%s quality=%s flags=%s",
                sym, spot, indicator_score, volume_score, bidask_score, oi_score,
                gamma_trend, iv_trend, delta, strong_resistance_nearby,
                result.expected_move_pct, result.expected_move, result.target_price,
                result.confidence, result.direction, result.direction_score,
                result.multiplier, result.move_quality, result.data_quality_flags,
            )

            payload = _payload_to_dict(result)
            payload["ts_ms"] = str(now_ms)
            payload["symbol"] = sym

            r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
            r.set(
                f"{LATEST_KEY_PREFIX}{sym}",
                json.dumps(payload, separators=(",", ":")),
                ex=LATEST_TTL_SEC,
            )

            if result.expected_move is not None:
                log.info(
                    "EMIT symbol=%s direction=%s pct=%.4f move=%.2f target=%.2f conf=%s quality=%s",
                    sym, result.direction, result.expected_move_pct,
                    result.expected_move, result.target_price or 0.0,
                    result.confidence, result.move_quality,
                )


if __name__ == "__main__":
    main()

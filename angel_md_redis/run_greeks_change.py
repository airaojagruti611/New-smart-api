"""
run_greeks_change.py
───────────────────────
Module 9 — Greeks Change Predictor.

Word-doc flow: Expected Move + Greeks + Liquidity → predict how Greeks
evolve if the underlying moves into the expected range. This runner is
event-driven off md:expected_move:signal (Module 8), NOT gated on a
trade-entry BUY / strike_select OK.

Candidate contract (first match):
  1. md:strike:select:latest:{SYMBOL} status=OK
  2. md:strikeflow:latest:{SYMBOL} status=OK
  3. ATM CE (bullish EM) / ATM PE (bearish EM) from
     md:greeks:phase:underlying:latest:{SYMBOL}:{CE|PE}

Then pulls:
  md:expected_move:latest:{SYMBOL}          magnitude + direction
  md:greeks:phase:latest:{TRADINGSYMBOL}    current IV/delta/gamma/theta/vega
      tagged with the payload's greeks_source (broker_api vs
      theoretical_black_scholes) — never silently relabeled
  md:bidask:latest:{TRADINGSYMBOL}          current premium (mid)

Emits:
  Stream : md:greeks_change:signal
  Key    : md:greeks_change:latest:{TRADINGSYMBOL}
  Key    : md:greeks_change:latest:{SYMBOL}
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import time
from typing import List, Optional

import redis

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except Exception:  # pragma: no cover
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

from app.config import load_symbols
from app.expected_move import iv_from_percent  # greeks-phase `iv` is PERCENT
from app.greeks_change import (
    _direction_family,
    build_scenario_matrix,
    build_summary,
    generate_iv_scenarios,
    generate_spot_scenarios,
    generate_time_scenarios,
    resolve_current_state,
    validate_option_inputs,
)
from app.logging_setup import setup_logger
from app.option_pricing import DAYS_PER_YEAR_PRICING

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

IN_STREAM = os.getenv("STREAM_EXPECTED_MOVE", "md:expected_move:signal")

EXPECTED_MOVE_LATEST_PREFIX = os.getenv("EXPECTED_MOVE_LATEST_PREFIX", "md:expected_move:latest:")
GREEKS_PHASE_LATEST_PREFIX = os.getenv("GREEKS_PHASE_LATEST_PREFIX", "md:greeks:phase:latest:")
GREEKS_PHASE_UNDERLYING_PREFIX = os.getenv("GREEKS_PHASE_UNDERLYING_PREFIX", "md:greeks:phase:underlying:latest:")
BIDASK_LATEST_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")
STRIKE_SELECT_LATEST_PREFIX = os.getenv("STRIKE_SELECT_LATEST_PREFIX", "md:strike:select:latest:")
STRIKEFLOW_LATEST_PREFIX = os.getenv("STRIKEFLOW_LATEST_PREFIX", "md:strikeflow:latest:")
ACTIVE_EXPIRY_HASH = os.getenv("ACTIVE_EXPIRY_HASH", "md:active_expiry")

OUT_STREAM = os.getenv("STREAM_GREEKS_CHANGE", "md:greeks_change:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_GREEKS_CHANGE", "50000"))
LATEST_KEY_PREFIX = os.getenv("GREEKS_CHANGE_LATEST_PREFIX", "md:greeks_change:latest:")

GROUP = os.getenv("GREEKS_CHANGE_GROUP", "greeks-change-em")
CONSUMER = os.getenv("GREEKS_CHANGE_CONSUMER", "greeks-change-1")

LATEST_TTL_SEC = int(os.getenv("GREEKS_CHANGE_LATEST_TTL_SEC", "3600"))

# "Configured" per spec 5.3 -- no upstream source anywhere in this pipeline.
RISK_FREE_RATE = float(os.getenv("GREEKS_CHANGE_RISK_FREE_RATE", "0.065"))
DIVIDEND_OR_CARRY = float(os.getenv("GREEKS_CHANGE_DIVIDEND_OR_CARRY", "0.0"))

EXPIRY_CLOSE_HHMM = os.getenv("EXPIRY_CLOSE_HHMM", "15:30")

log = setup_logger("greeks_change")


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


def _parse_flag_list(raw) -> List[str]:
    if raw is None or raw == "":
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:
            return [raw]
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    return []


def _time_to_expiry_years(expiry_iso: str) -> Optional[float]:
    """expiry_iso: 'YYYY-MM-DD'. Assumes market close (EXPIRY_CLOSE_HHMM IST) on that date."""
    try:
        y, m, d = (int(x) for x in expiry_iso.split("-"))
    except Exception:
        return None
    try:
        hh, mm = (int(x) for x in EXPIRY_CLOSE_HHMM.split(":"))
    except Exception:
        hh, mm = 15, 30

    expiry_dt = dt.datetime(y, m, d, hh, mm, tzinfo=IST)
    now = dt.datetime.now(tz=IST)
    delta_years = (expiry_dt - now).total_seconds() / (DAYS_PER_YEAR_PRICING * 24.0 * 3600.0)
    return delta_years


def _scenario_result_to_dict(r) -> dict:
    return {
        "spot_label": r.spot_scenario.label,
        "spot": r.spot_scenario.spot,
        "iv_label": r.iv_scenario.label,
        "iv": r.iv_scenario.iv,
        "time_label": r.time_scenario.label,
        "remaining_years": r.time_scenario.remaining_years,
        "premium": r.predicted_state.premium,
        "delta": r.predicted_state.delta,
        "gamma": r.predicted_state.gamma,
        "theta_per_day": r.predicted_state.theta_per_day,
        "vega_per_point": r.predicted_state.vega_per_point,
        "premium_change": r.comparison.premium_change,
        "delta_change": r.comparison.delta_change,
        "gamma_change": r.comparison.gamma_change,
        "theta_change": r.comparison.theta_change,
        "vega_change": r.comparison.vega_change,
        "risk_flags": r.risk_flags,
    }


def _strike_from_tsym(tsym: str) -> Optional[float]:
    m = re.search(r"(\d+)(CE|PE)$", (tsym or "").upper())
    if not m:
        return None
    return float(m.group(1))


def _candidate_from_strike_select(doc: dict, expiry_fallback: str) -> Optional[dict]:
    if str(doc.get("status") or "").strip().upper() != "OK":
        return None
    tsym = str(doc.get("tradingsymbol") or "").strip().upper()
    strike = _safe_float(doc.get("strike"))
    side = str(doc.get("side") or "").strip().upper()
    if not tsym or strike is None or side not in ("CE", "PE"):
        return None
    return {
        "tradingsymbol": tsym,
        "strike": strike,
        "option_type": side,
        "spot": _safe_float(doc.get("spot")),
        "expiry": str(doc.get("expiry") or expiry_fallback).strip(),
        "origin": "strike_select",
    }


def _candidate_from_strikeflow(doc: dict, expiry: str) -> Optional[dict]:
    if str(doc.get("status") or "").strip().upper() != "OK":
        return None
    tsym = str(doc.get("chosen_tradingsymbol") or "").strip().upper()
    strike = _safe_float(doc.get("chosen_strike"))
    side = str(doc.get("chosen_cp") or "").strip().upper()
    if not tsym or strike is None or side not in ("CE", "PE"):
        return None
    return {
        "tradingsymbol": tsym,
        "strike": strike,
        "option_type": side,
        "spot": _safe_float(doc.get("spot")),
        "expiry": expiry,
        "origin": "strikeflow",
    }


def _candidate_from_atm_phase(
    r: redis.Redis, sym: str, direction: str, expiry: str, spot: Optional[float]
) -> Optional[dict]:
    family = _direction_family(direction)
    preferred = "CE" if family != "bearish" else "PE"
    sides = [preferred, "PE" if preferred == "CE" else "CE"]
    for side in sides:
        gp = _load_json(r, f"{GREEKS_PHASE_UNDERLYING_PREFIX}{sym}:{side}") or {}
        tsym = str(gp.get("tradingsymbol") or "").strip().upper()
        strike = _safe_float(gp.get("strike")) or _strike_from_tsym(tsym)
        option_type = str(gp.get("cp") or gp.get("side") or side).strip().upper()
        if not tsym or strike is None or option_type not in ("CE", "PE"):
            continue
        return {
            "tradingsymbol": tsym,
            "strike": strike,
            "option_type": option_type,
            "spot": spot,
            "expiry": expiry,
            "origin": f"atm_phase_{side}",
        }
    return None


def resolve_candidate(
    r: redis.Redis,
    sym: str,
    direction: str,
    spot: Optional[float],
) -> Optional[dict]:
    expiry = str(r.hget(ACTIVE_EXPIRY_HASH, sym) or "").strip()
    ss = _candidate_from_strike_select(
        _load_json(r, f"{STRIKE_SELECT_LATEST_PREFIX}{sym}") or {}, expiry
    )
    if ss:
        return ss
    sf = _candidate_from_strikeflow(
        _load_json(r, f"{STRIKEFLOW_LATEST_PREFIX}{sym}") or {}, expiry
    )
    if sf:
        return sf
    return _candidate_from_atm_phase(r, sym, direction, expiry, spot)


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_STREAM, GROUP)

    log.info(
        "START reading %s -> %s + %s{{SYMBOL|TRADINGSYMBOL}} (rf=%s carry=%s symbols=%d)",
        IN_STREAM, OUT_STREAM, LATEST_KEY_PREFIX, RISK_FREE_RATE, DIVIDEND_OR_CARRY, len(symbols),
    )

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={IN_STREAM: ">"},
            count=50,
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
                if not sym or sym not in symbols:
                    continue

                em_doc = dict(fields)
                latest_em = _load_json(r, f"{EXPECTED_MOVE_LATEST_PREFIX}{sym}")
                if latest_em:
                    em_doc.update(latest_em)

                spot = _safe_float(em_doc.get("spot_price") or em_doc.get("spot"))
                expected_move = _safe_float(em_doc.get("final_expected_move"))
                if expected_move is None:
                    expected_move = _safe_float(em_doc.get("expected_move"))
                if expected_move is not None:
                    expected_move = abs(expected_move)
                direction = str(em_doc.get("direction") or "")

                cand = resolve_candidate(r, sym, direction, spot)
                if not cand:
                    log.debug("SKIP no_candidate symbol=%s direction=%s", sym, direction)
                    continue

                tsym = cand["tradingsymbol"]
                strike = cand["strike"]
                option_type = cand["option_type"]
                expiry_iso = cand["expiry"]
                if spot is None:
                    spot = cand.get("spot")

                gp_doc = _load_json(r, f"{GREEKS_PHASE_LATEST_PREFIX}{tsym}") or {}
                current_iv = iv_from_percent(_safe_float(gp_doc.get("iv")))
                observed_delta = _safe_float(gp_doc.get("delta"))
                observed_gamma = _safe_float(gp_doc.get("gamma"))
                observed_theta = _safe_float(gp_doc.get("theta"))
                observed_vega = _safe_float(gp_doc.get("vega"))
                observed_greeks_source = str(
                    gp_doc.get("greeks_source") or gp_doc.get("source") or ""
                ).strip() or "unavailable"
                if strike is None:
                    strike = _safe_float(gp_doc.get("strike")) or _strike_from_tsym(tsym)

                ba_doc = _load_json(r, f"{BIDASK_LATEST_PREFIX}{tsym}") or {}
                observed_premium = _safe_float(ba_doc.get("mid"))

                time_to_expiry = _time_to_expiry_years(expiry_iso) if expiry_iso else None

                validation = validate_option_inputs(
                    underlying_spot=spot, strike=strike, option_type=option_type,
                    time_to_expiry_years=time_to_expiry, current_iv=current_iv,
                    risk_free_rate=RISK_FREE_RATE,
                )
                flags = list(validation.flags)
                if not em_doc:
                    flags.append("expected_move_output_missing")
                elif expected_move is None:
                    flags.append("expected_move_unavailable")
                for f in _parse_flag_list(em_doc.get("data_quality_flags")):
                    tagged = f if f.startswith("expected_move:") else f"expected_move:{f}"
                    if tagged not in flags:
                        flags.append(tagged)

                base_payload = {
                    "ts_ms": str(now_ms),
                    "symbol": sym,
                    "tradingsymbol": tsym,
                    "candidate_origin": cand.get("origin") or "",
                    "status": validation.status,
                    "flags": json.dumps(flags, separators=(",", ":")),
                }

                if not validation.valid:
                    log.info(
                        "SKIP invalid symbol=%s tsym=%s status=%s flags=%s origin=%s",
                        sym, tsym, validation.status, flags, cand.get("origin"),
                    )
                    encoded = json.dumps(base_payload, separators=(",", ":"))
                    r.xadd(OUT_STREAM, base_payload, maxlen=OUT_MAXLEN, approximate=True)
                    r.set(f"{LATEST_KEY_PREFIX}{tsym}", encoded, ex=LATEST_TTL_SEC)
                    r.set(f"{LATEST_KEY_PREFIX}{sym}", encoded, ex=LATEST_TTL_SEC)
                    continue

                current = resolve_current_state(
                    spot=spot, strike=strike, option_type=option_type,
                    time_to_expiry_years=time_to_expiry, current_iv=current_iv,
                    risk_free_rate=RISK_FREE_RATE, dividend_or_carry=DIVIDEND_OR_CARRY,
                    observed_premium=observed_premium, observed_premium_source="market_mid",
                    observed_delta=observed_delta, observed_gamma=observed_gamma,
                    observed_theta_per_day=observed_theta, observed_vega_per_point=observed_vega,
                    observed_greeks_source=observed_greeks_source,
                )

                spot_scenarios = generate_spot_scenarios(spot, expected_move)
                iv_scenarios = generate_iv_scenarios(current_iv)
                time_scenarios = generate_time_scenarios(time_to_expiry)

                matrix = build_scenario_matrix(
                    current=current, strike=strike, option_type=option_type,
                    risk_free_rate=RISK_FREE_RATE, dividend_or_carry=DIVIDEND_OR_CARRY,
                    spot_scenarios=spot_scenarios, iv_scenarios=iv_scenarios, time_scenarios=time_scenarios,
                )
                summary = build_summary(matrix, direction, option_type=option_type)

                log.debug(
                    "LOGIC symbol=%s tsym=%s origin=%s spot=%s strike=%s type=%s iv=%s T=%.6f em=%s dir=%s "
                    "premium=%s(src=%s) greeks_src=%s scenarios=%d",
                    sym, tsym, cand.get("origin"), spot, strike, option_type, current_iv, time_to_expiry,
                    expected_move, direction, current.premium, current.premium_source,
                    current.greeks_source, len(matrix),
                )

                payload = dict(base_payload)
                payload.update({
                    "flags": json.dumps(flags, separators=(",", ":")),
                    "risk_free_rate": str(RISK_FREE_RATE),
                    "dividend_or_carry": str(DIVIDEND_OR_CARRY),
                    "expiry": expiry_iso,
                    "time_to_expiry_years": f"{time_to_expiry:.8f}" if time_to_expiry is not None else "",
                    "expected_move": "" if expected_move is None else str(expected_move),
                    "direction": direction,
                    "current_state": json.dumps({
                        "premium": current.premium, "premium_source": current.premium_source,
                        "iv": current.iv, "delta": current.delta, "gamma": current.gamma,
                        "theta_per_day": current.theta_per_day, "vega_per_point": current.vega_per_point,
                        "greeks_source": current.greeks_source,
                        # premium_change in every scenario = model(future) - model_premium
                        "model_premium": current.model_premium,
                    }, separators=(",", ":")),
                    "summary": json.dumps({
                        "base": _scenario_result_to_dict(summary.base) if summary.base else None,
                        "expected_direction": _scenario_result_to_dict(summary.expected_direction) if summary.expected_direction else None,
                        "adverse": _scenario_result_to_dict(summary.adverse) if summary.adverse else None,
                        "stress": _scenario_result_to_dict(summary.stress) if summary.stress else None,
                        "notes": summary.notes,
                    }, separators=(",", ":")),
                    "scenario_count": str(len(matrix)),
                })

                encoded = json.dumps(payload, separators=(",", ":"))
                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(f"{LATEST_KEY_PREFIX}{tsym}", encoded, ex=LATEST_TTL_SEC)
                r.set(f"{LATEST_KEY_PREFIX}{sym}", encoded, ex=LATEST_TTL_SEC)

                log.info(
                    "EMIT symbol=%s tsym=%s origin=%s scenarios=%d direction=%s expected_move=%s "
                    "greeks_src=%s stress_premium_change=%s",
                    sym, tsym, cand.get("origin"), len(matrix), direction, expected_move,
                    current.greeks_source,
                    summary.stress.comparison.premium_change if summary.stress else None,
                )

        if ack_ids:
            r.xack(IN_STREAM, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

"""
run_greeks_change.py
───────────────────────
Module 9 — Greeks Change Predictor. Event-driven off md:strike:select OK
signals (the "selected option" per spec 5.1) -- for each selected
contract, pulls:

  md:expected_move:latest:{SYMBOL}          (run_expected_move.py, Module 8)
      -> expected_move magnitude + direction classification (Mandatory input)
  md:greeks:phase:latest:{TRADINGSYMBOL}    (run_greeks_analyzer.py)
      -> current_iv, current delta/gamma/theta/vega (broker-API-sourced,
         tagged "broker_api" -- NOT recomputed, per the handover notes'
         "do not silently mix" rule)
  md:bidask:latest:{TRADINGSYMBOL}          (run_bidask_analyzer.py)
      -> current_premium proxy (mid), tagged "market_mid"

...builds the spot x IV x time scenario grid, reprices every cell with
the Black-Scholes model in app/option_pricing.py, and emits:

  Stream : md:greeks_change:signal
  Key    : md:greeks_change:latest:{TRADINGSYMBOL}

risk_free_rate / dividend_or_carry have no upstream source anywhere in
this pipeline (spec: "Configured") -- read from env with documented
defaults, and the values actually used are always included in the output
payload for transparency (never silently applied without a trace).

time_to_expiry is derived from md:strike:select's own "expiry" field
(ISO date), assuming market close (15:30 IST) on expiry day -- a
documented assumption, not given anywhere else in the pipeline.

IV from Angel's REST Greeks is typically a percent ("18.5"), same as
Module 8; converted to an annualized decimal via as_annualized_decimal
before any pricing call.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time
from typing import List, Optional

import redis

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except Exception:  # pragma: no cover
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

from app.config import load_symbols
from app.expected_move import as_annualized_decimal
from app.greeks_change import (
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

IN_STREAM = os.getenv("STREAM_STRIKE_SELECT", "md:strike:select")

EXPECTED_MOVE_LATEST_PREFIX = os.getenv("EXPECTED_MOVE_LATEST_PREFIX", "md:expected_move:latest:")
GREEKS_PHASE_LATEST_PREFIX = os.getenv("GREEKS_PHASE_LATEST_PREFIX", "md:greeks:phase:latest:")
BIDASK_LATEST_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")

OUT_STREAM = os.getenv("STREAM_GREEKS_CHANGE", "md:greeks_change:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_GREEKS_CHANGE", "50000"))
LATEST_KEY_PREFIX = os.getenv("GREEKS_CHANGE_LATEST_PREFIX", "md:greeks_change:latest:")

GROUP = os.getenv("GREEKS_CHANGE_GROUP", "greeks-change")
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


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_STREAM, GROUP)

    log.info(
        "START reading %s -> %s + %s{{TRADINGSYMBOL}} (rf=%s carry=%s symbols=%d)",
        IN_STREAM, OUT_STREAM, LATEST_KEY_PREFIX, RISK_FREE_RATE, DIVIDEND_OR_CARRY, len(symbols),
    )

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={IN_STREAM: ">"},
            count=200,
            block=2000,
        )
        if not resp:
            continue

        now_ms = int(time.time() * 1000)
        ack_ids = []

        for _stream, msgs in resp:
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)

                status = str(fields.get("status") or "").strip().upper()
                sym = str(fields.get("symbol") or "").strip().upper()
                tsym = str(fields.get("tradingsymbol") or "").strip().upper()
                if status != "OK" or not sym or sym not in symbols or not tsym:
                    log.debug("SKIP not_ok_or_unknown status=%s symbol=%s tsym=%s", status, sym, tsym)
                    continue

                spot = _safe_float(fields.get("spot"))
                strike = _safe_float(fields.get("strike"))
                option_type = str(fields.get("side") or "").strip().upper()
                expiry_iso = str(fields.get("expiry") or "").strip()

                gp_doc = _load_json(r, f"{GREEKS_PHASE_LATEST_PREFIX}{tsym}") or {}
                current_iv = as_annualized_decimal(_safe_float(gp_doc.get("iv")))
                observed_delta = _safe_float(gp_doc.get("delta"))
                observed_gamma = _safe_float(gp_doc.get("gamma"))
                observed_theta = _safe_float(gp_doc.get("theta"))
                observed_vega = _safe_float(gp_doc.get("vega"))

                ba_doc = _load_json(r, f"{BIDASK_LATEST_PREFIX}{tsym}") or {}
                observed_premium = _safe_float(ba_doc.get("mid"))

                time_to_expiry = _time_to_expiry_years(expiry_iso) if expiry_iso else None

                validation = validate_option_inputs(
                    underlying_spot=spot, strike=strike, option_type=option_type,
                    time_to_expiry_years=time_to_expiry, current_iv=current_iv,
                    risk_free_rate=RISK_FREE_RATE,
                )
                flags = list(validation.flags)

                base_payload = {
                    "ts_ms": str(now_ms),
                    "symbol": sym,
                    "tradingsymbol": tsym,
                    "status": validation.status,
                    "flags": json.dumps(flags, separators=(",", ":")),
                }

                if not validation.valid:
                    log.info("SKIP invalid symbol=%s tsym=%s status=%s flags=%s", sym, tsym, validation.status, flags)
                    r.xadd(OUT_STREAM, base_payload, maxlen=OUT_MAXLEN, approximate=True)
                    r.set(f"{LATEST_KEY_PREFIX}{tsym}", json.dumps(base_payload, separators=(",", ":")), ex=LATEST_TTL_SEC)
                    continue

                em_doc = _load_json(r, f"{EXPECTED_MOVE_LATEST_PREFIX}{sym}") or {}
                expected_move = _safe_float(em_doc.get("final_expected_move"))
                direction = str(em_doc.get("direction") or "")
                if not em_doc:
                    flags.append("expected_move_output_missing")
                elif expected_move is None:
                    flags.append("expected_move_unavailable")
                # Module 8's "no source" gaps (indicator_score / realized_volatility)
                # carry through rather than being dropped at this boundary.
                for f in _parse_flag_list(em_doc.get("data_quality_flags")):
                    tagged = f if f.startswith("expected_move:") else f"expected_move:{f}"
                    if tagged not in flags:
                        flags.append(tagged)

                current = resolve_current_state(
                    spot=spot, strike=strike, option_type=option_type,
                    time_to_expiry_years=time_to_expiry, current_iv=current_iv,
                    risk_free_rate=RISK_FREE_RATE, dividend_or_carry=DIVIDEND_OR_CARRY,
                    observed_premium=observed_premium, observed_premium_source="market_mid",
                    observed_delta=observed_delta, observed_gamma=observed_gamma,
                    observed_theta_per_day=observed_theta, observed_vega_per_point=observed_vega,
                    observed_greeks_source="broker_api",
                )

                spot_scenarios = generate_spot_scenarios(spot, expected_move)
                iv_scenarios = generate_iv_scenarios(current_iv)
                time_scenarios = generate_time_scenarios(time_to_expiry)

                matrix = build_scenario_matrix(
                    current=current, strike=strike, option_type=option_type,
                    risk_free_rate=RISK_FREE_RATE, dividend_or_carry=DIVIDEND_OR_CARRY,
                    spot_scenarios=spot_scenarios, iv_scenarios=iv_scenarios, time_scenarios=time_scenarios,
                )
                summary = build_summary(matrix, direction)

                log.debug(
                    "LOGIC symbol=%s tsym=%s spot=%s strike=%s type=%s iv=%s T=%.6f em=%s dir=%s "
                    "premium=%s(src=%s) greeks_src=%s scenarios=%d",
                    sym, tsym, spot, strike, option_type, current_iv, time_to_expiry,
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

                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(f"{LATEST_KEY_PREFIX}{tsym}", json.dumps(payload, separators=(",", ":")), ex=LATEST_TTL_SEC)

                log.info(
                    "EMIT symbol=%s tsym=%s scenarios=%d direction=%s expected_move=%s stress_premium_change=%s",
                    sym, tsym, len(matrix), direction, expected_move,
                    summary.stress.comparison.premium_change if summary.stress else None,
                )

        if ack_ids:
            r.xack(IN_STREAM, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

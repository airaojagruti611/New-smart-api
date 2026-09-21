"""
run_greeks_analyzer.py
───────────────────────
Greeks Analyzer — Trend Phase Engine. Reads per-contract Greeks from
md:features:opt (opt ticks + IV/Delta/Gamma/Theta/Vega, joined by
run_joiner.py) plus underlying spot (md:ticks:eq) + previous-day pivots
(md:pivots:prevday:{SYMBOL}, for the "price > previous resistance" check),
and classifies each contract's phase:

  Stream : md:greeks:phase:signal
  Key    : md:greeks:phase:latest:{TRADINGSYMBOL}
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, Optional

import redis

from app.config import load_symbols
from app.greeks_phase import GreeksPhaseTracker
from app.level_entry import parse_pivots_payload
from app.logging_setup import setup_logger

log = setup_logger("greeks_phase")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")
FEATURES_STREAM = os.getenv("STREAM_OPT_FEATURES", "md:features:opt")

PIVOTS_KEY_PREFIX = os.getenv("PIVOTS_PREVDAY_PREFIX", "md:pivots:prevday:")

OUT_STREAM = os.getenv("STREAM_GREEKS_PHASE", "md:greeks:phase:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_GREEKS_PHASE", "200000"))
LATEST_KEY_PREFIX = os.getenv("GREEKS_PHASE_LATEST_PREFIX", "md:greeks:phase:latest:")
UNDERLYING_LATEST_KEY_PREFIX = os.getenv("GREEKS_PHASE_UNDERLYING_PREFIX", "md:greeks:phase:underlying:latest:")

GROUP = os.getenv("GREEKS_PHASE_GROUP", "greeks-phase")
CONSUMER = os.getenv("GREEKS_PHASE_CONSUMER", "greeks-phase-1")

LATEST_TTL_SEC = int(os.getenv("GREEKS_PHASE_LATEST_TTL_SEC", "3600"))


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


def _load_pivots(r: redis.Redis, symbol: str):
    raw = r.get(f"{PIVOTS_KEY_PREFIX}{symbol}")
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    return parse_pivots_payload(data)


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)

    ensure_group(r, EQ_STREAM, GROUP)
    ensure_group(r, FEATURES_STREAM, GROUP)

    spot_by_sym: Dict[str, float] = {}
    last_spot_by_sym: Dict[str, float] = {}  # prior EQ LTP for recent move %
    trackers: Dict[str, GreeksPhaseTracker] = {}
    last_action: Dict[str, str] = {}
    atm_strike: Dict[tuple, float] = {}

    log.info(
        "START reading %s + %s -> %s + %s{{TRADINGSYMBOL}} symbols=%d",
        EQ_STREAM, FEATURES_STREAM, OUT_STREAM, LATEST_KEY_PREFIX, len(symbols),
    )

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={EQ_STREAM: ">", FEATURES_STREAM: ">"},
            count=2000,
            block=2000,
        )
        if not resp:
            continue

        now_ms = int(time.time() * 1000)

        for stream, msgs in resp:
            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)

                if stream == EQ_STREAM:
                    sym = str(fields.get("symbol") or "").strip().upper()
                    if not sym or sym not in symbols:
                        continue
                    ltp = _safe_float(fields.get("ltp"))
                    if ltp:
                        prev_spot = spot_by_sym.get(sym)
                        if prev_spot is not None:
                            last_spot_by_sym[sym] = prev_spot
                        spot_by_sym[sym] = ltp
                    continue

                # md:features:opt tick
                und = str(fields.get("underlying") or "").strip().upper()
                tsym = str(fields.get("tradingsymbol") or "").strip().upper()
                if not und or not tsym or und not in symbols:
                    log.debug("SKIP unknown_or_missing underlying=%s tsym=%s", und, tsym)
                    continue

                cp = str(fields.get("cp") or "").strip().upper()
                delta = _safe_float(fields.get("delta"))
                gamma = _safe_float(fields.get("gamma"))
                theta = _safe_float(fields.get("theta"))
                vega = _safe_float(fields.get("vega"))
                iv = _safe_float(fields.get("iv"))
                strike = _safe_float(fields.get("strike"))
                greeks_source = str(
                    fields.get("greeks_source") or fields.get("source") or ""
                ).strip()

                spot = spot_by_sym.get(und)
                prev_spot = last_spot_by_sym.get(und)
                # Recent underlying move (tick-to-tick), not day-change vs prev close.
                price_change_pct = None
                if spot is not None and prev_spot and prev_spot != 0:
                    price_change_pct = round((spot - prev_spot) / prev_spot * 100.0, 4)

                # None = pivots unavailable -> do not block Markup.
                pivots = _load_pivots(r, und)
                breakout: Optional[bool] = None
                if spot is not None and pivots is not None:
                    if cp == "CE":
                        breakout = spot > pivots.R1
                    elif cp == "PE":
                        breakout = spot < pivots.S1

                if tsym not in trackers:
                    trackers[tsym] = GreeksPhaseTracker()

                res = trackers[tsym].analyze(
                    cp=cp, delta=delta, gamma=gamma, theta=theta, vega=vega, iv=iv,
                    price_change_pct=price_change_pct, breakout=breakout,
                )

                log.debug(
                    "LOGIC tsym=%s cp=%s delta=%s(pct=%s) gamma=%s(pct=%s) iv=%s(pct=%s) "
                    "theta=%s(pct=%s) price_chg=%s breakout=%s -> phase=%s action=%s reason=%s",
                    tsym, cp, delta, res.delta_pct, gamma, res.gamma_pct,
                    iv, res.iv_pct, theta, res.theta_pct, price_change_pct, breakout,
                    res.phase, res.action, res.reason,
                )

                def _f(v: Optional[float]) -> str:
                    return "" if v is None else str(v)

                if breakout is None:
                    breakout_s = ""
                else:
                    breakout_s = "1" if breakout else "0"

                payload = {
                    "ts_ms": str(now_ms),
                    "tradingsymbol": tsym,
                    "underlying": und,
                    "cp": cp,
                    "strike": _f(strike),
                    "phase": res.phase,
                    "action": res.action,
                    "side": res.side,
                    "delta": _f(res.delta),
                    "gamma": _f(res.gamma),
                    "theta": _f(res.theta),
                    "vega": _f(res.vega),
                    "iv": _f(res.iv),
                    "greeks_source": greeks_source,
                    "delta_pct": _f(res.delta_pct),
                    "gamma_pct": _f(res.gamma_pct),
                    "iv_pct": _f(res.iv_pct),
                    "theta_pct": _f(res.theta_pct),
                    "price_change_pct": _f(price_change_pct),
                    "breakout": breakout_s,
                    "reason": res.reason,
                }
                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(
                    f"{LATEST_KEY_PREFIX}{tsym}",
                    json.dumps(payload, separators=(",", ":")),
                    ex=LATEST_TTL_SEC,
                )

                # Publish the nearest-to-spot contract per (underlying, CE/PE)
                # so entry_trigger/composite gate on ATM, not the last strike seen.
                if spot is not None:
                    if strike is not None and cp in ("CE", "PE"):
                        key = (und, cp)
                        prev_k = atm_strike.get(key)
                        if prev_k is None or strike == prev_k or abs(strike - spot) < abs(prev_k - spot):
                            atm_strike[key] = strike
                            r.set(
                                f"{UNDERLYING_LATEST_KEY_PREFIX}{und}:{cp}",
                                json.dumps(payload, separators=(",", ":")),
                                ex=LATEST_TTL_SEC,
                            )

                prev_action = last_action.get(tsym)
                if res.action != "HOLD" or prev_action not in (None, "HOLD"):
                    log.info("EMIT tsym=%s phase=%s action=%s payload=%s", tsym, res.phase, res.action, payload)
                last_action[tsym] = res.action

            if ack_ids:
                r.xack(stream, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

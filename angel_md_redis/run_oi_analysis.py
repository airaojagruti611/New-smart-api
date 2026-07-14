"""
run_oi_analysis.py
───────────────────────
Open Interest Analysis — full module (Steps 1-6). Reads md:ticks:opt
(ltp, vol, oi) + md:ticks:eq (spot, for ATM/moneyness), tracks each
contract's previous price/OI/volume snapshot across evaluation cycles,
and emits two streams:

  Per-contract (Steps 1-3: OI change, smart money participation, buildup):
    Stream : md:oi:signal
    Key    : md:oi:latest:{TRADINGSYMBOL}

  Per-underlying (Steps 4-6: OI concentration S/R, max pain, positioning):
    Stream : md:oi:underlying:signal
    Key    : md:oi:underlying:latest:{SYMBOL}

Evaluated on a periodic cycle (OI updates far slower than tick rate; no
reason to reclassify on every raw tick).

Dominant buildup (feeding Step 6) is read from the ATM CALL contract's
buildup_type — ATM is where positioning sentiment is most actively
expressed. Not specified in the brief; change source_of_dominant_buildup()
if you want the ATM PUT, or an OI-weighted vote across strikes, instead.
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, List, Optional

import redis

from app.config import load_symbols
from app.logging_setup import setup_logger
from app.oi_analysis import (
    BuildupResult,
    OIConcentration,
    OILevel,
    RollingStat,
    classify_buildup,
    max_pain,
    oi_concentration,
    positioning_signal,
    smart_money_participation,
)
from app.strike_flow import moneyness

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")
OPT_STREAM = os.getenv("STREAM_OPT", "md:ticks:opt")

OUT_STREAM = os.getenv("STREAM_OI_SIGNAL", "md:oi:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_OI", "50000"))
LATEST_KEY_PREFIX = os.getenv("OI_LATEST_PREFIX", "md:oi:latest:")

OUT_UNDERLYING_STREAM = os.getenv("STREAM_OI_UNDERLYING_SIGNAL", "md:oi:underlying:signal")
OUT_UNDERLYING_MAXLEN = int(os.getenv("STREAM_MAXLEN_OI_UNDERLYING", "20000"))
UNDERLYING_LATEST_KEY_PREFIX = os.getenv("OI_UNDERLYING_LATEST_PREFIX", "md:oi:underlying:latest:")

GROUP = os.getenv("OI_GROUP", "oi_analysis")
CONSUMER = os.getenv("OI_CONSUMER", "oi-analysis-1")

PRICE_CHANGE_THRESHOLD_PCT = float(os.getenv("OI_PRICE_THRESHOLD_PCT", "0.05"))
OI_CHANGE_THRESHOLD_PCT = float(os.getenv("OI_CHANGE_THRESHOLD_PCT", "1.0"))
AVG_VOLUME_WINDOW = int(os.getenv("OI_AVG_VOLUME_WINDOW", "20"))
EVAL_INTERVAL_SEC = float(os.getenv("OI_EVAL_INTERVAL_SEC", "5.0"))
LATEST_TTL_SEC = int(os.getenv("OI_LATEST_TTL_SEC", "3600"))

log = setup_logger("oi_analysis")


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


def _to_contract_payload(
    tsym: str, underlying: str, cp: str, res: BuildupResult, smart_money: bool, now_ms: int
) -> Dict[str, str]:
    return {
        "ts_ms": str(now_ms),
        "tradingsymbol": tsym,
        "underlying": underlying,
        "cp": cp,
        "strike": str(res.strike),
        "price": str(res.price),
        "previous_price": str(res.previous_price),
        "price_change": str(res.price_change),
        "price_change_pct": "" if res.price_change_pct is None else f"{res.price_change_pct:.4f}",
        "open_interest": str(res.open_interest),
        "previous_open_interest": str(res.previous_open_interest),
        "oi_change": str(res.oi_change),
        "oi_change_pct": "" if res.oi_change_pct is None else f"{res.oi_change_pct:.4f}",
        "buildup_type": res.buildup_type,
        "smart_money_participation": "1" if smart_money else "0",
    }


class ContractSnapshot:
    __slots__ = ("underlying", "cp", "strike", "price", "oi", "cum_vol")

    def __init__(self):
        self.underlying = ""
        self.cp = ""
        self.strike = 0.0
        self.price = 0.0
        self.oi = 0.0
        self.cum_vol = 0.0


def _source_of_dominant_buildup(
    book: Dict[str, ContractSnapshot],
    buildups: Dict[str, str],
    atm: float,
) -> str:
    """ATM CALL contract's buildup_type drives Step 6's positioning signal."""
    for tsym, snap in book.items():
        if snap.cp == "CE" and abs(snap.strike - atm) < 1e-6:
            return buildups.get(tsym, "NEUTRAL")
    return "NEUTRAL"


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, EQ_STREAM, GROUP)
    ensure_group(r, OPT_STREAM, GROUP)

    spot_by_sym: Dict[str, float] = {}
    current: Dict[str, ContractSnapshot] = {}
    previous: Dict[str, ContractSnapshot] = {}
    period_volume_avg: Dict[str, RollingStat] = {}

    next_eval = time.time() + EVAL_INTERVAL_SEC

    log.info(
        "START reading %s + %s -> %s + %s / %s + %s "
        "(eval_interval=%ss price_thresh=%s%% oi_thresh=%s%% avg_vol_window=%s symbols=%d)",
        EQ_STREAM, OPT_STREAM, OUT_STREAM, LATEST_KEY_PREFIX,
        OUT_UNDERLYING_STREAM, UNDERLYING_LATEST_KEY_PREFIX,
        EVAL_INTERVAL_SEC, PRICE_CHANGE_THRESHOLD_PCT, OI_CHANGE_THRESHOLD_PCT, AVG_VOLUME_WINDOW, len(symbols),
    )

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={EQ_STREAM: ">", OPT_STREAM: ">"},
            count=2000,
            block=2000,
        )

        if resp:
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
                            spot_by_sym[sym] = ltp
                        continue

                    und = str(fields.get("underlying") or "").strip().upper()
                    tsym = str(fields.get("tradingsymbol") or "").strip().upper()
                    if not und or not tsym or und not in symbols:
                        continue

                    ltp = _safe_float(fields.get("ltp"))
                    oi = _safe_float(fields.get("oi"))
                    cum_vol = _safe_float(fields.get("vol")) or 0.0
                    strike = _safe_float(fields.get("strike")) or 0.0
                    cp = str(fields.get("cp") or "").strip().upper()
                    if ltp is None or oi is None:
                        continue

                    snap = current.setdefault(tsym, ContractSnapshot())
                    snap.underlying, snap.cp, snap.strike = und, cp, strike
                    snap.price, snap.oi, snap.cum_vol = ltp, oi, cum_vol

                if ack_ids:
                    r.xack(stream, GROUP, *ack_ids)

        now = time.time()
        if now < next_eval:
            continue
        next_eval = now + EVAL_INTERVAL_SEC
        now_ms = int(now * 1000)

        buildups: Dict[str, str] = {}
        by_underlying: Dict[str, Dict[str, ContractSnapshot]] = {}

        # ── Per-contract pass: Steps 1-3 ──────────────────────────────
        for tsym, snap in current.items():
            prev = previous.get(tsym)
            prev_price = prev.price if prev else snap.price
            prev_oi = prev.oi if prev else snap.oi
            prev_cum_vol = prev.cum_vol if prev else snap.cum_vol

            period_volume = max(0.0, snap.cum_vol - prev_cum_vol)
            vol_stat = period_volume_avg.setdefault(tsym, RollingStat(AVG_VOLUME_WINDOW))
            avg_volume = vol_stat.avg

            res = classify_buildup(
                symbol=tsym,
                strike=snap.strike,
                price=snap.price,
                previous_price=prev_price,
                volume=period_volume,
                current_oi=snap.oi,
                previous_oi=prev_oi,
                price_threshold_pct=PRICE_CHANGE_THRESHOLD_PCT,
                oi_threshold_pct=OI_CHANGE_THRESHOLD_PCT,
            )
            smart_money = smart_money_participation(res.oi_change, period_volume, avg_volume)
            vol_stat.push(period_volume)

            buildups[tsym] = res.buildup_type
            by_underlying.setdefault(snap.underlying, {})[tsym] = snap

            log.debug(
                "LOGIC tsym=%s underlying=%s strike=%s cp=%s price=%s->%s oi=%s->%s "
                "period_vol=%s avg_vol=%s smart_money=%s buildup=%s",
                tsym, snap.underlying, snap.strike, snap.cp, prev_price, snap.price,
                prev_oi, snap.oi, period_volume, avg_volume, smart_money, res.buildup_type,
            )

            payload = _to_contract_payload(tsym, snap.underlying, snap.cp, res, smart_money, now_ms)
            r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
            r.set(f"{LATEST_KEY_PREFIX}{tsym}", json.dumps(payload, separators=(",", ":")), ex=LATEST_TTL_SEC)

            if res.buildup_type != "NEUTRAL" or smart_money:
                log.info("EMIT tsym=%s buildup=%s smart_money=%s payload=%s", tsym, res.buildup_type, smart_money, payload)

            snap_copy = ContractSnapshot()
            snap_copy.underlying, snap_copy.cp, snap_copy.strike = snap.underlying, snap.cp, snap.strike
            snap_copy.price, snap_copy.oi, snap_copy.cum_vol = snap.price, snap.oi, snap.cum_vol
            previous[tsym] = snap_copy

        # ── Per-underlying pass: Steps 4-6 ────────────────────────────
        for und, book in by_underlying.items():
            spot = spot_by_sym.get(und)
            if not spot:
                continue

            by_strike: Dict[float, Dict[str, float]] = {}
            for snap in book.values():
                if snap.strike <= 0 or not snap.cp:
                    continue
                d = by_strike.setdefault(snap.strike, {"call_oi": 0.0, "put_oi": 0.0})
                if snap.cp == "CE":
                    d["call_oi"] = snap.oi
                elif snap.cp == "PE":
                    d["put_oi"] = snap.oi

            levels = [OILevel(strike=k, call_oi=v["call_oi"], put_oi=v["put_oi"]) for k, v in by_strike.items()]
            if not levels:
                continue

            strikes_sorted = sorted(by_strike.keys())
            atm = min(strikes_sorted, key=lambda s: abs(s - spot))

            concentration: OIConcentration = oi_concentration(levels)
            mp = max_pain(levels)
            dominant_buildup = _source_of_dominant_buildup(book, buildups, atm)
            positioning = positioning_signal(dominant_buildup, concentration)

            log.debug(
                "UNDERLYING underlying=%s spot=%s atm=%s max_pain=%s dominant_buildup=%s "
                "resistance=%s support=%s positioning=%s",
                und, spot, atm, mp, dominant_buildup,
                concentration.primary_resistance, concentration.primary_support, positioning.signal,
            )

            payload = {
                "ts_ms": str(now_ms),
                "underlying": und,
                "spot": f"{spot:.2f}",
                "atm": f"{atm:.2f}",
                "max_pain": "" if mp is None else str(mp),
                "primary_resistance": "" if concentration.primary_resistance is None else str(concentration.primary_resistance),
                "primary_support": "" if concentration.primary_support is None else str(concentration.primary_support),
                "resistance_strikes": json.dumps(concentration.resistance_strikes, separators=(",", ":")),
                "support_strikes": json.dumps(concentration.support_strikes, separators=(",", ":")),
                "dominant_buildup": dominant_buildup,
                "positioning": positioning.signal,
                "positioning_reason": positioning.reason,
            }
            r.xadd(OUT_UNDERLYING_STREAM, payload, maxlen=OUT_UNDERLYING_MAXLEN, approximate=True)
            r.set(
                f"{UNDERLYING_LATEST_KEY_PREFIX}{und}",
                json.dumps(payload, separators=(",", ":")),
                ex=LATEST_TTL_SEC,
            )

            if positioning.signal != "NEUTRAL":
                log.info("EMIT underlying=%s positioning=%s payload=%s", und, positioning.signal, payload)


if __name__ == "__main__":
    main()

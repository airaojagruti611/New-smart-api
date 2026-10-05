"""
run_oi_analysis.py
───────────────────────
Open Interest Analysis — full module (Steps 1-6). Reads md:ticks:opt
(ltp, vol, oi) + md:ticks:eq (spot, for ATM/moneyness).

Build-up (Steps 1, 3) uses a session-open baseline for previous_price /
previous_open_interest (the feed does not publish prior-day OI; first
print of the IST day is the spec's previous_* proxy). Smart money
(Step 2) uses the 5s interval OI/volume delta vs a rolling average.

  Per-contract (Steps 1-3: OI change, smart money participation, buildup):
    Stream : md:oi:signal
    Key    : md:oi:latest:{TRADINGSYMBOL}

  Per-underlying (Steps 4-6: OI concentration S/R, max pain, positioning):
    Stream : md:oi:underlying:signal
    Key    : md:oi:underlying:latest:{SYMBOL}

Dominant buildup for Step 6 is an OI-change-weighted vote across the
chain (CE/PE mapped to underlying direction), not ATM-CE only.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time
from typing import Dict, Optional

import redis

from app.config import load_symbols
from app.freshness import env_ms
from app.logging_setup import setup_logger
from app.order_flow import newest_data_ts, prune_stale_book, tick_ts_ms
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
    vote_dominant_buildup,
)

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except Exception:
    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

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

# Spec uses raw sign of price/OI change. Keep env knobs; default 0.
PRICE_CHANGE_THRESHOLD_PCT = float(os.getenv("OI_PRICE_THRESHOLD_PCT", "0"))
OI_CHANGE_THRESHOLD_PCT = float(os.getenv("OI_CHANGE_THRESHOLD_PCT", "0"))
AVG_VOLUME_WINDOW = int(os.getenv("OI_AVG_VOLUME_WINDOW", "20"))
EVAL_INTERVAL_SEC = float(os.getenv("OI_EVAL_INTERVAL_SEC", "5.0"))
LATEST_TTL_SEC = int(os.getenv("OI_LATEST_TTL_SEC", "3600"))
SESSION_TTL_SEC = int(os.getenv("OI_SESSION_TTL_SEC", "43200"))
SESSION_KEY_PREFIX = os.getenv("OI_SESSION_PREFIX", "md:oi:session:")
# Contracts not updated within this window are pruned (not republished as
# fresh); payload ts_ms is the source tick time of the data used. OI stays
# valid for the whole session and quiet strikes can go many minutes without a
# tick, so the default keeps a full session (6h15m) — otherwise illiquid
# strikes drop out of max pain / OI support-resistance. Previous-day ticks are
# already rejected by the session-date guard and SESSION_RESET.
BOOK_MAX_AGE_MS = env_ms("OI_BOOK_MAX_AGE_SEC", 22500)

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


def _ist_today() -> str:
    return dt.datetime.now(IST).date().isoformat()


def _ist_date_of_ms(ts_ms: int) -> str:
    return dt.datetime.fromtimestamp(ts_ms / 1000.0, IST).date().isoformat()


def tick_is_current_session(ts_ms: Optional[int], today: str) -> bool:
    """A tick may update state / seed the day's baseline only if its source
    time falls on today's IST session date (replayed old ticks must not
    become today's previous_price / previous_oi)."""
    if not ts_ms:
        return False
    return _ist_date_of_ms(ts_ms) == today


def _to_contract_payload(
    tsym: str, underlying: str, cp: str, res: BuildupResult, smart_money: bool, now_ms: int
) -> Dict[str, str]:
    # now_ms here is the SOURCE tick time of the contract snapshot.
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
    __slots__ = ("underlying", "cp", "strike", "price", "oi", "cum_vol", "data_ts_ms")

    def __init__(self):
        self.underlying = ""
        self.cp = ""
        self.strike = 0.0
        self.price = 0.0
        self.oi = 0.0
        self.cum_vol = 0.0
        self.data_ts_ms = 0


def _copy_snap(snap: ContractSnapshot) -> ContractSnapshot:
    out = ContractSnapshot()
    out.underlying, out.cp, out.strike = snap.underlying, snap.cp, snap.strike
    out.price, out.oi, out.cum_vol = snap.price, snap.oi, snap.cum_vol
    out.data_ts_ms = snap.data_ts_ms
    return out


def _load_session_baseline(r: redis.Redis, tsym: str, day: str) -> Optional[ContractSnapshot]:
    raw = r.get(f"{SESSION_KEY_PREFIX}{day}:{tsym}")
    if not raw:
        return None
    try:
        doc = json.loads(raw)
    except Exception:
        return None
    snap = ContractSnapshot()
    snap.price = float(doc.get("price") or 0.0)
    snap.oi = float(doc.get("oi") or 0.0)
    snap.cum_vol = float(doc.get("cum_vol") or 0.0)
    snap.strike = float(doc.get("strike") or 0.0)
    snap.cp = str(doc.get("cp") or "")
    snap.underlying = str(doc.get("underlying") or "")
    return snap


def _save_session_baseline(r: redis.Redis, tsym: str, day: str, snap: ContractSnapshot) -> None:
    payload = {
        "price": snap.price,
        "oi": snap.oi,
        "cum_vol": snap.cum_vol,
        "strike": snap.strike,
        "cp": snap.cp,
        "underlying": snap.underlying,
    }
    r.set(
        f"{SESSION_KEY_PREFIX}{day}:{tsym}",
        json.dumps(payload, separators=(",", ":")),
        ex=SESSION_TTL_SEC,
        nx=True,
    )


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, EQ_STREAM, GROUP)
    ensure_group(r, OPT_STREAM, GROUP)

    spot_by_sym: Dict[str, float] = {}
    current: Dict[str, ContractSnapshot] = {}
    previous: Dict[str, ContractSnapshot] = {}
    session_open: Dict[str, ContractSnapshot] = {}
    period_volume_avg: Dict[str, RollingStat] = {}
    session_day = _ist_today()

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
                    tick_ms = tick_ts_ms(fields)
                    if not tick_is_current_session(tick_ms, _ist_today()):
                        log.debug("SKIP old_session_tick tsym=%s ts=%s", tsym, tick_ms)
                        continue

                    snap = current.setdefault(tsym, ContractSnapshot())
                    snap.underlying, snap.cp, snap.strike = und, cp, strike
                    snap.price, snap.oi, snap.cum_vol = ltp, oi, cum_vol
                    snap.data_ts_ms = tick_ms

                if ack_ids:
                    r.xack(stream, GROUP, *ack_ids)

        now = time.time()
        if now < next_eval:
            continue
        next_eval = now + EVAL_INTERVAL_SEC
        now_ms = int(now * 1000)

        today = _ist_today()
        if today != session_day:
            log.info("SESSION_RESET prev=%s new=%s", session_day, today)
            session_day = today
            session_open.clear()
            previous.clear()
            period_volume_avg.clear()
            current.clear()  # yesterday's snapshots must not seed today's baseline

        dropped = prune_stale_book(current, now_ms, BOOK_MAX_AGE_MS)
        if dropped:
            log.debug("PRUNE stale_contracts=%s", dropped)

        buildups: Dict[str, str] = {}
        oi_change_abs: Dict[str, float] = {}
        smart_by_tsym: Dict[str, bool] = {}
        by_underlying: Dict[str, Dict[str, ContractSnapshot]] = {}

        # ── Per-contract pass: Steps 1-3 ──────────────────────────────
        for tsym, snap in current.items():
            if tsym not in session_open:
                stored = _load_session_baseline(r, tsym, session_day)
                if stored is None:
                    stored = _copy_snap(snap)
                    _save_session_baseline(r, tsym, session_day, stored)
                session_open[tsym] = stored

            baseline = session_open[tsym]
            prev = previous.get(tsym)
            prev_cum_vol = prev.cum_vol if prev else snap.cum_vol
            prev_cycle_oi = prev.oi if prev else snap.oi

            period_volume = max(0.0, snap.cum_vol - prev_cum_vol)
            vol_stat = period_volume_avg.setdefault(tsym, RollingStat(AVG_VOLUME_WINDOW))
            avg_volume = vol_stat.avg
            interval_oi_chg = snap.oi - prev_cycle_oi

            res = classify_buildup(
                symbol=tsym,
                strike=snap.strike,
                price=snap.price,
                previous_price=baseline.price,
                volume=period_volume,
                current_oi=snap.oi,
                previous_oi=baseline.oi,
                price_threshold_pct=PRICE_CHANGE_THRESHOLD_PCT,
                oi_threshold_pct=OI_CHANGE_THRESHOLD_PCT,
            )
            smart_money = smart_money_participation(interval_oi_chg, period_volume, avg_volume)
            vol_stat.push(period_volume)

            buildups[tsym] = res.buildup_type
            oi_change_abs[tsym] = abs(res.oi_change)
            smart_by_tsym[tsym] = smart_money
            by_underlying.setdefault(snap.underlying, {})[tsym] = snap

            log.debug(
                "LOGIC tsym=%s underlying=%s strike=%s cp=%s price=%s->%s oi=%s->%s "
                "period_vol=%s avg_vol=%s interval_oi=%s smart_money=%s buildup=%s",
                tsym, snap.underlying, snap.strike, snap.cp, baseline.price, snap.price,
                baseline.oi, snap.oi, period_volume, avg_volume, interval_oi_chg,
                smart_money, res.buildup_type,
            )

            payload = _to_contract_payload(tsym, snap.underlying, snap.cp, res, smart_money, snap.data_ts_ms)
            r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
            r.set(f"{LATEST_KEY_PREFIX}{tsym}", json.dumps(payload, separators=(",", ":")), ex=LATEST_TTL_SEC)

            if res.buildup_type != "NEUTRAL" or smart_money:
                log.info("EMIT tsym=%s buildup=%s smart_money=%s payload=%s", tsym, res.buildup_type, smart_money, payload)

            previous[tsym] = _copy_snap(snap)

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

            concentration: OIConcentration = oi_concentration(levels, spot=spot)
            mp = max_pain(levels)
            vote = vote_dominant_buildup(
                (snap.cp, buildups.get(tsym, "NEUTRAL"), oi_change_abs.get(tsym, 0.0))
                for tsym, snap in book.items()
            )
            dominant_buildup = vote.buildup
            volume_participation = any(smart_by_tsym.get(tsym) for tsym in book)
            positioning = positioning_signal(
                dominant_buildup,
                concentration,
                spot=spot,
                max_pain_strike=mp,
                volume_participation=volume_participation,
            )

            log.debug(
                "UNDERLYING underlying=%s spot=%s atm=%s max_pain=%s dominant_buildup=%s "
                "vote_bull=%s vote_bear=%s n_voted=%s resistance=%s support=%s positioning=%s reason=%s",
                und, spot, atm, mp, dominant_buildup,
                vote.bullish_weight, vote.bearish_weight, vote.n_voted,
                concentration.primary_resistance, concentration.primary_support,
                positioning.signal, positioning.reason,
            )

            payload = {
                "ts_ms": str(newest_data_ts(book.values()) or now_ms),
                "eval_ts_ms": str(now_ms),
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

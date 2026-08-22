"""
run_expected_move.py
───────────────────────
Module 8 — Expected Move Calculator. Reads live spot from md:ticks:eq,
plus already-published latest: keys from FIVE existing modules (no
producer changes required anywhere):

  md:greeks:phase:underlying:latest:{SYM}:CE / :PE  (run_greeks_analyzer.py)
      -> implied_volatility, and the ATM CE/PE tradingsymbol to look up
  md:bidask:latest:{tradingsymbol}                  (run_bidask_analyzer.py)
      -> atm_call_mid / atm_put_mid (the "mid" field)
  md:imbalance:latest:{SYMBOL}                      (run_bidask_imbalance.py)
      -> bidask_score (final_score is already -1..+1, used directly)
  md:volume:latest                                  (run_volume_analyzer.py)
      -> volume_score (adapted from the string signal)
  md:oi:underlying:latest:{SYM}                      (run_oi_analysis.py)
      -> oi_score (adapted from the positioning string)

indicator_score has no upstream source anywhere in this pipeline and is
passed as None (flagged in data_quality_flags, not fabricated).
realized_volatility likewise has no source and is passed as None.

Emits:
  Stream : md:expected_move:signal
  Key    : md:expected_move:latest:{SYMBOL}

Evaluated on a periodic cycle (like run_composite.py / run_strike_flow.py)
since it synthesizes several independently-updating cached signals rather
than reacting to a single stream.
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
    clip_bidask_score,
    compute_expected_move,
    normalize_oi_signal,
    normalize_volume_signal,
)
from app.logging_setup import setup_logger

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")

GREEKS_PHASE_UNDERLYING_PREFIX = os.getenv("GREEKS_PHASE_UNDERLYING_PREFIX", "md:greeks:phase:underlying:latest:")
BIDASK_LATEST_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")
IMBALANCE_LATEST_PREFIX = os.getenv("IMBALANCE_LATEST_PREFIX", "md:imbalance:latest:")
VOLUME_LATEST_KEY = os.getenv("VOLUME_LATEST_KEY", "md:volume:latest")
OI_UNDERLYING_LATEST_PREFIX = os.getenv("OI_UNDERLYING_LATEST_PREFIX", "md:oi:underlying:latest:")

OUT_STREAM = os.getenv("STREAM_EXPECTED_MOVE", "md:expected_move:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_EXPECTED_MOVE", "50000"))
LATEST_KEY_PREFIX = os.getenv("EXPECTED_MOVE_LATEST_PREFIX", "md:expected_move:latest:")

GROUP = os.getenv("EXPECTED_MOVE_GROUP", "expected-move")
CONSUMER = os.getenv("EXPECTED_MOVE_CONSUMER", "expected-move-1")

EVAL_INTERVAL_SEC = float(os.getenv("EXPECTED_MOVE_EVAL_INTERVAL_SEC", "5.0"))
LATEST_TTL_SEC = int(os.getenv("EXPECTED_MOVE_LATEST_TTL_SEC", "3600"))

# Spec example uses 60 minutes; override per-deployment via env.
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
        "START reading %s + greeks-phase/bidask/imbalance/volume/oi latest -> %s + %s{{SYMBOL}} "
        "(eval_interval=%ss horizon_min=%s symbols=%d)",
        EQ_STREAM, OUT_STREAM, LATEST_KEY_PREFIX, EVAL_INTERVAL_SEC, HORIZON_MINUTES, len(symbols),
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

            iv_ce = _safe_float(gp_ce.get("iv"))
            iv_pe = _safe_float(gp_pe.get("iv"))
            iv_vals = [v for v in (iv_ce, iv_pe) if v is not None and v > 0]
            implied_volatility = round(sum(iv_vals) / len(iv_vals), 6) if iv_vals else None

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

            oi_doc = _load_json(r, f"{OI_UNDERLYING_LATEST_PREFIX}{sym}") or {}
            oi_positioning = oi_doc.get("positioning")
            oi_score = normalize_oi_signal(oi_positioning) if oi_positioning is not None else None

            # No upstream source anywhere in this pipeline for either of these:
            indicator_score = None
            realized_volatility = None

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
                trading_minutes_per_day=TRADING_MINUTES_PER_DAY,
            )

            log.debug(
                "LOGIC symbol=%s spot=%s iv=%s call_mid=%s put_mid=%s bidask=%s vol=%s oi=%s -> "
                "final_move=%s direction=%s(%.4f) quality=%s flags=%s",
                sym, spot, implied_volatility, atm_call_mid, atm_put_mid,
                bidask_score, volume_score, oi_score,
                result.final_expected_move, result.direction, result.direction_score,
                result.magnitude_quality, result.data_quality_flags,
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

            if result.final_expected_move is not None:
                log.info(
                    "EMIT symbol=%s direction=%s move=%.2f range=[%.2f,%.2f] quality=%s",
                    sym, result.direction, result.final_expected_move,
                    result.lower_range or 0.0, result.upper_range or 0.0, result.magnitude_quality,
                )


if __name__ == "__main__":
    main()

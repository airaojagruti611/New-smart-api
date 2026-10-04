"""
run_probability.py
──────────────────
Module 12 — Probability Engine Redis wiring (DECISION.md D7/D8).

Consumes md:strike:intel OK messages (the SIE payload echoes the entry
trigger as entry_*), adds a few latest: keys and calls the pure
app/probability_engine.compute_probability():

  confluence  <- votes: HTF bias, Supertrend bias, EMA state, volume signal,
                 entry `aligned`, composite score sign  (md:composite:latest:{SYM})
  direction   <- Module 8 direction_score (-1..+1), side-aligned
  intensity   <- entry volume_signal + volume_surge + Module 8 confidence
  amd         <- md:greeks:phase:latest:{TSYM}.phase (chosen strike)
  historical  <- md:journal:stats bucket win rate (neutral 50 until 30 trades)
  oi          <- entry oi_positioning, side-aligned
  liquidity   <- SIE liquidity_score of the chosen strike
  greeks      <- SIE greeks_score of the chosen strike

Sideways (hard filter): Module 8 direction NEUTRAL AND market breadth
(md:regime:latest) NEUTRAL — a stock with no expected direction in a
directionless market.

Emits:
  Stream : md:probability
  Key    : md:probability:latest:{SYMBOL}
  ZSet   : md:probability:rank  (symbol -> probability; Trade Ranking view)

The payload carries the SIE context forward so ICARE needs one message.
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, Optional

import redis

from app.config import load_symbols
from app.logging_setup import setup_logger
from app.probability_engine import (
    FilterConfig,
    ProbabilityInputs,
    ProbabilityResult,
    amd_score,
    compute_probability,
    confluence_score,
    historical_score,
    intensity_score,
    normalize_side,
    oi_score,
    signed_to_score,
)
from app.trade_journal import pick_bucket


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

IN_STREAM = os.getenv("STREAM_STRIKE_INTEL", "md:strike:intel")
OUT_STREAM = os.getenv("STREAM_PROBABILITY", "md:probability")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_PROBABILITY", "200000"))
LATEST_KEY_PREFIX = os.getenv("PROBABILITY_LATEST_PREFIX", "md:probability:latest:")
RANK_KEY = os.getenv("PROBABILITY_RANK_KEY", "md:probability:rank")

COMPOSITE_PREFIX = os.getenv("COMPOSITE_LATEST_PREFIX", "md:composite:latest:")
GREEKS_PREFIX = os.getenv("GREEKS_PHASE_LATEST_PREFIX", "md:greeks:phase:latest:")
REGIME_LATEST_KEY = os.getenv("REGIME_LATEST_KEY", "md:regime:latest")
JOURNAL_STATS_KEY = os.getenv("JOURNAL_STATS_KEY", "md:journal:stats")

GROUP = os.getenv("PROBABILITY_GROUP", "probability")
CONSUMER = os.getenv("PROBABILITY_CONSUMER", "probability-1")

FILTERS = FilterConfig(
    min_liquidity=float(os.getenv("PROB_MIN_LIQUIDITY", "40")),
    max_spread_pct=float(os.getenv("PROB_MAX_SPREAD_PCT", "3.0")),
    min_historical_samples=int(os.getenv("PROB_MIN_HISTORICAL_SAMPLES", "30")),
    enforce_min_samples=os.getenv("PROB_ENFORCE_MIN_SAMPLES", "0") == "1",
)

# SIE fields forwarded to ICARE unchanged.
FORWARD_FIELDS = (
    "side", "spot", "expiry", "market_phase", "delta_band", "expected_move", "em_direction",
    "em_confidence", "em_conflict", "hold_minutes", "strike", "token", "tradingsymbol",
    "exchange", "strike_score", "confidence", "greeks_score", "execution_quality", "premium",
    "delta", "gamma", "theta_per_day", "vega_per_point", "iv", "greeks_source", "theta_risk_pct",
    "liquidity_score", "liquidity_band", "spread_pct", "lot_size", "projected_premium",
    "projected_premium_gain", "projected_premium_gain_iv_down", "projected_premium_change_adverse",
    "projected_delta", "reasons", "top", "entry_signal", "entry_strength", "entry_price",
    "entry_bar_ts_ms",
)

log = setup_logger("probability")


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


def _vote(value: Optional[str], bull: str, bear: str, side: str) -> Optional[bool]:
    v = (value or "").strip().upper()
    if v == bull:
        return side == "CE"
    if v == bear:
        return side == "PE"
    return None


def confluence_votes(sie: dict, composite: dict, side: str) -> list:
    vol = (sie.get("entry_volume_signal") or "").strip().upper()
    vol_vote = None
    if "BULLISH" in vol:
        vol_vote = side == "CE"
    elif "BEARISH" in vol:
        vol_vote = side == "PE"
    comp = _safe_float(composite.get("score"))
    comp_vote = None if comp is None or comp == 0 else ((comp > 0) == (side == "CE"))
    aligned = sie.get("entry_aligned")
    return [
        _vote(sie.get("entry_htf_bias"), "CALL", "PUT", side),
        _vote(sie.get("entry_st_bias"), "CALL", "PUT", side),
        _vote(sie.get("entry_ema_state"), "BULLISH", "BEARISH", side),
        vol_vote,
        None if aligned in (None, "") else aligned == "1",
        comp_vote,
    ]


def is_sideways(em_direction: str, regime: dict) -> bool:
    return (em_direction or "").strip().upper() == "NEUTRAL" and (
        str(regime.get("regime") or "").strip().upper() == "NEUTRAL"
    )


def build_inputs(
    sie: dict,
    composite: dict,
    phase_doc: dict,
    regime: dict,
    stats: Optional[Dict[str, dict]],
    min_samples: int,
) -> tuple[ProbabilityInputs, str, dict]:
    """Returns (inputs, journal bucket name, bucket summary)."""
    side = normalize_side(sie.get("side"))
    sym = str(sie.get("symbol") or "").upper()
    bucket, summary = pick_bucket(stats, sym, str(sie.get("market_phase") or ""), min_samples)
    surge = str(sie.get("entry_volume_surge") or "").strip().lower() in ("1", "true", "yes")
    inp = ProbabilityInputs(
        symbol=sym,
        confluence=confluence_score(confluence_votes(sie, composite, side)),
        direction=signed_to_score(_safe_float(sie.get("em_direction_score")), side),
        intensity=intensity_score(
            sie.get("entry_volume_signal"), side, surge, _safe_float(sie.get("em_confidence"))
        ),
        amd=amd_score(phase_doc.get("phase")),
        historical=historical_score(summary["win_rate"], summary["samples"], min_samples),
        oi=oi_score(sie.get("entry_oi_positioning"), side),
        liquidity=_safe_float(sie.get("liquidity_score")),
        greeks=_safe_float(sie.get("greeks_score")),
        spread_pct=_safe_float(sie.get("spread_pct")),
        regime="SIDEWAYS" if is_sideways(str(sie.get("em_direction") or ""), regime) else str(regime.get("regime") or ""),
        historical_samples=summary["samples"],
        option_liquidity_band=str(sie.get("liquidity_band") or ""),
    )
    return inp, bucket, summary


def live_sie_fields(
    base: dict, *, htf: dict, st: dict, ema: dict, volume: dict, oi_und: dict, em: dict, liq: dict, bidask: dict,
) -> dict:
    """
    SIE-shaped dict for re-scoring an EXISTING contract now (Module 18 re-entry,
    DECISION.md §5 T9): `base` is the original probability input; the entry_*
    votes, Module 8 fields, liquidity and spread are replaced by live values.
    There is no fresh entry trigger, so `entry_aligned` is unknown (not voted).
    """
    out = dict(base)
    vol_sig = str(volume.get("signal") or "")
    out.update({
        "entry_htf_bias": str(htf.get("bias") or ""),
        "entry_st_bias": str(st.get("bias") or ""),
        "entry_ema_state": str(ema.get("state") or ""),
        "entry_volume_signal": vol_sig,
        "entry_volume_surge": "1" if vol_sig.strip().lower().startswith("strong") else "0",
        "entry_aligned": "",
        "entry_oi_positioning": str(oi_und.get("positioning") or ""),
        "em_direction": str(em.get("direction") or ""),
        "em_direction_score": str(em.get("direction_score") or ""),
        "em_confidence": str(em.get("confidence") or ""),
        "liquidity_score": str(liq.get("liquidity_score") or ""),
        "liquidity_band": str(liq.get("liquidity_band") or ""),
        "spread_pct": str(bidask.get("spread_pct") or ""),
    })
    return out


def build_payload(res: ProbabilityResult, sie: dict, bucket: str, summary: dict, now_ms: int) -> Dict[str, str]:
    d = res.to_dict()
    payload = {
        "ts_ms": str(now_ms),
        "symbol": res.symbol,
        "probability": str(res.probability),
        "raw_probability": str(res.raw_probability),
        "grade": res.grade,
        "decision": res.decision,
        "reject_reasons": json.dumps(res.reject_reasons),
        "flags": json.dumps(res.flags),
        "missing": json.dumps(res.missing),
        "history_bucket": bucket,
        "history_samples": str(summary.get("samples") or 0),
        "history_win_rate": "" if summary.get("win_rate") is None else f"{summary['win_rate']:.4f}",
        "history_avg_win_pct": "" if summary.get("avg_win_pct") is None else f"{summary['avg_win_pct']:.4f}",
        "history_avg_loss_pct": "" if summary.get("avg_loss_pct") is None else f"{summary['avg_loss_pct']:.4f}",
        "probability_json": json.dumps(d, separators=(",", ":")),
    }
    for k in ("confluence", "direction", "intensity", "amd", "historical", "oi", "liquidity", "greeks"):
        payload[f"p_{k}"] = "" if d.get(k) is None else str(d[k])
    for k in FORWARD_FIELDS:
        payload.setdefault(k, str(sie.get(k) or ""))
    return payload


def main():
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_STREAM, GROUP)
    log.info("START reading %s, writing %s (filters=%s symbols=%d)", IN_STREAM, OUT_STREAM, FILTERS, len(symbols))

    while True:
        resp = r.xreadgroup(groupname=GROUP, consumername=CONSUMER, streams={IN_STREAM: ">"}, count=2000, block=2000)
        if not resp:
            continue

        now_ms = int(time.time() * 1000)
        ack_ids = []
        regime = _load_json(r, REGIME_LATEST_KEY)
        stats = _load_json(r, JOURNAL_STATS_KEY)

        for _stream, msgs in resp:
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)
                sym = str(fields.get("symbol") or "").strip().upper()
                log.debug("MSG_IN id=%s symbol=%s", msg_id, sym)
                if not sym or sym not in symbols:
                    continue
                if str(fields.get("status") or "").upper() != "OK":
                    log.debug("SKIP sie_not_ok symbol=%s reason=%s", sym, fields.get("reason"))
                    continue

                tsym = str(fields.get("tradingsymbol") or "")
                inp, bucket, summary = build_inputs(
                    fields,
                    _load_json(r, f"{COMPOSITE_PREFIX}{sym}"),
                    _load_json(r, f"{GREEKS_PREFIX}{tsym}"),
                    regime,
                    stats,
                    FILTERS.min_historical_samples,
                )
                log.debug("LOGIC_IN symbol=%s inputs=%s", sym, inp)
                res = compute_probability(inp, FILTERS)
                payload = build_payload(res, fields, bucket, summary, now_ms)

                log.info(
                    "LOGIC symbol=%s tsym=%s probability=%s grade=%s decision=%s rejects=%s flags=%s missing=%s",
                    sym, tsym, res.probability, res.grade, res.decision, res.reject_reasons, res.flags, res.missing,
                )
                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(f"{LATEST_KEY_PREFIX}{sym}", json.dumps(payload, separators=(",", ":")), ex=3600)
                r.zadd(RANK_KEY, {sym: res.probability})

        if ack_ids:
            r.xack(IN_STREAM, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

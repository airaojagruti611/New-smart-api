"""
run_liquidity_score.py
───────────────────────
Liquidity Score Module — Redis wiring. Maintains per-underlying option
chain state (strike -> OI, for chain ranking + cluster/wall detection,
same pattern as run_oi_analysis.py / run_strike_flow.py), cross-references
cached spread/depth signals from run_bidask_analyzer.py, and OI-change
signals from run_oi_analysis.py.

Emits:
  Stream : md:liquidity:score:signal
  Key    : md:liquidity:score:latest:{TRADINGSYMBOL}

Evaluated on a periodic cycle (chain-ranking + scoring is a synthesis
step over independently-updating cached signals, not a per-tick reaction
— same rationale as run_composite.py / run_strike_flow.py).
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, List, Optional

import redis

from app.config import load_symbols
from app.liquidity_score import (
    GAMMA_JUMP_PCT,
    OI_WALL_MULT,
    PRICE_NEAR_STRIKE_PCT,
    SessionOpenOI,
    compute_liquidity_score,
    degradation_multiplier,
    entry_size,
    oi_cluster_targets,
    scale_out_decision,
)
from app.logging_setup import setup_logger

log = setup_logger("liquidity_score")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")
OPT_STREAM = os.getenv("STREAM_OPT", "md:ticks:opt")

BIDASK_LATEST_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")
OI_LATEST_PREFIX = os.getenv("OI_LATEST_PREFIX", "md:oi:latest:")
ORDERFLOW_LATEST_PREFIX = os.getenv("ORDERFLOW_LATEST_PREFIX", "md:orderflow:latest:")

OUT_STREAM = os.getenv("STREAM_LIQUIDITY_SCORE", "md:liquidity:score:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_LIQUIDITY_SCORE", "50000"))
LATEST_KEY_PREFIX = os.getenv("LIQUIDITY_SCORE_LATEST_PREFIX", "md:liquidity:score:latest:")

GROUP = os.getenv("LIQUIDITY_SCORE_GROUP", "liquidity-score")
CONSUMER = os.getenv("LIQUIDITY_SCORE_CONSUMER", "liquidity-score-1")

EVAL_INTERVAL_SEC = float(os.getenv("LIQUIDITY_SCORE_EVAL_INTERVAL_SEC", "3.0"))
LATEST_TTL_SEC = int(os.getenv("LIQUIDITY_SCORE_LATEST_TTL_SEC", "3600"))
AVG_VOLUME_WINDOW = int(os.getenv("LIQUIDITY_AVG_VOLUME_WINDOW", "20"))


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


def _parse_csv_floats(raw: str) -> List[float]:
    if not raw:
        return []
    out = []
    for part in raw.split(","):
        v = _safe_float(part)
        if v is not None:
            out.append(v)
    return out


class ContractState:
    __slots__ = ("underlying", "cp", "strike", "oi", "cum_vol", "bid_sizes", "ltp")

    def __init__(self):
        self.underlying = ""
        self.cp = ""
        self.strike = 0.0
        self.oi = 0.0
        self.cum_vol = 0.0
        self.bid_sizes: List[float] = []
        self.ltp = 0.0


class AvgVolTracker:
    """Rolling avg of PERIOD (not cumulative) volume per contract."""

    def __init__(self, window: int):
        self.window = window
        self._buf: Dict[str, List[float]] = {}

    def push(self, tsym: str, period_vol: float) -> None:
        buf = self._buf.setdefault(tsym, [])
        buf.append(period_vol)
        if len(buf) > self.window:
            buf.pop(0)

    def avg(self, tsym: str) -> Optional[float]:
        buf = self._buf.get(tsym)
        if not buf:
            return None
        return sum(buf) / len(buf)


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)

    ensure_group(r, EQ_STREAM, GROUP)
    ensure_group(r, OPT_STREAM, GROUP)

    spot_by_sym: Dict[str, float] = {}
    prev_spot_by_sym: Dict[str, float] = {}
    book_by_underlying: Dict[str, Dict[str, ContractState]] = {}
    # Eval-cycle previous cumulative volume (oi_analysis pattern) — NOT
    # updated on every tick, otherwise period_vol at eval is always 0.
    last_eval_cum_vol: Dict[str, float] = {}
    session_oi = SessionOpenOI()
    avg_vol = AvgVolTracker(AVG_VOLUME_WINDOW)

    next_eval = time.time() + EVAL_INTERVAL_SEC

    log.info(
        "START reading %s + %s -> %s + %s{{TRADINGSYMBOL}} (eval_interval=%ss symbols=%d)",
        EQ_STREAM, OPT_STREAM, OUT_STREAM, LATEST_KEY_PREFIX, EVAL_INTERVAL_SEC, len(symbols),
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

                    oi = _safe_float(fields.get("oi")) or 0.0
                    cum_vol = _safe_float(fields.get("vol")) or 0.0
                    strike = _safe_float(fields.get("strike")) or 0.0
                    cp = str(fields.get("cp") or "").strip().upper()
                    ltp = _safe_float(fields.get("ltp")) or 0.0
                    bid_sizes = _parse_csv_floats(fields.get("bid_depth5") or "")

                    book = book_by_underlying.setdefault(und, {})
                    st = book.setdefault(tsym, ContractState())
                    st.underlying, st.cp, st.strike = und, cp, strike
                    st.oi, st.cum_vol, st.bid_sizes, st.ltp = oi, cum_vol, bid_sizes, ltp

                    session_oi.observe(tsym, oi)

                if ack_ids:
                    r.xack(stream, GROUP, *ack_ids)

        now = time.time()
        if now < next_eval:
            continue
        next_eval = now + EVAL_INTERVAL_SEC
        now_ms = int(now * 1000)

        for und, book in book_by_underlying.items():
            spot = spot_by_sym.get(und)
            if not spot or not book:
                continue

            prev_spot = prev_spot_by_sym.get(und)
            prev_spot_by_sym[und] = spot
            underlying_move_pct = (
                abs(spot - prev_spot) / prev_spot * 100.0 if prev_spot else 0.0
            )

            of_doc = _load_json(r, f"{ORDERFLOW_LATEST_PREFIX}{und}") or {}
            net_delta_flattening = str(of_doc.get("bias") or "").upper() == "NEUTRAL"

            # Chain-wide OI ranking, per side (CE/PE separately — ranking
            # calls vs calls, puts vs puts, since that's what's actionable
            # for a directional position).
            ce_rows = sorted(
                ((s.strike, s.oi) for s in book.values() if s.cp == "CE" and s.strike > 0),
                key=lambda x: x[1], reverse=True,
            )
            pe_rows = sorted(
                ((s.strike, s.oi) for s in book.values() if s.cp == "PE" and s.strike > 0),
                key=lambda x: x[1], reverse=True,
            )
            ce_rank_by_strike = {s: i + 1 for i, (s, _oi) in enumerate(ce_rows)}
            pe_rank_by_strike = {s: i + 1 for i, (s, _oi) in enumerate(pe_rows)}

            for tsym, st in book.items():
                if st.strike <= 0 or not st.cp:
                    continue

                bidask_doc = _load_json(r, f"{BIDASK_LATEST_PREFIX}{tsym}") or {}
                spread_ratio = _safe_float(bidask_doc.get("spread_ratio"))
                bidask_depth_score = _safe_float(bidask_doc.get("liquidity_score"))

                oi_doc = _load_json(r, f"{OI_LATEST_PREFIX}{tsym}") or {}
                oi_change_pct = _safe_float(oi_doc.get("oi_change_pct"))

                prev_cv = last_eval_cum_vol.get(tsym)
                period_vol = max(0.0, st.cum_vol - prev_cv) if prev_cv is not None else 0.0
                last_eval_cum_vol[tsym] = st.cum_vol
                avg_vol.push(tsym, period_vol)
                avg_daily_volume = avg_vol.avg(tsym)

                bid_top3 = sum(st.bid_sizes[:3])
                price_moving_toward = (
                    (spot >= st.strike) if st.cp == "CE" else (spot <= st.strike)
                ) if spot else None

                # today_volume = session cumulative vol (Angel `vol` field);
                # period_vol only feeds the rolling "avg daily volume" proxy.
                ent = entry_size(
                    oi=st.oi,
                    avg_daily_volume=avg_daily_volume,
                    today_volume=st.cum_vol,
                    bid_top3_size=bid_top3,
                    spread_ratio=spread_ratio,
                    price_moving_toward_strike=price_moving_toward,
                )

                rank = (ce_rank_by_strike if st.cp == "CE" else pe_rank_by_strike).get(st.strike)
                chain_size = len(ce_rank_by_strike) if st.cp == "CE" else len(pe_rank_by_strike)

                score_result = compute_liquidity_score(
                    vol_oi=ent.vol_oi,
                    spread_ratio=spread_ratio,
                    oi_rank=rank,
                    chain_size=chain_size,
                    current_oi=st.oi,
                    expected_oi=ent.expected_oi,
                    bidask_depth_score=bidask_depth_score,
                    entry=ent,
                )

                # Scale-out check (Module 2) — only meaningful once "in a
                # trade"; caller/downstream decides whether a position is
                # actually open. This module reports the CONDITIONS, same
                # signal-only convention as stock_entry_exit.py.
                same_side_rows = ce_rows if st.cp == "CE" else pe_rows
                above_entry = [
                    (s, o) for (s, o) in same_side_rows
                    if (s > st.strike if st.cp == "CE" else s < st.strike)
                ]
                clusters = oi_cluster_targets(above_entry)
                near_top_oi = any(
                    abs(spot - c.strike) / c.strike * 100.0 <= PRICE_NEAR_STRIKE_PCT for c in clusters
                ) if spot else False

                next_strike_oi = None
                same_side_sorted = sorted(same_side_rows, key=lambda x: x[0])
                strikes_only = [s for s, _ in same_side_sorted]
                if st.strike in strikes_only:
                    idx = strikes_only.index(st.strike)
                    nxt_idx = idx + 1 if st.cp == "CE" else idx - 1
                    if 0 <= nxt_idx < len(same_side_sorted):
                        next_strike_oi = same_side_sorted[nxt_idx][1]
                wall_ahead = (
                    next_strike_oi is not None and st.oi > 0
                    and next_strike_oi > OI_WALL_MULT * st.oi
                )

                vol_oi_dropping = (
                    oi_change_pct is not None and oi_change_pct < 0
                )

                scale_out = scale_out_decision(
                    price_near_top_oi_strike=near_top_oi,
                    spread_ratio_vs_avg=spread_ratio,
                    next_strike_oi_is_wall=wall_ahead,
                    vol_oi_dropping=vol_oi_dropping,
                    net_delta_flattening=net_delta_flattening,
                )

                deg_mult = degradation_multiplier(underlying_move_pct)
                jump_pct = session_oi.jump_pct(tsym, st.oi)
                speed_bump = jump_pct is not None and jump_pct > GAMMA_JUMP_PCT

                payload = {
                    "ts_ms": str(now_ms),
                    "tradingsymbol": tsym,
                    "underlying": und,
                    "cp": st.cp,
                    "strike": str(st.strike),
                    "oi": str(st.oi),
                    "vol_oi": "" if ent.vol_oi is None else str(ent.vol_oi),
                    "vol_oi_class": ent.vol_oi_class,
                    "oi_cap": str(ent.oi_cap),
                    "volume_cap": str(ent.volume_cap),
                    "depth_cap": str(ent.depth_cap),
                    "max_safe_entry": str(ent.max_safe_entry),
                    "confidence_multiplier": str(ent.confidence_multiplier),
                    "final_entry_size": str(ent.final_entry_size),
                    "expected_oi": str(ent.expected_oi),
                    "opening_ratio": str(ent.opening_ratio),
                    "liquidity_score": str(score_result.score),
                    "liquidity_band": score_result.band,
                    "component_vol_oi": str(score_result.components["vol_oi"]),
                    "component_spread": str(score_result.components["spread"]),
                    "component_oi_rank": str(score_result.components["oi_rank"]),
                    "component_oi_expansion": str(score_result.components["oi_expansion"]),
                    "component_depth": str(score_result.components["depth"]),
                    "oi_rank": "" if rank is None else str(rank),
                    "chain_size": str(chain_size),
                    "oi_cluster_targets": json.dumps(
                        [{"strike": c.strike, "oi": c.oi, "rank": c.rank} for c in clusters],
                        separators=(",", ":"),
                    ),
                    "gamma_speed_bump": "1" if speed_bump else "0",
                    "session_oi_jump_pct": "" if jump_pct is None else str(jump_pct),
                    "degradation_multiplier": str(deg_mult),
                    "scale_out_conditions_met": str(scale_out.conditions_met),
                    "scale_out_exit_pct": str(scale_out.exit_pct),
                    "scale_out_conditions": json.dumps(scale_out.conditions, separators=(",", ":")),
                }

                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(
                    f"{LATEST_KEY_PREFIX}{tsym}",
                    json.dumps(payload, separators=(",", ":")),
                    ex=LATEST_TTL_SEC,
                )

                if score_result.band in ("RED",) or scale_out.exit_pct > 0:
                    log.info(
                        "EMIT tsym=%s band=%s score=%.2f scale_out_pct=%s payload=%s",
                        tsym, score_result.band, score_result.score, scale_out.exit_pct, payload,
                    )
                else:
                    log.debug("EMIT tsym=%s band=%s score=%.2f", tsym, score_result.band, score_result.score)


if __name__ == "__main__":
    main()

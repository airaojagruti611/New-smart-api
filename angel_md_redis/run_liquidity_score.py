"""
run_liquidity_score.py
───────────────────────
Liquidity Score Module — Redis wiring. Maintains per-underlying option
chain state (strike -> OI, for chain ranking + cluster/wall detection,
same pattern as run_oi_analysis.py / run_strike_flow.py), cross-references
cached spread signals from run_bidask_analyzer.py.

Emits:
  Stream : md:liquidity:score:signal
  Key    : md:liquidity:score:latest:{TRADINGSYMBOL}

Persists (survives restarts):
  md:liquidity:hist:{TRADINGSYMBOL}   daily OI/volume for overnight gamma + ADV
  md:liquidity:session_spot:{SYMBOL}  first underlying print of the session

Evaluated on a periodic cycle (chain-ranking + scoring is a synthesis
step over independently-updating cached signals, not a per-tick reaction
— same rationale as run_composite.py / run_strike_flow.py).
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import redis

from app.config import BASE_DIR, load_symbols
from app.liquidity_score import (
    GAMMA_JUMP_PCT,
    OI_WALL_MULT,
    ContractDayHist,
    approaching_strike,
    cluster_top_n_for_band,
    compute_liquidity_score,
    degradation_multiplier,
    entry_size,
    gamma_speed_bump_strikes,
    net_delta_is_flattening,
    oi_cluster_targets,
    qty_to_lots,
    scale_out_decision,
    vol_oi_is_dropping,
)
from app.freshness import env_ms
from app.logging_setup import setup_logger
from app.order_flow import prune_stale_book, tick_ts_ms

log = setup_logger("liquidity_score")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")
OPT_STREAM = os.getenv("STREAM_OPT", "md:ticks:opt")

BIDASK_LATEST_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")
ORDERFLOW_LATEST_PREFIX = os.getenv("ORDERFLOW_LATEST_PREFIX", "md:orderflow:latest:")
POSITION_OPEN_PREFIX = os.getenv("POSITION_OPEN_PREFIX", "md:position:open:")
HIST_KEY_PREFIX = os.getenv("LIQUIDITY_HIST_PREFIX", "md:liquidity:hist:")
SESSION_SPOT_PREFIX = os.getenv("LIQUIDITY_SESSION_SPOT_PREFIX", "md:liquidity:session_spot:")

OUT_STREAM = os.getenv("STREAM_LIQUIDITY_SCORE", "md:liquidity:score:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_LIQUIDITY_SCORE", "50000"))
LATEST_KEY_PREFIX = os.getenv("LIQUIDITY_SCORE_LATEST_PREFIX", "md:liquidity:score:latest:")

GROUP = os.getenv("LIQUIDITY_SCORE_GROUP", "liquidity-score")
CONSUMER = os.getenv("LIQUIDITY_SCORE_CONSUMER", "liquidity-score-1")

EVAL_INTERVAL_SEC = float(os.getenv("LIQUIDITY_SCORE_EVAL_INTERVAL_SEC", "3.0"))
LATEST_TTL_SEC = int(os.getenv("LIQUIDITY_SCORE_LATEST_TTL_SEC", "3600"))
HIST_TTL_SEC = int(os.getenv("LIQUIDITY_HIST_TTL_SEC", str(14 * 24 * 3600)))
HIST_FLUSH_EVERY_EVALS = int(os.getenv("LIQUIDITY_HIST_FLUSH_EVERY", "10"))
# Contracts / spot not updated within this window are pruned from the
# in-memory chain instead of being republished with a fresh ts_ms.
BOOK_MAX_AGE_MS = env_ms("LIQUIDITY_SCORE_BOOK_MAX_AGE_SEC", 120)

SCRIPMASTER_PATH = Path(os.getenv("SCRIPMASTER_PATH") or (BASE_DIR / "OpenAPIScripMaster.json"))


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


def _open_position(r: redis.Redis, tsym: str) -> Optional[dict]:
    """
    Live fill, if the execution layer writes one.
    Expected JSON: {qty|quantity|lots > 0, optional entry_spot|spot}.
    """
    doc = _load_json(r, f"{POSITION_OPEN_PREFIX}{tsym}")
    if not doc:
        return None
    qty = _safe_float(doc.get("qty") or doc.get("quantity") or doc.get("lots"))
    if qty is None or qty <= 0:
        return None
    return doc


def load_option_lot_sizes(path: Path) -> Dict[str, float]:
    """symbol -> lot size from Angel scrip master (NFO options)."""
    if not path.exists():
        log.warning("scripmaster missing at %s — entry size stays in shares", path)
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("scripmaster unreadable %s: %s", path, e)
        return {}
    out: Dict[str, float] = {}
    if not isinstance(data, list):
        return out
    for row in data:
        if not isinstance(row, dict):
            continue
        if str(row.get("exch_seg") or "").upper() != "NFO":
            continue
        inst = str(row.get("instrumenttype") or "").upper()
        if "OPT" not in inst:
            continue
        sym = str(row.get("symbol") or "").strip().upper()
        lot = _safe_float(row.get("lotsize"))
        if sym and lot and lot > 0:
            out[sym] = lot
    log.info("loaded %d option lot sizes from %s", len(out), path)
    return out


def load_hist_from_redis(r: redis.Redis, hist: ContractDayHist) -> int:
    n = 0
    for key in r.scan_iter(match=f"{HIST_KEY_PREFIX}*", count=500):
        tsym = str(key)[len(HIST_KEY_PREFIX):].strip().upper()
        if not tsym:
            continue
        raw = r.get(key)
        if not raw:
            continue
        try:
            rows = json.loads(raw)
        except Exception:
            continue
        if isinstance(rows, list):
            hist.hydrate(tsym, rows)
            n += 1
    return n


def persist_hist(r: redis.Redis, hist: ContractDayHist, tsym: str) -> None:
    rows = hist.dump(tsym)
    if not rows:
        return
    r.set(
        f"{HIST_KEY_PREFIX}{tsym}",
        json.dumps(rows, separators=(",", ":")),
        ex=HIST_TTL_SEC,
    )


class ContractState:
    __slots__ = (
        "underlying", "cp", "strike", "oi_shares", "cum_vol_shares",
        "bid_sizes_shares", "ltp", "lot_size", "data_ts_ms",
    )

    def __init__(self):
        self.underlying = ""
        self.cp = ""
        self.strike = 0.0
        self.oi_shares = 0.0
        self.cum_vol_shares = 0.0
        self.bid_sizes_shares: List[float] = []
        self.ltp = 0.0
        self.lot_size = 0.0
        self.data_ts_ms = 0  # source time (ts_exch -> ts_recv) of the last tick

    @property
    def oi(self) -> float:
        return qty_to_lots(self.oi_shares, self.lot_size)

    @property
    def cum_vol(self) -> float:
        return qty_to_lots(self.cum_vol_shares, self.lot_size)

    @property
    def bid_top3(self) -> float:
        return qty_to_lots(sum(self.bid_sizes_shares[:3]), self.lot_size)

    @property
    def size_unit(self) -> str:
        return "lots" if self.lot_size and self.lot_size > 0 else "shares"


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)

    ensure_group(r, EQ_STREAM, GROUP)
    ensure_group(r, OPT_STREAM, GROUP)

    lot_by_tsym = load_option_lot_sizes(SCRIPMASTER_PATH)
    hist = ContractDayHist()
    loaded = load_hist_from_redis(r, hist)
    log.info("hydrated daily OI/volume hist for %d contracts", loaded)

    spot_by_sym: Dict[str, float] = {}
    spot_ts_by_sym: Dict[str, int] = {}
    prev_spot_by_sym: Dict[str, float] = {}
    session_spot_by_sym: Dict[str, Tuple[str, float]] = {}
    last_of_bias: Dict[str, str] = {}
    book_by_underlying: Dict[str, Dict[str, ContractState]] = {}
    last_vol_oi: Dict[str, float] = {}
    # First time we observed an open position for this tsym this session:
    # (entry_spot, qty). Used for Method C when the position doc has no spot.
    position_entry_spot: Dict[str, Tuple[float, float]] = {}
    evals_since_flush = 0

    next_eval = time.time() + EVAL_INTERVAL_SEC

    log.info(
        "START reading %s + %s -> %s + %s{{TRADINGSYMBOL}} (eval_interval=%ss symbols=%d lots=%d)",
        EQ_STREAM, OPT_STREAM, OUT_STREAM, LATEST_KEY_PREFIX, EVAL_INTERVAL_SEC,
        len(symbols), len(lot_by_tsym),
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
                            spot_ts_by_sym[sym] = tick_ts_ms(fields, int(time.time() * 1000)) or 0
                            today = dt.date.today().isoformat()
                            held = session_spot_by_sym.get(sym)
                            if not held or held[0] != today:
                                cached = _load_json(r, f"{SESSION_SPOT_PREFIX}{sym}") or {}
                                if cached.get("date") == today and _safe_float(cached.get("spot")):
                                    session_spot_by_sym[sym] = (today, float(cached["spot"]))
                                else:
                                    session_spot_by_sym[sym] = (today, ltp)
                                    r.set(
                                        f"{SESSION_SPOT_PREFIX}{sym}",
                                        json.dumps({"date": today, "spot": ltp}, separators=(",", ":")),
                                        ex=HIST_TTL_SEC,
                                    )
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
                    st.oi_shares, st.cum_vol_shares = oi, cum_vol
                    st.bid_sizes_shares, st.ltp = bid_sizes, ltp
                    st.lot_size = lot_by_tsym.get(tsym, 0.0)
                    st.data_ts_ms = tick_ts_ms(fields, int(time.time() * 1000)) or 0

                    hist.observe(tsym, oi, cum_vol)

                if ack_ids:
                    r.xack(stream, GROUP, *ack_ids)

        now = time.time()
        if now < next_eval:
            continue
        next_eval = now + EVAL_INTERVAL_SEC
        now_ms = int(now * 1000)
        evals_since_flush += 1
        flush_hist = evals_since_flush >= HIST_FLUSH_EVERY_EVALS
        if flush_hist:
            evals_since_flush = 0

        for und, book in book_by_underlying.items():
            dropped = prune_stale_book(book, now_ms, BOOK_MAX_AGE_MS)
            if dropped:
                log.debug("PRUNE underlying=%s stale_contracts=%s", und, dropped)
            spot = spot_by_sym.get(und)
            spot_ts = spot_ts_by_sym.get(und, 0)
            if BOOK_MAX_AGE_MS > 0 and (not spot_ts or now_ms - spot_ts > BOOK_MAX_AGE_MS):
                spot = None
            if not spot or not book:
                continue

            prev_spot = prev_spot_by_sym.get(und)
            prev_spot_by_sym[und] = spot

            of_doc = _load_json(r, f"{ORDERFLOW_LATEST_PREFIX}{und}") or {}
            of_bias = str(of_doc.get("bias") or "").upper()
            net_delta_flattening = net_delta_is_flattening(of_bias, last_of_bias.get(und))
            if of_bias in ("UP", "DOWN"):
                last_of_bias[und] = of_bias

            # Rank only strikes with live OI — zero-OI contracts must not dilute the chain.
            ce_rows = sorted(
                ((s.strike, s.oi) for s in book.values() if s.cp == "CE" and s.strike > 0 and s.oi > 0),
                key=lambda x: x[1], reverse=True,
            )
            pe_rows = sorted(
                ((s.strike, s.oi) for s in book.values() if s.cp == "PE" and s.strike > 0 and s.oi > 0),
                key=lambda x: x[1], reverse=True,
            )
            ce_rank_by_strike = {s: i + 1 for i, (s, _oi) in enumerate(ce_rows)}
            pe_rank_by_strike = {s: i + 1 for i, (s, _oi) in enumerate(pe_rows)}

            ce_max_depth = max((s.bid_top3 for s in book.values() if s.cp == "CE"), default=0.0)
            pe_max_depth = max((s.bid_top3 for s in book.values() if s.cp == "PE"), default=0.0)

            ce_jumps = {
                s.strike: hist.overnight_jump_pct(tsym, s.oi_shares)[0]
                for tsym, s in book.items() if s.cp == "CE" and s.strike > 0
            }
            pe_jumps = {
                s.strike: hist.overnight_jump_pct(tsym, s.oi_shares)[0]
                for tsym, s in book.items() if s.cp == "PE" and s.strike > 0
            }
            ce_bumps = gamma_speed_bump_strikes(ce_jumps)
            pe_bumps = gamma_speed_bump_strikes(pe_jumps)

            for tsym, st in book.items():
                if st.strike <= 0 or not st.cp:
                    continue

                bidask_doc = _load_json(r, f"{BIDASK_LATEST_PREFIX}{tsym}") or {}
                spread_ratio = _safe_float(bidask_doc.get("spread_ratio"))

                pos_doc = _open_position(r, tsym)
                in_position = pos_doc is not None
                if in_position:
                    entry_spot = _safe_float(
                        pos_doc.get("entry_spot") or pos_doc.get("spot") or pos_doc.get("entry_price")
                    )
                    qty = float(
                        _safe_float(pos_doc.get("qty") or pos_doc.get("quantity") or pos_doc.get("lots")) or 0
                    )
                    if tsym not in position_entry_spot:
                        position_entry_spot[tsym] = (entry_spot or spot, qty)
                    elif entry_spot:
                        position_entry_spot[tsym] = (entry_spot, position_entry_spot[tsym][1])
                else:
                    position_entry_spot.pop(tsym, None)

                if in_position and tsym in position_entry_spot and position_entry_spot[tsym][0] > 0:
                    base_spot = position_entry_spot[tsym][0]
                else:
                    sess = session_spot_by_sym.get(und)
                    base_spot = sess[1] if sess and sess[1] > 0 else spot
                if base_spot and base_spot > 0:
                    underlying_move_pct = abs(spot - base_spot) / base_spot * 100.0
                else:
                    underlying_move_pct = 0.0

                # Spec: "price moving toward/away from strike" = distance shrinking,
                # not ITM/OTM level.
                if prev_spot and st.strike > 0:
                    price_moving_toward = abs(spot - st.strike) < abs(prev_spot - st.strike)
                else:
                    price_moving_toward = None

                adv_shares = hist.adv(tsym)
                adv_lots = qty_to_lots(adv_shares, st.lot_size) if adv_shares else None
                bid_top3 = st.bid_top3

                ent = entry_size(
                    oi=st.oi,
                    avg_daily_volume=adv_lots,
                    today_volume=st.cum_vol,
                    bid_top3_size=bid_top3,
                    spread_ratio=spread_ratio,
                    price_moving_toward_strike=price_moving_toward,
                )

                rank = (ce_rank_by_strike if st.cp == "CE" else pe_rank_by_strike).get(st.strike)
                chain_size = len(ce_rank_by_strike) if st.cp == "CE" else len(pe_rank_by_strike)
                chain_max_depth = ce_max_depth if st.cp == "CE" else pe_max_depth

                score_result = compute_liquidity_score(
                    vol_oi=ent.vol_oi,
                    spread_ratio=spread_ratio,
                    oi_rank=rank,
                    chain_size=chain_size,
                    current_oi=st.oi,
                    expected_oi=ent.expected_oi,
                    bid_top3=bid_top3,
                    chain_max_bid_top3=chain_max_depth,
                    entry=ent,
                )
                ent = score_result.entry_size

                same_side_rows = ce_rows if st.cp == "CE" else pe_rows
                above_entry = [
                    (s, o) for (s, o) in same_side_rows
                    if (s > st.strike if st.cp == "CE" else s < st.strike)
                ]
                clusters = oi_cluster_targets(
                    above_entry, top_n=cluster_top_n_for_band(score_result.band),
                )
                near_top_oi = any(approaching_strike(spot, c.strike) for c in clusters)

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

                prev_ratio = last_vol_oi.get(tsym)
                vol_oi_dropping = vol_oi_is_dropping(ent.vol_oi, prev_ratio)
                if ent.vol_oi is not None:
                    last_vol_oi[tsym] = ent.vol_oi

                bumps = ce_bumps if st.cp == "CE" else pe_bumps
                jump_pct, jump_src = hist.overnight_jump_pct(tsym, st.oi_shares)
                speed_bump = jump_pct is not None and jump_pct > GAMMA_JUMP_PCT
                profitable_bumps = [
                    b for b in bumps
                    if (b > st.strike if st.cp == "CE" else b < st.strike)
                ]
                approaching_gamma = any(
                    approaching_strike(spot, b) for b in profitable_bumps
                )

                deg_mult = degradation_multiplier(underlying_move_pct)
                scale_out = scale_out_decision(
                    price_near_top_oi_strike=near_top_oi,
                    spread_ratio_vs_avg=spread_ratio,
                    next_strike_oi_is_wall=wall_ahead,
                    vol_oi_dropping=vol_oi_dropping,
                    net_delta_flattening=net_delta_flattening,
                    in_position=in_position,
                    approaching_gamma_bump=approaching_gamma,
                    band=score_result.band,
                    degradation_mult=deg_mult,
                )

                payload = {
                    # Source time of the data used (older of this contract's
                    # last tick and the spot tick) — never processing time.
                    "ts_ms": str(min(st.data_ts_ms, spot_ts)),
                    "eval_ts_ms": str(now_ms),
                    "tradingsymbol": tsym,
                    "underlying": und,
                    "cp": st.cp,
                    "strike": str(st.strike),
                    "oi": str(st.oi),
                    "oi_shares": str(st.oi_shares),
                    "lot_size": str(st.lot_size or ""),
                    "size_unit": st.size_unit,
                    "vol_oi": "" if ent.vol_oi is None else str(ent.vol_oi),
                    "vol_oi_class": ent.vol_oi_class,
                    "oi_cap": str(ent.oi_cap),
                    "volume_cap": str(ent.volume_cap),
                    "depth_cap": str(ent.depth_cap),
                    "max_safe_entry": str(ent.max_safe_entry),
                    "confidence_multiplier": str(ent.confidence_multiplier),
                    "band_size_mult": str(ent.band_size_mult),
                    "final_entry_size": str(ent.final_entry_size),
                    "entry_reason": ent.reason,
                    "expected_oi": str(ent.expected_oi),
                    "opening_ratio": str(ent.opening_ratio),
                    "adv": "" if adv_lots is None else str(adv_lots),
                    "adv_source": "prior_days" if adv_lots else "today_volume",
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
                    "approaching_gamma_bump": "1" if approaching_gamma else "0",
                    "in_position": "1" if in_position else "0",
                    "session_oi_jump_pct": "" if jump_pct is None else str(jump_pct),
                    "oi_jump_source": jump_src,
                    "degradation_multiplier": str(deg_mult),
                    "underlying_move_pct": str(round(underlying_move_pct, 4)),
                    "scale_out_conditions_met": str(scale_out.conditions_met),
                    "scale_out_exit_pct": str(scale_out.exit_pct),
                    "scale_out_reason": scale_out.reason,
                    "scale_out_advisory": "1" if scale_out.advisory else "0",
                    "scale_out_conditions": json.dumps(scale_out.conditions, separators=(",", ":")),
                }

                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(
                    f"{LATEST_KEY_PREFIX}{tsym}",
                    json.dumps(payload, separators=(",", ":")),
                    ex=LATEST_TTL_SEC,
                )
                if flush_hist:
                    persist_hist(r, hist, tsym)

                if scale_out.exit_pct > 0 and not (
                    scale_out.advisory and score_result.band == "RED"
                ):
                    log.info(
                        "SCALE_OUT tsym=%s band=%s score=%.2f exit_pct=%s advisory=%s reason=%s",
                        tsym, score_result.band, score_result.score,
                        scale_out.exit_pct, scale_out.advisory, scale_out.reason,
                    )
                elif score_result.band == "RED":
                    log.debug("RED tsym=%s score=%.2f exit_pct=%s", tsym, score_result.score, scale_out.exit_pct)
                else:
                    log.debug(
                        "EMIT tsym=%s band=%s score=%.2f size=%s %s",
                        tsym, score_result.band, score_result.score,
                        ent.final_entry_size, st.size_unit,
                    )


if __name__ == "__main__":
    main()

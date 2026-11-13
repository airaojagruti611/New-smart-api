"""
Paper-trade outcome schema + MFE/MAE tracker (pure, no I/O). See DECISION.MD C.

Premium units are rupees per unit; MFE/MAE are in premium points
(MFE = best excursion above entry, MAE = worst excursion below entry, >= 0).
Missing data => no invented prices: a trade with no premium waits for its
first tick; one that never gets a tick closes as exit_reason=no_ticks and is
excluded from stats.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from .learning_keys import bucket_key, delta_band, em_bucket, strike_bucket
from .logging_setup import setup_logger

log = setup_logger("trade_outcome")

IST = timezone(timedelta(hours=5, minutes=30))


def _f(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        x = float(v)
        return x if x == x else None
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class PaperConfig:
    target_pct: float = float(os.getenv("PAPER_TARGET_PCT", "30"))  # fallback target
    sl_pct: float = float(os.getenv("PAPER_SL_PCT", "20"))
    max_hold_min: float = float(os.getenv("PAPER_MAX_HOLD_MIN", "60"))
    no_tick_timeout_sec: float = float(os.getenv("PAPER_NO_TICK_TIMEOUT_SEC", "120"))
    session_end_hhmm: int = int(os.getenv("PAPER_SESSION_END_HHMM", "1520"))  # IST


@dataclass(frozen=True)
class PaperTrade:
    symbol: str
    tradingsymbol: str
    side: str
    open_ts_ms: int
    entry_premium: Optional[float]  # None until first tick (pending)
    entry_ts_ms: Optional[int]
    lot_size: int
    hold_min: float
    delta: Optional[float]
    spot: Optional[float]
    em_pct: Optional[float]
    target_px: Optional[float] = None
    sl_px: Optional[float] = None
    max_px: Optional[float] = None
    min_px: Optional[float] = None
    last_px: Optional[float] = None
    last_tick_ms: Optional[int] = None
    entry_spread: Optional[float] = None
    ctx: Optional[Dict[str, str]] = None  # context strings carried to the outcome row
    spot_last: Optional[float] = None     # underlying price path (md:ticks:eq) for actual-vs-expected move
    spot_max: Optional[float] = None
    spot_min: Optional[float] = None


def target_price(entry: float, delta: Optional[float], em_pct: Optional[float],
                 spot: Optional[float], target_pct: float) -> float:
    """entry + |delta| * EM points; fallback entry*(1+target_pct/100)."""
    if delta is not None and em_pct and spot and em_pct > 0 and spot > 0:
        gain = abs(delta) * spot * em_pct / 100.0
        if gain > 0:
            return entry + gain
    return entry * (1.0 + target_pct / 100.0)


def _arm(t: PaperTrade, px: float, ts_ms: int, cfg: PaperConfig, spread: Optional[float]) -> PaperTrade:
    return replace(
        t, entry_premium=px, entry_ts_ms=ts_ms, max_px=px, min_px=px, last_px=px,
        last_tick_ms=ts_ms, entry_spread=spread,
        target_px=target_price(px, t.delta, t.em_pct, t.spot, cfg.target_pct),
        sl_px=px * (1.0 - cfg.sl_pct / 100.0),
    )


def _top_field(sel: Dict[str, str], key: str) -> str:
    try:
        top = json.loads(sel.get("top3") or "[]")
        if top and isinstance(top[0], dict):
            v = top[0].get(key)
            return "" if v is None else str(v)
    except Exception:
        pass
    return ""


def open_trade(sel: Dict[str, str], now_ms: int, cfg: PaperConfig = PaperConfig()) -> Optional[PaperTrade]:
    """Build a trade from a md:strike:select OK row. None if no tradingsymbol.
    Entry = `premium` field if present, else the first option tick."""
    ts = (sel.get("tradingsymbol") or "").strip()
    if not ts:
        return None
    premium = _f(sel.get("premium"))
    hold = _f(sel.get("hold_min"))
    side = (sel.get("side") or "").upper()
    strike, atm, step = _f(sel.get("strike")), _f(sel.get("atm")), _f(sel.get("step"))
    delta = _f(sel.get("delta"))
    em = _f(sel.get("em_pct"))
    sb = sel.get("strike_bucket") or (
        strike_bucket(strike, atm, step, side) if None not in (strike, atm, step) else "NA")
    ctx = {
        "regime": sel.get("regime") or "NO_DATA", "strike": sel.get("strike") or "",
        "strike_bucket": sb,
        "em_bucket": sel.get("em_bucket") or em_bucket(em),
        "delta_band": sel.get("delta_band") or delta_band(delta),
        "theta": sel.get("theta") or "", "gamma": sel.get("gamma") or "",
        "vega": sel.get("vega") or "", "iv": sel.get("iv") or "",
        "strike_score": sel.get("strike_score") or "", "confidence": sel.get("confidence") or "",
        "liquidity_score": _top_field(sel, "liquidity_score"),
        "expiry": sel.get("expiry") or "", "token": sel.get("token") or "",
    }
    t = PaperTrade(
        symbol=sel.get("symbol") or "", tradingsymbol=ts, side=side, open_ts_ms=now_ms,
        entry_premium=None, entry_ts_ms=None,
        lot_size=int(_f(sel.get("lot_size")) or 0),
        hold_min=hold if hold and hold > 0 else cfg.max_hold_min,
        delta=delta, spot=_f(sel.get("spot")), em_pct=em, ctx=ctx,
    )
    if premium and premium > 0:
        t = _arm(t, premium, now_ms, cfg, None)
    return t


def on_tick(t: PaperTrade, ltp: Optional[float], ts_ms: int, cfg: PaperConfig = PaperConfig(),
            bid: Optional[float] = None, ask: Optional[float] = None) -> PaperTrade:
    """Fold one tick into the trade (arms pending trades, updates MFE/MAE reference)."""
    if ltp is None or ltp <= 0:
        return t
    if t.entry_premium is None:
        spread = (ask - bid) if (bid and ask and ask >= bid) else None
        return _arm(t, ltp, ts_ms, cfg, spread)
    return replace(
        t, last_px=ltp, last_tick_ms=ts_ms,
        max_px=max(t.max_px if t.max_px is not None else ltp, ltp),
        min_px=min(t.min_px if t.min_px is not None else ltp, ltp),
    )


def on_spot(t: PaperTrade, px: Optional[float]) -> PaperTrade:
    """Fold one underlying (spot) tick into the trade. Ignored until the trade has an entry."""
    if px is None or px <= 0 or t.entry_premium is None:
        return t
    return replace(
        t, spot_last=px,
        spot_max=max(t.spot_max if t.spot_max is not None else px, px),
        spot_min=min(t.spot_min if t.spot_min is not None else px, px),
    )


def move_fields(t: PaperTrade) -> Dict[str, Optional[float]]:
    """Actual underlying move vs the expected move (Expected Move step 6).

    actual_move_pct  signed % change of spot from entry to exit
    dir_move_pct     same, signed in the trade's favour (CE: up +, PE: down +)
    move_vs_em       |actual| / expected move %   (1.0 = moved exactly one EM)
    fav_move_pct     best favourable spot excursion %, adv_move_pct worst adverse (>= 0)
    """
    out: Dict[str, Optional[float]] = {"spot_exit": t.spot_last, "actual_move_pct": None,
                                       "dir_move_pct": None, "move_vs_em": None,
                                       "fav_move_pct": None, "adv_move_pct": None}
    s0 = t.spot
    if not s0 or s0 <= 0 or t.spot_last is None:
        return out
    sign = -1.0 if t.side == "PE" else 1.0
    act = (t.spot_last - s0) / s0 * 100.0
    out["actual_move_pct"] = act
    out["dir_move_pct"] = act * sign
    if t.em_pct and t.em_pct > 0:
        out["move_vs_em"] = abs(act) / t.em_pct
    hi = (t.spot_max - s0) / s0 * 100.0 if t.spot_max is not None else act
    lo = (t.spot_min - s0) / s0 * 100.0 if t.spot_min is not None else act
    fav, adv = (hi, lo) if sign > 0 else (-lo, -hi)
    out["fav_move_pct"] = max(fav, 0.0)
    out["adv_move_pct"] = max(-adv, 0.0)
    return out


def is_session_end(ts_ms: int, cfg: PaperConfig = PaperConfig()) -> bool:
    d = datetime.fromtimestamp(ts_ms / 1000.0, IST)
    return d.hour * 100 + d.minute >= cfg.session_end_hhmm


def exit_reason(t: PaperTrade, now_ms: int, cfg: PaperConfig = PaperConfig()) -> Optional[str]:
    """Exit reason or None. Stop is checked before target (conservative)."""
    if t.entry_premium is None:
        if (now_ms - t.open_ts_ms) >= cfg.no_tick_timeout_sec * 1000:
            return "no_ticks"
        return None
    px = t.last_px
    if px is not None and t.sl_px is not None and px <= t.sl_px:
        return "stop"
    if px is not None and t.target_px is not None and px >= t.target_px:
        return "target"
    if is_session_end(now_ms, cfg):
        return "session_end"
    if t.entry_ts_ms is not None and (now_ms - t.entry_ts_ms) >= t.hold_min * 60_000:
        return "timeout"
    return None


def _fmt(v: Optional[float], nd: int = 4) -> str:
    return "" if v is None else f"{v:.{nd}f}"


def close_trade(t: PaperTrade, reason: str, now_ms: int) -> Dict[str, str]:
    """Outcome row (all strings) for md:journal:outcome."""
    ctx = t.ctx or {}
    entry, exit_px = t.entry_premium, t.last_px
    excluded = reason == "no_ticks" or entry is None or exit_px is None
    pnl_pts = pnl_pct = pnl_lot = mfe = mae = None
    if not excluded:
        pnl_pts = exit_px - entry
        pnl_pct = pnl_pts / entry * 100.0
        pnl_lot = pnl_pts * t.lot_size if t.lot_size > 0 else None
        mfe = (t.max_px - entry) if t.max_px is not None else None
        mae = (entry - t.min_px) if t.min_px is not None else None
    hold_s = ((now_ms - t.entry_ts_ms) / 1000.0) if t.entry_ts_ms else None
    row = {
        "mode": "paper", "symbol": t.symbol, "tradingsymbol": t.tradingsymbol, "side": t.side,
        "open_ts_ms": str(t.open_ts_ms),
        "entry_ts_ms": "" if t.entry_ts_ms is None else str(t.entry_ts_ms),
        "exit_ts_ms": str(now_ms), "exit_reason": reason,
        "excluded_from_stats": "true" if excluded else "false",
        "entry_premium": _fmt(entry), "exit_premium": _fmt(exit_px),
        "target_px": _fmt(t.target_px), "sl_px": _fmt(t.sl_px),
        "lot_size": str(t.lot_size), "hold_sec": _fmt(hold_s, 1),
        "planned_hold_min": _fmt(t.hold_min, 1),
        "pnl_pts": _fmt(pnl_pts), "pnl_pct": _fmt(pnl_pct, 3), "pnl_per_lot": _fmt(pnl_lot, 2),
        "mfe": _fmt(mfe), "mae": _fmt(mae),
        "mfe_pct": _fmt(mfe / entry * 100.0 if mfe is not None else None, 3),
        "mae_pct": _fmt(mae / entry * 100.0 if mae is not None else None, 3),
        "delta": _fmt(t.delta), "em_pct": _fmt(t.em_pct), "spot_entry": _fmt(t.spot),
        "spread_entry": _fmt(t.entry_spread),
        **{k: _fmt(v, 2 if k == "spot_exit" else 4) for k, v in move_fields(t).items()},
        **ctx,
    }
    row["bucket_key"] = bucket_key(ctx.get("regime", ""), ctx.get("em_bucket", "EM_NA"),
                                   ctx.get("strike_bucket", "NA"), ctx.get("delta_band", "D_NA"))
    return row

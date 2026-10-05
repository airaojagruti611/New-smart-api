"""
app/trade_journal.py
───────────────────────
Module 22 — Trade Journal + adaptive-learning statistics (DECISION.md D11).

ICARE-APPROVED trades are PAPER-traded:
  entry  = ask at approval (a buyer pays the offer); with the Order Executor in
           EXEC_MODE=paper|live, the ACTUAL average fill and filled lots from
           md:exec:fill instead (DECISION.md §7 E16). ICARE's SL is kept as a PRICE
           (never re-anchored wider); an entry at/below it is refused (PRICE_BELOW_STOP)
           and lots are capped so (entry - SL) x qty <= max_risk_allowed
  marks  = bid (what the position could be sold for)
  exit   = first of SL / TARGET / TIME (hold window) / EOD, filled at bid;
           with Module 18 in TSL_MODE=active also TRAILING_STOP (validated by the
           engine), and TARGET no longer closes while the engine is live (it only
           tightens the trail). In shadow mode the engine's would-be exit is
           stored on the record as tsl_* fields (DECISION.md §5 T12).

Each closed trade becomes a journal record holding every field both
"Adaptive Learning" sections of the design list (phase, EM, strike,
delta/theta at entry, liquidity, spread, hold, PnL, MFE, MAE, quality,
lots, margin, risk, drawdown).

Statistics are kept per bucket so the engines can read history without
scanning the journal:
  ALL                     every trade
  SYM:{symbol}            per underlying
  SYM_PHASE:{sym}|{phase} per underlying x SIE market phase
pick_bucket() returns the most specific bucket with >= min_samples.

Learning is OFFLINE only (learn_weights.py) — nothing here changes weights.

No I/O here — Redis wiring lives in run_trade_journal.py.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from typing import Dict, Optional, Tuple

ENTRY_CONTEXT_FIELDS = (
    "market_phase", "expected_move", "em_direction", "delta", "theta_per_day", "gamma",
    "iv", "liquidity_score", "spread_pct", "strike_score", "probability", "grade",
    "trade_quality", "risk_class", "expected_value", "ev_source", "recommended_lots",
    "margin_used", "risk_taken", "reward_risk", "hold_minutes",
    "reentry", "chain_id", "parent_trade_id", "reentry_no",
    "trade_score", "rank", "rank_decision", "rank_confidence", "rank_mode", "rank_components",
    # Module 14 Order Executor (DECISION.md §7 E15/E16)
    "entry_charges", "icare_recommended_lots",
    "exec_trade_id", "exec_execution_status", "exec_requested_lots", "exec_filled_lots",
    "exec_average_fill_price", "exec_reference_price", "exec_arrival_mid", "exec_slippage", "exec_slippage_pct",
    "exec_slippage_vs_mid", "exec_signal_decay", "exec_implementation_shortfall", "exec_total_execution_cost",
    "exec_orders_used", "exec_execution_duration_ms", "exec_cancel_reason", "exec_mode", "exec_charges",
)


def _f(v, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


@dataclass(frozen=True)
class PaperPosition:
    trade_id: str
    symbol: str
    tradingsymbol: str
    side: str
    strike: float
    lots: int
    lot_size: float
    qty: float
    entry_premium: float
    entry_ts_ms: int
    sl_premium: float
    target_premium: float
    time_stop_ms: int
    entry_spot: Optional[float]
    context: Dict[str, str] = field(default_factory=dict)
    last_premium: Optional[float] = None
    max_premium: Optional[float] = None
    min_premium: Optional[float] = None
    # live exits (E17): the position closes only on the broker-confirmed SELL fill
    orig_qty: float = 0.0
    exec_mode: str = ""
    exit_pending: str = ""          # exit reason while the SELL is working
    exit_requested_ms: int = 0      # last md:exec:exit_request (0 = request again now)
    opened_forced: str = ""         # a real fill opened despite a rule ("OPENED_FORCED:<reasons>")

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "PaperPosition":
        return PaperPosition(**{k: d[k] for k in PaperPosition.__dataclass_fields__ if k in d})


def open_position(icare: dict, entry_premium: float, now_ms: int, exec_mode: str = "",
                  force: bool = False) -> Optional[PaperPosition]:
    """
    Paper entry from an ICARE APPROVED payload, filled at `entry_premium` (ask or actual fill).

    E16: ICARE's SL stays a PRICE. An entry at or below it is refused (PRICE_BELOW_STOP: the
    signal is invalidated), and lots are cut so (entry - SL) x qty <= max_risk_allowed. Only a
    target the fill already passed is re-anchored (that never adds risk). Missing levels
    fall back to 30 % below / above ICARE's premium.

    force=True (a REAL md:exec:fill — fills are facts): never refused or resized; a fill at or
    below the SL opens with opened_forced="OPENED_FORCED:PRICE_BELOW_STOP" (the caller exits it).
    """
    lots = int(_f(icare.get("recommended_lots"), 0) or 0)
    lot_size = _f(icare.get("lot_size"), 0.0) or 0.0
    if lots <= 0 or lot_size <= 0 or not entry_premium or entry_premium <= 0:
        return None
    ref = _f(icare.get("premium"), entry_premium) or entry_premium
    sl = _f(icare.get("stop_loss_premium"))
    target = _f(icare.get("target_premium"))
    if sl is None:
        sl = entry_premium - abs(ref - ref * 0.7)
    forced = []
    if entry_premium <= sl:
        if not force:
            return None                                # PRICE_BELOW_STOP
        forced.append("PRICE_BELOW_STOP")
    if target is None or target <= entry_premium:
        tgt_pts = (target if target is not None else ref * 1.3) - ref
        target = entry_premium + abs(tgt_pts)
    max_risk = _f(icare.get("max_risk_allowed"))
    if max_risk is not None and max_risk > 0 and not force:
        lots = min(lots, int(max_risk / ((entry_premium - sl) * lot_size) + 1e-9))
        if lots <= 0:
            return None                                # one lot already exceeds ICARE's risk
    hold_min = _f(icare.get("hold_minutes"), 60.0) or 60.0
    return PaperPosition(
        trade_id=f"{icare.get('symbol', '')}-{now_ms}",
        symbol=str(icare.get("symbol") or ""),
        tradingsymbol=str(icare.get("tradingsymbol") or ""),
        side=str(icare.get("side") or ""),
        strike=_f(icare.get("strike"), 0.0) or 0.0,
        lots=lots,
        lot_size=lot_size,
        qty=lots * lot_size,
        entry_premium=round(entry_premium, 4),
        entry_ts_ms=now_ms,
        sl_premium=round(sl, 4),
        target_premium=round(target, 4),
        time_stop_ms=now_ms + int(hold_min * 60_000),
        entry_spot=_f(icare.get("spot")),
        context={k: str(icare.get(k) or "") for k in ENTRY_CONTEXT_FIELDS},
        last_premium=entry_premium,
        max_premium=entry_premium,
        min_premium=entry_premium,
        orig_qty=lots * lot_size,
        exec_mode=exec_mode,
        opened_forced=("OPENED_FORCED:" + ",".join(forced)) if forced else "",
    )


def forced_reasons(pos: PaperPosition) -> list:
    f = pos.opened_forced or ""
    return [x for x in f.split(":", 1)[1].split(",") if x] if f.startswith("OPENED_FORCED:") else []


def with_forced(pos: PaperPosition, reasons) -> PaperPosition:
    allr = list(dict.fromkeys(forced_reasons(pos) + [x for x in reasons if x]))
    return replace(pos, opened_forced=("OPENED_FORCED:" + ",".join(allr)) if allr else "")


def merge_fill(pos: PaperPosition, add: PaperPosition) -> PaperPosition:
    """A second real fill on an already-open contract: one position, qty-weighted average entry."""
    qty = pos.qty + add.qty
    avg = (pos.entry_premium * pos.qty + add.entry_premium * add.qty) / qty
    ctx = dict(pos.context)
    ec = (_f(pos.context.get("entry_charges"), 0.0) or 0.0) + (_f(add.context.get("entry_charges"), 0.0) or 0.0)
    if pos.context.get("entry_charges") or add.context.get("entry_charges"):
        ctx["entry_charges"] = str(round(ec, 2))
    merged = replace(
        pos, qty=qty, lots=pos.lots + add.lots, entry_premium=round(avg, 4),
        orig_qty=(pos.orig_qty or pos.qty) + (add.orig_qty or add.qty), context=ctx,
        exit_requested_ms=0 if pos.exit_pending else pos.exit_requested_ms,   # an exit in flight covers the new qty too
    )
    return with_forced(merged, forced_reasons(add))


def mark(pos: PaperPosition, premium: Optional[float]) -> PaperPosition:
    if premium is None or premium <= 0:
        return pos
    return replace(
        pos,
        last_premium=premium,
        max_premium=max(pos.max_premium or premium, premium),
        min_premium=min(pos.min_premium or premium, premium),
    )


def exit_reason(
    pos: PaperPosition, premium: Optional[float], now_ms: int, eod: bool,
    use_target: bool = True, force: Optional[str] = None,
) -> Optional[str]:
    if premium is not None and premium > 0:
        if premium <= pos.sl_premium:
            return "SL"
        if force:
            return force
        if use_target and premium >= pos.target_premium:
            return "TARGET"
    if now_ms >= pos.time_stop_ms:
        return "TIME"
    if eod:
        return "EOD"
    return None


def close_position(pos: PaperPosition, exit_premium: float, now_ms: int, reason: str, mode: str = "paper") -> dict:
    """Journal record (flat dict) for a closed paper (or live, broker-filled) trade."""
    pnl = (exit_premium - pos.entry_premium) * pos.qty
    mfe = ((pos.max_premium or pos.entry_premium) - pos.entry_premium) * pos.qty
    mae = ((pos.min_premium or pos.entry_premium) - pos.entry_premium) * pos.qty
    rec = {
        "trade_id": pos.trade_id,
        "mode": mode,
        "symbol": pos.symbol,
        "tradingsymbol": pos.tradingsymbol,
        "side": pos.side,
        "strike": pos.strike,
        "executed_lots": pos.lots,
        "lot_size": pos.lot_size,
        "qty": pos.qty,
        "entry_ts_ms": pos.entry_ts_ms,
        "exit_ts_ms": now_ms,
        "entry_premium": pos.entry_premium,
        "exit_premium": round(exit_premium, 4),
        "sl_premium": pos.sl_premium,
        "target_premium": pos.target_premium,
        "exit_reason": reason,
        "holding_minutes": round((now_ms - pos.entry_ts_ms) / 60_000.0, 2),
        "pnl": round(pnl, 2),
        "pnl_pct": round((exit_premium / pos.entry_premium - 1.0) * 100.0, 4),
        "mfe": round(mfe, 2),
        "mae": round(mae, 2),
        "drawdown": round(min(mae, 0.0), 2),
        "win": 1 if pnl > 0 else 0,
        "entry_spot": pos.entry_spot,
        "opened_forced": pos.opened_forced,
    }
    rec.update(pos.context)
    return rec


def exit_request(pos: PaperPosition, reason: str, now_ms: int, mode: str, bid: Optional[float] = None,
                 limit_floor: Optional[float] = None) -> Dict[str, str]:
    """md:exec:exit_request payload (E17). limit_floor empty = the executor's slippage floor."""
    return {
        "trade_id": pos.trade_id, "tradingsymbol": pos.tradingsymbol, "symbol": pos.symbol,
        "qty": str(pos.qty), "orig_qty": str(pos.orig_qty or pos.qty), "lot_size": str(pos.lot_size),
        "reason": reason, "limit_floor": "" if limit_floor is None else str(limit_floor),
        "bid": "" if bid is None else str(bid), "exec_mode": mode, "ts_ms": str(now_ms),
    }


def apply_exit_fill(pos: PaperPosition, fill: dict) -> Tuple[Optional[PaperPosition], Optional[PaperPosition], float]:
    """
    A broker-confirmed SELL report for this position -> (closed part, remaining part, price).
    The closed part has qty = filled units; the remaining part (if any) keeps its exit
    reason with exit_requested_ms = 0, so the journal asks for the rest at once.
    """
    filled = _f(fill.get("filled_qty"), 0.0) or 0.0
    px = _f(fill.get("avg_price"))
    filled = min(filled, pos.qty)
    if filled <= 0 or not px:
        return None, replace(pos, exit_requested_ms=0), 0.0
    lots_closed = int(round(filled / pos.lot_size)) if pos.lot_size else pos.lots
    closed = replace(pos, qty=filled, lots=lots_closed)
    left = pos.qty - filled
    if left <= 1e-9:
        return closed, None, px
    remaining = replace(pos, qty=left, lots=int(round(left / pos.lot_size)) if pos.lot_size else 0,
                        exit_requested_ms=0)
    return closed, remaining, px


def apply_charges(rec: dict, entry_charges: float, exit_charges: float) -> dict:
    """
    Journal PnL net of charges (DECISION.md §7 X-Q8); the premium move stays in gross_pnl.
    pnl_pct / win follow the NET result so the learning buckets see what was really earned.
    """
    out = dict(rec)
    total = round((entry_charges or 0.0) + (exit_charges or 0.0), 2)
    gross = float(rec["pnl"])
    net = round(gross - total, 2)
    notional = float(rec["entry_premium"]) * float(rec["qty"])
    out.update(
        gross_pnl=gross,
        gross_pnl_pct=rec["pnl_pct"],
        charges=total,
        entry_charges=round(entry_charges or 0.0, 2),
        exit_charges=round(exit_charges or 0.0, 2),
        pnl=net,
        pnl_pct=round(net / notional * 100.0, 4) if notional else rec["pnl_pct"],
        win=1 if net > 0 else 0,
    )
    return out


# ── Module 18 (Adaptive TSL) hooks ─────────────────────────────────────

def tsl_exit_decision(tsl: dict, trade_id: str, now_ms: int, mode: str, max_age_ms: int) -> Tuple[Optional[str], bool]:
    """
    (forced exit reason, use_target) from the engine's md:tsl:state doc.
    Only TSL_MODE=active changes behaviour, and only while the engine is
    live (state updated within max_age_ms) — if the engine stops, the fixed
    SL / TARGET / TIME exits apply again.
    """
    if mode != "active" or not tsl or str(tsl.get("trade_id") or "") != trade_id:
        return None, True
    if now_ms - int(_f(tsl.get("updated_ms"), 0) or 0) > max_age_ms:
        return None, True
    if str(tsl.get("status") or "") == "EXIT":
        return "TRAILING_STOP", False
    return None, False


def tsl_journal_fields(tsl: dict, pos: PaperPosition) -> Dict[str, object]:
    """Counterfactual / audit fields from the engine state, added to the journal record."""
    if not tsl or str(tsl.get("trade_id") or "") != pos.trade_id:
        return {"tsl_status": ""}
    out: Dict[str, object] = {
        "tsl_status": str(tsl.get("status") or ""),
        "tsl_mode": str(tsl.get("mode") or ""),
        "tsl_activated": 1 if tsl.get("activated") else 0,
        "tsl_last_stop": _f(tsl.get("stop")),
        "tsl_pct": _f(tsl.get("tsl_pct")),
        "tsl_rule": str(tsl.get("tsl_rule") or ""),
        "tsl_exit_trigger": str(tsl.get("exit_trigger") or ""),
    }
    px = _f(tsl.get("exit_price"))
    if str(tsl.get("status") or "") == "EXIT" and px is not None:
        out["tsl_would_exit_ts"] = tsl.get("exit_ts_ms")
        out["tsl_would_exit_px"] = px
        out["tsl_shadow_pnl"] = round((px - pos.entry_premium) * pos.qty, 2)
    return out


# ── Bucket statistics ──────────────────────────────────────────────────

def bucket_keys(symbol: str, phase: str) -> Tuple[str, ...]:
    sym = (symbol or "").upper()
    keys = ["ALL"]
    if sym:
        keys.append(f"SYM:{sym}")
        if phase:
            keys.append(f"SYM_PHASE:{sym}|{phase.upper()}")
    return tuple(keys)


def update_stats(stats: Dict[str, dict], rec: dict) -> Dict[str, dict]:
    out = {k: dict(v) for k, v in (stats or {}).items()}
    pnl_pct = float(rec.get("pnl_pct") or 0.0)
    win = pnl_pct > 0
    for key in bucket_keys(str(rec.get("symbol") or ""), str(rec.get("market_phase") or "")):
        b = out.setdefault(key, {"samples": 0, "wins": 0, "sum_win_pct": 0.0, "sum_loss_pct": 0.0, "pnl": 0.0})
        b["samples"] += 1
        b["pnl"] = round(b["pnl"] + float(rec.get("pnl") or 0.0), 2)
        if win:
            b["wins"] += 1
            b["sum_win_pct"] = round(b["sum_win_pct"] + pnl_pct, 4)
        else:
            b["sum_loss_pct"] = round(b["sum_loss_pct"] + abs(pnl_pct), 4)
    return out


def summarize(b: Optional[dict]) -> dict:
    """samples, win_rate (0..1), avg_win_pct / avg_loss_pct (% of premium), pnl."""
    b = b or {}
    n = int(b.get("samples") or 0)
    w = int(b.get("wins") or 0)
    losses = n - w
    return {
        "samples": n,
        "win_rate": (w / n) if n else None,
        "avg_win_pct": (b.get("sum_win_pct", 0.0) / w) if w else None,
        "avg_loss_pct": (b.get("sum_loss_pct", 0.0) / losses) if losses else None,
        "pnl": b.get("pnl", 0.0),
    }


def pick_bucket(stats: Optional[Dict[str, dict]], symbol: str, phase: str, min_samples: int) -> Tuple[str, dict]:
    """Most specific bucket with >= min_samples; else the largest one (for its sample count)."""
    stats = stats or {}
    keys = bucket_keys(symbol, phase)
    for key in reversed(keys):
        s = summarize(stats.get(key))
        if s["samples"] >= min_samples:
            return key, s
    best = max(keys, key=lambda k: summarize(stats.get(k))["samples"])
    return best, summarize(stats.get(best))

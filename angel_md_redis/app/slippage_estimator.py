"""
app/slippage_estimator.py
─────────────────────────
Module 19 — Slippage Estimator. Estimates the execution cost of a marketable
order BEFORE it is placed: best bid / ask, spread, and the VWAP fill price from
walking the visible order-book depth (up to 5 levels) for the order quantity.

  BUY  walks the ask ladder (ascending), SELL walks the bid ladder (descending).
  slippage per unit = VWAP fill - best price on that side (>= 0 = worse for us);
  total cost vs mid = (VWAP - mid) x filled  (BUY) — half-spread plus depth impact.
  Quantity the 5 levels cannot fill is reported as `unfilled_qty` (never priced).

Advisory only: nothing here places or blocks an order.

No I/O here — Redis wiring lives in run_slippage_estimator.py.
"""

from __future__ import annotations

import math
from typing import Dict, List, Optional, Sequence, Tuple

from app.order_executor.market import Quote

Level = Tuple[float, float]     # (price, units)


def walk_book(levels: Sequence[Level], qty: float, side: str) -> Dict[str, object]:
    """
    Consume `qty` units from `levels` (best first). Returns
    {filled_qty, unfilled_qty, vwap, worst_price, levels_used, cost}. vwap / worst_price None when nothing fills.
    """
    want = max(float(qty or 0.0), 0.0)
    remaining, cost, used, worst = want, 0.0, 0, None
    for px, sz in levels:
        if remaining <= 1e-9:
            break
        take = min(remaining, sz)
        if take <= 0 or px <= 0:
            continue
        cost += take * px
        remaining -= take
        used += 1
        worst = px
    filled = want - max(remaining, 0.0)
    return {
        "filled_qty": round(filled, 4),
        "unfilled_qty": round(max(remaining, 0.0), 4),
        "vwap": round(cost / filled, 4) if filled > 0 else None,
        "worst_price": worst,
        "levels_used": used,
        "cost": round(cost, 2),
    }


def _book(q: Quote, side: str) -> Tuple[List[Level], bool]:
    """(levels best-first, from_depth). Falls back to the single best level when no depth is published."""
    side = side.upper()
    if side == "BUY":
        lv = sorted(q.ask_levels, key=lambda x: x[0])
        if lv:
            return lv, True
        if q.ask and q.ask > 0 and q.ask_qty and q.ask_qty > 0:
            return [(q.ask, q.ask_qty)], False
    else:
        lv = sorted(q.bid_levels, key=lambda x: -x[0])
        if lv:
            return lv, True
        if q.bid and q.bid > 0 and q.bid_qty and q.bid_qty > 0:
            return [(q.bid, q.bid_qty)], False
    return [], False


def estimate_slippage(
    q: Optional[Quote], lots: float, lot_size: float, side: str = "BUY", reference: Optional[float] = None,
    max_slippage_pct: Optional[float] = None,
) -> Dict[str, object]:
    """
    Pre-trade slippage estimate for `lots` x `lot_size` units. `reference` (the approval premium) adds
    slippage_vs_ref_pct; `max_slippage_pct` adds `within_limit` (True when VWAP slippage vs best is inside it).
    Always returns a dict with `status` OK / PARTIAL (depth too thin) / NO_QUOTE / NO_DEPTH / BAD_QTY and `flags`.
    """
    side = (side or "BUY").upper()
    qty = _num(lots) * _num(lot_size)
    out: Dict[str, object] = {"side": side, "lots": _num(lots), "lot_size": _num(lot_size), "qty": qty, "flags": []}
    flags: List[str] = out["flags"]  # type: ignore[assignment]
    if qty <= 0:
        out["status"] = "BAD_QTY"
        return out
    if q is None or not q.tradable:
        out["status"] = "NO_QUOTE"
        if q is not None:
            out.update({"best_bid": q.bid, "best_ask": q.ask})
        return out

    levels, from_depth = _book(q, side)
    out.update({"best_bid": q.bid, "best_ask": q.ask, "mid": round(q.mid, 4), "spread": round(q.spread, 4),
                "spread_pct": round(q.spread_pct, 4)})
    if not levels:
        out["status"] = "NO_DEPTH"
        return out
    if not from_depth:
        flags.append("TOP_OF_BOOK_ONLY")

    w = walk_book(levels, qty, side)
    best = levels[0][0]
    sign = 1.0 if side == "BUY" else -1.0
    out.update(w)
    out["best_price"] = best
    out["status"] = "OK" if w["unfilled_qty"] <= 1e-9 else "PARTIAL"
    if out["status"] == "PARTIAL":
        flags.append("INSUFFICIENT_DEPTH")
    vwap = w["vwap"]
    if vwap is not None:
        slip = sign * (vwap - best)                         # per unit, vs the best price on our side
        out["slippage_per_unit"] = round(slip, 4)
        out["slippage_pct"] = round(slip / best * 100.0, 4)
        out["slippage_cost"] = round(slip * w["filled_qty"], 2)
        vs_mid = sign * (vwap - q.mid)                      # half-spread + depth impact
        out["cost_vs_mid_per_unit"] = round(vs_mid, 4)
        out["cost_vs_mid"] = round(vs_mid * w["filled_qty"], 2)
        out["cost_vs_mid_pct"] = round(vs_mid / q.mid * 100.0, 4)
        if reference and reference > 0:
            out["slippage_vs_ref_pct"] = round(sign * (vwap - reference) / reference * 100.0, 4)
        if max_slippage_pct is not None:
            out["within_limit"] = out["slippage_pct"] <= max_slippage_pct + 1e-9
            if not out["within_limit"]:
                flags.append("SLIPPAGE_ABOVE_LIMIT")
    return out


def round_trip_cost(entry: Dict[str, object], exit_: Dict[str, object]) -> Optional[float]:
    """Entry (BUY) cost vs mid + exit (SELL) cost vs mid, Rs; None when either side has no fill estimate."""
    a, b = entry.get("cost_vs_mid"), exit_.get("cost_vs_mid")
    if a is None or b is None:
        return None
    return round(float(a) + float(b), 2)


def _num(v) -> float:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return 0.0
    return x if math.isfinite(x) else 0.0

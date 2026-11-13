"""Fill aggregation and the brokerage-aware completion decision (DECISION.md §7 E9)."""

from __future__ import annotations

import math
from typing import List, Optional, Sequence, Tuple


def avg_price(fills: Sequence[Sequence[float]]) -> Optional[float]:
    """fills = [(price, units, ts_ms), ...] -> volume-weighted average price."""
    qty = sum(f[1] for f in fills)
    if qty <= 0:
        return None
    return sum(f[0] * f[1] for f in fills) / qty


def fill_delta(prev_filled: float, prev_avg: Optional[float], new_filled: float,
               new_avg: Optional[float], limit_price: float) -> Optional[Tuple[float, float]]:
    """
    Broker updates are cumulative per order (filled units, average price).
    Return the NEW (price, units) since the last update, or None.
    """
    units = new_filled - prev_filled
    if units <= 0:
        return None
    if new_avg is None:
        return limit_price, units
    if prev_filled > 0 and prev_avg is not None:
        px = (new_avg * new_filled - prev_avg * prev_filled) / units
    else:
        px = new_avg
    return round(px, 4), units


def continue_decision(
    remaining_lots: int, lot_size: float, ask: float, avg_fill: Optional[float],
    signal_premium: Optional[float], ev_per_lot: Optional[float], slice_size: int,
    per_order_cost: float, cfg,
) -> Tuple[bool, str, dict]:
    """
    Complete the remaining quantity only if the expected benefit beats the ADDITIONAL
    execution cost (per-order brokerage + extra slippage vs fills so far) by cfg.cost_margin.
    Statutory charges scale with turnover and are paid either way, so they are not "additional".
    """
    remaining_qty = remaining_lots * lot_size
    detail: dict = {"remaining_lots": remaining_lots}
    if remaining_lots <= 0:
        return False, "NOTHING_LEFT", detail
    residual_value = remaining_qty * ask
    detail["residual_value"] = round(residual_value, 2)
    if residual_value < cfg.min_residual_value:
        return False, "RESIDUAL_TOO_SMALL", detail
    orders_needed = math.ceil(remaining_lots / max(slice_size, 1))
    extra_slip = max(0.0, ask - avg_fill) * remaining_qty if avg_fill is not None else 0.0
    cost = per_order_cost * orders_needed + extra_slip
    detail.update(orders_needed=orders_needed, additional_cost=round(cost, 2))
    if ev_per_lot is None:
        detail["benefit"] = None
        return True, "EV_UNKNOWN_CONTINUE", detail
    ratio = (signal_premium / ask) if signal_premium and ask > 0 else 1.0
    benefit = ev_per_lot * remaining_lots * min(ratio, 1.0)
    detail["benefit"] = round(benefit, 2)
    if benefit > cost * cfg.cost_margin:
        return True, "BENEFIT_EXCEEDS_COST", detail
    return False, "COST_EXCEEDS_BENEFIT", detail

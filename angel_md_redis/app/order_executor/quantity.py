"""Executable quantity MIN() and liquidity slicing (DECISION.md §7 E7/E8)."""

from __future__ import annotations

import math
from typing import List, Optional, Tuple

from .config import ExecConfig


def _floor(x: float) -> int:
    return int(math.floor(x + 1e-9))


def max_lots_per_order(lot_size: float, freeze_qty: Optional[float], cfg: ExecConfig) -> int:
    """NSE freeze quantity is in UNITS; whole lots below it. Fallback EXEC_MAX_LOTS_PER_ORDER."""
    if freeze_qty and lot_size > 0:
        # an order must stay strictly below the freeze quantity
        return max(_floor((freeze_qty - 1) / lot_size), 1)
    return max(cfg.max_lots_per_order, 1)


def depth_lots(units: Optional[float], lot_size: float, cfg: ExecConfig) -> Optional[int]:
    if units is None or lot_size <= 0:
        return None
    return _floor(units * cfg.depth_take / lot_size)


def executable_lots(
    requested: int,
    lot_size: float,
    cap_price: float,
    available_margin: Optional[float],
    max_risk_amount: Optional[float],
    sl_points: Optional[float],
    depth_units: Optional[float],
    freeze_qty: Optional[float],
    cfg: ExecConfig,
) -> Tuple[int, str, List[str], dict]:
    """
    (lots, limiting factor, reductions[], per-limit lots). Never above `requested`.

    Depth limits the whole quantity only when even ONE slice cannot be filled
    (slicing handles the rest, E8); the per-order cap is a slicing limit, not a total limit.
    """
    limits = {"requested": max(int(requested), 0)}
    if available_margin is not None and cap_price > 0:
        per_lot = cap_price * lot_size * (1.0 + cfg.margin_buffer_pct / 100.0)
        limits["margin"] = _floor(max(available_margin, 0.0) / per_lot)
    if max_risk_amount is not None and max_risk_amount > 0 and sl_points is not None:
        # risk = (worst fill = cap - SL) x qty must stay <= ICARE max_risk_allowed. A cap at or
        # below the SL means the signal is invalid: 0 lots, never "no limit" (E7).
        limits["risk"] = _floor(max_risk_amount / (sl_points * lot_size)) if sl_points > 0 else 0
    dl = depth_lots(depth_units, lot_size, cfg)
    if dl is not None and dl < 1:
        limits["depth"] = 0
    lots = min(limits.values())
    limiting = min(limits, key=lambda k: (limits[k], k != "requested"))
    reductions = [f"REDUCED_{k.upper()}" for k, v in limits.items() if k != "requested" and v < limits["requested"]]
    if lots < limits["requested"] and not cfg.allow_reduce:
        lots = 0
    return lots, limiting, reductions, limits


def slice_lots(remaining: int, depth_units: Optional[float], lot_size: float,
               freeze_qty: Optional[float], cfg: ExecConfig) -> int:
    """Next order size: MIN(remaining, visible depth inside the cap, per-order max)."""
    n = min(remaining, max_lots_per_order(lot_size, freeze_qty, cfg))
    dl = depth_lots(depth_units, lot_size, cfg)
    if dl is not None:
        n = min(n, dl)
    return max(n, 0)

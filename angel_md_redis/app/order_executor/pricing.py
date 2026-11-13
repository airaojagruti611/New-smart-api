"""Slippage cap and the controlled limit-price ladder (DECISION.md §7 E6). Never a market order."""

from __future__ import annotations

import math

from .config import ExecConfig
from .market import Quote

_EPS = 1e-9


def floor_tick(x: float, tick: float) -> float:
    return round(math.floor(x / tick + _EPS) * tick, 4)


def ceil_tick(x: float, tick: float) -> float:
    return round(math.ceil(x / tick - _EPS) * tick, 4)


def price_cap(reference: float, tick: float, cfg: ExecConfig) -> float:
    """Highest price a BUY may ever pay: 101 x 1.005 = 101.505 -> 101.50. Anchored for the command."""
    return floor_tick(reference * (1.0 + cfg.max_slippage_pct / 100.0), tick)


def start_price(q: Quote, tick: float, cap: float, cfg: ExecConfig) -> float:
    """Mid rounded up to the tick; at the ask when the spread is already <= N ticks."""
    if q.spread is not None and q.spread <= cfg.tight_spread_ticks * tick + _EPS:
        return min(q.ask, cap)
    return min(ceil_tick(q.mid, tick), cap)


def ladder_step(start: float, cap: float, tick: float, cfg: ExecConfig) -> float:
    return max(tick, ceil_tick((cap - start) / max(cfg.ladder_steps, 1), tick))


def next_price(current: float, step: float, cap: float) -> float:
    return round(min(current + step, cap), 4)

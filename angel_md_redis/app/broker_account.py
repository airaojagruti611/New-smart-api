"""
app/broker_account.py
───────────────────────
Account / margin snapshot for ICARE (DECISION.md D10).

Two sources, same AccountSnapshot shape:

  paper (default) — TOTAL_CAPITAL + open paper positions + today's realized
                    paper PnL. Margin for a long option = premium x qty.
  live            — Angel SmartAPI getRMS (`SmartConnect.rmsLimit()`, 2 req/s):
                      net            -> available margin
                      availablecash  -> available cash
                      utiliseddebits -> used (blocked) margin
                      m2mrealized / m2munrealized -> day PnL

Option BUYING needs only the premium as margin, so ICARE computes margin per
lot as premium x lot size itself; this snapshot only supplies how much is
available / already used.

No I/O here — Redis + broker calls live in run_account.py.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable, Optional


def _f(v, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


@dataclass(frozen=True)
class AccountSnapshot:
    mode: str                      # "paper" / "live"
    source: str                    # "paper_ledger" / "angel_rms"
    total_capital: float
    available_cash: float
    available_margin: float
    used_margin: float
    margin_utilization_pct: float
    realized_pnl: float
    unrealized_pnl: float
    day_pnl: float
    open_positions: int
    open_risk: float               # sum of (entry - SL) x qty over open positions
    ts_ms: int

    def to_dict(self) -> dict:
        return asdict(self)


def _utilization(used: float, total: float) -> float:
    return round(used / total * 100.0, 4) if total > 0 else 0.0


def position_exposure(positions: Iterable[dict]) -> tuple[int, float, float, float]:
    """(count, margin used, unrealized pnl, open risk) for paper position dicts."""
    n, used, upnl, risk = 0, 0.0, 0.0, 0.0
    for p in positions:
        qty = _f(p.get("qty"))
        entry = _f(p.get("entry_premium"))
        if qty <= 0 or entry <= 0:
            continue
        last = _f(p.get("last_premium"), entry) or entry
        sl = _f(p.get("sl_premium"), 0.0)
        n += 1
        used += entry * qty
        upnl += (last - entry) * qty
        risk += max(entry - sl, 0.0) * qty
    return n, round(used, 2), round(upnl, 2), round(risk, 2)


def paper_snapshot(
    total_capital: float,
    positions: Iterable[dict],
    realized_pnl_today: float,
    ts_ms: int,
) -> AccountSnapshot:
    positions = list(positions)
    n, used, upnl, risk = position_exposure(positions)
    capital = max(_f(total_capital), 0.0) + _f(realized_pnl_today)
    available = max(capital - used, 0.0)
    return AccountSnapshot(
        mode="paper",
        source="paper_ledger",
        total_capital=round(capital, 2),
        available_cash=round(available, 2),
        available_margin=round(available, 2),
        used_margin=used,
        margin_utilization_pct=_utilization(used, capital),
        realized_pnl=round(_f(realized_pnl_today), 2),
        unrealized_pnl=upnl,
        day_pnl=round(_f(realized_pnl_today) + upnl, 2),
        open_positions=n,
        open_risk=risk,
        ts_ms=ts_ms,
    )


def parse_rms(data: Optional[dict], positions: Iterable[dict], ts_ms: int) -> Optional[AccountSnapshot]:
    """Angel getRMS `data` block -> snapshot. Open count / risk still from tracked positions."""
    if not data:
        return None
    available = _f(data.get("net"))
    used = _f(data.get("utiliseddebits"))
    realized = _f(data.get("m2mrealized"))
    unrealized = _f(data.get("m2munrealized"))
    n, _used, _upnl, risk = position_exposure(positions)
    total = available + used
    return AccountSnapshot(
        mode="live",
        source="angel_rms",
        total_capital=round(total, 2),
        available_cash=round(_f(data.get("availablecash")), 2),
        available_margin=round(available, 2),
        used_margin=round(used, 2),
        margin_utilization_pct=_utilization(used, total),
        realized_pnl=round(realized, 2),
        unrealized_pnl=round(unrealized, 2),
        day_pnl=round(realized + unrealized, 2),
        open_positions=n,
        open_risk=risk,
        ts_ms=ts_ms,
    )

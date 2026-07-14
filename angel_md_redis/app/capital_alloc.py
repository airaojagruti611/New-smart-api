from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class CapitalAllocResult:
    status: str  # "OK" / "SKIP"
    signal: str
    side: str  # "CE" / "PE" / ""
    regime: str
    call_alloc_pct: int
    put_alloc_pct: int
    side_alloc_pct: int
    total_capital: float
    side_budget: float
    trade_notional: float
    reason: str


def _safe_int(v: Any, default: int) -> int:
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def capital_alloc(
    signal: str,
    regime_snapshot: Optional[Dict[str, Any]],
    total_capital: float,
    max_risk_per_trade_pct: float = 5.0,
) -> CapitalAllocResult:
    """
    Bias trade size using market-regime CALL/PUT capital split.

    side_budget   = total_capital * side_alloc_pct / 100
    trade_notional = side_budget * max_risk_per_trade_pct / 100
    Missing / NO_DATA regime -> 50/50 fallback.
    """
    sig = (signal or "").strip().upper()
    total = max(_safe_float(total_capital, 0.0), 0.0)
    risk_pct = max(_safe_float(max_risk_per_trade_pct, 5.0), 0.0)

    if sig == "BUY CALL":
        side = "CE"
    elif sig == "BUY PUT":
        side = "PE"
    else:
        return CapitalAllocResult(
            status="SKIP",
            signal=sig or "NEUTRAL",
            side="",
            regime="NA",
            call_alloc_pct=50,
            put_alloc_pct=50,
            side_alloc_pct=0,
            total_capital=total,
            side_budget=0.0,
            trade_notional=0.0,
            reason="not_buy_signal",
        )

    snap = regime_snapshot or {}
    regime = str(snap.get("regime") or "NO_DATA").strip().upper() or "NO_DATA"
    call_pct = _safe_int(snap.get("call_alloc_pct"), 50)
    put_pct = _safe_int(snap.get("put_alloc_pct"), 50)

    if regime in ("", "NO_DATA", "NA") or (call_pct <= 0 and put_pct <= 0):
        regime = "NO_DATA" if regime in ("", "NA") else regime
        call_pct, put_pct = 50, 50

    side_pct = call_pct if side == "CE" else put_pct
    side_budget = total * (side_pct / 100.0)
    trade_notional = side_budget * (risk_pct / 100.0)

    if total <= 0:
        return CapitalAllocResult(
            status="SKIP",
            signal=sig,
            side=side,
            regime=regime,
            call_alloc_pct=call_pct,
            put_alloc_pct=put_pct,
            side_alloc_pct=side_pct,
            total_capital=total,
            side_budget=side_budget,
            trade_notional=0.0,
            reason="no_capital",
        )

    if trade_notional <= 0:
        return CapitalAllocResult(
            status="SKIP",
            signal=sig,
            side=side,
            regime=regime,
            call_alloc_pct=call_pct,
            put_alloc_pct=put_pct,
            side_alloc_pct=side_pct,
            total_capital=total,
            side_budget=side_budget,
            trade_notional=0.0,
            reason="no_budget",
        )

    return CapitalAllocResult(
        status="OK",
        signal=sig,
        side=side,
        regime=regime,
        call_alloc_pct=call_pct,
        put_alloc_pct=put_pct,
        side_alloc_pct=side_pct,
        total_capital=total,
        side_budget=round(side_budget, 2),
        trade_notional=round(trade_notional, 2),
        reason="ok",
    )

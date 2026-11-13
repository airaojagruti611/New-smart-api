"""
app/portfolio_exposure.py
─────────────────────────
Module 21 — Portfolio Exposure Monitor. Live exposure across the open (paper)
positions written by the trade journal (md:position:open:*):

  per position  premium deployed (entry x qty), unrealized PnL marked at the BID
                (long options exit at the bid; falls back to last_premium, then
                entry), risk at entry ((entry - SL) x qty) and risk to SL from the
                current mark (max(mark - SL, 0) x qty), signed delta exposure
  totals        deployed, unrealized, risk, utilization % of capital
  groups        per underlying, per sector, per side (CE / PE: count, deployed,
                net delta in underlying units when the position carries a delta)
  flags         ICARE limits breached (open trades, portfolio risk %, sector %,
                margin utilization, daily loss) — advisory, nothing is blocked here

No I/O here — Redis wiring lives in run_portfolio_exposure.py.
"""

from __future__ import annotations

import math
from typing import Dict, List, Mapping, Optional

from app.icare import ICAREConfig


def _f(v) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _delta(pos: dict) -> Optional[float]:
    ctx = pos.get("context") if isinstance(pos.get("context"), dict) else {}
    d = _f(pos.get("delta"))
    return d if d is not None else _f(ctx.get("delta"))


def position_exposure(pos: dict, bid: Optional[float] = None) -> Dict[str, object]:
    """Exposure of one open position. `bid` is the live option bid (None -> last_premium -> entry)."""
    qty = _f(pos.get("qty")) or 0.0
    entry = _f(pos.get("entry_premium")) or 0.0
    sl = _f(pos.get("sl_premium"))
    if bid is not None and bid > 0:
        mark, src = bid, "bid"
    elif _f(pos.get("last_premium")):
        mark, src = _f(pos.get("last_premium")), "last_premium"
    else:
        mark, src = entry, "entry"
    side = str(pos.get("side") or "").upper()
    d = _delta(pos)
    sign = -1.0 if side == "PE" else 1.0
    return {
        "trade_id": pos.get("trade_id"),
        "symbol": str(pos.get("symbol") or "").upper(),
        "tradingsymbol": pos.get("tradingsymbol"),
        "side": side,
        "qty": qty,
        "entry_premium": entry,
        "mark": round(mark, 4),
        "mark_source": src,
        "deployed": round(entry * qty, 2),
        "market_value": round(mark * qty, 2),
        "unrealized_pnl": round((mark - entry) * qty, 2),
        "risk_at_entry": round(max(entry - sl, 0.0) * qty, 2) if sl is not None else None,
        "risk_to_sl": round(max(mark - sl, 0.0) * qty, 2) if sl is not None else None,
        "delta_exposure": round(sign * abs(d) * qty, 2) if d is not None else None,
        "executing": bool(pos.get("executing")),
    }


def _bucket() -> Dict[str, float]:
    return {"count": 0, "deployed": 0.0, "unrealized_pnl": 0.0, "risk_to_sl": 0.0, "net_delta": 0.0}


def _add(b: Dict[str, float], p: Dict[str, object]) -> None:
    b["count"] += 1
    b["deployed"] += p["deployed"]
    b["unrealized_pnl"] += p["unrealized_pnl"]
    b["risk_to_sl"] += p["risk_to_sl"] or 0.0
    b["net_delta"] += p["delta_exposure"] or 0.0


def _round(groups: Dict[str, Dict[str, float]]) -> Dict[str, Dict[str, float]]:
    return {k: {m: (v if m == "count" else round(v, 2)) for m, v in b.items()} for k, b in sorted(groups.items())}


def compute_exposure(
    positions: List[dict],
    bids: Mapping[str, Optional[float]],
    account: Optional[dict],
    sectors: Mapping[str, str],
    cfg: ICAREConfig,
    total_capital: float,
    now_ms: int,
) -> Dict[str, object]:
    """
    `positions` md:position:open:* docs (placeholders with executing=1 count as open trades but carry no
    premium); `bids` tradingsymbol -> live bid; `account` md:account:latest (may be empty / stale);
    `total_capital` fallback capital when the account has none.
    """
    account = account or {}
    capital = _f(account.get("total_capital")) or total_capital or 0.0
    rows = [position_exposure(p, bids.get(str(p.get("tradingsymbol") or ""))) for p in positions]

    tot = _bucket()
    by_under: Dict[str, Dict[str, float]] = {}
    by_sector: Dict[str, Dict[str, float]] = {}
    by_side: Dict[str, Dict[str, float]] = {}
    for p in rows:
        _add(tot, p)
        _add(by_under.setdefault(p["symbol"] or "?", _bucket()), p)
        _add(by_sector.setdefault(sectors.get(p["symbol"]) or "UNMAPPED", _bucket()), p)
        _add(by_side.setdefault(p["side"] or "?", _bucket()), p)

    pct = (lambda x: round(x / capital * 100.0, 4)) if capital > 0 else (lambda x: None)
    risk_at_entry = round(sum(p["risk_at_entry"] or 0.0 for p in rows), 2)
    sector_pct = {k: pct(b["deployed"]) for k, b in by_sector.items()}
    flags: List[str] = []
    limits = {
        "max_open_trades": cfg.max_open_trades,
        "max_portfolio_risk_pct": cfg.max_portfolio_risk_pct,
        "max_sector_exposure_pct": cfg.max_sector_exposure_pct,
        "max_margin_util_pct": cfg.max_margin_util_pct,
        "daily_loss_limit_pct": cfg.daily_loss_limit_pct,
    }
    if tot["count"] >= cfg.max_open_trades:
        flags.append("MAX_OPEN_TRADES_REACHED")
    if capital > 0 and risk_at_entry >= capital * cfg.max_portfolio_risk_pct / 100.0:
        flags.append("PORTFOLIO_RISK_LIMIT")
    for k, v in sector_pct.items():
        if k != "UNMAPPED" and v is not None and v > cfg.max_sector_exposure_pct:
            flags.append(f"SECTOR_LIMIT:{k}")
    if "UNMAPPED" in sector_pct:
        flags.append("SECTOR_UNMAPPED")
    margin_util = _f(account.get("margin_utilization_pct"))
    util = pct(tot["deployed"])
    if (margin_util if margin_util is not None else util or 0.0) > cfg.max_margin_util_pct:
        flags.append("MARGIN_UTILIZATION_LIMIT")
    day_pnl = _f(account.get("day_pnl"))
    if day_pnl is not None and capital > 0 and day_pnl <= -capital * cfg.daily_loss_limit_pct / 100.0:
        flags.append("DAILY_LOSS_LIMIT")
    if any(p["mark_source"] == "entry" and not p["executing"] for p in rows):
        flags.append("STALE_MARKS")
    acct_ts = int(_f(account.get("ts_ms")) or 0)
    if not account:
        flags.append("NO_ACCOUNT")
    elif acct_ts <= 0:
        flags.append("ACCOUNT_NO_TS")

    return {
        "ts_ms": now_ms,
        "total_capital": capital,
        "open_positions": int(tot["count"]),
        "executing": sum(1 for p in rows if p["executing"]),
        "deployed": round(tot["deployed"], 2),
        "utilization_pct": util,
        "unrealized_pnl": round(tot["unrealized_pnl"], 2),
        "risk_at_entry": risk_at_entry,
        "risk_at_entry_pct": pct(risk_at_entry),
        "risk_to_sl": round(tot["risk_to_sl"], 2),
        "net_delta": round(tot["net_delta"], 2),
        "by_underlying": _round(by_under),
        "by_sector": _round(by_sector),
        "by_sector_pct": sector_pct,
        "by_side": _round(by_side),
        "limits": limits,
        "flags": flags,
        "positions": rows,
    }

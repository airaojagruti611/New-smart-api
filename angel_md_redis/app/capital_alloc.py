from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


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


# ---------------------------------------------------------------------------
# Dynamic Capital Engine (DECISION.MD section B). Pure functions, no I/O.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ChargeRates:
    """
    Long-option round trip (buy then sell). Defaults per Angel One / NSE schedule
    effective 1 Apr 2026 (overridable by env in the runner):
      brokerage Rs20 flat per order; STT 0.15% of SELL premium (Budget 2026);
      NSE option exchange txn 0.03503% of premium (Rs 3503/crore); SEBI Rs10/crore;
      stamp duty 0.003% on BUY; GST 18% on (brokerage + exchange + SEBI).
    Source: https://partnerassist.angelone.in/knowledge-hub/blogs/stt-charges-hike-futures-options-budget-2026
    """
    brokerage_per_order: float = 20.0
    stt_sell_pct: float = 0.15
    exch_txn_pct: float = 0.03503
    sebi_per_crore: float = 10.0
    stamp_buy_pct: float = 0.003
    gst_pct: float = 18.0


def estimate_charges(premium: float, qty: int, rates: ChargeRates = ChargeRates()) -> float:
    """Estimated round-trip charges (INR) for buying and selling qty at premium."""
    if premium <= 0 or qty <= 0:
        return 0.0
    turnover = premium * qty  # both legs approximated at entry premium
    brokerage = 2.0 * rates.brokerage_per_order
    stt = turnover * rates.stt_sell_pct / 100.0
    exch = 2.0 * turnover * rates.exch_txn_pct / 100.0
    sebi = 2.0 * turnover * rates.sebi_per_crore / 1e7
    stamp = turnover * rates.stamp_buy_pct / 100.0
    gst = (brokerage + exch + sebi) * rates.gst_pct / 100.0
    return brokerage + stt + exch + sebi + stamp + gst


@dataclass(frozen=True)
class CapitalConfig:
    reserve_pct: float = 20.0
    u_max_pct: float = 80.0
    max_loss_pct: float = 5.0          # max loss per trade, % of available capital
    sl_pct: float = 30.0               # configured SL % of premium (until Trailing SL exists)
    liq_depth_fraction: float = 0.25   # fraction of ask-side depth we may take
    max_position_pct: float = 25.0     # L_portfolio: one position <= x% of available capital
    high_vol_factor: float = 0.7       # weight scale in high-volatility regime
    poor_liq_factor: float = 0.7       # weight scale when liquidity_score < poor_liq_below
    poor_liq_below: float = 40.0
    rates: ChargeRates = ChargeRates()


@dataclass(frozen=True)
class StrikeOption:
    strike: float
    token: str
    tradingsymbol: str
    premium: float
    lot_size: int
    freeze_qty: int = 0
    ask_depth_qty: Optional[float] = None
    liquidity_score: Optional[float] = None


@dataclass(frozen=True)
class Candidate:
    symbol: str
    side: str                      # CE / PE
    strike_score: float
    confidence: float              # 0-100
    regime: str
    side_alloc_pct: Optional[int]  # None -> no regime side cap
    options: Tuple[StrikeOption, ...]  # best-first (primary then top3 fallbacks)
    regime_class: str = ""         # strike_select's STRONG_TREND/NORMAL/HIGH_VOL/EXPIRY_DAY

    @property
    def s(self) -> float:
        return max(self.strike_score, 0.0) * max(self.confidence, 0.0) / 100.0


@dataclass(frozen=True)
class Allocation:
    symbol: str
    side: str
    status: str            # OK / SKIP
    reason: str
    lots: int = 0
    qty: int = 0
    cost_per_lot: float = 0.0
    est_charges: float = 0.0
    allocated: float = 0.0
    deployable_budget: float = 0.0
    reserve: float = 0.0
    binding_limit: str = ""
    weight: float = 0.0
    tradingsymbol: str = ""
    token: str = ""
    strike: float = 0.0
    premium: float = 0.0
    lot_size: int = 0


def deployable_budget(c_available: float, cfg: CapitalConfig = CapitalConfig()) -> Tuple[float, float, float]:
    """
    Returns (c_reserve, c_deploy, b_new).
      C_reserve = C_available * reserve_pct
      C_deploy  = max(0, C_available - C_reserve)
      B_new     = min(C_deploy, C_deploy * U_max)  (U_max applied to deployable capital;
                  yields Rs 64,000 on Rs 1,00,000 at 20% / 80%)
    """
    ca = max(float(c_available or 0.0), 0.0)
    reserve = ca * cfg.reserve_pct / 100.0
    c_deploy = max(0.0, ca - reserve)
    b_new = min(c_deploy, c_deploy * cfg.u_max_pct / 100.0)
    return reserve, c_deploy, b_new


def _floor(x: Optional[float]) -> int:
    if x is None or not math.isfinite(x) or x <= 0:
        return 0
    return int(math.floor(x + 1e-9))


def lot_limits(
    opt: StrikeOption,
    cost_per_lot: float,
    allocated_budget: float,
    c_available: float,
    cfg: CapitalConfig = CapitalConfig(),
    side_budget_left: Optional[float] = None,
) -> Dict[str, int]:
    """Whole-lot limits (each rounded DOWN). L_margin == L_budget for long options."""
    out: Dict[str, int] = {}
    out["L_budget"] = _floor(allocated_budget / cost_per_lot) if cost_per_lot > 0 else 0
    risk_per_lot = opt.premium * cfg.sl_pct / 100.0 * opt.lot_size
    max_loss = c_available * cfg.max_loss_pct / 100.0
    out["L_risk"] = _floor(max_loss / risk_per_lot) if risk_per_lot > 0 else 0
    liq = 10**9
    if opt.ask_depth_qty is not None and opt.lot_size > 0:
        liq = min(liq, _floor(opt.ask_depth_qty * cfg.liq_depth_fraction / opt.lot_size))
    if opt.freeze_qty and opt.lot_size > 0:
        liq = min(liq, _floor(opt.freeze_qty / opt.lot_size))
    out["L_liquidity"] = liq
    out["L_portfolio"] = (
        _floor(c_available * cfg.max_position_pct / 100.0 / cost_per_lot) if cost_per_lot > 0 else 0
    )
    if side_budget_left is not None:
        out["L_side"] = _floor(side_budget_left / cost_per_lot) if cost_per_lot > 0 else 0
    return out


def _weight_scale(c: Candidate, cfg: CapitalConfig) -> float:
    f = 1.0
    if "VOL" in (c.regime or "").upper() or "VOL" in (c.regime_class or "").upper():
        f *= cfg.high_vol_factor
    o = c.options[0] if c.options else None
    if o is not None and o.liquidity_score is not None and o.liquidity_score < cfg.poor_liq_below:
        f *= cfg.poor_liq_factor
    return f


def allocate(candidates: List[Candidate], c_available: float, cfg: CapitalConfig = CapitalConfig()) -> List[Allocation]:
    """
    Score-weighted batch allocation: W_i = S_i/sum(S), A_i = B_new * W_i, then whole-lot
    limits and low-capital fallback through each candidate's ranked options.
    Missing premium / lot size => reject with reason (never invent prices).
    """
    reserve, _c_deploy, b_new = deployable_budget(c_available, cfg)
    scaled = [c.s * _weight_scale(c, cfg) for c in candidates]
    total = sum(scaled)
    base = dict(deployable_budget=round(b_new, 2), reserve=round(reserve, 2))

    if not candidates or total <= 0 or b_new <= 0:
        reason = "no_budget" if b_new <= 0 else "zero_score_sum"
        return [Allocation(c.symbol, c.side, "SKIP", reason, **base) for c in candidates]

    side_left: Dict[str, float] = {}
    for c in candidates:
        if c.side_alloc_pct is not None:
            side_left[c.side] = b_new * c.side_alloc_pct / 100.0

    res: Dict[int, Allocation] = {}
    order = sorted(range(len(candidates)), key=lambda i: -scaled[i])
    for i in order:
        c = candidates[i]
        w = scaled[i] / total
        a_i = b_new * w
        if w <= 0:
            res[i] = Allocation(c.symbol, c.side, "SKIP", "zero_weight", **base)
            continue
        fail = "no_priced_option"
        chosen: Optional[Allocation] = None
        for opt in c.options:
            if opt.premium <= 0 or opt.lot_size <= 0:
                continue
            cpl = opt.premium * opt.lot_size + estimate_charges(opt.premium, opt.lot_size, cfg.rates)
            lim = lot_limits(opt, cpl, a_i, c_available, cfg, side_left.get(c.side))
            binder = min(lim, key=lambda k: lim[k])
            lots = lim[binder]
            if lots <= 0:
                fail = f"zero_lots_{binder}"
                continue
            qty = lots * opt.lot_size
            est = estimate_charges(opt.premium, qty, cfg.rates)
            amt = opt.premium * qty + est
            chosen = Allocation(
                c.symbol, c.side, "OK", "ok", lots=lots, qty=qty,
                cost_per_lot=round(cpl, 2), est_charges=round(est, 2),
                allocated=round(amt, 2), binding_limit=binder, weight=round(w, 6),
                tradingsymbol=opt.tradingsymbol, token=opt.token, strike=opt.strike,
                premium=opt.premium, lot_size=opt.lot_size, **base,
            )
            if c.side in side_left:
                side_left[c.side] = max(0.0, side_left[c.side] - amt)
            break
        res[i] = chosen or Allocation(c.symbol, c.side, "SKIP", fail, weight=round(w, 6), **base)
    return [res[i] for i in range(len(candidates))]


def _f(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def _opt_from_dict(d: Dict[str, Any], fb: Dict[str, Any]) -> Optional[StrikeOption]:
    # Premium only from this contract's own row: never borrow another strike's price.
    prem = _f(d.get("premium")) or _f(d.get("ask")) or _f(d.get("ltp"))
    lot = _f(d.get("lot_size")) or _f(fb.get("lot_size"))
    if not prem or not lot:
        return None
    return StrikeOption(
        strike=_f(d.get("strike")) or 0.0,
        token=str(d.get("token") or ""),
        tradingsymbol=str(d.get("tradingsymbol") or ""),
        premium=prem,
        lot_size=int(lot),
        freeze_qty=int(_f(d.get("freeze_qty")) or _f(fb.get("freeze_qty")) or 0),
        ask_depth_qty=_f(d.get("ask_depth_qty")),
        liquidity_score=_f(d.get("liquidity_score")),
    )


def candidate_from_fields(fields: Dict[str, Any], regime_snapshot: Optional[Dict[str, Any]] = None) -> Optional[Candidate]:
    """Build a Candidate from an md:strike:select OK row (tolerates missing new fields)."""
    sig = str(fields.get("signal") or "").strip().upper()
    side = "CE" if sig == "BUY CALL" else "PE" if sig == "BUY PUT" else ""
    if not side:
        return None
    snap = regime_snapshot or {}
    regime = str(fields.get("regime") or snap.get("regime") or "NO_DATA").strip().upper() or "NO_DATA"
    cp, pp = _safe_int(snap.get("call_alloc_pct"), 0), _safe_int(snap.get("put_alloc_pct"), 0)
    side_pct = None if regime == "NO_DATA" or (cp <= 0 and pp <= 0) else (cp if side == "CE" else pp)

    top: List[Dict[str, Any]] = []
    raw = fields.get("top3")
    if raw:
        try:
            v = json.loads(raw) if isinstance(raw, str) else raw
            top = [x for x in v if isinstance(x, dict)] if isinstance(v, list) else []
        except Exception:
            top = []
    primary = {k: fields.get(k) for k in ("strike", "token", "tradingsymbol", "premium", "lot_size", "freeze_qty")}
    for d in top:  # enrich primary with its own top3 row (liquidity_score etc.)
        if primary.get("tradingsymbol") and d.get("tradingsymbol") == primary.get("tradingsymbol"):
            primary = {**d, **{k: v for k, v in primary.items() if v not in (None, "")}}
            break
    opts: List[StrikeOption] = []
    seen = set()
    for d in [primary] + top:
        o = _opt_from_dict(d, fields)
        if o is None:
            continue
        key = o.tradingsymbol or o.token or o.strike
        if key in seen:
            continue
        seen.add(key)
        opts.append(o)
    return Candidate(
        symbol=str(fields.get("symbol") or "").strip().upper(),
        side=side,
        strike_score=_f(fields.get("strike_score")) or 0.0,
        confidence=_f(fields.get("confidence")) or 0.0,
        regime=regime,
        side_alloc_pct=side_pct,
        options=tuple(opts),
        regime_class=str(fields.get("regime_class") or "").strip().upper(),
    )

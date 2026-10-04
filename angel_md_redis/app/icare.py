"""
app/icare.py
───────────────────────
Modules 11 + 20 — Intelligent Capital Allocation & Risk Engine (ICARE),
DECISION.md D9. The last decision layer before order execution: decides
WHETHER to trade, HOW MUCH, and whether it is worth the capital at risk.

  1. Trade Quality = 20% trend + 20% expected move + 20% probability
                   + 15% strike + 10% liquidity + 10% greeks + 5% bid-ask
  2. Risk class    >95 A+ (15%) | >=90 A (10%) | >=80 B (7%) | >=70 C (3%)
                   | <70 REJECT (0%).  A Probability decision of
                   SMALL_POSITION caps the allocation at class C's 3%.
  3. Stop loss     premium move for one EM AGAINST the trade (SIE adverse
                   repricing), clamped to [MIN_SL_PCT, MAX_SL_PCT] of premium.
     Target        premium + SIE projected gain for one EM in favour.
  4. EV per lot    p_win x avg_win - (1 - p_win) x avg_loss
                   journal stats once >= min_samples trades (ev_source=journal),
                   else the model: p = probability/100, win = projected gain,
                   loss = stop distance (ev_source=model). Only EV > 0 proceeds.
  5. Lots = MIN(margin, risk, capital-allocation, portfolio, liquidity) where
       margin    = available margin / (premium x lot x (1 + buffer))
       risk      = min(MAX_RISK_PER_TRADE, capital x MAX_RISK_PCT) / (SL pts x lot)
       capital   = capital x class% / (premium x lot)
       portfolio = min(MAX_LOTS_PER_TRADE, remaining portfolio-risk budget / risk per lot),
                   0 once MAX_OPEN_TRADES are open
       liquidity = Module 7 final_entry_size (lots), when published
  6. Portfolio protection rejects: margin utilization, daily loss,
     portfolio risk, open-trade count, one position per underlying, sector exposure (sector map optional — flagged if absent).
  7. Module 18 re-entries (DECISION.md §5 T10/T11): a 6th limit `reentry_cap`
     (never above the original trade's lots); rejected while Module 18 runs in
     shadow mode (`tsl_shadow_mode`); any entry for a (symbol, side) blocked
     after two failed re-entries is rejected (`reentry_blocked`).
  8. Module 13 (DECISION.md §6 R16): a Trade Ranking CONDITIONAL candidate
     (aggressive profile only) is capped at class C's 3%, like SMALL_POSITION.

Confidence never lifts risk above the per-trade / daily / portfolio caps
(design's closing rule): every cap is inside the MIN() or a hard reject.

Missing quality components count 0 (conservative for a capital decision).

No I/O here — Redis wiring lives in run_icare.py.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional

QUALITY_WEIGHTS = {
    "trend": 0.20,
    "expected_move": 0.20,
    "probability": 0.20,
    "strike": 0.15,
    "liquidity": 0.10,
    "greeks": 0.10,
    "bidask": 0.05,
}

ALLOWED_PROB_DECISIONS = frozenset({"SMALL_POSITION", "TRADE", "HIGH_CONVICTION"})
SMALL_POSITION_MAX_ALLOC_PCT = 3.0


@dataclass(frozen=True)
class ICAREConfig:
    max_risk_per_trade: float = 7500.0      # design example
    max_risk_pct: float = 2.0               # and never more than 2% of capital
    max_lots_per_trade: int = 10
    max_open_trades: int = 5
    max_margin_util_pct: float = 80.0
    daily_loss_limit_pct: float = 2.0
    max_portfolio_risk_pct: float = 5.0
    max_sector_exposure_pct: float = 30.0
    min_sl_pct: float = 10.0
    max_sl_pct: float = 30.0
    margin_buffer_pct: float = 1.0          # charges / slippage headroom on premium
    min_ev: float = 0.0
    min_history_samples: int = 30


@dataclass(frozen=True)
class ICAREInputs:
    symbol: str
    side: str
    tradingsymbol: str
    strike: float
    premium: Optional[float]
    lot_size: Optional[float]
    probability: Optional[float]
    probability_decision: str
    probability_reject_reasons: List[str] = field(default_factory=list)
    # Trade-quality components (0..100)
    trend: Optional[float] = None
    expected_move_score: Optional[float] = None
    strike_score: Optional[float] = None
    liquidity: Optional[float] = None
    greeks: Optional[float] = None
    bidask: Optional[float] = None
    # Per-unit premium projections from SIE
    projected_gain: Optional[float] = None
    adverse_change: Optional[float] = None
    # Journal history (bucket summary)
    history_samples: int = 0
    history_win_rate: Optional[float] = None
    history_avg_win_pct: Optional[float] = None
    history_avg_loss_pct: Optional[float] = None
    liquidity_max_lots: Optional[float] = None
    sector: str = ""
    # Module 18 re-entry / block
    reentry_max_lots: Optional[float] = None
    reentry_shadow: bool = False
    blocked: bool = False
    # Module 13: Trade Ranking CONDITIONAL (60-69) is capped like SMALL_POSITION
    rank_conditional: bool = False


@dataclass(frozen=True)
class PortfolioState:
    total_capital: float
    available_margin: float
    margin_utilization_pct: float
    day_pnl: float
    open_positions: int
    open_risk: float
    sector_exposure: Dict[str, float] = field(default_factory=dict)   # sector -> premium deployed
    open_symbols: frozenset = frozenset()                               # underlyings with an open position


@dataclass(frozen=True)
class ICAREResult:
    status: str                         # "APPROVED" / "REJECTED"
    symbol: str
    side: str
    tradingsymbol: str
    strike: float
    trade_quality: float
    quality_components: Dict[str, Optional[float]]
    risk_class: str
    allocation_pct: float
    max_capital: float
    premium: Optional[float]
    lot_size: Optional[float]
    stop_loss_premium: Optional[float]
    target_premium: Optional[float]
    sl_points: Optional[float]
    expected_value: Optional[float]     # per lot
    ev_source: str
    win_probability: Optional[float]
    margin_per_lot: Optional[float]
    risk_per_lot: Optional[float]
    max_risk_allowed: float
    lots_by_margin: Optional[int]
    lots_by_risk: Optional[int]
    lots_by_capital: Optional[int]
    lots_by_portfolio: Optional[int]
    lots_by_liquidity: Optional[int]
    recommended_lots: int
    limiting_factor: str
    margin_used: float
    expected_max_loss: float
    expected_reward: float
    reward_risk: Optional[float]
    reasons: List[str]
    flags: List[str]

    def to_dict(self) -> dict:
        return asdict(self)


# ── Steps ───────────────────────────────────────────────────────────────

def _clip(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def trade_quality(components: Dict[str, Optional[float]]) -> float:
    return round(sum(_clip(float(components.get(k) or 0.0)) * w for k, w in QUALITY_WEIGHTS.items()), 4)


def classify_risk(quality: float) -> tuple[str, float]:
    if quality > 95:
        return "A+", 15.0
    if quality >= 90:
        return "A", 10.0
    if quality >= 80:
        return "B", 7.0
    if quality >= 70:
        return "C", 3.0
    return "REJECT", 0.0


def stop_loss_points(premium: float, adverse_change: Optional[float], cfg: ICAREConfig) -> float:
    lo = premium * cfg.min_sl_pct / 100.0
    hi = premium * cfg.max_sl_pct / 100.0
    if adverse_change is None:
        return round(hi, 4)
    return round(max(lo, min(hi, -adverse_change)), 4)


def expected_value(
    inp: ICAREInputs, premium: float, lot: float, sl_pts: float, gain: float, cfg: ICAREConfig
) -> tuple[Optional[float], str, Optional[float]]:
    """(EV per lot, source, win probability used)."""
    if (
        inp.history_samples >= cfg.min_history_samples
        and inp.history_win_rate is not None
        and inp.history_avg_win_pct is not None
        and inp.history_avg_loss_pct is not None
    ):
        p = inp.history_win_rate
        win = inp.history_avg_win_pct / 100.0 * premium * lot
        loss = inp.history_avg_loss_pct / 100.0 * premium * lot
        source = "journal"
    elif inp.probability is not None:
        p = _clip(inp.probability) / 100.0
        win = gain * lot
        loss = sl_pts * lot
        source = "model"
    else:
        return None, "none", None
    return round(p * win - (1.0 - p) * loss, 2), source, round(p, 4)


def _floor_lots(numer: float, per_lot: float) -> int:
    if per_lot <= 0:
        return 0
    return max(int(math.floor(numer / per_lot + 1e-9)), 0)


def margin_per_lot(premium: float, lot: float, cfg: ICAREConfig) -> float:
    return round(premium * lot * (1.0 + cfg.margin_buffer_pct / 100.0), 2)


def max_risk_allowed(capital: float, cfg: ICAREConfig) -> float:
    return round(min(cfg.max_risk_per_trade, max(capital, 0.0) * cfg.max_risk_pct / 100.0), 2)


def presize_lots(
    premium: float, lot: float, sl_pts: float, available_margin: float, capital: float,
    liquidity_max_lots: Optional[float], cfg: ICAREConfig,
) -> Dict[str, int]:
    """
    Lot limits that need no trade-quality class or portfolio state (margin,
    risk, per-trade cap, liquidity) — the Trade Ranking pre-sizing check
    (DECISION.md §6 R10). ICARE's evaluate() applies the same formulas plus
    the capital-allocation and portfolio limits.
    """
    limits = {
        "margin": _floor_lots(available_margin, margin_per_lot(premium, lot, cfg)),
        "risk": _floor_lots(max_risk_allowed(capital, cfg), round(sl_pts * lot, 2)),
        "max_lots_per_trade": cfg.max_lots_per_trade,
    }
    if liquidity_max_lots is not None:
        limits["liquidity"] = max(int(math.floor(liquidity_max_lots)), 0)
    return limits


def evaluate(inp: ICAREInputs, pf: PortfolioState, cfg: Optional[ICAREConfig] = None) -> ICAREResult:
    cfg = cfg or ICAREConfig()
    reasons: List[str] = []
    flags: List[str] = []

    comps = {
        "trend": inp.trend,
        "expected_move": inp.expected_move_score,
        "probability": inp.probability,
        "strike": inp.strike_score,
        "liquidity": inp.liquidity,
        "greeks": inp.greeks,
        "bidask": inp.bidask,
    }
    quality = trade_quality(comps)
    risk_class, alloc_pct = classify_risk(quality)
    decision = (inp.probability_decision or "").strip().upper()
    if decision == "SMALL_POSITION" and alloc_pct > SMALL_POSITION_MAX_ALLOC_PCT:
        alloc_pct = SMALL_POSITION_MAX_ALLOC_PCT
        flags.append("SMALL_POSITION_CAP")
    if inp.rank_conditional and alloc_pct > SMALL_POSITION_MAX_ALLOC_PCT:
        alloc_pct = SMALL_POSITION_MAX_ALLOC_PCT
        flags.append("CONDITIONAL_CAP")

    capital = max(pf.total_capital, 0.0)
    max_capital = round(capital * alloc_pct / 100.0, 2)
    max_risk = max_risk_allowed(capital, cfg)

    # ── Gates that need no sizing ──
    if decision not in ALLOWED_PROB_DECISIONS:
        reasons.append(f"probability_{decision.lower() or 'missing'}")
        reasons.extend(f"prob:{r}" for r in inp.probability_reject_reasons)
    if risk_class == "REJECT":
        reasons.append("trade_quality_below_70")

    premium, lot = inp.premium, inp.lot_size
    if not premium or premium <= 0:
        reasons.append("no_premium")
    if not lot or lot <= 0:
        reasons.append("no_lot_size")
    gain = inp.projected_gain
    if gain is None or gain <= 0:
        reasons.append("no_projected_gain")

    # ── Portfolio protection ──
    if pf.margin_utilization_pct > cfg.max_margin_util_pct:
        reasons.append("margin_utilization_exceeded")
    if capital > 0 and pf.day_pnl < -capital * cfg.daily_loss_limit_pct / 100.0:
        reasons.append("daily_loss_limit_hit")
    portfolio_risk_cap = capital * cfg.max_portfolio_risk_pct / 100.0
    if pf.open_risk >= portfolio_risk_cap:
        reasons.append("portfolio_risk_exceeded")
    if pf.open_positions >= cfg.max_open_trades:
        reasons.append("max_open_trades")
    if inp.symbol.upper() in pf.open_symbols:
        reasons.append("position_already_open")
    if inp.blocked:
        reasons.append("reentry_blocked")
    if inp.reentry_shadow:
        reasons.append("tsl_shadow_mode")

    sl_pts = stop = target = margin_lot = risk_per_lot = ev = p_win = None
    ev_source = "none"
    lots_margin = lots_risk = lots_capital = lots_portfolio = lots_liq = None
    final = 0
    limiting = ""

    if premium and premium > 0 and lot and lot > 0:
        sl_pts = stop_loss_points(premium, inp.adverse_change, cfg)
        stop = round(premium - sl_pts, 4)
        cost_per_lot = premium * lot
        margin_lot = margin_per_lot(premium, lot, cfg)
        risk_per_lot = round(sl_pts * lot, 2)

        if gain is not None and gain > 0:
            target = round(premium + gain, 4)
            ev, ev_source, p_win = expected_value(inp, premium, lot, sl_pts, gain, cfg)
            if ev is None or ev <= cfg.min_ev:
                reasons.append("expected_value_not_positive")

        # Sector exposure (premium deployed per sector vs capital)
        if inp.sector:
            deployed = pf.sector_exposure.get(inp.sector, 0.0) + cost_per_lot
            if capital > 0 and deployed / capital * 100.0 > cfg.max_sector_exposure_pct:
                reasons.append("sector_exposure_exceeded")
        else:
            flags.append("SECTOR_UNCHECKED")

        lots_margin = _floor_lots(pf.available_margin, margin_lot)
        lots_risk = _floor_lots(max_risk, risk_per_lot)
        lots_capital = _floor_lots(max_capital, cost_per_lot)
        budget = max(portfolio_risk_cap - pf.open_risk, 0.0)
        lots_portfolio = 0 if pf.open_positions >= cfg.max_open_trades else min(
            cfg.max_lots_per_trade, _floor_lots(budget, risk_per_lot)
        )
        limits = {
            "margin": lots_margin,
            "risk": lots_risk,
            "capital_allocation": lots_capital,
            "portfolio": lots_portfolio,
        }
        if inp.liquidity_max_lots is not None:
            lots_liq = max(int(math.floor(inp.liquidity_max_lots)), 0)
            limits["liquidity"] = lots_liq
        if inp.reentry_max_lots is not None:
            limits["reentry_cap"] = max(int(math.floor(inp.reentry_max_lots)), 0)
        limiting = min(limits, key=lambda k: limits[k])
        final = limits[limiting]
        if final <= 0:
            reasons.append(f"zero_lots_by_{limiting}")

    approved = not reasons
    lots = final if approved else 0
    reward_risk = round(gain / sl_pts, 4) if gain and sl_pts else None

    return ICAREResult(
        status="APPROVED" if approved else "REJECTED",
        symbol=inp.symbol,
        side=inp.side,
        tradingsymbol=inp.tradingsymbol,
        strike=inp.strike,
        trade_quality=quality,
        quality_components=comps,
        risk_class=risk_class,
        allocation_pct=alloc_pct,
        max_capital=max_capital,
        premium=premium,
        lot_size=lot,
        stop_loss_premium=stop,
        target_premium=target,
        sl_points=sl_pts,
        expected_value=ev,
        ev_source=ev_source,
        win_probability=p_win,
        margin_per_lot=margin_lot,
        risk_per_lot=risk_per_lot,
        max_risk_allowed=max_risk,
        lots_by_margin=lots_margin,
        lots_by_risk=lots_risk,
        lots_by_capital=lots_capital,
        lots_by_portfolio=lots_portfolio,
        lots_by_liquidity=lots_liq,
        recommended_lots=lots,
        limiting_factor=limiting,
        margin_used=round(lots * (margin_lot or 0.0), 2),
        expected_max_loss=round(lots * (risk_per_lot or 0.0), 2),
        expected_reward=round(lots * (gain or 0.0) * (lot or 0.0), 2),
        reward_risk=reward_risk,
        reasons=reasons,
        flags=flags,
    )

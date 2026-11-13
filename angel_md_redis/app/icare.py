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
  4. EV per lot    gross = p_win x avg_win - (1 - p_win) x avg_loss
                   journal stats once >= min_samples trades (ev_source=journal);
                   a bucket with 0 wins or 0 losses (no avg win / loss) uses the
                   empirical win rate with the model payoff sizes
                   (ev_source=journal_rate); else the model: p = probability/100,
                   win = projected gain, loss = stop distance (ev_source=model).
                   net  = gross - expected round-trip charges per lot
                   (charges.json via app/order_executor/charges.py: buy at the
                   premium, sell at target with p_win / at stop with 1 - p_win,
                   one order per leg, for the sized lot count).
                   Only net EV > min_ev proceeds (`expected_value` is NET;
                   `gross_ev`, `charges`, `charges_per_lot` are reported).
  5. Lots = MIN(margin, risk, capital-allocation, portfolio, liquidity) where
       margin    = available margin / (premium x lot x (1 + buffer))
       risk      = min(MAX_RISK_PER_TRADE, capital x MAX_RISK_PCT) / (SL pts x lot)
       capital   = capital x class% / (premium x lot)
       portfolio = min(MAX_LOTS_PER_TRADE, remaining portfolio-risk budget / risk per lot),
                   0 once MAX_OPEN_TRADES are open
       liquidity = Module 7 final_entry_size (lots), when published
       daily_loss_room = (daily loss limit - today's loss) / risk per lot, so a
                   stop-out cannot take the day past the limit
       sector_exposure = (capital x max sector % - sector premium deployed) / cost per lot
  6. Portfolio protection rejects: kill switch (md:control:kill_switch), margin
     utilization, daily loss (day_pnl <= -limit, same boundary as Trade Ranking;
     an unknown day PnL fails closed), portfolio risk, open-trade count, one
     position per underlying, sector exposure (sector map optional — flagged if absent).
  7. Module 18 re-entries (DECISION.md §5 T10/T11): a 6th limit `reentry_cap`
     (never above the original trade's lots); rejected while Module 18 runs in
     shadow mode (`tsl_shadow_mode`); any entry for a (symbol, side) blocked
     after two failed re-entries is rejected (`reentry_blocked`).
     A stock in NSE's F&O ban period (md:fo:ban) is rejected (`fo_ban_period`).
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

from app.order_executor.charges import ChargeRates, load_rates
from app.order_executor.charges import charges as leg_charges

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
    include_charges: bool = True            # net EV = gross - round-trip charges
    charge_rates: ChargeRates = field(default_factory=load_rates)


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
    # NSE F&O ban period (md:fo:ban, run_fo_universe.py): no fresh positions
    fo_banned: bool = False
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
    day_pnl_known: bool = True          # False -> realized PnL unknown: fail closed (daily_loss_unknown)
    kill_switch: bool = False           # md:control:kill_switch == "1"


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
    gross_ev: Optional[float] = None            # per lot, before charges
    charges: Optional[float] = None             # expected round-trip charges, whole position
    charges_per_lot: Optional[float] = None
    lots_by_sector: Optional[int] = None
    lots_by_daily_loss: Optional[int] = None
    daily_loss_room: Optional[float] = None

    def to_dict(self) -> dict:
        return asdict(self)


# ── Steps ───────────────────────────────────────────────────────────────

def _finite(v) -> Optional[float]:
    """float(v), or None when missing / unparseable / NaN / inf (NaN is missing, never 100)."""
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _clip(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return lo
    return max(lo, min(hi, v))


def trade_quality(components: Dict[str, Optional[float]]) -> float:
    return round(sum(_clip(_finite(components.get(k)) or 0.0) * w for k, w in QUALITY_WEIGHTS.items()), 4)


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


def _ev_terms(
    inp: ICAREInputs, premium: float, sl_pts: float, gain: Optional[float], cfg: ICAREConfig
) -> Optional[tuple[float, float, float, str]]:
    """(p_win, win per unit, loss per unit, source) or None when nothing is known."""
    wr = _finite(inp.history_win_rate)
    aw, al = _finite(inp.history_avg_win_pct), _finite(inp.history_avg_loss_pct)
    enough = inp.history_samples >= cfg.min_history_samples and wr is not None
    if enough and aw is not None and al is not None:
        return min(max(wr, 0.0), 1.0), aw / 100.0 * premium, al / 100.0 * premium, "journal"
    if enough and gain is not None:
        # 0 wins or 0 losses in the bucket (no avg win / loss): the empirical
        # rate is still the evidence; payoff sizes come from the model.
        return min(max(wr, 0.0), 1.0), gain, sl_pts, "journal_rate"
    prob = _finite(inp.probability)
    if prob is not None and gain is not None:
        return _clip(prob) / 100.0, gain, sl_pts, "model"
    return None


def expected_value(
    inp: ICAREInputs, premium: float, lot: float, sl_pts: float, gain: float, cfg: ICAREConfig
) -> tuple[Optional[float], str, Optional[float]]:
    """(GROSS EV per lot — before charges, source, win probability used). See ev_breakdown for net."""
    t = _ev_terms(inp, premium, sl_pts, gain, cfg)
    if t is None:
        return None, "none", None
    p, win, loss, source = t
    return round(p * win * lot - (1.0 - p) * loss * lot, 2), source, round(p, 4)


def round_trip_charges(
    premium: float, lot: float, lots: int, p_win: float, win_pts: float, loss_pts: float, rates: ChargeRates,
) -> float:
    """
    Expected brokerage + statutory charges (Rs) for `lots` lots: buy at the
    premium, sell at premium + win (prob p_win) or premium - loss (1 - p_win),
    one order per leg.
    """
    qty = lot * max(int(lots), 1)
    buy = leg_charges("BUY", premium * qty, 1, rates)["total"]
    sell_win = leg_charges("SELL", (premium + win_pts) * qty, 1, rates)["total"]
    sell_loss = leg_charges("SELL", max(premium - loss_pts, 0.0) * qty, 1, rates)["total"]
    return round(buy + p_win * sell_win + (1.0 - p_win) * sell_loss, 2)


def ev_breakdown(
    inp: ICAREInputs, premium: float, lot: float, sl_pts: float, gain: Optional[float], cfg: ICAREConfig,
    lots: int = 1,
) -> Optional[Dict[str, object]]:
    """
    {gross_ev, charges, charges_per_lot, net_ev, source, p_win} for `lots` lots
    (gross / net per lot; charges for the whole position). None when unknown.
    """
    t = _ev_terms(inp, premium, sl_pts, gain, cfg)
    if t is None:
        return None
    p, win, loss, source = t
    n = max(int(lots), 1)
    gross = round(p * win * lot - (1.0 - p) * loss * lot, 2)
    total = round_trip_charges(premium, lot, n, p, win, loss, cfg.charge_rates) if cfg.include_charges else 0.0
    per_lot = round(total / n, 2)
    return {
        "gross_ev": gross,
        "charges": total,
        "charges_per_lot": per_lot,
        "net_ev": round(gross - total / n, 2),
        "source": source,
        "p_win": round(p, 4),
    }


def daily_loss_room(capital: float, day_pnl: float, limit_pct: float) -> float:
    """Loss still allowed today: limit - today's loss (profits never extend it), >= 0."""
    limit = max(capital, 0.0) * limit_pct / 100.0
    return round(max(limit - max(-(day_pnl or 0.0), 0.0), 0.0), 2)


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
    liquidity_max_lots: Optional[float], cfg: ICAREConfig, risk_room: Optional[float] = None,
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
    if risk_room is not None:
        limits["daily_loss_room"] = _floor_lots(risk_room, round(sl_pts * lot, 2))
    return limits


def evaluate(inp: ICAREInputs, pf: PortfolioState, cfg: Optional[ICAREConfig] = None) -> ICAREResult:
    cfg = cfg or ICAREConfig()
    reasons: List[str] = []
    flags: List[str] = []

    probability = _finite(inp.probability)
    comps = {
        "trend": _finite(inp.trend),
        "expected_move": _finite(inp.expected_move_score),
        "probability": probability,
        "strike": _finite(inp.strike_score),
        "liquidity": _finite(inp.liquidity),
        "greeks": _finite(inp.greeks),
        "bidask": _finite(inp.bidask),
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

    capital = max(_finite(pf.total_capital) or 0.0, 0.0)
    max_capital = round(capital * alloc_pct / 100.0, 2)
    max_risk = max_risk_allowed(capital, cfg)

    # ── Gates that need no sizing ──
    if decision not in ALLOWED_PROB_DECISIONS:
        reasons.append(f"probability_{decision.lower() or 'missing'}")
        reasons.extend(f"prob:{r}" for r in inp.probability_reject_reasons)
    if risk_class == "REJECT":
        reasons.append("trade_quality_below_70")

    premium, lot = _finite(inp.premium), _finite(inp.lot_size)
    if not premium or premium <= 0:
        reasons.append("no_premium")
    if not lot or lot <= 0:
        reasons.append("no_lot_size")
    gain = _finite(inp.projected_gain)
    if gain is None or gain <= 0:
        reasons.append("no_projected_gain")

    # ── Portfolio protection ──
    if pf.kill_switch:
        reasons.append("kill_switch_active")
    if pf.margin_utilization_pct > cfg.max_margin_util_pct:
        reasons.append("margin_utilization_exceeded")
    day_room = daily_loss_room(capital, _finite(pf.day_pnl) or 0.0, cfg.daily_loss_limit_pct)
    if not pf.day_pnl_known:
        reasons.append("daily_loss_unknown")
    elif capital > 0 and day_room <= 0:          # day_pnl <= -limit (same boundary as Trade Ranking)
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
    if inp.fo_banned:
        reasons.append("fo_ban_period")
    if inp.reentry_shadow:
        reasons.append("tsl_shadow_mode")

    sl_pts = stop = target = margin_lot = risk_per_lot = ev = p_win = None
    gross_ev = charges_total = charges_lot = None
    ev_source = "none"
    lots_margin = lots_risk = lots_capital = lots_portfolio = lots_liq = lots_sector = lots_daily = None
    final = 0
    limiting = ""

    if premium and premium > 0 and lot and lot > 0:
        sl_pts = stop_loss_points(premium, _finite(inp.adverse_change), cfg)
        stop = round(premium - sl_pts, 4)
        cost_per_lot = premium * lot
        margin_lot = margin_per_lot(premium, lot, cfg)
        risk_per_lot = round(sl_pts * lot, 2)

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
        if capital > 0:
            # A stop-out of the whole position must not take the day past the limit.
            lots_daily = _floor_lots(day_room, risk_per_lot)
            limits["daily_loss_room"] = lots_daily
        if inp.liquidity_max_lots is not None:
            lots_liq = max(int(math.floor(inp.liquidity_max_lots)), 0)
            limits["liquidity"] = lots_liq
        if inp.reentry_max_lots is not None:
            limits["reentry_cap"] = max(int(math.floor(inp.reentry_max_lots)), 0)

        # Sector exposure: existing premium deployed + lots x cost must stay <= cap.
        if inp.sector:
            if capital > 0:
                room = capital * cfg.max_sector_exposure_pct / 100.0 - pf.sector_exposure.get(inp.sector, 0.0)
                lots_sector = _floor_lots(max(room, 0.0), cost_per_lot)
                limits["sector_exposure"] = lots_sector
                if lots_sector <= 0:
                    reasons.append("sector_exposure_exceeded")
        else:
            flags.append("SECTOR_UNCHECKED")

        limiting = min(limits, key=lambda k: limits[k])
        final = limits[limiting]
        if final <= 0 and not (limiting == "sector_exposure" or (limiting == "daily_loss_room" and day_room <= 0)):
            reasons.append(f"zero_lots_by_{limiting}")

        if gain is not None and gain > 0:
            target = round(premium + gain, 4)
            # Charges are for the lots actually sized (1 when sizing gave 0, for the report).
            bd = ev_breakdown(inp, premium, lot, sl_pts, gain, cfg, lots=final if final > 0 else 1)
            if bd is not None:
                ev, ev_source, p_win = bd["net_ev"], bd["source"], bd["p_win"]
                gross_ev, charges_total, charges_lot = bd["gross_ev"], bd["charges"], bd["charges_per_lot"]
            if ev is None or ev <= cfg.min_ev:
                reasons.append("expected_value_not_positive")

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
        gross_ev=gross_ev,
        charges=charges_total,
        charges_per_lot=charges_lot,
        lots_by_sector=lots_sector,
        lots_by_daily_loss=lots_daily,
        daily_loss_room=day_room,
    )

"""
app/strike_intel.py
───────────────────────
Module 10 — Strike Intelligence Engine (SIE), DECISION.md D5/D6.

Sits between the Expected Move Engine (Module 8) and execution. Does not
predict the future: it RANKS the candidate strikes (ATM ± N, trade side
only) by how well each trades off directional exposure, decay, liquidity
and execution quality, and returns the top 3.

  strike_score = 25% liquidity        (liquidity_score 0-100, reject < 70)
               + 25% expected-move fit (inside spot ± EM = 100, linear to 0
                                        one further EM beyond the range)
               + 20% delta suitability (100 inside the phase's |delta| band,
                                        linear to 0 at DELTA_DECAY_WIDTH out)
               + 15% theta efficiency  (theta over the hold as % of premium)
               + 10% gamma opportunity (gamma / chain max; inverted if choppy)
               +  5% vega stability    (vega per vol point as % of premium,
                                        doubled penalty if IV is falling)

Missing components are excluded and the score is renormalized over the
weight that IS known; `confidence = strike_score x completeness` so a
strike scored on partial data never looks as certain as a fully-scored one.

Market phase -> preferred |delta| (first match):
  EXPIRY_DAY      today == expiry                         0.55-0.70
  HIGH_VOLATILITY iv_trend up or |EM %| >= 2%             0.35-0.50
  STRONG_TREND    EM STRONG_* with side + HTF/ST aligned  0.60-0.75
  NORMAL_TREND    otherwise                               0.45-0.60

Greeks change (design Module 4): every candidate is repriced with
app/option_pricing.py's BSM at spot moved one EM in the trade direction
after `hold_minutes`, IV unchanged (plus an IV -2 points stress). The
premium gain is the MODEL difference (projected - current model price)
added to the observed premium, which cancels model-vs-market level bias.
It is an approximation: no skew, no higher-order IV dynamics.

No I/O here — Redis wiring lives in run_strike_intel.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Dict, List, Optional, Sequence, Tuple

from .option_pricing import bs_price_greeks

# ── Weights (design "Strike Scoring Model") ─────────────────────────────
WEIGHTS = {
    "liquidity": 0.25,
    "em_fit": 0.25,
    "delta": 0.20,
    "theta": 0.15,
    "gamma": 0.10,
    "vega": 0.05,
}
GREEK_COMPONENTS = ("delta", "theta", "gamma", "vega")

# ── Hard rejects ────────────────────────────────────────────────────────
MIN_LIQUIDITY = 70.0
POOR_LIQUIDITY_BANDS = frozenset({"RED", "POOR"})
MAX_SPREAD_PCT = 3.0             # same default as probability_engine

# ── Component shapes ────────────────────────────────────────────────────
DELTA_DECAY_WIDTH = 0.25         # |delta| this far outside the band -> 0
THETA_RISK_PCT_ZERO = 10.0       # losing 10% of premium to decay over the hold -> 0
VEGA_PCT_ZERO = 10.0             # 10% premium move per vol point -> 0
IV_FALLING_VEGA_PENALTY = 2.0
# TIME CONVENTION (QA fix): option_pricing's BSM T is CALENDAR time and its
# theta_per_day is per CALENDAR day (annual theta / 365). Both the theta
# decay estimate and the repricing horizon therefore use calendar minutes:
# a 60-minute hold costs theta_per_day * 60/1440. (Previously decay used
# 375 trading minutes/day, overstating hold decay 3.84x vs the repricer.)
MINUTES_PER_DAY_CALENDAR = 24.0 * 60.0
MINUTES_PER_YEAR_CALENDAR = 365.0 * MINUTES_PER_DAY_CALENDAR  # option_pricing's T convention
IV_STRESS_POINTS = 2.0

# ── Execution quality (spread + top-of-book depth) ──────────────────────
EXEC_SPREAD_WEIGHT = 0.7
EXEC_DEPTH_WEIGHT = 0.3
EXEC_DEPTH_FULL_LOTS = 20.0      # depth worth 20 lots = full depth score

# ── Market phase ────────────────────────────────────────────────────────
HIGH_VOL_EM_PCT = 2.0
DELTA_BANDS: Dict[str, Tuple[float, float]] = {
    "EXPIRY_DAY": (0.55, 0.70),
    "HIGH_VOLATILITY": (0.35, 0.50),
    "STRONG_TREND": (0.60, 0.75),
    "NORMAL_TREND": (0.45, 0.60),
}

TOP_N = 3
DEFAULT_HOLD_MINUTES = 60.0

# Reason-checklist thresholds
REASON_THETA_OK = 70.0
REASON_LIQUIDITY_HIGH = 85.0

_BULLISH = frozenset({"BULLISH", "STRONG_BULLISH"})
_BEARISH = frozenset({"BEARISH", "STRONG_BEARISH"})


def _clip(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


# ── Inputs ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class StrikeCandidate:
    tradingsymbol: str
    strike: float
    cp: str                                   # "CE" / "PE"
    token: str = ""
    premium: Optional[float] = None           # observed mid
    bid: Optional[float] = None
    ask: Optional[float] = None
    spread_pct: Optional[float] = None        # (ask-bid)/mid*100
    depth: Optional[float] = None             # top-5 bid qty (shares)
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta_per_day: Optional[float] = None
    vega_per_point: Optional[float] = None
    iv: Optional[float] = None                # annualized decimal (0.28)
    greeks_source: str = ""
    liquidity_score: Optional[float] = None
    liquidity_band: str = ""
    lot_size: Optional[float] = None


@dataclass(frozen=True)
class SIEContext:
    symbol: str
    side: str                                 # "CE" / "PE"
    spot: float
    expected_move: Optional[float]            # absolute points (Module 8 final_expected_move)
    em_direction: str = ""                    # STRONG_BULLISH / ... / NEUTRAL
    em_pct: Optional[float] = None            # |expected move| as % of spot
    iv_trend: str = ""                        # "up" / "down" / "flat"
    trend_aligned: Optional[bool] = None      # HTF + Supertrend agree with side
    time_to_expiry_years: Optional[float] = None
    expiry_iso: str = ""
    today_iso: str = ""
    hold_minutes: float = DEFAULT_HOLD_MINUTES
    risk_free_rate: float = 0.0
    dividend_or_carry: float = 0.0


@dataclass(frozen=True)
class SIEConfig:
    min_liquidity: float = MIN_LIQUIDITY
    max_spread_pct: float = MAX_SPREAD_PCT
    top_n: int = TOP_N


# ── Market phase / delta band ───────────────────────────────────────────

def em_agrees_with_side(em_direction: str, side: str) -> Optional[bool]:
    d = (em_direction or "").strip().upper()
    if d in _BULLISH:
        return side == "CE"
    if d in _BEARISH:
        return side == "PE"
    return None


def classify_market_phase(ctx: SIEContext) -> Tuple[str, Tuple[float, float]]:
    if ctx.expiry_iso and ctx.today_iso and ctx.expiry_iso == ctx.today_iso:
        phase = "EXPIRY_DAY"
    elif (ctx.iv_trend or "").lower() == "up" or (
        ctx.em_pct is not None and abs(ctx.em_pct) >= HIGH_VOL_EM_PCT
    ):
        phase = "HIGH_VOLATILITY"
    elif (
        (ctx.em_direction or "").upper().startswith("STRONG_")
        and em_agrees_with_side(ctx.em_direction, ctx.side)
        and ctx.trend_aligned
    ):
        phase = "STRONG_TREND"
    else:
        phase = "NORMAL_TREND"
    return phase, DELTA_BANDS[phase]


def is_choppy(ctx: SIEContext) -> bool:
    return (ctx.em_direction or "").strip().upper() in ("", "NEUTRAL")


# ── Window ──────────────────────────────────────────────────────────────

def select_window(
    candidates: Sequence[StrikeCandidate], side: str, spot: float, around: int
) -> List[StrikeCandidate]:
    """Trade-side candidates, deduped by strike, ATM ± `around`."""
    by_strike: Dict[float, StrikeCandidate] = {}
    for c in candidates:
        if (c.cp or "").upper() == side and c.strike not in by_strike:
            by_strike[c.strike] = c
    strikes = sorted(by_strike)
    if not strikes:
        return []
    atm_i = min(range(len(strikes)), key=lambda i: abs(strikes[i] - spot))
    lo, hi = max(0, atm_i - around), atm_i + around + 1
    return [by_strike[s] for s in strikes[lo:hi]]


# ── Components (each 0..100, None = missing) ────────────────────────────

def intrinsic_extrinsic(spot: float, strike: float, cp: str, premium: Optional[float]) -> Tuple[float, Optional[float]]:
    intrinsic = max(spot - strike, 0.0) if cp == "CE" else max(strike - spot, 0.0)
    if premium is None:
        return round(intrinsic, 4), None
    return round(intrinsic, 4), round(max(premium - intrinsic, 0.0), 4)


def expected_range(spot: float, expected_move: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    if expected_move is None or expected_move <= 0:
        return None, None
    em = abs(expected_move)
    return round(spot - em, 4), round(spot + em, 4)


def em_fit_score(strike: float, spot: float, expected_move: Optional[float]) -> Optional[float]:
    lower, upper = expected_range(spot, expected_move)
    if lower is None:
        return None
    if lower <= strike <= upper:
        return 100.0
    beyond = (lower - strike) if strike < lower else (strike - upper)
    return round(_clip(100.0 * (1.0 - beyond / abs(expected_move))), 4)


def delta_suitability_score(delta: Optional[float], band: Tuple[float, float]) -> Optional[float]:
    if delta is None:
        return None
    d = abs(delta)
    lo, hi = band
    if lo <= d <= hi:
        return 100.0
    dist = (lo - d) if d < lo else (d - hi)
    return round(_clip(100.0 * (1.0 - dist / DELTA_DECAY_WIDTH)), 4)


def theta_risk(theta_per_day: Optional[float], premium: Optional[float], hold_minutes: float) -> Tuple[Optional[float], Optional[float]]:
    """
    (premium points lost to decay over the hold, same as % of premium).
    theta_per_day is per CALENDAR day (BSM / Angel convention), so the hold
    is converted with calendar minutes (1440/day), matching project_greeks.
    """
    if theta_per_day is None:
        return None, None
    pts = abs(theta_per_day) * max(hold_minutes, 0.0) / MINUTES_PER_DAY_CALENDAR
    pct = (pts / premium * 100.0) if premium and premium > 0 else None
    return round(pts, 4), (None if pct is None else round(pct, 4))


def theta_efficiency_score(theta_risk_pct: Optional[float]) -> Optional[float]:
    if theta_risk_pct is None:
        return None
    return round(_clip(100.0 * (1.0 - theta_risk_pct / THETA_RISK_PCT_ZERO)), 4)


def gamma_opportunity_score(gamma: Optional[float], max_gamma: Optional[float], choppy: bool) -> Optional[float]:
    if gamma is None or not max_gamma or max_gamma <= 0:
        return None
    s = _clip(100.0 * abs(gamma) / max_gamma)
    return round(100.0 - s if choppy else s, 4)


def vega_stability_score(vega_per_point: Optional[float], premium: Optional[float], iv_trend: str) -> Optional[float]:
    if vega_per_point is None or not premium or premium <= 0:
        return None
    pct = abs(vega_per_point) / premium * 100.0
    if (iv_trend or "").lower() == "down":
        pct *= IV_FALLING_VEGA_PENALTY
    return round(_clip(100.0 * (1.0 - pct / VEGA_PCT_ZERO)), 4)


def execution_quality_score(spread_pct: Optional[float], depth: Optional[float], lot_size: Optional[float], max_spread_pct: float = MAX_SPREAD_PCT) -> Optional[float]:
    parts: List[Tuple[float, float]] = []
    if spread_pct is not None and max_spread_pct > 0:
        parts.append((EXEC_SPREAD_WEIGHT, _clip(100.0 * (1.0 - spread_pct / max_spread_pct))))
    if depth is not None and lot_size and lot_size > 0:
        parts.append((EXEC_DEPTH_WEIGHT, _clip(100.0 * (depth / lot_size) / EXEC_DEPTH_FULL_LOTS)))
    if not parts:
        return None
    w = sum(p[0] for p in parts)
    return round(sum(p[0] * p[1] for p in parts) / w, 4)


def weighted_score(components: Dict[str, Optional[float]], weights: Dict[str, float] = WEIGHTS) -> Tuple[float, float]:
    """(score renormalized over known weight, completeness 0..1)."""
    known = {k: v for k, v in components.items() if k in weights and v is not None}
    w_known = sum(weights[k] for k in known)
    if w_known <= 0:
        return 0.0, 0.0
    score = sum(weights[k] * _clip(v) for k, v in known.items()) / w_known
    return round(score, 4), round(w_known / sum(weights.values()), 4)


# ── Greeks change projection ────────────────────────────────────────────

@dataclass(frozen=True)
class GreeksProjection:
    target_spot: float
    delta: float
    gamma: float
    theta_per_day: float
    premium: float
    premium_gain: float              # rupees per unit (per share)
    premium_gain_pct: Optional[float]
    premium_gain_iv_down: float      # same, with IV -IV_STRESS_POINTS
    premium_change_adverse: float    # spot one EM AGAINST the trade after the hold (<= 0 normally)


def project_greeks(c: StrikeCandidate, ctx: SIEContext) -> Optional[GreeksProjection]:
    if (
        c.iv is None or c.iv <= 0
        or ctx.time_to_expiry_years is None
        or ctx.expected_move is None or ctx.expected_move <= 0
    ):
        return None
    direction = 1.0 if ctx.side == "CE" else -1.0
    target = ctx.spot + direction * abs(ctx.expected_move)
    t_now = ctx.time_to_expiry_years
    t_then = t_now - max(ctx.hold_minutes, 0.0) / MINUTES_PER_YEAR_CALENDAR
    r, q = ctx.risk_free_rate, ctx.dividend_or_carry

    now = bs_price_greeks(ctx.spot, c.strike, c.cp, t_now, c.iv, r, q)
    then = bs_price_greeks(target, c.strike, c.cp, t_then, c.iv, r, q)
    stress = bs_price_greeks(target, c.strike, c.cp, t_then, max(c.iv - IV_STRESS_POINTS / 100.0, 1e-6), r, q)
    adverse = bs_price_greeks(ctx.spot - direction * abs(ctx.expected_move), c.strike, c.cp, t_then, c.iv, r, q)

    gain = then.premium - now.premium
    gain_iv_down = stress.premium - now.premium
    base = c.premium if c.premium and c.premium > 0 else now.premium
    projected_premium = base + gain
    return GreeksProjection(
        target_spot=round(target, 4),
        delta=then.delta,
        gamma=then.gamma,
        theta_per_day=then.theta_per_day,
        premium=round(projected_premium, 4),
        premium_gain=round(gain, 4),
        premium_gain_pct=round(gain / base * 100.0, 4) if base > 0 else None,
        premium_gain_iv_down=round(gain_iv_down, 4),
        premium_change_adverse=round(adverse.premium - now.premium, 4),
    )


# ── Result types ────────────────────────────────────────────────────────

@dataclass(frozen=True)
class RankedStrike:
    tradingsymbol: str
    token: str
    strike: float
    cp: str
    status: str                       # "OK" / "REJECTED"
    reject_reasons: List[str]
    rank: int
    strike_score: float
    confidence: float
    completeness: float
    components: Dict[str, Optional[float]]
    greeks_score: Optional[float]     # greek-only sub-score (Probability Engine "greeks")
    execution_quality: Optional[float]
    premium: Optional[float]
    intrinsic: float
    extrinsic: Optional[float]
    delta: Optional[float]
    gamma: Optional[float]
    theta_per_day: Optional[float]
    vega_per_point: Optional[float]
    iv: Optional[float]
    greeks_source: str
    theta_risk_pts: Optional[float]
    theta_risk_pct: Optional[float]
    liquidity_score: Optional[float]
    liquidity_band: str
    spread_pct: Optional[float]
    lot_size: Optional[float]
    inside_expected_move: Optional[bool]
    projection: Optional[GreeksProjection]
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = {k: getattr(self, k) for k in self.__dataclass_fields__ if k != "projection"}
        p = self.projection
        d["projection"] = None if p is None else {k: getattr(p, k) for k in p.__dataclass_fields__}
        return d


@dataclass(frozen=True)
class SIEResult:
    status: str                       # "OK" / "SKIP"
    symbol: str
    side: str
    spot: float
    market_phase: str
    delta_band: Tuple[float, float]
    choppy: bool
    expected_move: Optional[float]
    lower_range: Optional[float]
    upper_range: Optional[float]
    hold_minutes: float
    em_conflict: bool                 # Module 8 direction opposes the trade side
    top: List[RankedStrike]
    ranked: List[RankedStrike]
    reason: str

    @property
    def best(self) -> Optional[RankedStrike]:
        return self.top[0] if self.top else None


# ── Core ────────────────────────────────────────────────────────────────

def _reject_reasons(c: StrikeCandidate, cfg: SIEConfig) -> List[str]:
    reasons: List[str] = []
    if c.premium is None or c.premium <= 0:
        reasons.append("no_premium")
    if c.delta is None:
        reasons.append("no_greeks")
    if c.liquidity_score is None:
        reasons.append("liquidity_missing")
    elif c.liquidity_score < cfg.min_liquidity:
        reasons.append(f"liquidity_below_{cfg.min_liquidity:g}")
    if (c.liquidity_band or "").upper() in POOR_LIQUIDITY_BANDS:
        reasons.append("liquidity_band_red")
    if c.spread_pct is not None and c.spread_pct > cfg.max_spread_pct:
        reasons.append(f"spread_above_{cfg.max_spread_pct:g}pct")
    return reasons


def _checklist(r: RankedStrike, band: Tuple[float, float]) -> List[str]:
    def mark(ok: bool, text: str) -> str:
        return ("✓ " if ok else "✗ ") + text

    out = []
    if r.delta is not None:
        out.append(mark(band[0] <= abs(r.delta) <= band[1], f"Delta {abs(r.delta):.2f} vs target {band[0]:.2f}-{band[1]:.2f}"))
    if r.components.get("theta") is not None:
        out.append(mark(r.components["theta"] >= REASON_THETA_OK, f"Theta risk {r.theta_risk_pct:.1f}% of premium over hold"))
    if r.liquidity_score is not None:
        out.append(mark(r.liquidity_score >= REASON_LIQUIDITY_HIGH, f"Liquidity {r.liquidity_score:.0f}"))
    if r.inside_expected_move is not None:
        out.append(mark(r.inside_expected_move, "Inside expected move" if r.inside_expected_move else "Outside expected move"))
    if r.projection is not None:
        out.append(mark(r.projection.premium_gain > 0, f"Projected premium gain {r.projection.premium_gain:+.2f} on EM"))
    return out


def score_candidate(
    c: StrikeCandidate,
    ctx: SIEContext,
    band: Tuple[float, float],
    max_gamma: Optional[float],
    choppy: bool,
    cfg: SIEConfig,
) -> RankedStrike:
    intrinsic, extrinsic = intrinsic_extrinsic(ctx.spot, c.strike, c.cp, c.premium)
    t_pts, t_pct = theta_risk(c.theta_per_day, c.premium, ctx.hold_minutes)
    components = {
        "liquidity": None if c.liquidity_score is None else _clip(c.liquidity_score),
        "em_fit": em_fit_score(c.strike, ctx.spot, ctx.expected_move),
        "delta": delta_suitability_score(c.delta, band),
        "theta": theta_efficiency_score(t_pct),
        "gamma": gamma_opportunity_score(c.gamma, max_gamma, choppy),
        "vega": vega_stability_score(c.vega_per_point, c.premium, ctx.iv_trend),
    }
    score, completeness = weighted_score(components)
    greek_weights = {k: WEIGHTS[k] for k in GREEK_COMPONENTS}
    g_score, g_comp = weighted_score(components, greek_weights)
    lower, upper = expected_range(ctx.spot, ctx.expected_move)
    rejects = _reject_reasons(c, cfg)

    ranked = RankedStrike(
        tradingsymbol=c.tradingsymbol,
        token=c.token,
        strike=c.strike,
        cp=c.cp,
        status="REJECTED" if rejects else "OK",
        reject_reasons=rejects,
        rank=0,
        strike_score=score,
        confidence=round(score * completeness, 2),
        completeness=completeness,
        components=components,
        greeks_score=g_score if g_comp > 0 else None,
        execution_quality=execution_quality_score(c.spread_pct, c.depth, c.lot_size, cfg.max_spread_pct),
        premium=c.premium,
        intrinsic=intrinsic,
        extrinsic=extrinsic,
        delta=c.delta,
        gamma=c.gamma,
        theta_per_day=c.theta_per_day,
        vega_per_point=c.vega_per_point,
        iv=c.iv,
        greeks_source=c.greeks_source,
        theta_risk_pts=t_pts,
        theta_risk_pct=t_pct,
        liquidity_score=c.liquidity_score,
        liquidity_band=c.liquidity_band,
        spread_pct=c.spread_pct,
        lot_size=c.lot_size,
        inside_expected_move=None if lower is None else (lower <= c.strike <= upper),
        projection=project_greeks(c, ctx),
    )
    return replace(ranked, reasons=_checklist(ranked, band))


def _skip(ctx: SIEContext, reason: str, phase: str = "", band=(0.0, 0.0)) -> SIEResult:
    lower, upper = expected_range(ctx.spot or 0.0, ctx.expected_move)
    return SIEResult(
        status="SKIP", symbol=ctx.symbol, side=ctx.side, spot=ctx.spot or 0.0,
        market_phase=phase, delta_band=band, choppy=False,
        expected_move=ctx.expected_move, lower_range=lower, upper_range=upper,
        hold_minutes=ctx.hold_minutes, em_conflict=False, top=[], ranked=[], reason=reason,
    )


def rank_strikes(
    candidates: Sequence[StrikeCandidate],
    ctx: SIEContext,
    cfg: Optional[SIEConfig] = None,
) -> SIEResult:
    cfg = cfg or SIEConfig()
    if ctx.side not in ("CE", "PE"):
        return _skip(ctx, "invalid_side")
    if not ctx.spot or ctx.spot <= 0:
        return _skip(ctx, "invalid_spot")

    phase, band = classify_market_phase(ctx)
    side_cands = [c for c in candidates if (c.cp or "").upper() == ctx.side]
    if not side_cands:
        return _skip(ctx, f"no_{ctx.side}_candidates", phase, band)

    choppy = is_choppy(ctx)
    gammas = [abs(c.gamma) for c in side_cands if c.gamma is not None]
    max_gamma = max(gammas) if gammas else None

    scored = [score_candidate(c, ctx, band, max_gamma, choppy, cfg) for c in side_cands]
    # OK before REJECTED; within each, best score, then confidence, then nearer ATM.
    scored.sort(key=lambda r: (r.status != "OK", -r.strike_score, -r.confidence, abs(r.strike - ctx.spot)))
    ranked = [replace(r, rank=i + 1) for i, r in enumerate(scored)]
    top = [r for r in ranked if r.status == "OK"][: cfg.top_n]

    lower, upper = expected_range(ctx.spot, ctx.expected_move)
    return SIEResult(
        status="OK" if top else "SKIP",
        symbol=ctx.symbol,
        side=ctx.side,
        spot=ctx.spot,
        market_phase=phase,
        delta_band=band,
        choppy=choppy,
        expected_move=ctx.expected_move,
        lower_range=lower,
        upper_range=upper,
        hold_minutes=ctx.hold_minutes,
        em_conflict=em_agrees_with_side(ctx.em_direction, ctx.side) is False,
        top=top,
        ranked=ranked,
        reason="ok" if top else "all_strikes_rejected",
    )

"""
app/greeks_change.py
───────────────────────
Module 9 — Greeks Change Predictor, per the functional spec.

Deterministic scenario + repricing engine (spec 5.1: "not an ML
predictor"). Builds a spot x IV x time scenario grid off Module 8's
expected-move output, reprices every cell with app/option_pricing.py's
Black-Scholes-Merton model, and compares each scenario's Greeks/premium
back to the contract's CURRENT state.

Provenance discipline (handover notes: "Do not silently mix LTP Greeks
from one source with theoretical Greeks from another without identifying
the source"): CurrentState below carries an explicit `source` field per
value pulled in by the runner (e.g. "broker_api" for Angel's REST Greeks,
"market_mid" for an observed premium, "theoretical_black_scholes" for a
computed fallback when no observed value exists) so nothing gets silently
relabeled as one type when it's really the other.

No I/O here -- pure dataclasses + functions. Redis wiring lives in
run_greeks_change.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from typing import List, Optional

from .option_pricing import PricingResult, bs_price_greeks

# ── Scenario grid defaults (spec 5.4, all overridable) ───────────────────

DEFAULT_SPOT_MULTIPLIERS = [-2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0]
DEFAULT_SPOT_LABELS = {
    -2.0: "-2.0 EM", -1.0: "-1.0 EM", -0.5: "-0.5 EM",
    0.0: "Base", 0.5: "+0.5 EM", 1.0: "+1.0 EM", 2.0: "+2.0 EM",
}

DEFAULT_IV_SHIFT_POINTS = [-2.0, 0.0, 2.0]   # in volatility POINTS (e.g. -2 = IV drops 2 points)
DEFAULT_IV_FLOOR = 0.01                       # spec: "floor at configured minimum" on negative future IV

DEFAULT_TIME_ELAPSED_MINUTES = [0.0, 15.0, 30.0, 60.0]
MINUTES_PER_YEAR_CALENDAR = 365.0 * 24.0 * 60.0  # matches option_pricing's calendar-day T convention

# Risk-classification thresholds: NOT given numeric values in the spec
# (only named as "configurable thresholds" in 6's classify_risk row) --
# defaulted to conservative round numbers here, overridable via the
# runner. Flag if calibrated values were intended.
GAMMA_CONCENTRATION_DOLLAR_GAMMA_PCT = 0.20   # dollar-gamma (1% move) > this x premium -> flag
THETA_DECAY_HIGH_PCT = 0.05                    # |theta_per_day| > this x premium -> flag
IV_SENSITIVITY_HIGH_PCT = 0.05                 # |vega_per_point| > this x premium -> flag

# Module 8 emits STRONG_BULLISH / BULLISH / ... ; the spec's worked
# example uses title case ("Bullish"). Both are accepted.
_BULLISH_LABELS = frozenset({"STRONG_BULLISH", "BULLISH", "STRONG BULLISH"})
_BEARISH_LABELS = frozenset({"STRONG_BEARISH", "BEARISH", "STRONG BEARISH"})


# ── 7. Validation ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    status: str   # "OK" / "EXPIRED" / "INVALID"
    flags: List[str]


def validate_option_inputs(
    underlying_spot: Optional[float],
    strike: Optional[float],
    option_type: Optional[str],
    time_to_expiry_years: Optional[float],
    current_iv: Optional[float],
    risk_free_rate: Optional[float] = None,
) -> ValidationResult:
    """
    Per spec section 7's table:
      spot<=0            -> reject
      IV missing/<=0     -> flag invalid (magnitude/repricing can't run)
      expired option     -> do not run future scenarios; return
                             expired-contract status (not a hard reject --
                             the caller may still want current_state).
    """
    flags: List[str] = []

    if underlying_spot is None or underlying_spot <= 0:
        flags.append("invalid_spot")
        return ValidationResult(valid=False, status="INVALID", flags=flags)

    if strike is None or strike <= 0:
        flags.append("invalid_strike")
        return ValidationResult(valid=False, status="INVALID", flags=flags)

    ot = (option_type or "").strip().upper()
    if ot not in ("CE", "PE"):
        flags.append("invalid_option_type")
        return ValidationResult(valid=False, status="INVALID", flags=flags)

    if current_iv is None:
        flags.append("missing_current_iv")
        return ValidationResult(valid=False, status="INVALID", flags=flags)
    if current_iv <= 0:
        flags.append("invalid_current_iv")
        return ValidationResult(valid=False, status="INVALID", flags=flags)

    if risk_free_rate is None:
        flags.append("risk_free_rate_using_default")

    if time_to_expiry_years is None:
        flags.append("missing_time_to_expiry")
        return ValidationResult(valid=False, status="INVALID", flags=flags)
    if time_to_expiry_years <= 0:
        flags.append("expired_contract")
        return ValidationResult(valid=False, status="EXPIRED", flags=flags)

    return ValidationResult(valid=True, status="OK", flags=flags)


# ── 5.4 Scenario generation ──────────────────────────────────────────────

@dataclass(frozen=True)
class SpotScenario:
    label: str
    multiplier: float
    spot: float


def generate_spot_scenarios(
    spot: float,
    expected_move: Optional[float],
    multipliers: List[float] = DEFAULT_SPOT_MULTIPLIERS,
) -> List[SpotScenario]:
    """
    Spec 5.4's default grid: Spot +/- {2.0, 1.0, 0.5} x Expected Move, plus
    Base. If expected_move is unavailable (Module 8 flagged INSUFFICIENT_DATA
    upstream), every non-zero multiplier collapses to Base -- returning an
    empty/degenerate grid would silently hide that Module 8 didn't have a
    magnitude; instead every scenario still exists but reduces to current
    spot, and the caller should check for this via a flag (the runner does).
    """
    em = expected_move if expected_move is not None else 0.0
    out = []
    for m in multipliers:
        label = DEFAULT_SPOT_LABELS.get(m, f"{m:+.1f} EM")
        out.append(SpotScenario(label=label, multiplier=m, spot=round(spot + m * em, 4)))
    return out


@dataclass(frozen=True)
class IVScenario:
    label: str
    shift_points: float
    iv: float
    floored: bool


def generate_iv_scenarios(
    current_iv: float,
    shift_points: List[float] = DEFAULT_IV_SHIFT_POINTS,
    floor_min_iv: float = DEFAULT_IV_FLOOR,
) -> List[IVScenario]:
    """
    shift_points are volatility POINTS (e.g. -2.0 means IV drops 2
    percentage points -> -0.02 in decimal terms). Negative future IV after
    a shock is floored at floor_min_iv per spec 7's validation table, and
    flagged rather than silently clamped without a trace.
    """
    out = []
    for pts in shift_points:
        raw_iv = current_iv + (pts / 100.0)
        floored = raw_iv < floor_min_iv
        iv = max(raw_iv, floor_min_iv)
        label = "Current IV" if pts == 0.0 else f"IV {pts:+.1f}pt"
        out.append(IVScenario(label=label, shift_points=pts, iv=round(iv, 6), floored=floored))
    return out


@dataclass(frozen=True)
class TimeScenario:
    label: str
    elapsed_minutes: float
    remaining_years: float
    clamped: bool
    expired: bool


def generate_time_scenarios(
    time_to_expiry_years: float,
    elapsed_minutes: List[float] = DEFAULT_TIME_ELAPSED_MINUTES,
    minutes_per_year: float = MINUTES_PER_YEAR_CALENDAR,
) -> List[TimeScenario]:
    """
    Reduces time-to-expiry by each configured elapsed-time step. Per spec
    7 ("scenario time exceeds expiry -> clamp to expiry") and GR-04
    ("scenario grid never creates negative time to expiry"), remaining
    time is floored at 0 and flagged both clamped and expired when that
    happens -- never allowed to go negative.
    """
    out = []
    for mins in elapsed_minutes:
        elapsed_years = mins / minutes_per_year
        raw_remaining = time_to_expiry_years - elapsed_years
        clamped = raw_remaining < 0
        remaining = max(raw_remaining, 0.0)
        label = "Now" if mins == 0.0 else f"+{mins:.0f}min"
        out.append(TimeScenario(
            label=label, elapsed_minutes=mins, remaining_years=round(remaining, 8),
            clamped=clamped, expired=remaining <= 0.0,
        ))
    return out


# ── Current state (provenance-tagged) ────────────────────────────────────

@dataclass(frozen=True)
class CurrentState:
    premium: Optional[float]
    premium_source: str          # "market_mid" / "market_ltp" / "theoretical_black_scholes" / "unavailable"
    iv: float
    delta: Optional[float]
    gamma: Optional[float]
    theta_per_day: Optional[float]
    vega_per_point: Optional[float]
    greeks_source: str            # "broker_api" / "theoretical_black_scholes" / "unavailable"


def resolve_current_state(
    spot: float,
    strike: float,
    option_type: str,
    time_to_expiry_years: float,
    current_iv: float,
    risk_free_rate: float,
    dividend_or_carry: float,
    observed_premium: Optional[float] = None,
    observed_premium_source: str = "market_mid",
    observed_delta: Optional[float] = None,
    observed_gamma: Optional[float] = None,
    observed_theta_per_day: Optional[float] = None,
    observed_vega_per_point: Optional[float] = None,
    observed_greeks_source: str = "broker_api",
) -> CurrentState:
    """
    Prefers OBSERVED premium/Greeks (e.g. Angel's broker API, or the
    market mid) when supplied. Falls back to a theoretical Black-Scholes
    calculation ONLY when the observed value is missing, and tags the
    resulting field's source accordingly -- so a downstream consumer can
    always tell which numbers were real market data vs. modeled, per the
    handover notes' explicit "do not silently mix" requirement.
    """
    have_all_greeks = None not in (observed_delta, observed_gamma, observed_theta_per_day, observed_vega_per_point)

    if have_all_greeks and observed_premium is not None:
        return CurrentState(
            premium=observed_premium, premium_source=observed_premium_source,
            iv=current_iv,
            delta=observed_delta, gamma=observed_gamma,
            theta_per_day=observed_theta_per_day, vega_per_point=observed_vega_per_point,
            greeks_source=observed_greeks_source,
        )

    theo = bs_price_greeks(spot, strike, option_type, time_to_expiry_years, current_iv, risk_free_rate, dividend_or_carry)

    return CurrentState(
        premium=observed_premium if observed_premium is not None else theo.premium,
        premium_source=observed_premium_source if observed_premium is not None else "theoretical_black_scholes",
        iv=current_iv,
        delta=observed_delta if observed_delta is not None else theo.delta,
        gamma=observed_gamma if observed_gamma is not None else theo.gamma,
        theta_per_day=observed_theta_per_day if observed_theta_per_day is not None else theo.theta_per_day,
        vega_per_point=observed_vega_per_point if observed_vega_per_point is not None else theo.vega_per_point,
        greeks_source=observed_greeks_source if have_all_greeks else "theoretical_black_scholes",
    )


# ── 5.5 / functional contract: reprice + compare ─────────────────────────

def reprice_option(
    spot: float,
    strike: float,
    option_type: str,
    time_to_expiry_years: float,
    iv: float,
    risk_free_rate: float,
    dividend_or_carry: float,
) -> PricingResult:
    """Thin named wrapper matching the spec's functional-contract table (section 6)."""
    return bs_price_greeks(spot, strike, option_type, time_to_expiry_years, iv, risk_free_rate, dividend_or_carry)


@dataclass(frozen=True)
class ComparisonResult:
    premium_change: float
    delta_change: Optional[float]
    gamma_change: Optional[float]
    theta_change: Optional[float]
    vega_change: Optional[float]


def compare_greeks(current: CurrentState, predicted: PricingResult) -> ComparisonResult:
    """Future - Current, per spec 5.7. None current values propagate as None (not silently zeroed)."""
    return ComparisonResult(
        premium_change=round(predicted.premium - (current.premium or 0.0), 4),
        delta_change=None if current.delta is None else round(predicted.delta - current.delta, 4),
        gamma_change=None if current.gamma is None else round(predicted.gamma - current.gamma, 6),
        theta_change=None if current.theta_per_day is None else round(predicted.theta_per_day - current.theta_per_day, 4),
        vega_change=None if current.vega_per_point is None else round(predicted.vega_per_point - current.vega_per_point, 4),
    )


# ── Risk classification ──────────────────────────────────────────────────

def classify_risk(
    predicted: PricingResult,
    iv_scenario: IVScenario,
    time_scenario: TimeScenario,
    current: CurrentState,
    gamma_threshold_pct: float = GAMMA_CONCENTRATION_DOLLAR_GAMMA_PCT,
    theta_threshold_pct: float = THETA_DECAY_HIGH_PCT,
    vega_threshold_pct: float = IV_SENSITIVITY_HIGH_PCT,
) -> List[str]:
    """
    Informational flags per spec 5.7 ("Gamma concentration, theta decay,
    IV sensitivity and data-quality flags"). Thresholds are configurable
    defaults, not calibrated constants -- see module docstring.

    GAMMA_CONCENTRATION is intentionally NOT decided here -- a meaningful
    dollar-gamma check needs the scenario's SPOT price, which this
    function doesn't receive (only the repriced/IV/time state). See
    _gamma_concentration_flag(), applied in build_scenario_matrix() where
    spot is in scope.
    """
    del gamma_threshold_pct  # applied in build_scenario_matrix via _gamma_concentration_flag
    flags: List[str] = []

    if predicted.intrinsic_fallback or time_scenario.expired:
        flags.append("EXPIRED_OR_DEGENERATE_SCENARIO")
    if time_scenario.clamped:
        flags.append("TIME_CLAMPED_TO_EXPIRY")
    if iv_scenario.floored:
        flags.append("NEGATIVE_IV_FLOORED")

    premium_ref = current.premium if current.premium and current.premium > 0 else predicted.premium
    if premium_ref and premium_ref > 0 and not predicted.intrinsic_fallback:
        if abs(predicted.theta_per_day) > theta_threshold_pct * premium_ref:
            flags.append("THETA_DECAY_HIGH")
        if abs(predicted.vega_per_point) > vega_threshold_pct * premium_ref:
            flags.append("IV_SENSITIVITY_HIGH")

    return flags


def _gamma_concentration_flag(
    predicted: PricingResult,
    scenario_spot: float,
    premium_ref: float,
    threshold_pct: float = GAMMA_CONCENTRATION_DOLLAR_GAMMA_PCT,
) -> bool:
    """
    Dollar-gamma for a 1% move in the underlying = 0.5 * Gamma * (Spot*0.01)^2
    (standard dollar-gamma convention). Flagged when that exceeds
    threshold_pct of the reference premium -- i.e. a 1% underlying move
    would reprice the position by more than threshold_pct x premium just
    from convexity, independent of delta.
    """
    if premium_ref <= 0 or predicted.intrinsic_fallback:
        return False
    dollar_gamma = 0.5 * predicted.gamma * (scenario_spot * 0.01) ** 2
    return abs(dollar_gamma) > threshold_pct * premium_ref


# ── build_scenario_matrix / build_summary ────────────────────────────────

@dataclass(frozen=True)
class ScenarioResult:
    spot_scenario: SpotScenario
    iv_scenario: IVScenario
    time_scenario: TimeScenario
    predicted_state: PricingResult
    comparison: ComparisonResult
    risk_flags: List[str]


def build_scenario_matrix(
    current: CurrentState,
    strike: float,
    option_type: str,
    risk_free_rate: float,
    dividend_or_carry: float,
    spot_scenarios: List[SpotScenario],
    iv_scenarios: List[IVScenario],
    time_scenarios: List[TimeScenario],
    gamma_threshold_pct: float = GAMMA_CONCENTRATION_DOLLAR_GAMMA_PCT,
    theta_threshold_pct: float = THETA_DECAY_HIGH_PCT,
    vega_threshold_pct: float = IV_SENSITIVITY_HIGH_PCT,
) -> List[ScenarioResult]:
    """Full cross of spot x IV x time scenarios (spec 5.4: "each spot scenario must be crossed with IV and time scenarios")."""
    matrix: List[ScenarioResult] = []
    premium_ref = current.premium if current.premium and current.premium > 0 else None

    for sp, ivs, ts in product(spot_scenarios, iv_scenarios, time_scenarios):
        predicted = reprice_option(sp.spot, strike, option_type, ts.remaining_years, ivs.iv, risk_free_rate, dividend_or_carry)
        comparison = compare_greeks(current, predicted)
        flags = classify_risk(predicted, ivs, ts, current, gamma_threshold_pct, theta_threshold_pct, vega_threshold_pct)

        ref = premium_ref if premium_ref is not None else predicted.premium
        if ref and _gamma_concentration_flag(predicted, sp.spot, ref, gamma_threshold_pct):
            flags.append("GAMMA_CONCENTRATION")

        matrix.append(ScenarioResult(
            spot_scenario=sp, iv_scenario=ivs, time_scenario=ts,
            predicted_state=predicted, comparison=comparison, risk_flags=flags,
        ))
    return matrix


@dataclass(frozen=True)
class ScenarioSummary:
    base: Optional[ScenarioResult]
    expected_direction: Optional[ScenarioResult]
    adverse: Optional[ScenarioResult]
    stress: Optional[ScenarioResult]
    notes: List[str]


def _find(matrix: List[ScenarioResult], spot_mult: float, iv_shift: float, elapsed_minutes: float) -> Optional[ScenarioResult]:
    for r in matrix:
        if (
            r.spot_scenario.multiplier == spot_mult
            and r.iv_scenario.shift_points == iv_shift
            and r.time_scenario.elapsed_minutes == elapsed_minutes
        ):
            return r
    return None


def _direction_family(direction: str) -> str:
    """Map Module 8 labels (STRONG_BULLISH) and spec title-case (Bullish) onto one family."""
    d = (direction or "").strip().upper().replace("-", "_")
    compact = d.replace(" ", "_")
    spaced = d.replace("_", " ")
    if compact in _BULLISH_LABELS or spaced in _BULLISH_LABELS:
        return "bullish"
    if compact in _BEARISH_LABELS or spaced in _BEARISH_LABELS:
        return "bearish"
    return "neutral"


def build_summary(
    matrix: List[ScenarioResult],
    direction: str,
    stress_iv_shift_points: Optional[float] = None,
    stress_elapsed_minutes: Optional[float] = None,
) -> ScenarioSummary:
    """
    Identifies base / expected-direction / adverse / stress scenarios for
    downstream consumption (spec's build_summary row). "Expected direction"
    and "adverse" are read off Module 8's direction classification
    (Bullish family -> +1.0 EM is expected, -1.0 EM is adverse; Bearish
    family mirrored). Neutral has no defined directional pair -- summary
    returns None for both rather than guessing a direction Module 8 didn't
    assert, with a note explaining why.

    Accepts both Module 8's actual labels (STRONG_BULLISH / BULLISH /
    NEUTRAL) and the spec's title-case ("Bullish" / "Strong Bullish").

    "Stress" is read as the worst-case combination for a LONG option
    holder: the adverse spot direction (or -2.0 EM if direction is
    Neutral, as a conservative default) crossed with the most negative
    available IV shift (IV crush hurts long vega) and the longest
    available time-elapsed step (maximum theta decay). This combination
    isn't spelled out explicitly in the spec beyond "stress scenario" --
    flagged as an interpretation, not a literal requirement.
    """
    notes: List[str] = []
    family = _direction_family(direction)

    base = _find(matrix, 0.0, 0.0, 0.0)

    if family == "bullish":
        expected_mult, adverse_mult = 1.0, -1.0
    elif family == "bearish":
        expected_mult, adverse_mult = -1.0, 1.0
    else:
        expected_mult, adverse_mult = None, None
        notes.append("neutral_or_unknown_direction: expected_direction/adverse left undefined")

    expected = _find(matrix, expected_mult, 0.0, 0.0) if expected_mult is not None else None
    adverse = _find(matrix, adverse_mult, 0.0, 0.0) if adverse_mult is not None else None

    stress_mult = adverse_mult if adverse_mult is not None else -2.0
    if adverse_mult is None:
        notes.append("stress_scenario_defaulted_to_-2.0EM_due_to_neutral_direction")

    if stress_iv_shift_points is None:
        available_iv_shifts = sorted({r.iv_scenario.shift_points for r in matrix})
        stress_iv_shift_points = available_iv_shifts[0] if available_iv_shifts else 0.0
    if stress_elapsed_minutes is None:
        available_times = sorted({r.time_scenario.elapsed_minutes for r in matrix})
        stress_elapsed_minutes = available_times[-1] if available_times else 0.0

    stress = _find(matrix, stress_mult, stress_iv_shift_points, stress_elapsed_minutes)

    return ScenarioSummary(base=base, expected_direction=expected, adverse=adverse, stress=stress, notes=notes)

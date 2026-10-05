"""
app/expected_move.py
───────────────────────
Module 8 — Expected Move Calculator (pure prediction).

Word spec ("Option Rider Algo For Algobuilders", Expected Move Engine):
ONLY predict Direction, Magnitude, Confidence. No strike / SL / risk.

  total_score      = indicator + volume + bidask + oi     # -7 .. +7
  normalized_score = total_score / 7                      # -1 .. +1
  multiplier       = 1
                   + 0.4 if gamma_trend == "up"
                   + 0.3 if iv_trend == "up"
                   + 0.3 if 0.5 <= |delta| <= 0.8
                   + 0.4 if vacuum_zone
                   - 0.4 if strong_resistance_nearby
  expected_move_pct = normalized_score * multiplier * 0.02
  expected_move     = spot * expected_move_pct
  target_price      = spot + expected_move
  confidence        = abs(total_score)*10
                    + 10 if gamma_trend == "up"
                    + 10 if volume_strong                 # 0 .. 100

The spec writes "* 2" with comment "final range ≈ -3% to +3%" and example
expected_move_pct=0.018. The 2 is 2 percentage points (0.02), not a 200%
scale: |normalized|<=1 and typical multiplier ~1.5 → ~3%.

IV / ATM-straddle numbers are optional diagnostics only. They never drive
the published move (a 60-minute IV sigma and an expiry straddle are not
the same quantity).

Missing inputs are flagged and treated as 0 / unused — never fabricated.

No I/O here — Redis wiring lives in run_expected_move.py.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional, Tuple

# NSE cash/FO session is 09:15–15:30 = 375 minutes (IV diagnostic only).
TRADING_MINUTES_PER_DAY = 375.0
TRADING_DAYS_PER_YEAR = 252.0
DEFAULT_HORIZON_MINUTES = 60.0

# Direction score = (indicator + volume + bidask + oi) / DIRECTION_DENOM
# Max |sum| = 2+2+1+2 = 7, matching the spec's -7..+7 total / 7 normalize.
DIRECTION_DENOM = 7.0
DIRECTION_NEUTRAL_ABS = 0.20
DIRECTION_STRONG_ABS = 0.50

# Spec "* 2" + "~-3% to +3%" + example 0.018 → 2 percentage points.
MOVE_PCT_SCALE = 0.02

# Momentum multiplier (spec Step 3). Thresholds match app/greeks_phase.py.
GAMMA_TREND_UP_PCT = 10.0
IV_TREND_UP_PCT = 5.0
DELTA_BAND_MIN = 0.5
DELTA_BAND_MAX = 0.8
MULTIPLIER_GAMMA_UP = 0.4
MULTIPLIER_IV_UP = 0.3
MULTIPLIER_DELTA_BAND = 0.3
MULTIPLIER_VACUUM = 0.4
MULTIPLIER_RESISTANCE = 0.4

# Same Word doc's OI-wall language: price approaching top OI strike within 0.5%.
RESISTANCE_NEAR_PCT = 0.005

# Confidence (spec Step 7).
CONFIDENCE_GAMMA_UP = 10.0
CONFIDENCE_VOLUME_STRONG = 10.0
VOLUME_STRONG_ABS = 2.0

_VOLUME_SIGNAL_MAP = {
    "STRONG BULLISH VOLUME": 2.0,
    "BULLISH VOLUME": 1.0,
    "POSSIBLE WRONG ENTRY": 0.0,
    "BEARISH VOLUME": -1.0,
    "STRONG BEARISH VOLUME": -2.0,
}

_OI_SIGNAL_MAP = {
    "BULLISH_POSITIONING": 2.0,
    "BEARISH_POSITIONING": -2.0,
    "NEUTRAL": 0.0,
}


# ── Helpers the runner uses to adapt upstream string/float payloads ─────

def clip_bidask_score(value: Optional[float]) -> Optional[float]:
    """Imbalance final_score is already -1..+1; clip anyway so a bad tick can't blow the denom."""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v):
        return None
    return max(-1.0, min(1.0, v))


def normalize_volume_signal(signal: Optional[str]) -> Optional[float]:
    """Map run_volume_analyzer.py's signal string onto the spec's -2..+2 volume_score."""
    if signal is None:
        return None
    key = str(signal).strip().upper()
    if not key or key == "NO_DATA":
        return None
    return _VOLUME_SIGNAL_MAP.get(key)


def normalize_oi_signal(positioning: Optional[str]) -> Optional[float]:
    """Map run_oi_analysis.py's positioning string onto the spec's -2..+2 oi_score."""
    if positioning is None:
        return None
    key = str(positioning).strip().upper()
    if not key:
        return None
    return _OI_SIGNAL_MAP.get(key)


# IV / RV unit handling (QA HIGH fix): units are declared by the SOURCE,
# never guessed from magnitude. Guessing (">1 means percent") turned a
# percent reading of 0.01 (= 0.01%) into a decimal 0.01 (= 1%) — a 100x error.
#   "percent": Angel REST optionGreek / joiner / greeks_poller / greeks-phase
#              latest `iv` (18.5 == 18.5%).
#   "decimal": annualized decimal (0.185 == 18.5%) — option_pricing sigma,
#              StrikeCandidate.iv, ExpectedMoveResult.implied_volatility.
IV_UNITS_PERCENT = "percent"
IV_UNITS_DECIMAL = "decimal"
# Plausibility band for an annualized vol (decimal). Outside -> missing.
MIN_PLAUSIBLE_IV = 0.01   # 1% annualized
MAX_PLAUSIBLE_IV = 3.00   # 300% annualized


def as_annualized_decimal(vol: Optional[float], units: str) -> Optional[float]:
    """
    Convert a vol reading to an annualized decimal using the source's
    DECLARED units ("percent" or "decimal"). Returns None for missing,
    non-finite, non-positive, or implausible (< 1% / > 300% annualized)
    values. Raises ValueError on an unknown units string (programming error).
    """
    u = str(units or "").strip().lower()
    if u not in (IV_UNITS_PERCENT, IV_UNITS_DECIMAL):
        raise ValueError(f"as_annualized_decimal: units must be 'percent' or 'decimal', got {units!r}")
    if vol is None:
        return None
    try:
        v = float(vol)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v) or v <= 0:
        return None
    if u == IV_UNITS_PERCENT:
        v = v / 100.0
    if v < MIN_PLAUSIBLE_IV or v > MAX_PLAUSIBLE_IV:
        return None
    return v


def iv_from_percent(vol: Optional[float]) -> Optional[float]:
    """Percent-unit source (Angel / greeks-phase `iv`) -> annualized decimal or None."""
    return as_annualized_decimal(vol, IV_UNITS_PERCENT)


def iv_from_decimal(vol: Optional[float]) -> Optional[float]:
    """Decimal-unit source -> validated annualized decimal or None."""
    return as_annualized_decimal(vol, IV_UNITS_DECIMAL)


def _finite(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(x):
        return None
    return x


def _finite_positive(v: Optional[float]) -> Optional[float]:
    x = _finite(v)
    if x is None or x <= 0:
        return None
    return x


def _clip(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def classify_pct_trend(pct: Optional[float], up_threshold: float) -> Optional[str]:
    """Map a % change onto up / down / flat. None if the reading is missing."""
    v = _finite(pct)
    if v is None:
        return None
    if v >= up_threshold:
        return "up"
    if v <= -up_threshold:
        return "down"
    return "flat"


def combine_trend(*trends: Optional[str]) -> Optional[str]:
    """Prefer 'up' if any ATM side is up; None only when every side is missing."""
    present = [str(t).strip().lower() for t in trends if t is not None and str(t).strip()]
    if not present:
        return None
    if any(t == "up" for t in present):
        return "up"
    if any(t == "down" for t in present):
        return "down"
    return "flat"


def _delta_in_band(delta: Optional[float]) -> bool:
    d = _finite(delta)
    return d is not None and DELTA_BAND_MIN <= abs(d) <= DELTA_BAND_MAX


def pick_atm_delta(
    delta_ce: Optional[float],
    delta_pe: Optional[float],
) -> Optional[float]:
    """Prefer the ATM side whose |delta| is in 0.5..0.8 (spec Step 3); else CE, else PE."""
    ce = _finite(delta_ce)
    pe = _finite(delta_pe)
    if _delta_in_band(ce):
        return ce
    if _delta_in_band(pe):
        return pe
    if ce is not None:
        return ce
    return pe


def resistance_is_nearby(
    spot_price: Optional[float],
    resistance: Optional[float],
    near_pct: float = RESISTANCE_NEAR_PCT,
) -> Optional[bool]:
    """True when primary OI resistance is within near_pct of spot. None if either input is missing."""
    spot = _finite_positive(spot_price)
    wall = _finite_positive(resistance)
    if spot is None or wall is None:
        return None
    return abs(spot - wall) / spot <= near_pct


# ── Spec contract ───────────────────────────────────────────────────────

def validate_inputs(
    spot_price: Optional[float],
    implied_volatility: Optional[float] = None,
    horizon_minutes: Optional[float] = None,
    atm_call_mid: Optional[float] = None,
    atm_put_mid: Optional[float] = None,
    realized_volatility: Optional[float] = None,
    indicator_score: Optional[float] = None,
    volume_score: Optional[float] = None,
    bidask_score: Optional[float] = None,
    oi_score: Optional[float] = None,
    gamma_trend: Optional[str] = None,
    iv_trend: Optional[str] = None,
    delta: Optional[float] = None,
    vacuum_zone: Optional[bool] = None,
    strong_resistance_nearby: Optional[bool] = None,
    trading_minutes_per_day: float = TRADING_MINUTES_PER_DAY,
) -> List[str]:
    """Return data_quality_flags. Never raises; missing/invalid inputs are flagged."""
    flags: List[str] = []

    if _finite_positive(spot_price) is None:
        flags.append("invalid_spot_price")

    if horizon_minutes is not None and _finite_positive(horizon_minutes) is None:
        flags.append("invalid_horizon_minutes")

    if _finite_positive(trading_minutes_per_day) is None:
        flags.append("invalid_trading_minutes_per_day")

    if iv_from_decimal(implied_volatility) is None:
        flags.append("missing_implied_volatility")

    if _finite_positive(atm_call_mid) is None:
        flags.append("missing_atm_call_mid")
    if _finite_positive(atm_put_mid) is None:
        flags.append("missing_atm_put_mid")

    if iv_from_decimal(realized_volatility) is None:
        flags.append("missing_realized_volatility")

    if indicator_score is None:
        flags.append("missing_indicator_score")
    if volume_score is None:
        flags.append("missing_volume_score")
    if bidask_score is None:
        flags.append("missing_bidask_score")
    if oi_score is None:
        flags.append("missing_oi_score")
    if gamma_trend is None:
        flags.append("missing_gamma_trend")
    if iv_trend is None:
        flags.append("missing_iv_trend")
    if _finite(delta) is None:
        flags.append("missing_delta")
    if vacuum_zone is None:
        flags.append("missing_vacuum_zone")
    if strong_resistance_nearby is None:
        flags.append("missing_resistance_zone")

    return flags


def _component_scores(
    indicator_score: Optional[float],
    volume_score: Optional[float],
    bidask_score: Optional[float],
    oi_score: Optional[float],
) -> Tuple[float, float, float, float]:
    ind = 0.0 if indicator_score is None else float(indicator_score)
    vol = 0.0 if volume_score is None else float(volume_score)
    ba = 0.0 if bidask_score is None else float(bidask_score)
    oi = 0.0 if oi_score is None else float(oi_score)
    return (
        _clip(ind, -2.0, 2.0),
        _clip(vol, -2.0, 2.0),
        _clip(ba, -1.0, 1.0),
        _clip(oi, -2.0, 2.0),
    )


def calculate_total_score(
    indicator_score: Optional[float],
    volume_score: Optional[float],
    bidask_score: Optional[float],
    oi_score: Optional[float],
) -> float:
    """Spec Step 1. Missing treated as 0."""
    ind, vol, ba, oi = _component_scores(
        indicator_score, volume_score, bidask_score, oi_score
    )
    return round(ind + vol + ba + oi, 4)


def calculate_direction_score(
    indicator_score: Optional[float],
    volume_score: Optional[float],
    bidask_score: Optional[float],
    oi_score: Optional[float],
) -> Tuple[float, str]:
    """
    Spec Step 2: normalized_score = total / 7  (-1..+1).
    Label bands: |s| < 0.20 Neutral; |s| >= 0.50 Strong.
    """
    total = calculate_total_score(
        indicator_score, volume_score, bidask_score, oi_score
    )
    score = round(total / DIRECTION_DENOM, 4)
    return score, _classify_direction(score)


def _classify_direction(score: float) -> str:
    if score >= DIRECTION_STRONG_ABS:
        return "STRONG_BULLISH"
    if score >= DIRECTION_NEUTRAL_ABS:
        return "BULLISH"
    if score <= -DIRECTION_STRONG_ABS:
        return "STRONG_BEARISH"
    if score <= -DIRECTION_NEUTRAL_ABS:
        return "BEARISH"
    return "NEUTRAL"


def calculate_multiplier(
    gamma_trend: Optional[str] = None,
    iv_trend: Optional[str] = None,
    delta: Optional[float] = None,
    vacuum_zone: Optional[bool] = None,
    strong_resistance_nearby: Optional[bool] = None,
) -> float:
    """Spec Steps 3–4. Missing extras leave the base multiplier at 1.0."""
    multiplier = 1.0
    if gamma_trend is not None and str(gamma_trend).strip().lower() == "up":
        multiplier += MULTIPLIER_GAMMA_UP
    if iv_trend is not None and str(iv_trend).strip().lower() == "up":
        multiplier += MULTIPLIER_IV_UP
    d = _finite(delta)
    if d is not None and DELTA_BAND_MIN <= abs(d) <= DELTA_BAND_MAX:
        multiplier += MULTIPLIER_DELTA_BAND
    if vacuum_zone is True:
        multiplier += MULTIPLIER_VACUUM
    if strong_resistance_nearby is True:
        multiplier -= MULTIPLIER_RESISTANCE
    return round(max(0.0, multiplier), 4)


def calculate_expected_move_pct(
    normalized_score: float,
    multiplier: float,
    scale: float = MOVE_PCT_SCALE,
) -> float:
    """Spec Step 5. Signed decimal (0.018 = +1.8%)."""
    return round(float(normalized_score) * float(multiplier) * float(scale), 6)


def calculate_confidence(
    total_score: float,
    gamma_trend: Optional[str] = None,
    volume_score: Optional[float] = None,
) -> int:
    """Spec Step 7. Clipped to 0–100."""
    conf = abs(float(total_score)) * 10.0
    if gamma_trend is not None and str(gamma_trend).strip().lower() == "up":
        conf += CONFIDENCE_GAMMA_UP
    vol = _finite(volume_score)
    if vol is not None and abs(vol) >= VOLUME_STRONG_ABS:
        conf += CONFIDENCE_VOLUME_STRONG
    return int(max(0, min(100, round(conf))))


def classify_move_quality(confidence: int) -> str:
    """Spec output move_quality. Example is 'strong' alongside confidence 78."""
    if confidence >= 70:
        return "strong"
    if confidence >= 40:
        return "moderate"
    return "weak"


def calculate_range(
    spot_price: float,
    expected_move: Optional[float],
) -> Tuple[Optional[float], Optional[float]]:
    """One-sided spec target expressed as [min(spot, target), max(spot, target)]."""
    if expected_move is None or spot_price <= 0:
        return None, None
    target = spot_price + expected_move
    lo, hi = (spot_price, target) if expected_move >= 0 else (target, spot_price)
    return round(lo, 2), round(hi, 2)


# ── Optional IV diagnostics (not the published magnitude) ───────────────

def calculate_iv_move(
    spot_price: float,
    implied_volatility: Optional[float],
    horizon_minutes: float,
    trading_minutes_per_day: float = TRADING_MINUTES_PER_DAY,
    trading_days_per_year: float = TRADING_DAYS_PER_YEAR,
) -> Optional[float]:
    """1-sigma IV move over the horizon. Diagnostic only.

    implied_volatility MUST be an annualized decimal (0.185); convert
    percent-unit sources with iv_from_percent() first.
    """
    sigma = iv_from_decimal(implied_volatility)
    if sigma is None or spot_price <= 0 or horizon_minutes <= 0 or trading_minutes_per_day <= 0:
        return None
    t_years = (horizon_minutes / trading_minutes_per_day) / trading_days_per_year
    if t_years <= 0:
        return None
    return round(spot_price * sigma * math.sqrt(t_years), 1)


def calculate_straddle_reference(
    atm_call_mid: Optional[float],
    atm_put_mid: Optional[float],
) -> Optional[float]:
    """ATM straddle mid (call + put). Diagnostic only — expiry-horizon, not 60-min."""
    call = _finite_positive(atm_call_mid)
    put = _finite_positive(atm_put_mid)
    if call is None or put is None:
        return None
    return round(call + put, 2)


def calculate_rv_move(
    spot_price: float,
    realized_volatility: Optional[float],
    horizon_minutes: float,
    trading_minutes_per_day: float = TRADING_MINUTES_PER_DAY,
    trading_days_per_year: float = TRADING_DAYS_PER_YEAR,
) -> Optional[float]:
    return calculate_iv_move(
        spot_price,
        realized_volatility,
        horizon_minutes,
        trading_minutes_per_day=trading_minutes_per_day,
        trading_days_per_year=trading_days_per_year,
    )


@dataclass(frozen=True)
class ExpectedMoveResult:
    spot_price: Optional[float]
    implied_volatility: Optional[float]
    horizon_minutes: float
    total_score: float
    direction_score: float
    direction: str
    multiplier: float
    gamma_trend: Optional[str]
    iv_trend: Optional[str]
    delta: Optional[float]
    vacuum_zone: Optional[bool]
    strong_resistance_nearby: Optional[bool]
    expected_move_pct: float
    expected_move: Optional[float]
    target_price: Optional[float]
    confidence: int
    move_quality: str
    final_expected_move: Optional[float]
    lower_range: Optional[float]
    upper_range: Optional[float]
    iv_move: Optional[float]
    straddle_reference: Optional[float]
    rv_move: Optional[float]
    magnitude_quality: str
    indicator_score: Optional[float]
    volume_score: Optional[float]
    bidask_score: Optional[float]
    oi_score: Optional[float]
    data_quality_flags: List[str]
    valid: bool


def compute_expected_move(
    spot_price: Optional[float],
    implied_volatility: Optional[float] = None,
    horizon_minutes: float = DEFAULT_HORIZON_MINUTES,
    atm_call_mid: Optional[float] = None,
    atm_put_mid: Optional[float] = None,
    realized_volatility: Optional[float] = None,
    indicator_score: Optional[float] = None,
    volume_score: Optional[float] = None,
    bidask_score: Optional[float] = None,
    oi_score: Optional[float] = None,
    gamma_trend: Optional[str] = None,
    iv_trend: Optional[str] = None,
    delta: Optional[float] = None,
    vacuum_zone: Optional[bool] = None,
    strong_resistance_nearby: Optional[bool] = None,
    trading_minutes_per_day: float = TRADING_MINUTES_PER_DAY,
    trading_days_per_year: float = TRADING_DAYS_PER_YEAR,
) -> ExpectedMoveResult:
    """Orchestrator. Always returns a result; invalid/missing inputs degrade via flags."""
    flags = validate_inputs(
        spot_price=spot_price,
        implied_volatility=implied_volatility,
        horizon_minutes=horizon_minutes,
        atm_call_mid=atm_call_mid,
        atm_put_mid=atm_put_mid,
        realized_volatility=realized_volatility,
        indicator_score=indicator_score,
        volume_score=volume_score,
        bidask_score=bidask_score,
        oi_score=oi_score,
        gamma_trend=gamma_trend,
        iv_trend=iv_trend,
        delta=delta,
        vacuum_zone=vacuum_zone,
        strong_resistance_nearby=strong_resistance_nearby,
        trading_minutes_per_day=trading_minutes_per_day,
    )

    total_score = calculate_total_score(
        indicator_score, volume_score, bidask_score, oi_score
    )
    direction_score, direction = calculate_direction_score(
        indicator_score, volume_score, bidask_score, oi_score
    )
    multiplier = calculate_multiplier(
        gamma_trend=gamma_trend,
        iv_trend=iv_trend,
        delta=delta,
        vacuum_zone=vacuum_zone,
        strong_resistance_nearby=strong_resistance_nearby,
    )
    expected_move_pct = calculate_expected_move_pct(direction_score, multiplier)
    confidence = calculate_confidence(total_score, gamma_trend, volume_score)
    move_quality = classify_move_quality(confidence)

    spot = _finite_positive(spot_price)
    horizon = _finite_positive(horizon_minutes) or 0.0
    tpd = _finite_positive(trading_minutes_per_day) or TRADING_MINUTES_PER_DAY
    valid = spot is not None

    expected_move = round(spot * expected_move_pct, 2) if spot is not None else None
    target_price = (
        round(spot + expected_move, 2) if spot is not None and expected_move is not None else None
    )
    # Unsigned rupee magnitude for Module 9's ± expected-move scenario grid.
    final_expected_move = abs(expected_move) if expected_move is not None else None
    lower_range, upper_range = (
        calculate_range(spot, expected_move) if valid else (None, None)
    )

    iv_move = None
    straddle_reference = None
    rv_move = None
    if valid and horizon > 0:
        iv_move = calculate_iv_move(
            spot, implied_volatility, horizon, tpd, trading_days_per_year
        )
        straddle_reference = calculate_straddle_reference(atm_call_mid, atm_put_mid)
        rv_move = calculate_rv_move(
            spot, realized_volatility, horizon, tpd, trading_days_per_year
        )

    sigma = iv_from_decimal(implied_volatility)
    return ExpectedMoveResult(
        spot_price=round(spot, 4) if spot is not None else spot_price,
        implied_volatility=round(sigma, 6) if sigma is not None else None,
        horizon_minutes=float(horizon_minutes) if horizon_minutes is not None else DEFAULT_HORIZON_MINUTES,
        total_score=total_score,
        direction_score=direction_score,
        direction=direction,
        multiplier=multiplier,
        gamma_trend=None if gamma_trend is None else str(gamma_trend).strip().lower(),
        iv_trend=None if iv_trend is None else str(iv_trend).strip().lower(),
        delta=round(delta, 6) if _finite(delta) is not None else None,
        vacuum_zone=vacuum_zone,
        strong_resistance_nearby=strong_resistance_nearby,
        expected_move_pct=expected_move_pct,
        expected_move=expected_move,
        target_price=target_price,
        confidence=confidence,
        move_quality=move_quality,
        final_expected_move=final_expected_move,
        lower_range=lower_range,
        upper_range=upper_range,
        iv_move=iv_move,
        straddle_reference=straddle_reference,
        rv_move=rv_move,
        magnitude_quality=move_quality,
        indicator_score=indicator_score,
        volume_score=volume_score,
        bidask_score=bidask_score,
        oi_score=oi_score,
        data_quality_flags=list(flags),
        valid=valid,
    )

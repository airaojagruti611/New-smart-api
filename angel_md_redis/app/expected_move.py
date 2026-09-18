"""
app/expected_move.py
───────────────────────
Module 8 — Expected Move Calculator (pure prediction).

Predicts Direction, Magnitude, and Magnitude-Quality for an underlying over
a horizon. No strike, stop-loss, or risk decisions live here.

Inputs (all optional except a valid spot for a numeric range):
  spot_price, implied_volatility, horizon_minutes,
  atm_call_mid, atm_put_mid, realized_volatility,
  indicator_score (-2..+2), volume_score (-2..+2),
  bidask_score (-1..+1), oi_score (-2..+2)

Two inputs historically had no upstream source:
  - indicator_score      now published by run_momentum_confirm.py
                         (md:indicator:score:latest:{SYMBOL})
  - realized_volatility  (no RV calculator publishes a vol number)
Callers pass None only when the key is missing. Missing values are flagged
in data_quality_flags and treated as 0.0 / unused — never fabricated.

IV-based 1-sigma magnitude (annualized IV, NSE session):
  iv_move = spot * σ * sqrt(horizon_minutes / (trading_minutes_per_day * 252))
σ is an annualized decimal (0.18 = 18%). Values > 1.0 are treated as
percent (Angel Greeks typically publish "18.5", not 0.185).

Spec 3.7 — direction never shrinks magnitude:
  final_expected_move is the IV (or fallback) magnitude as-is.
  Range is always spot ± magnitude. A Neutral 0.057 direction score
  leaves a ±215.4 IV-move untouched; it is a label, not a multiplier.

No I/O here — pure dataclasses + functions. Redis wiring lives in
run_expected_move.py.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

# NSE cash/FO session is 09:15–15:30 = 375 minutes.
TRADING_MINUTES_PER_DAY = 375.0
TRADING_DAYS_PER_YEAR = 252.0
DEFAULT_HORIZON_MINUTES = 60.0

# Direction score = (indicator + volume + bidask + oi) / DIRECTION_DENOM
# Max |sum| = 2+2+1+2 = 7, matching the spec's -7..+7 total / 7 normalize.
DIRECTION_DENOM = 7.0
DIRECTION_NEUTRAL_ABS = 0.20
DIRECTION_STRONG_ABS = 0.50

# IV vs straddle agreement bands for magnitude_quality.
QUALITY_HIGH_MAX_DIVERGENCE = 0.20
QUALITY_MEDIUM_MAX_DIVERGENCE = 0.40

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


def as_annualized_decimal(vol: Optional[float]) -> Optional[float]:
    """
    Accept either a decimal (0.18) or a percent (18.0 / "18.5" from Angel Greeks).
    Values > 1.0 are treated as percent. Non-positive / non-finite -> None.
    """
    if vol is None:
        return None
    try:
        v = float(vol)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v) or v <= 0:
        return None
    if v > 1.0:
        v = v / 100.0
    return v


def _finite_positive(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(x) or x <= 0:
        return None
    return x


# ── Spec contract ───────────────────────────────────────────────────────

def validate_inputs(
    spot_price: Optional[float],
    implied_volatility: Optional[float],
    horizon_minutes: Optional[float],
    atm_call_mid: Optional[float] = None,
    atm_put_mid: Optional[float] = None,
    realized_volatility: Optional[float] = None,
    indicator_score: Optional[float] = None,
    volume_score: Optional[float] = None,
    bidask_score: Optional[float] = None,
    oi_score: Optional[float] = None,
    trading_minutes_per_day: float = TRADING_MINUTES_PER_DAY,
) -> List[str]:
    """Return data_quality_flags. Never raises; missing/invalid inputs are flagged."""
    flags: List[str] = []

    if _finite_positive(spot_price) is None:
        flags.append("invalid_spot_price")

    if _finite_positive(horizon_minutes) is None:
        flags.append("invalid_horizon_minutes")

    if _finite_positive(trading_minutes_per_day) is None:
        flags.append("invalid_trading_minutes_per_day")

    if as_annualized_decimal(implied_volatility) is None:
        flags.append("missing_implied_volatility")

    if _finite_positive(atm_call_mid) is None:
        flags.append("missing_atm_call_mid")
    if _finite_positive(atm_put_mid) is None:
        flags.append("missing_atm_put_mid")

    if as_annualized_decimal(realized_volatility) is None:
        flags.append("missing_realized_volatility")

    if indicator_score is None:
        flags.append("missing_indicator_score")
    if volume_score is None:
        flags.append("missing_volume_score")
    if bidask_score is None:
        flags.append("missing_bidask_score")
    if oi_score is None:
        flags.append("missing_oi_score")

    return flags


def calculate_iv_move(
    spot_price: float,
    implied_volatility: Optional[float],
    horizon_minutes: float,
    trading_minutes_per_day: float = TRADING_MINUTES_PER_DAY,
    trading_days_per_year: float = TRADING_DAYS_PER_YEAR,
) -> Optional[float]:
    """1-sigma expected move from annualized IV over the horizon. None if IV unusable."""
    sigma = as_annualized_decimal(implied_volatility)
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
    """ATM straddle mid (call + put). Market's own expected-move price tag."""
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
    """Same 1-sigma formula as IV, using realized vol. None when RV is not supplied."""
    return calculate_iv_move(
        spot_price,
        realized_volatility,
        horizon_minutes,
        trading_minutes_per_day=trading_minutes_per_day,
        trading_days_per_year=trading_days_per_year,
    )


def calculate_direction_score(
    indicator_score: Optional[float],
    volume_score: Optional[float],
    bidask_score: Optional[float],
    oi_score: Optional[float],
) -> Tuple[float, str]:
    """
    total = indicator + volume + bidask + oi   # missing treated as 0, still / 7
    score = total / 7                          # -1..+1
    Label bands: |s| < 0.20 Neutral; |s| >= 0.50 Strong.
    """
    ind = 0.0 if indicator_score is None else float(indicator_score)
    vol = 0.0 if volume_score is None else float(volume_score)
    ba = 0.0 if bidask_score is None else float(bidask_score)
    oi = 0.0 if oi_score is None else float(oi_score)

    ind = max(-2.0, min(2.0, ind))
    vol = max(-2.0, min(2.0, vol))
    ba = max(-1.0, min(1.0, ba))
    oi = max(-2.0, min(2.0, oi))

    score = round((ind + vol + ba + oi) / DIRECTION_DENOM, 4)
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


def calculate_range(
    spot_price: float,
    magnitude: Optional[float],
    direction_score: float = 0.0,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Spec 3.7: direction_score is accepted so callers can pass it, and is
    deliberately unused. Magnitude is never scaled by direction.
    """
    _ = direction_score  # spec 3.7 — do not multiply magnitude by this
    if magnitude is None or magnitude < 0 or spot_price <= 0:
        return None, None
    return round(spot_price - magnitude, 2), round(spot_price + magnitude, 2)


def calculate_magnitude_quality(
    iv_move: Optional[float],
    straddle_reference: Optional[float],
    rv_move: Optional[float] = None,
) -> str:
    """
    HIGH   — IV and straddle both present and agree within 20%
    MEDIUM — IV present (straddle missing or moderate disagreement)
    LOW    — only straddle/RV fallback, or IV vs straddle diverge > 40%
    NONE   — no usable magnitude source
    """
    iv_ok = iv_move is not None and iv_move > 0
    st_ok = straddle_reference is not None and straddle_reference > 0
    rv_ok = rv_move is not None and rv_move > 0

    if not (iv_ok or st_ok or rv_ok):
        return "NONE"

    if iv_ok and st_ok:
        denom = max(iv_move, straddle_reference)
        divergence = abs(iv_move - straddle_reference) / denom
        if divergence <= QUALITY_HIGH_MAX_DIVERGENCE:
            return "HIGH"
        if divergence <= QUALITY_MEDIUM_MAX_DIVERGENCE:
            return "MEDIUM"
        return "LOW"

    if iv_ok:
        return "MEDIUM"
    return "LOW"


def _pick_magnitude(
    iv_move: Optional[float],
    straddle_reference: Optional[float],
    rv_move: Optional[float],
) -> Optional[float]:
    """IV is primary; straddle then RV are fallbacks. Never a direction-scaled blend."""
    for src in (iv_move, straddle_reference, rv_move):
        if src is not None and src > 0:
            return src
    return None


@dataclass(frozen=True)
class ExpectedMoveResult:
    spot_price: Optional[float]
    implied_volatility: Optional[float]
    horizon_minutes: float
    iv_move: Optional[float]
    straddle_reference: Optional[float]
    rv_move: Optional[float]
    final_expected_move: Optional[float]
    lower_range: Optional[float]
    upper_range: Optional[float]
    direction: str
    direction_score: float
    magnitude_quality: str
    indicator_score: Optional[float]
    volume_score: Optional[float]
    bidask_score: Optional[float]
    oi_score: Optional[float]
    data_quality_flags: List[str]
    valid: bool


def build_output(
    spot_price: Optional[float],
    implied_volatility: Optional[float],
    horizon_minutes: float,
    iv_move: Optional[float],
    straddle_reference: Optional[float],
    rv_move: Optional[float],
    final_expected_move: Optional[float],
    lower_range: Optional[float],
    upper_range: Optional[float],
    direction: str,
    direction_score: float,
    magnitude_quality: str,
    indicator_score: Optional[float],
    volume_score: Optional[float],
    bidask_score: Optional[float],
    oi_score: Optional[float],
    data_quality_flags: Sequence[str],
    valid: bool,
) -> ExpectedMoveResult:
    return ExpectedMoveResult(
        spot_price=spot_price,
        implied_volatility=implied_volatility,
        horizon_minutes=horizon_minutes,
        iv_move=iv_move,
        straddle_reference=straddle_reference,
        rv_move=rv_move,
        final_expected_move=final_expected_move,
        lower_range=lower_range,
        upper_range=upper_range,
        direction=direction,
        direction_score=direction_score,
        magnitude_quality=magnitude_quality,
        indicator_score=indicator_score,
        volume_score=volume_score,
        bidask_score=bidask_score,
        oi_score=oi_score,
        data_quality_flags=list(data_quality_flags),
        valid=valid,
    )


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
        trading_minutes_per_day=trading_minutes_per_day,
    )

    direction_score, direction = calculate_direction_score(
        indicator_score, volume_score, bidask_score, oi_score
    )

    spot = _finite_positive(spot_price)
    horizon = _finite_positive(horizon_minutes) or 0.0
    tpd = _finite_positive(trading_minutes_per_day) or TRADING_MINUTES_PER_DAY
    valid = spot is not None and horizon > 0

    iv_move = None
    straddle_reference = None
    rv_move = None
    if valid:
        iv_move = calculate_iv_move(
            spot, implied_volatility, horizon, tpd, trading_days_per_year
        )
        straddle_reference = calculate_straddle_reference(atm_call_mid, atm_put_mid)
        rv_move = calculate_rv_move(
            spot, realized_volatility, horizon, tpd, trading_days_per_year
        )

    final_expected_move = _pick_magnitude(iv_move, straddle_reference, rv_move)
    lower_range, upper_range = (
        calculate_range(spot, final_expected_move, direction_score) if valid else (None, None)
    )
    magnitude_quality = calculate_magnitude_quality(iv_move, straddle_reference, rv_move)

    sigma = as_annualized_decimal(implied_volatility)
    return build_output(
        spot_price=round(spot, 4) if spot is not None else spot_price,
        implied_volatility=round(sigma, 6) if sigma is not None else None,
        horizon_minutes=float(horizon_minutes) if horizon_minutes is not None else DEFAULT_HORIZON_MINUTES,
        iv_move=iv_move,
        straddle_reference=straddle_reference,
        rv_move=rv_move,
        final_expected_move=final_expected_move,
        lower_range=lower_range,
        upper_range=upper_range,
        direction=direction,
        direction_score=direction_score,
        magnitude_quality=magnitude_quality,
        indicator_score=indicator_score,
        volume_score=volume_score,
        bidask_score=bidask_score,
        oi_score=oi_score,
        data_quality_flags=flags,
        valid=valid,
    )

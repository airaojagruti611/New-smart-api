"""
app/option_pricing.py
───────────────────────
Approved option-pricing model for Module 9 (Greeks Change Predictor).

Broker Greeks (Angel REST optionGreek) are preferred when the API
returns them. When that endpoint is down / AG8004, Module 6
(greeks_poller + joiner) falls back to implying IV from the live
option premium and computing Greeks here. Those values are tagged
source="theoretical_black_scholes" so they are never silently mixed
with broker_api numbers. Module 9 also reprices future scenarios
with the same model.

Model: Black-Scholes-Merton with a continuous dividend/carry yield q.
  d1 = (ln(S/K) + (r - q + 0.5*sigma^2)*T) / (sigma*sqrt(T))
  d2 = d1 - sigma*sqrt(T)

UNIT CONVENTIONS (spec 5.6 / GR-06 requires these be documented and
consistent everywhere):
  - spot, strike, premium: currency units (rupees).
  - iv (sigma), risk_free_rate, dividend_or_carry: annualized decimals
    (0.15 = 15%), same convention as app/expected_move.py.
  - time_to_expiry: YEARS, using DAYS_PER_YEAR_PRICING (calendar-day
    convention, default 365.0) -- deliberately a SEPARATE constant from
    app/expected_move.py's TRADING_DAYS_PER_YEAR (252, trading-day
    convention used there for volatility scaling). Pricing conventionally
    discounts on a calendar-day basis; scaling IV into a move conventionally
    uses trading days. Both are named constants so a caller can align them
    if a single convention is preferred end-to-end.
  - delta: unitless, per 1.00 (100%) change in spot as a fraction (i.e.
    the standard 0..1 / -1..0 convention already used by
    app/greeks_phase.py's CE/PE sign convention).
  - gamma: change in delta per 1.00 change in spot.
  - theta: reported PER CALENDAR DAY (raw annualized theta / DAYS_PER_YEAR_PRICING),
    the conventional retail "premium decay per day" quoting.
  - vega: reported PER 1 VOLATILITY POINT (per 0.01 change in IV, e.g.
    IV 15% -> 16%), matching spec 5.4's "IV +/- 2 volatility points"
    framing, NOT the raw per-100%-vol-change academic convention.

No I/O here -- pure functions/dataclasses.
"""

from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass
from typing import Optional

DAYS_PER_YEAR_PRICING = 365.0  # calendar-day convention for T; see module docstring
MIN_TIME_YEARS = 1e-6           # floor to avoid division by zero at/after expiry
MIN_IV = 1e-6                   # floor to avoid division by zero on degenerate IV
MAX_IV = 5.0                    # 500% annualized — solver cap
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
EXPIRY_CLOSE_HHMM = "15:30"     # NSE F&O close on expiry day


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


@dataclass(frozen=True)
class PricingResult:
    premium: float
    delta: float
    gamma: float
    theta_per_day: float
    vega_per_point: float
    d1: float
    d2: float
    intrinsic_fallback: bool  # True if T or IV had to be floored (expiry-edge / degenerate case)
    source: str = "theoretical_black_scholes"


def _intrinsic_value(spot: float, strike: float, option_type: str) -> float:
    if option_type == "CE":
        return max(spot - strike, 0.0)
    return max(strike - spot, 0.0)


def bs_price_greeks(
    spot: float,
    strike: float,
    option_type: str,
    time_to_expiry_years: float,
    iv: float,
    risk_free_rate: float = 0.0,
    dividend_or_carry: float = 0.0,
) -> PricingResult:
    """
    Reprice one contract at one scenario. option_type: "CE" or "PE".

    At/after expiry (time_to_expiry_years <= 0) or degenerate IV (<= 0),
    falls back to intrinsic value with zero second-order Greeks and a
    boundary delta (1/0 for calls, 0/-1 for puts) rather than dividing by
    zero -- flagged via intrinsic_fallback=True so callers (classify_risk)
    can raise an EXPIRED_OR_DEGENERATE_SCENARIO flag instead of silently
    trusting a Greeks-shaped zero.
    """
    option_type = (option_type or "").strip().upper()
    s, k = float(spot), float(strike)

    fallback = time_to_expiry_years <= 0 or iv <= 0
    t = max(time_to_expiry_years, MIN_TIME_YEARS)
    sigma = max(iv, MIN_IV)

    if fallback:
        premium = _intrinsic_value(s, k, option_type)
        if option_type == "CE":
            delta = 1.0 if s > k else (0.0 if s < k else 0.5)
        else:
            delta = -1.0 if s < k else (0.0 if s > k else -0.5)
        return PricingResult(
            premium=round(premium, 4), delta=round(delta, 4), gamma=0.0,
            theta_per_day=0.0, vega_per_point=0.0, d1=0.0, d2=0.0,
            intrinsic_fallback=True,
        )

    r = risk_free_rate
    q = dividend_or_carry
    sqrt_t = math.sqrt(t)

    d1 = (math.log(s / k) + (r - q + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t

    disc_q = math.exp(-q * t)
    disc_r = math.exp(-r * t)
    nd1 = _norm_pdf(d1)

    if option_type == "CE":
        premium = s * disc_q * _norm_cdf(d1) - k * disc_r * _norm_cdf(d2)
        delta = disc_q * _norm_cdf(d1)
        theta_annual = (
            -(s * disc_q * nd1 * sigma) / (2.0 * sqrt_t)
            - r * k * disc_r * _norm_cdf(d2)
            + q * s * disc_q * _norm_cdf(d1)
        )
    else:
        premium = k * disc_r * _norm_cdf(-d2) - s * disc_q * _norm_cdf(-d1)
        delta = disc_q * (_norm_cdf(d1) - 1.0)
        theta_annual = (
            -(s * disc_q * nd1 * sigma) / (2.0 * sqrt_t)
            + r * k * disc_r * _norm_cdf(-d2)
            - q * s * disc_q * _norm_cdf(-d1)
        )

    gamma = disc_q * nd1 / (s * sigma * sqrt_t)
    vega_raw = s * disc_q * nd1 * sqrt_t          # per 1.00 (100%) IV change
    vega_per_point = vega_raw * 0.01               # per 1 volatility point
    theta_per_day = theta_annual / DAYS_PER_YEAR_PRICING

    return PricingResult(
        premium=round(premium, 4),
        delta=round(delta, 4),
        gamma=round(gamma, 6),
        theta_per_day=round(theta_per_day, 4),
        vega_per_point=round(vega_per_point, 4),
        d1=round(d1, 6),
        d2=round(d2, 6),
        intrinsic_fallback=False,
    )


def time_to_expiry_years(
    expiry_iso: str,
    now: Optional[dt.datetime] = None,
    close_hhmm: str = EXPIRY_CLOSE_HHMM,
) -> Optional[float]:
    """Calendar-year fraction from now to NSE expiry close on YYYY-MM-DD."""
    try:
        y, m, d = (int(x) for x in str(expiry_iso).split("-"))
    except Exception:
        return None
    try:
        hh, mm = (int(x) for x in str(close_hhmm).split(":"))
    except Exception:
        hh, mm = 15, 30
    expiry_dt = dt.datetime(y, m, d, hh, mm, tzinfo=IST)
    now_dt = now if now is not None else dt.datetime.now(tz=IST)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=IST)
    return (expiry_dt - now_dt).total_seconds() / (DAYS_PER_YEAR_PRICING * 24.0 * 3600.0)


def implied_vol(
    spot: float,
    strike: float,
    option_type: str,
    time_to_expiry_years: float,
    premium: float,
    risk_free_rate: float = 0.0,
    dividend_or_carry: float = 0.0,
    lo: float = 1e-4,
    hi: float = MAX_IV,
    tol: float = 1e-6,
    max_iter: int = 60,
) -> Optional[float]:
    """
    Invert bs_price_greeks for IV (annualized decimal) given a market premium.
    Returns None when the quote cannot be solved: non-positive inputs /
    expired, or the premium lies outside the open (price(lo), price(hi))
    bracket (below intrinsic/no-arb lower bound or above the sigma=hi price).
    Never returns the bracket bounds as a fake IV.
    """
    try:
        s, k, p = float(spot), float(strike), float(premium)
        t = float(time_to_expiry_years)
    except (TypeError, ValueError):
        return None
    if s <= 0 or k <= 0 or p <= 0 or t <= 0:
        return None

    def _px(sigma: float) -> float:
        return bs_price_greeks(
            s, k, option_type, t, sigma, risk_free_rate, dividend_or_carry
        ).premium

    p_lo, p_hi = _px(lo), _px(hi)
    # No-solution quotes: premium at/below the sigma->0 price (the no-arb
    # lower bound, e.g. CE S - K*e^-rT) or at/above the sigma=hi price.
    # Returning the bracket bound here used to fabricate IV 0.01% / 500%
    # (and delta 1.0 / garbage Greeks). No solution -> None -> NO_DATA.
    if p <= p_lo or p >= p_hi:
        return None

    a, b = lo, hi
    sol = None
    for _ in range(max_iter):
        mid = 0.5 * (a + b)
        pm = _px(mid)
        if abs(pm - p) < tol:
            sol = mid
            break
        if pm < p:
            a = mid
        else:
            b = mid
    if sol is None:
        sol = 0.5 * (a + b)
    # A root pinned against either bracket edge is not a usable IV.
    if sol <= lo * (1.0 + 1e-3) or sol >= hi * (1.0 - 1e-3):
        return None
    return sol


def greeks_from_market_premium(
    spot: float,
    strike: float,
    option_type: str,
    time_to_expiry_years: float,
    premium: float,
    risk_free_rate: float = 0.0,
    dividend_or_carry: float = 0.0,
) -> Optional[dict]:
    """
    Imply IV from LTP/mid, then return Angel-shaped Greeks.

    impliedVolatility / iv are PERCENT (18.5) to match Angel REST so
    Module 6 phase % changes stay on one scale.
    """
    iv = implied_vol(
        spot, strike, option_type, time_to_expiry_years, premium,
        risk_free_rate, dividend_or_carry,
    )
    if iv is None:
        return None
    pr = bs_price_greeks(
        spot, strike, option_type, time_to_expiry_years, iv,
        risk_free_rate, dividend_or_carry,
    )
    iv_pct = round(iv * 100.0, 4)
    return {
        "delta": pr.delta,
        "gamma": pr.gamma,
        "theta": pr.theta_per_day,
        "vega": pr.vega_per_point,
        "impliedvolatility": iv_pct,
        "iv": iv_pct,
        "source": "theoretical_black_scholes",
    }


def local_approximation(
    current_premium: float,
    current_delta: float,
    current_gamma: float,
    current_vega_per_point: float,
    current_theta_per_day: float,
    d_spot: float,
    d_iv_points: float,
    d_time_days: float,
) -> float:
    """
    Spec 5.6: diagnostic-only local (Taylor) approximation -- NOT the
    production prediction for large moves. Comparing this against
    reprice_option()'s full repricing is the intended QA use.

    Approx. Premium Change = Delta*dS + 0.5*Gamma*(dS)^2 + Vega*dIV + Theta*dt
    dIV is in vol POINTS (matching vega_per_point's unit), dt in DAYS
    (matching theta_per_day's unit) -- consistent with this module's
    documented unit conventions throughout.
    """
    return round(
        current_delta * d_spot
        + 0.5 * current_gamma * (d_spot ** 2)
        + current_vega_per_point * d_iv_points
        + current_theta_per_day * d_time_days,
        4,
    )

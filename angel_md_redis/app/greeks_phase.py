"""
app/greeks_phase.py
───────────────────────
Greeks Analyzer — Trend Phase Engine.

Classifies each option contract's market phase from its own Greeks
history (per-contract state, keyed by tradingsymbol) + the underlying's
price behavior:

  ACCUMULATION  -> NO_TRADE   (quiet positioning, low gamma, stable IV)
  MARKUP        -> BUY CALL / BUY PUT on first entry; HOLD while still
                   in trade (does not re-fire Enter every tick)
  DISTRIBUTION  -> EXIT       (only fires after a MARKUP was flagged —
                                exiting a phase that was never entered
                                isn't meaningful)
  else          -> HOLD / NEUTRAL

Greek sign convention: CE delta lives in [0,1], PE delta in [-1,0]. The
brief's "delta >= 0.5 AND delta <= 0.8" is CE-side notation; PE mirrors
on magnitude (|delta| in the same band) per "BUY PUT (bearish mirror
logic)". Exit "delta decreasing" is also evaluated on |delta| so puts
weaken correctly (-0.60 -> -0.50).

Missing broker Greeks must NOT be coerced to 0.0 — that falsely triggers
Accumulation. When required fields are absent the result is NO_DATA.

Thresholds the brief doesn't pin a number to (gamma low/rising, iv
stable/rising/dropping-sharply, theta "increasing rapidly", price "small"
movement, what counts as "previous resistance") are defaulted below and
are all overridable by the runner.

No I/O here — pure dataclasses + functions/classes. Redis wiring lives
in run_greeks_analyzer.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .logging_setup import setup_logger

log = setup_logger("greeks_phase")

# ── Thresholds (all overridable via constructor) ────────────────────────
GAMMA_LOW_THRESHOLD = 0.02        # gamma below this -> "low gamma" (accumulation)
GAMMA_RISING_PCT = 10.0           # gamma up >= this % vs previous reading -> "increasing"
GAMMA_FALLING_PCT = -10.0         # gamma down >= this % vs previous reading -> "falling"

PRICE_CHANGE_SMALL_PCT = 0.15     # |recent underlying price change %| below this -> "low movement"

IV_STABLE_BAND_PCT = 3.0          # |iv change %| within this -> "stable"
IV_RISING_PCT = 5.0               # iv up >= this % vs previous reading -> "rising"
IV_DROP_SHARP_PCT = -8.0          # iv down >= this % vs previous reading -> "dropping sharply"

THETA_SURGE_PCT = 15.0            # |theta| change >= this % vs previous reading -> "increasing rapidly"

DELTA_ENTRY_MIN = 0.5
DELTA_ENTRY_MAX = 0.8


def _pct_change(curr: Optional[float], prev: Optional[float]) -> Optional[float]:
    if curr is None or prev is None or prev == 0:
        return None
    return round((curr - prev) / abs(prev) * 100.0, 4)


@dataclass(frozen=True)
class PhaseResult:
    phase: str            # "ACCUMULATION" / "MARKUP" / "DISTRIBUTION" / "NEUTRAL" / "NO_DATA"
    action: str            # "NO_TRADE" / "BUY CALL" / "BUY PUT" / "EXIT" / "HOLD"
    side: str               # "CE" / "PE" / ""
    delta: Optional[float]
    gamma: Optional[float]
    theta: Optional[float]
    vega: Optional[float]
    iv: Optional[float]
    delta_pct: Optional[float]
    gamma_pct: Optional[float]
    iv_pct: Optional[float]
    theta_pct: Optional[float]
    price_change_pct: Optional[float]
    breakout: Optional[bool]
    reason: str


class GreeksPhaseTracker:
    """Per-contract (tradingsymbol) stateful phase tracker."""

    def __init__(
        self,
        gamma_low: float = GAMMA_LOW_THRESHOLD,
        gamma_rising_pct: float = GAMMA_RISING_PCT,
        gamma_falling_pct: float = GAMMA_FALLING_PCT,
        price_small_pct: float = PRICE_CHANGE_SMALL_PCT,
        iv_stable_band: float = IV_STABLE_BAND_PCT,
        iv_rising_pct: float = IV_RISING_PCT,
        iv_drop_sharp_pct: float = IV_DROP_SHARP_PCT,
        theta_surge_pct: float = THETA_SURGE_PCT,
        delta_entry_min: float = DELTA_ENTRY_MIN,
        delta_entry_max: float = DELTA_ENTRY_MAX,
    ):
        self.gamma_low = gamma_low
        self.gamma_rising_pct = gamma_rising_pct
        self.gamma_falling_pct = gamma_falling_pct
        self.price_small_pct = price_small_pct
        self.iv_stable_band = iv_stable_band
        self.iv_rising_pct = iv_rising_pct
        self.iv_drop_sharp_pct = iv_drop_sharp_pct
        self.theta_surge_pct = theta_surge_pct
        self.delta_entry_min = delta_entry_min
        self.delta_entry_max = delta_entry_max

        self._prev_delta: Optional[float] = None
        self._prev_gamma: Optional[float] = None
        self._prev_iv: Optional[float] = None
        self._prev_theta: Optional[float] = None
        self._in_markup: bool = False  # gates DISTRIBUTION to only fire post-entry

    def analyze(
        self,
        cp: str,
        delta: Optional[float],
        gamma: Optional[float],
        theta: Optional[float],
        vega: Optional[float],
        iv: Optional[float],
        price_change_pct: Optional[float],
        breakout: Optional[bool] = None,
    ) -> PhaseResult:
        cp = (cp or "").strip().upper()
        side = "CE" if cp == "CE" else ("PE" if cp == "PE" else "")

        # Required for any real phase decision. Do not coerce to 0.0 —
        # missing broker Greeks previously looked like Accumulation.
        if delta is None or gamma is None or iv is None:
            missing = []
            if delta is None:
                missing.append("delta")
            if gamma is None:
                missing.append("gamma")
            if iv is None:
                missing.append("iv")
            result = PhaseResult(
                phase="NO_DATA", action="HOLD", side=side,
                delta=delta, gamma=gamma, theta=theta, vega=vega, iv=iv,
                delta_pct=None, gamma_pct=None, iv_pct=None, theta_pct=None,
                price_change_pct=price_change_pct, breakout=breakout,
                reason=f"missing_greeks:{'+'.join(missing)}",
            )
            log.debug("LOGIC_OUT %s", result)
            return result

        delta_pct = _pct_change(delta, self._prev_delta)
        gamma_pct = _pct_change(gamma, self._prev_gamma)
        iv_pct = _pct_change(iv, self._prev_iv)
        theta_pct = _pct_change(theta, self._prev_theta)

        abs_delta = abs(delta)
        prev_abs_delta = abs(self._prev_delta) if self._prev_delta is not None else None
        abs_delta_pct = _pct_change(abs_delta, prev_abs_delta)

        gamma_rising = gamma_pct is not None and gamma_pct >= self.gamma_rising_pct
        gamma_falling = gamma_pct is not None and gamma_pct <= self.gamma_falling_pct
        iv_rising = iv_pct is not None and iv_pct >= self.iv_rising_pct
        # IV "stable" requires a prior reading; first tick must not count as stable.
        iv_stable = iv_pct is not None and abs(iv_pct) <= self.iv_stable_band
        iv_dropping_sharply = iv_pct is not None and iv_pct <= self.iv_drop_sharp_pct
        # Magnitude: CE and PE both "weaken" when |delta| falls.
        delta_decreasing = abs_delta_pct is not None and abs_delta_pct < 0
        theta_surging = theta_pct is not None and abs(theta_pct) >= self.theta_surge_pct
        price_small = price_change_pct is not None and abs(price_change_pct) < self.price_small_pct
        # breakout is optional context (price vs pivot); when the caller
        # doesn't have it, None means "not evaluated" -> doesn't block entry.
        breakout_ok = True if breakout is None else breakout

        reasons = []

        if self._in_markup:
            # Already in a trade: only look for the exit trigger. Staying
            # in MARKUP re-emits HOLD, not another entry signal.
            exit_now = delta_decreasing or gamma_falling or iv_dropping_sharply or theta_surging
            if exit_now:
                phase, action = "DISTRIBUTION", "EXIT"
                self._in_markup = False
                if delta_decreasing:
                    reasons.append("delta_decreasing")
                if gamma_falling:
                    reasons.append("gamma_falling")
                if iv_dropping_sharply:
                    reasons.append("iv_dropping_sharply")
                if theta_surging:
                    reasons.append("theta_increasing_rapidly")
            else:
                phase, action = "MARKUP", "HOLD"
                reasons.append("in_trade_no_exit_trigger")

        elif (
            self.delta_entry_min <= abs_delta <= self.delta_entry_max
            and gamma_rising
            and iv_rising
            and breakout_ok
        ):
            phase = "MARKUP"
            action = "BUY CALL" if cp == "CE" else ("BUY PUT" if cp == "PE" else "HOLD")
            self._in_markup = True
            reasons.append("delta_in_range+gamma_rising+iv_rising+breakout")

        elif gamma <= self.gamma_low and price_small and iv_stable:
            phase = "ACCUMULATION"
            action = "NO_TRADE"
            reasons.append("low_gamma+low_price_move+iv_stable")

        else:
            phase, action = "NEUTRAL", "HOLD"
            reasons.append("no_phase_match")

        self._prev_delta, self._prev_gamma = delta, gamma
        self._prev_iv, self._prev_theta = iv, theta

        result = PhaseResult(
            phase=phase, action=action, side=side,
            delta=delta, gamma=gamma, theta=theta, vega=vega, iv=iv,
            delta_pct=abs_delta_pct if abs_delta_pct is not None else delta_pct,
            gamma_pct=gamma_pct, iv_pct=iv_pct, theta_pct=theta_pct,
            price_change_pct=price_change_pct, breakout=breakout,
            reason="|".join(reasons),
        )
        log.debug("LOGIC_OUT %s", result)
        return result

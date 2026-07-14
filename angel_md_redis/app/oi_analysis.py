"""
app/oi_analysis.py
───────────────────────
Open Interest Analysis Module — Steps 1-4 (OI change + price/OI buildup
classification). Support/resistance from OI concentration and the
market-positioning-bias aggregate are NOT implemented yet — the spec for
those sections (module objectives #2-#4) hadn't arrived when this was
written. Extend this file, don't replace it, once that arrives.

Required inputs (per the spec): symbol, strike, price, volume,
open_interest, previous_open_interest, previous_price.

Step 4 — OI change:
  oi_change = current_oi - previous_oi
  positive -> new positions added; negative -> positions closed

Buildup classification (standard price x OI-change matrix; the four
output labels the spec names):
                    OI Change +           OI Change -
  Price Change +    LONG_BUILDUP          SHORT_COVERING
  Price Change -    SHORT_BUILDUP         LONG_UNWINDING
  flat / below threshold -> NEUTRAL

Thresholds (PRICE_CHANGE_THRESHOLD_PCT, OI_CHANGE_THRESHOLD_PCT) are not
given in the spec — defaulted small to avoid noise-flapping on
sub-threshold moves, overridable via the runner. Flag if you had specific
values in mind.

No I/O here — pure dataclasses + functions. Redis wiring lives in
run_oi_analysis.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

PRICE_CHANGE_THRESHOLD_PCT = 0.05   # ignore price moves smaller than this
OI_CHANGE_THRESHOLD_PCT = 1.0       # ignore OI moves smaller than this

BUILDUP_LABELS = frozenset({
    "LONG_BUILDUP", "SHORT_BUILDUP", "SHORT_COVERING", "LONG_UNWINDING", "NEUTRAL",
})


def oi_change(current_oi: float, previous_oi: float) -> float:
    """Step 4: oi_change = current_oi - previous_oi."""
    return current_oi - previous_oi


def pct_change(current: float, previous: float) -> Optional[float]:
    if previous is None or previous == 0:
        return None
    return round((current - previous) / previous * 100.0, 4)


@dataclass(frozen=True)
class BuildupResult:
    symbol: str
    strike: float
    price: float
    previous_price: float
    price_change: float
    price_change_pct: Optional[float]
    open_interest: float
    previous_open_interest: float
    oi_change: float
    oi_change_pct: Optional[float]
    buildup_type: str  # LONG_BUILDUP / SHORT_BUILDUP / SHORT_COVERING / LONG_UNWINDING / NEUTRAL


def classify_buildup(
    symbol: str,
    strike: float,
    price: float,
    previous_price: float,
    volume: float,
    current_oi: float,
    previous_oi: float,
    price_threshold_pct: float = PRICE_CHANGE_THRESHOLD_PCT,
    oi_threshold_pct: float = OI_CHANGE_THRESHOLD_PCT,
) -> BuildupResult:
    price_chg = price - (previous_price or 0.0)
    price_chg_pct = pct_change(price, previous_price)
    oi_chg = oi_change(current_oi, previous_oi)
    oi_chg_pct = pct_change(current_oi, previous_oi)

    price_up = price_chg_pct is not None and price_chg_pct > price_threshold_pct
    price_down = price_chg_pct is not None and price_chg_pct < -price_threshold_pct
    oi_up = oi_chg_pct is not None and oi_chg_pct > oi_threshold_pct
    oi_down = oi_chg_pct is not None and oi_chg_pct < -oi_threshold_pct

    if price_up and oi_up:
        buildup = "LONG_BUILDUP"
    elif price_up and oi_down:
        buildup = "SHORT_COVERING"
    elif price_down and oi_up:
        buildup = "SHORT_BUILDUP"
    elif price_down and oi_down:
        buildup = "LONG_UNWINDING"
    else:
        buildup = "NEUTRAL"

    return BuildupResult(
        symbol=symbol,
        strike=strike,
        price=price,
        previous_price=previous_price,
        price_change=round(price_chg, 4),
        price_change_pct=price_chg_pct,
        open_interest=current_oi,
        previous_open_interest=previous_oi,
        oi_change=oi_chg,
        oi_change_pct=oi_chg_pct,
        buildup_type=buildup,
    )


# ── Step 2: Smart money participation ───────────────────────────────────

from collections import deque
from typing import Deque, List


@dataclass
class RollingStat:
    window: int
    _buf: Deque[float] = None  # set in __post_init__

    def __post_init__(self) -> None:
        self._buf = deque(maxlen=max(1, self.window))

    def push(self, v: float) -> None:
        if v and v > 0:
            self._buf.append(v)

    @property
    def avg(self) -> Optional[float]:
        if not self._buf:
            return None
        return sum(self._buf) / len(self._buf)


def smart_money_participation(oi_chg: float, volume: float, avg_volume: Optional[float]) -> bool:
    """
    Step 2: OI_change > 0 AND volume > avg_volume -> large traders entering
    (fresh positioning, not mere rollover/hedging noise).
    """
    if avg_volume is None:
        return False
    return oi_chg > 0 and volume > avg_volume


# ── Step 4: Support / Resistance from OI concentration ──────────────────

CONCENTRATION_MULT = 1.5  # strike's OI > this x avg OI across visible strikes -> flagged S/R candidate


@dataclass(frozen=True)
class OILevel:
    strike: float
    call_oi: float
    put_oi: float


@dataclass(frozen=True)
class OIConcentration:
    resistance_strikes: List[dict]        # [{strike, call_oi, ratio}], sorted strongest first
    support_strikes: List[dict]           # [{strike, put_oi, ratio}], sorted strongest first
    primary_resistance: Optional[float]   # single highest-call-OI strike
    primary_support: Optional[float]      # single highest-put-OI strike


def oi_concentration(levels: List[OILevel], multiplier: float = CONCENTRATION_MULT) -> OIConcentration:
    """
    Step 4: high call OI at a strike -> resistance (traders selling calls,
    capping upside there). High put OI at a strike -> support (traders
    selling puts, defending that level).

    A strike is flagged when its OI exceeds `multiplier` x the average OI
    across all visible strikes on that side. The single highest-OI strike
    per side is ALWAYS reported as primary_resistance/primary_support even
    if it doesn't clear the multiplier — matching the brief's own example,
    which just picks the max at two strikes with no threshold test.
    """
    if not levels:
        return OIConcentration([], [], None, None)

    call_ois = [lv.call_oi for lv in levels if lv.call_oi]
    put_ois = [lv.put_oi for lv in levels if lv.put_oi]
    avg_call = sum(call_ois) / len(call_ois) if call_ois else 0.0
    avg_put = sum(put_ois) / len(put_ois) if put_ois else 0.0

    resistance, support = [], []
    for lv in levels:
        if avg_call > 0 and lv.call_oi > multiplier * avg_call:
            resistance.append({"strike": lv.strike, "call_oi": lv.call_oi, "ratio": round(lv.call_oi / avg_call, 2)})
        if avg_put > 0 and lv.put_oi > multiplier * avg_put:
            support.append({"strike": lv.strike, "put_oi": lv.put_oi, "ratio": round(lv.put_oi / avg_put, 2)})

    resistance.sort(key=lambda x: x["call_oi"], reverse=True)
    support.sort(key=lambda x: x["put_oi"], reverse=True)

    primary_resistance = max(levels, key=lambda lv: lv.call_oi).strike if call_ois else None
    primary_support = max(levels, key=lambda lv: lv.put_oi).strike if put_ois else None

    return OIConcentration(resistance, support, primary_resistance, primary_support)


# ── Step 5: Max Pain ─────────────────────────────────────────────────────

def max_pain(levels: List[OILevel]) -> Optional[float]:
    """
    For each candidate settle price (each listed strike), total payout to
    option buyers = sum over all strikes of:
      call payout = max(settle - K, 0) * call_oi(K)
      put  payout = max(K - settle, 0) * put_oi(K)
    Max Pain = the strike with the LOWEST total payout (least loss to
    option sellers as a group -> where price tends to gravitate near expiry).
    """
    if not levels:
        return None
    best_strike, best_payout = None, None
    for settle in (lv.strike for lv in levels):
        payout = 0.0
        for lv in levels:
            payout += max(settle - lv.strike, 0.0) * lv.call_oi
            payout += max(lv.strike - settle, 0.0) * lv.put_oi
        if best_payout is None or payout < best_payout:
            best_payout, best_strike = payout, settle
    return best_strike


# ── Step 6: Market Positioning Signal ────────────────────────────────────

_BULLISH_BUILDUPS = frozenset({"LONG_BUILDUP", "SHORT_COVERING"})
_BEARISH_BUILDUPS = frozenset({"SHORT_BUILDUP", "LONG_UNWINDING"})


@dataclass(frozen=True)
class PositioningResult:
    signal: str  # "BULLISH_POSITIONING" / "BEARISH_POSITIONING" / "NEUTRAL"
    dominant_buildup: str
    reason: str


def positioning_signal(dominant_buildup: str, concentration: OIConcentration) -> PositioningResult:
    """
    Step 6: combine build-up type + OI concentration, per the brief's own
    two-factor examples:
      Bullish Positioning = Long buildup (or short covering) + strong put OI
      Bearish Positioning = Short buildup (or long unwinding) + strong call OI
    "Strong put/call OI" is read as: that side has at least as many flagged
    concentration strikes as the other, and at least one.
    Max pain and volume participation aren't folded into this boolean —
    the brief's examples only combine buildup + OI concentration; max pain
    and volume are carried alongside in the payload for the caller/
    downstream consumer to weigh separately.
    """
    b = (dominant_buildup or "").strip().upper()
    support_n = len(concentration.support_strikes)
    resistance_n = len(concentration.resistance_strikes)

    if b in _BULLISH_BUILDUPS and support_n >= resistance_n and support_n > 0:
        return PositioningResult("BULLISH_POSITIONING", b, "buildup_bullish+put_oi_dominant")
    if b in _BEARISH_BUILDUPS and resistance_n >= support_n and resistance_n > 0:
        return PositioningResult("BEARISH_POSITIONING", b, "buildup_bearish+call_oi_dominant")
    return PositioningResult("NEUTRAL", b, "balanced_or_insufficient_oi_data")

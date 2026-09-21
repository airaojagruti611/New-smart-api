"""
app/oi_analysis.py
───────────────────────
Open Interest Analysis Module — spec steps 1-6 from
"Option Rider Algo For Algobuilders" (Open Interest Calculations).

Required inputs (per the spec): symbol, strike, price, volume,
open_interest, previous_open_interest, previous_price.

Step 1 — OI change:
  oi_change = current_oi - previous_oi
  positive -> new positions added; negative -> positions closed

Step 2 — Smart money:
  OI_change > 0 AND Volume > avg_volume

Step 3 — Build-up type (spec sign test, no default % filter):
                    OI Change +           OI Change -
  Price Change +    LONG_BUILDUP          SHORT_COVERING
  Price Change -    SHORT_BUILDUP         LONG_UNWINDING
  flat -> NEUTRAL

Step 4 — High call OI -> resistance; high put OI -> support.
Step 5 — Max pain = strike with lowest payout to option buyers.
Step 6 — Market positioning = buildup + OI concentration + max pain
         + volume participation.

No I/O here — pure dataclasses + functions. Redis wiring lives in
run_oi_analysis.py.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque, Iterable, List, Optional, Tuple

# Spec uses raw sign of price_change / oi_change. Optional % filters stay
# available for the runner but default to 0 (spec-faithful).
PRICE_CHANGE_THRESHOLD_PCT = 0.0
OI_CHANGE_THRESHOLD_PCT = 0.0

BUILDUP_LABELS = frozenset({
    "LONG_BUILDUP", "SHORT_BUILDUP", "SHORT_COVERING", "LONG_UNWINDING", "NEUTRAL",
})

_BULLISH_BUILDUPS = frozenset({"LONG_BUILDUP", "SHORT_COVERING"})
_BEARISH_BUILDUPS = frozenset({"SHORT_BUILDUP", "LONG_UNWINDING"})


def oi_change(current_oi: float, previous_oi: float) -> float:
    """Step 1: oi_change = current_oi - previous_oi."""
    return current_oi - previous_oi


def pct_change(current: float, previous: float) -> Optional[float]:
    if previous is None or previous == 0:
        return None
    return round((current - previous) / previous * 100.0, 4)


def _signed_move(change: float, change_pct: Optional[float], threshold_pct: float) -> Tuple[bool, bool]:
    """
    Spec: direction is the sign of the raw change.
    If threshold_pct > 0 AND pct is available, require |pct| > threshold.
    If previous was 0 (pct is None), fall back to the raw sign so a new
    strike with OI going 0 -> N is still classified as OI-up.
    """
    if threshold_pct and threshold_pct > 0 and change_pct is not None:
        return change_pct > threshold_pct, change_pct < -threshold_pct
    return change > 0, change < 0


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

    price_up, price_down = _signed_move(price_chg, price_chg_pct, price_threshold_pct)
    oi_up, oi_down = _signed_move(oi_chg, oi_chg_pct, oi_threshold_pct)

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

@dataclass
class RollingStat:
    window: int
    _buf: Deque[float] = None  # set in __post_init__

    def __post_init__(self) -> None:
        self._buf = deque(maxlen=max(1, self.window))

    def push(self, v: float) -> None:
        # Include zeros so avg_volume is a true window mean (quiet periods
        # pull the average down; a burst then correctly exceeds it).
        try:
            x = float(v)
        except (TypeError, ValueError):
            x = 0.0
        if x < 0:
            x = 0.0
        self._buf.append(x)

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

CONCENTRATION_MULT = 1.5  # extra walls above this x avg OI; primary is always the max


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
    primary_call_oi: float = 0.0
    primary_put_oi: float = 0.0


def _ensure_primary(rows: List[dict], strike: float, oi: float, avg: float, oi_key: str) -> List[dict]:
    """Always include the max-OI strike, even when it does not clear 1.5x avg."""
    if strike is None:
        return rows
    if any(abs(float(x["strike"]) - strike) < 1e-9 for x in rows):
        return rows
    ratio = round(oi / avg, 2) if avg else 0.0
    rows.append({"strike": strike, oi_key: oi, "ratio": ratio})
    rows.sort(key=lambda x: x[oi_key], reverse=True)
    return rows


def oi_concentration(
    levels: List[OILevel],
    multiplier: float = CONCENTRATION_MULT,
    spot: Optional[float] = None,
) -> OIConcentration:
    """
    Step 4: high call OI at a strike -> resistance (traders selling calls,
    capping upside there). High put OI at a strike -> support (traders
    selling puts, defending that level).

    Spec example just picks the max call OI / max put OI with no multiplier.
    Strikes above `multiplier` x average are extra walls; the primary max
    on each side is always reported AND present in the strike lists.

    When `spot` is known, primary resistance is the highest call OI at or
    above spot, and primary support is the highest put OI at or below spot,
    so the two levels cannot collapse onto the same above-spot put wall.
    """
    if not levels:
        return OIConcentration([], [], None, None, 0.0, 0.0)

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

    call_levels = [lv for lv in levels if lv.call_oi]
    put_levels = [lv for lv in levels if lv.put_oi]
    if spot is not None:
        above = [lv for lv in call_levels if lv.strike >= spot]
        below = [lv for lv in put_levels if lv.strike <= spot]
        call_pool = above or call_levels
        put_pool = below or put_levels
    else:
        call_pool, put_pool = call_levels, put_levels

    primary_res_lv = max(call_pool, key=lambda lv: lv.call_oi) if call_pool else None
    primary_sup_lv = max(put_pool, key=lambda lv: lv.put_oi) if put_pool else None
    primary_resistance = primary_res_lv.strike if primary_res_lv is not None else None
    primary_support = primary_sup_lv.strike if primary_sup_lv is not None else None
    primary_call_oi = primary_res_lv.call_oi if primary_res_lv is not None else 0.0
    primary_put_oi = primary_sup_lv.put_oi if primary_sup_lv is not None else 0.0

    if primary_resistance is not None:
        resistance = _ensure_primary(resistance, primary_resistance, primary_call_oi, avg_call, "call_oi")
    if primary_support is not None:
        support = _ensure_primary(support, primary_support, primary_put_oi, avg_put, "put_oi")

    return OIConcentration(
        resistance, support, primary_resistance, primary_support,
        primary_call_oi, primary_put_oi,
    )


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

@dataclass(frozen=True)
class PositioningResult:
    signal: str  # "BULLISH_POSITIONING" / "BEARISH_POSITIONING" / "NEUTRAL"
    dominant_buildup: str
    reason: str


@dataclass(frozen=True)
class BuildupVote:
    buildup: str  # LONG_BUILDUP / SHORT_BUILDUP / NEUTRAL (underlying direction)
    bullish_weight: float
    bearish_weight: float
    n_voted: int


def underlying_direction(cp: str, buildup: str) -> int:
    """
    Map a per-contract buildup onto underlying direction.
    Spec matrix is applied to the option premium; for the underlying:
      CE long-buildup / short-covering -> bullish
      CE short-buildup / long-unwinding -> bearish
      PE long-buildup / short-covering -> bearish (puts bought / shorts covering)
      PE short-buildup / long-unwinding -> bullish (puts sold / longs exiting)
    """
    b = (buildup or "").strip().upper()
    side = (cp or "").strip().upper()
    if side == "CE":
        if b in _BULLISH_BUILDUPS:
            return 1
        if b in _BEARISH_BUILDUPS:
            return -1
    elif side == "PE":
        if b in _BULLISH_BUILDUPS:
            return -1
        if b in _BEARISH_BUILDUPS:
            return 1
    return 0


def vote_dominant_buildup(
    rows: Iterable[Tuple[str, str, float]],
) -> BuildupVote:
    """
    OI-change-weighted vote across the chain.
    rows: (cp, buildup_type, abs_oi_change)
    """
    bull = 0.0
    bear = 0.0
    n = 0
    for cp, buildup, weight in rows:
        direction = underlying_direction(cp, buildup)
        if direction == 0:
            continue
        w = abs(float(weight or 0.0))
        if w <= 0:
            continue
        n += 1
        if direction > 0:
            bull += w
        else:
            bear += w
    if n == 0 or bull == bear:
        return BuildupVote("NEUTRAL", bull, bear, n)
    if bull > bear:
        return BuildupVote("LONG_BUILDUP", bull, bear, n)
    return BuildupVote("SHORT_BUILDUP", bull, bear, n)


def positioning_signal(
    dominant_buildup: str,
    concentration: OIConcentration,
    spot: Optional[float] = None,
    max_pain_strike: Optional[float] = None,
    volume_participation: bool = False,
) -> PositioningResult:
    """
    Step 6: combine build-up type + OI concentration + max pain + volume,
    matching the brief's examples:
      Bullish Positioning = Long buildup (or short covering) + strong put OI
      Bearish Positioning = Short buildup (or long unwinding) + strong call OI
      Neutral = balanced / no buildup / max-pain conflict

    "Strong put/call OI" = the spec's own S/R rule: a primary support
    (highest put OI) or primary resistance (highest call OI) exists.
    Max pain is an expiry magnet: spot below MP pulls up (bullish), spot
    above MP pulls down (bearish). A conflict keeps the signal Neutral.
    Volume participation is recorded in the reason; it does not gate the
    signal on its own (the spec examples do not require it).
    """
    b = (dominant_buildup or "").strip().upper()
    strong_put = concentration.primary_support is not None and concentration.primary_put_oi > 0
    strong_call = concentration.primary_resistance is not None and concentration.primary_call_oi > 0

    mp_bias = 0
    if spot is not None and max_pain_strike is not None:
        if spot < max_pain_strike:
            mp_bias = 1
        elif spot > max_pain_strike:
            mp_bias = -1

    vol_tag = "volume_participation" if volume_participation else "no_volume_participation"
    mp_tag = "max_pain_na"
    if mp_bias > 0:
        mp_tag = "max_pain_bullish"
    elif mp_bias < 0:
        mp_tag = "max_pain_bearish"
    elif spot is not None and max_pain_strike is not None:
        mp_tag = "max_pain_at_spot"

    if b not in _BULLISH_BUILDUPS and b not in _BEARISH_BUILDUPS:
        return PositioningResult("NEUTRAL", b or "NEUTRAL", f"buildup_neutral+{mp_tag}+{vol_tag}")

    if b in _BULLISH_BUILDUPS:
        if not strong_put:
            return PositioningResult("NEUTRAL", b, f"buildup_bullish+no_put_oi+{mp_tag}+{vol_tag}")
        if mp_bias < 0:
            return PositioningResult("NEUTRAL", b, f"buildup_bullish+put_oi_support+max_pain_conflict+{vol_tag}")
        return PositioningResult(
            "BULLISH_POSITIONING", b, f"buildup_bullish+put_oi_support+{mp_tag}+{vol_tag}",
        )

    if not strong_call:
        return PositioningResult("NEUTRAL", b, f"buildup_bearish+no_call_oi+{mp_tag}+{vol_tag}")
    if mp_bias > 0:
        return PositioningResult("NEUTRAL", b, f"buildup_bearish+call_oi_resistance+max_pain_conflict+{vol_tag}")
    return PositioningResult(
        "BEARISH_POSITIONING", b, f"buildup_bearish+call_oi_resistance+{mp_tag}+{vol_tag}",
    )

"""
app/liquidity_score.py
───────────────────────
Liquidity Score Module for Option Chain — Module 1 (Entry Size Calculator)
+ Module 2 (Scale-Out Levels) + composite 0-100 Liquidity Score.

Deliberately reuses signals this pipeline already computes rather than
re-deriving them:
  - spread% vs rolling average  -> from run_bidask_analyzer.py (md:bidask:latest)
  - OI change                    -> from run_oi_analysis.py (md:oi:latest)
  - book depth top-5             -> from md:ticks:opt directly

Two data points the brief wants that this pipeline doesn't store anywhere
(flagged, not silently assumed away):
  - "Average daily volume" (multi-day). No historical daily-volume archive
    exists here yet -> approximated with an intraday rolling average of
    period volume (same RollingStat pattern as oi_analysis's
    AVG_VOLUME_WINDOW). This is today's volume texture, not a true
    multi-day average. Swap in a real daily-volume store if you build one.
  - "OI jumped >20% overnight" (gamma proxy). No prior-calendar-day OI
    snapshot exists -> approximated with SESSION-OPEN OI (first OI value
    seen for that contract each trading day, reset at day change) vs
    current OI. Same caveat as above.

No I/O here — pure dataclasses + functions/classes. Redis wiring lives in
run_liquidity_score.py.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional

# ── Module 1: Entry Size ────────────────────────────────────────────────

OI_CAP_PCT = 0.015          # 1.5% of OI (brief's "1-2%" -> mid-point default)
VOLUME_CAP_PCT = 0.05       # 5% of (approximated) average daily volume
BOOK_DEPTH_MULT = 10.0      # 10x top-3 bid size

VOL_OI_HOT = 3.0
VOL_OI_ACTIVE = 1.5
VOL_OI_NORMAL_HI = 0.5
VOL_OI_NORMAL_LO = 0.1

# Confidence multiplier rows: (vol_oi_min, spread_ratio_max, multiplier)
_CONFIDENCE_ROWS = [
    (1.0, 1.0, 1.0),
    (0.5, 1.5, 0.7),
    (0.2, 2.0, 0.4),
    (0.0, float("inf"), 0.2),
]


def vol_oi_ratio(volume: float, oi: float) -> Optional[float]:
    if not oi or oi <= 0:
        return None
    return round(volume / oi, 4)


def classify_vol_oi(ratio: Optional[float]) -> str:
    if ratio is None:
        return "NO_DATA"
    if ratio > VOL_OI_HOT:
        return "UNUSUAL"
    if ratio > VOL_OI_ACTIVE:
        return "HOT"
    if ratio > VOL_OI_NORMAL_HI:
        return "ACTIVE"
    if ratio >= VOL_OI_NORMAL_LO:
        return "NORMAL"
    return "STALE"


def estimate_opening_ratio(ratio: Optional[float], price_moving_toward_strike: Optional[bool]) -> float:
    """
    Fraction of today's volume estimated to be NEW positions (opening) vs
    closing old ones.
      Vol/OI > 1.0            -> 65% opening
      Vol/OI < 0.3             -> 40% opening (60% closing)
      price moving away from strike -> nudge toward "opening" (+10pp, capped)
      price moving toward strike (near expiry-style close-out) -> nudge
        toward "closing" (-10pp, floored)
      else -> 50/50 baseline
    """
    if ratio is None:
        base = 0.5
    elif ratio > 1.0:
        base = 0.65
    elif ratio < 0.3:
        base = 0.40
    else:
        base = 0.50

    if price_moving_toward_strike is True:
        base = max(0.0, base - 0.10)
    elif price_moving_toward_strike is False:
        base = min(1.0, base + 0.10)

    return round(base, 4)


def expected_oi_change(current_oi: float, volume: float, opening_ratio: float) -> float:
    """Expected new OI = current_oi + (today's volume x opening_ratio)."""
    return round(current_oi + (volume * opening_ratio), 2)


def _confidence_multiplier(ratio: Optional[float], spread_ratio: Optional[float]) -> float:
    """
    Two-column table (Vol/OI band, spread-vs-avg band) -> multiplier.
    When the two columns point to different rows (e.g. high vol/oi but a
    wide spread), take the MORE CONSERVATIVE (lower) of the two matches —
    the brief doesn't specify how to combine a disagreement, and a wide
    spread is a real fill-quality risk regardless of how hot the volume is.
    """
    if ratio is None:
        m_vol = 0.2
    else:
        m_vol = 0.2
        for min_ratio, _max_spread, mult in _CONFIDENCE_ROWS:
            if ratio >= min_ratio:
                m_vol = mult
                break

    if spread_ratio is None:
        m_spread = 0.2
    else:
        m_spread = 0.2
        # spread_ratio ASCENDS with the row index in the table (tighter
        # spread = better = higher multiplier), so walk rows in order and
        # take the last one whose ceiling still admits this spread.
        for _min_ratio, max_spread, mult in _CONFIDENCE_ROWS:
            if spread_ratio <= max_spread:
                m_spread = mult
                break

    return min(m_vol, m_spread)


@dataclass(frozen=True)
class EntrySizeResult:
    oi_cap: float
    volume_cap: float
    depth_cap: float
    max_safe_entry: float
    vol_oi: Optional[float]
    vol_oi_class: str
    confidence_multiplier: float
    final_entry_size: float
    expected_oi: float
    opening_ratio: float
    reason: str


def entry_size(
    oi: float,
    avg_daily_volume: Optional[float],
    today_volume: float,
    bid_top3_size: float,
    spread_ratio: Optional[float],
    price_moving_toward_strike: Optional[bool] = None,
) -> EntrySizeResult:
    oi = max(oi, 0.0)
    avg_daily_volume = avg_daily_volume or 0.0
    today_volume = max(today_volume, 0.0)
    bid_top3_size = max(bid_top3_size, 0.0)

    oi_cap = round(oi * OI_CAP_PCT, 2)
    volume_cap = round(avg_daily_volume * VOLUME_CAP_PCT, 2)
    depth_cap = round(bid_top3_size * BOOK_DEPTH_MULT, 2)

    caps = [c for c in (oi_cap, volume_cap, depth_cap) if c > 0]
    max_safe = min(caps) if caps else 0.0

    ratio = vol_oi_ratio(today_volume, oi)
    ratio_class = classify_vol_oi(ratio)
    mult = _confidence_multiplier(ratio, spread_ratio)

    opening_ratio = estimate_opening_ratio(ratio, price_moving_toward_strike)
    exp_oi = expected_oi_change(oi, today_volume, opening_ratio)

    final = round(max_safe * mult, 2)

    reason = "ok" if max_safe > 0 else "no_liquidity_data"
    return EntrySizeResult(
        oi_cap=oi_cap, volume_cap=volume_cap, depth_cap=depth_cap,
        max_safe_entry=max_safe, vol_oi=ratio, vol_oi_class=ratio_class,
        confidence_multiplier=mult, final_entry_size=final,
        expected_oi=exp_oi, opening_ratio=opening_ratio, reason=reason,
    )


# ── Session-open OI tracker (for the gamma-jump proxy) ──────────────────

def _today_str() -> str:
    return dt.date.today().isoformat()


class SessionOpenOI:
    """First OI value seen for a contract each trading day; resets daily."""

    def __init__(self):
        self._day = _today_str()
        self._open_oi: Dict[str, float] = {}

    def observe(self, tradingsymbol: str, oi: float) -> float:
        today = _today_str()
        if today != self._day:
            self._day = today
            self._open_oi = {}
        if tradingsymbol not in self._open_oi and oi > 0:
            self._open_oi[tradingsymbol] = oi
        return self._open_oi.get(tradingsymbol, oi)

    def jump_pct(self, tradingsymbol: str, current_oi: float) -> Optional[float]:
        open_oi = self._open_oi.get(tradingsymbol)
        if not open_oi or open_oi <= 0:
            return None
        return round((current_oi - open_oi) / open_oi * 100.0, 2)


# ── Module 2: Scale-Out Levels ───────────────────────────────────────────

GAMMA_JUMP_PCT = 20.0            # overnight/session OI jump -> "gamma building here"
OI_WALL_MULT = 2.0               # next strike OI > 2x current -> "wall ahead"
SPREAD_SCALE_TRIGGER = 1.4       # spread% > 1.4x session avg -> scale-out check
PRICE_NEAR_STRIKE_PCT = 0.5      # price within 0.5% of a target strike -> "approaching"

# Liquidity degradation curve multipliers by underlying-move band (Method C)
_DEGRADATION_BANDS = [
    (0.0, 1.0),
    (10.0, 1.3),
    (20.0, 1.8),
    (30.0, 2.75),
]


@dataclass(frozen=True)
class OIClusterLevel:
    strike: float
    oi: float
    rank: int  # 1 = highest OI above entry


def oi_cluster_targets(strikes_oi_above_entry: List[tuple], top_n: int = 3) -> List[OIClusterLevel]:
    """
    Method A. strikes_oi_above_entry: [(strike, oi), ...] for strikes on
    the profitable side of the entry strike. Returns top-N by OI, ranked.
    """
    ranked = sorted(strikes_oi_above_entry, key=lambda x: x[1], reverse=True)[:top_n]
    return [OIClusterLevel(strike=s, oi=o, rank=i + 1) for i, (s, o) in enumerate(ranked)]


def gamma_speed_bump_strikes(
    strikes_oi_jump_pct: Dict[float, Optional[float]],
    threshold_pct: float = GAMMA_JUMP_PCT,
) -> List[float]:
    """Method B proxy. Strikes whose session OI jump exceeds threshold."""
    return sorted(
        strike for strike, jump in strikes_oi_jump_pct.items()
        if jump is not None and jump > threshold_pct
    )


def degradation_multiplier(underlying_move_pct: float) -> float:
    """Method C. underlying_move_pct is the ABS % move since entry."""
    mult = _DEGRADATION_BANDS[0][1]
    for band_pct, band_mult in _DEGRADATION_BANDS:
        if underlying_move_pct >= band_pct:
            mult = band_mult
    return mult


@dataclass(frozen=True)
class ScaleOutCheck:
    conditions: Dict[str, bool]
    conditions_met: int
    exit_pct: int  # 0 / 25 / 40 / 65 (midpoint of "60-70")
    reason: str


def scale_out_decision(
    price_near_top_oi_strike: bool,
    spread_ratio_vs_avg: Optional[float],
    next_strike_oi_is_wall: bool,
    vol_oi_dropping: bool,
    net_delta_flattening: bool,
) -> ScaleOutCheck:
    """
    Decision matrix: 2-of-5 met -> 25%, 3-of-5 -> 40%, 4+/5 -> 65%
    (midpoint of the brief's "60-70%"), trailing a stop on the remainder
    in the 4+ case is a position-management action for the caller, not
    represented in this return value.
    """
    conditions = {
        "near_top_oi_strike": price_near_top_oi_strike,
        "spread_widened": spread_ratio_vs_avg is not None and spread_ratio_vs_avg > SPREAD_SCALE_TRIGGER,
        "oi_wall_ahead": next_strike_oi_is_wall,
        "vol_oi_dropping": vol_oi_dropping,
        "delta_flattening": net_delta_flattening,
    }
    n = sum(1 for v in conditions.values() if v)

    if n >= 4:
        exit_pct = 65
    elif n == 3:
        exit_pct = 40
    elif n == 2:
        exit_pct = 25
    else:
        exit_pct = 0

    return ScaleOutCheck(
        conditions=conditions, conditions_met=n, exit_pct=exit_pct,
        reason=f"{n}_of_5_conditions_met",
    )


# ── Composite Liquidity Score (0-100) ────────────────────────────────────

SCORE_GREEN = 75
SCORE_YELLOW = 50
SCORE_ORANGE = 25


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def vol_oi_component(ratio: Optional[float], cap: float = 3.0, max_pts: float = 30.0) -> float:
    if ratio is None:
        return 0.0
    return round(_clamp(ratio / cap, 0.0, 1.0) * max_pts, 2)


def spread_component(spread_ratio: Optional[float], max_pts: float = 25.0) -> float:
    """Inverted: ratio==1.0 (at average) -> full points; ratio>=2.0 -> 0."""
    if spread_ratio is None:
        return 0.0
    score = 1.0 - _clamp((spread_ratio - 1.0), 0.0, 1.0)
    return round(score * max_pts, 2)


def oi_rank_component(rank: Optional[int], total: int, max_pts: float = 20.0) -> float:
    """rank=1 (highest OI in chain) -> full points, linear decay."""
    if rank is None or total <= 0:
        return 0.0
    return round(_clamp(1.0 - (rank - 1) / total, 0.0, 1.0) * max_pts, 2)


def oi_expansion_component(current_oi: float, expected_oi: float, max_pts: float = 15.0) -> float:
    if current_oi <= 0:
        return 0.0
    growth_pct = (expected_oi - current_oi) / current_oi * 100.0
    return round(_clamp(growth_pct / 20.0, 0.0, 1.0) * max_pts, 2)


def depth_component(bidask_liquidity_score_0_100: Optional[float], max_pts: float = 10.0) -> float:
    """Reuses run_bidask_analyzer.py's own 0-100 depth score, rescaled."""
    if bidask_liquidity_score_0_100 is None:
        return 0.0
    return round(_clamp(bidask_liquidity_score_0_100 / 100.0, 0.0, 1.0) * max_pts, 2)


def classify_score_band(score: float) -> str:
    if score >= SCORE_GREEN:
        return "GREEN"
    if score >= SCORE_YELLOW:
        return "YELLOW"
    if score >= SCORE_ORANGE:
        return "ORANGE"
    return "RED"


@dataclass(frozen=True)
class LiquidityScoreResult:
    score: float
    band: str  # GREEN / YELLOW / ORANGE / RED
    components: Dict[str, float]
    entry_size: EntrySizeResult


def compute_liquidity_score(
    vol_oi: Optional[float],
    spread_ratio: Optional[float],
    oi_rank: Optional[int],
    chain_size: int,
    current_oi: float,
    expected_oi: float,
    bidask_depth_score: Optional[float],
    entry: EntrySizeResult,
) -> LiquidityScoreResult:
    components = {
        "vol_oi": vol_oi_component(vol_oi),
        "spread": spread_component(spread_ratio),
        "oi_rank": oi_rank_component(oi_rank, chain_size),
        "oi_expansion": oi_expansion_component(current_oi, expected_oi),
        "depth": depth_component(bidask_depth_score),
    }
    score = round(sum(components.values()), 2)
    band = classify_score_band(score)
    return LiquidityScoreResult(score=score, band=band, components=components, entry_size=entry)

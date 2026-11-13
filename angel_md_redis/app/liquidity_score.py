"""
app/liquidity_score.py
───────────────────────
Liquidity Score Module for Option Chain — Module 1 (Entry Size Calculator)
+ Module 2 (Scale-Out Levels) + composite 0-100 Liquidity Score.

Deliberately reuses signals this pipeline already computes rather than
re-deriving them:
  - spread% vs rolling average  -> from run_bidask_analyzer.py (md:bidask:latest)
  - book depth top-3            -> from md:ticks:opt bid_depth5[:3]

Persist-backed inputs (wired in run_liquidity_score.py):
  - Average daily volume: mean of prior session volumes (fallback: today).
  - Overnight OI jump (gamma proxy): previous calendar-day OI vs current
    (fallback: first OI persisted for today, survives worker restarts).

Sizes are in contracts/lots when the caller converts Angel quantity by
lot size. No I/O here — Redis wiring lives in run_liquidity_score.py.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, replace
from typing import Dict, List, Optional, Tuple

# ── Module 1: Entry Size ────────────────────────────────────────────────

OI_CAP_PCT = 0.015          # 1.5% of OI (brief's "1-2%" -> mid-point default)
VOLUME_CAP_PCT = 0.05       # 5% of average daily volume
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

# Spec Module 8.3 band actions applied to final entry size.
BAND_SIZE_MULT = {
    "GREEN": 1.0,    # full size
    "YELLOW": 0.6,   # reduce 40%
    "ORANGE": 0.2,   # probe size
    "RED": 0.0,      # do not enter
}

YELLOW_CLUSTER_N = 2        # Yellow: scale-out in 2 tranches only
DEFAULT_CLUSTER_N = 3
YELLOW_EXIT_CAP = 40        # 2-tranche cap (3-of-5 matrix)


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
    if ratio >= VOL_OI_NORMAL_HI:  # spec: 0.5–1.5 Active
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


def qty_to_lots(qty: float, lot_size: Optional[float]) -> float:
    """Angel FO quantity is shares; spec entry size is contracts/lots."""
    lot = float(lot_size or 0.0)
    if lot <= 0:
        return float(qty or 0.0)
    return round(float(qty or 0.0) / lot, 4)


def _confidence_multiplier(ratio: Optional[float], spread_ratio: Optional[float]) -> float:
    """
    Two-column table (Vol/OI band, spread-vs-avg band) -> multiplier.
    When the two columns point to different rows, take the MORE CONSERVATIVE
    (lower) of the two matches. Unusual Vol/OI (>3.0) is capped at the
    probe multiplier regardless of spread — spec flags it as reversal risk,
    not full-size liquidity.
    """
    if ratio is None:
        m_vol = 0.2
    else:
        m_vol = 0.2
        for min_ratio, _max_spread, mult in _CONFIDENCE_ROWS:
            if ratio >= min_ratio:
                m_vol = mult
                break
        if ratio > VOL_OI_HOT:
            m_vol = min(m_vol, 0.2)

    if spread_ratio is None:
        m_spread = 0.2
    else:
        m_spread = 0.2
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
    band_size_mult: float = 1.0


def entry_size(
    oi: float,
    avg_daily_volume: Optional[float],
    today_volume: float,
    bid_top3_size: float,
    spread_ratio: Optional[float],
    price_moving_toward_strike: Optional[bool] = None,
) -> EntrySizeResult:
    oi = max(oi, 0.0)
    today_volume = max(today_volume, 0.0)
    bid_top3_size = max(bid_top3_size, 0.0)
    # Spec: 5% of average daily volume. Today is only the fallback.
    if avg_daily_volume and avg_daily_volume > 0:
        volume_for_cap = float(avg_daily_volume)
    else:
        volume_for_cap = today_volume

    oi_cap = round(oi * OI_CAP_PCT, 2)
    volume_cap = round(volume_for_cap * VOLUME_CAP_PCT, 2)
    depth_cap = round(bid_top3_size * BOOK_DEPTH_MULT, 2)

    ratio = vol_oi_ratio(today_volume, oi)
    ratio_class = classify_vol_oi(ratio)
    mult = _confidence_multiplier(ratio, spread_ratio)
    opening_ratio = estimate_opening_ratio(ratio, price_moving_toward_strike)
    exp_oi = expected_oi_change(oi, today_volume, opening_ratio)

    if oi <= 0:
        max_safe = 0.0
        reason = "no_oi"
    else:
        # Spec: MIN of the three caps. A zero cap is a hard block, not a skip.
        max_safe = min(oi_cap, volume_cap, depth_cap)
        reason = "ok" if max_safe > 0 else "no_liquidity_data"

    final = round(max_safe * mult, 2)
    return EntrySizeResult(
        oi_cap=oi_cap, volume_cap=volume_cap, depth_cap=depth_cap,
        max_safe_entry=max_safe, vol_oi=ratio, vol_oi_class=ratio_class,
        confidence_multiplier=mult, final_entry_size=final,
        expected_oi=exp_oi, opening_ratio=opening_ratio, reason=reason,
    )


def apply_band_to_entry(entry: EntrySizeResult, band: str) -> EntrySizeResult:
    """Apply Green/Yellow/Orange/Red size action after the composite score."""
    band_mult = BAND_SIZE_MULT.get(band, 0.0)
    if entry.max_safe_entry <= 0:
        return replace(entry, final_entry_size=0.0, band_size_mult=band_mult)
    if band == "RED" or band_mult <= 0:
        reason = "red_do_not_enter" if band == "RED" else entry.reason
        return replace(
            entry,
            final_entry_size=0.0,
            band_size_mult=0.0,
            reason=reason,
        )
    final = round(entry.max_safe_entry * entry.confidence_multiplier * band_mult, 2)
    return replace(entry, final_entry_size=final, band_size_mult=band_mult)


def cluster_top_n_for_band(band: str) -> int:
    if band == "YELLOW":
        return YELLOW_CLUSTER_N
    return DEFAULT_CLUSTER_N


# ── Daily OI / volume history (overnight gamma + ADV) ───────────────────

def _today_str() -> str:
    return dt.date.today().isoformat()


ADV_LOOKBACK_DAYS = 5
HIST_MAX_DAYS = 10


@dataclass(frozen=True)
class DaySnap:
    date: str
    oi: float
    vol: float
    open_oi: float


class ContractDayHist:
    """
    Per-contract daily OI/volume snapshots.

    Overnight gamma: (today OI - previous date's last OI) / previous OI.
    If no prior date exists, fall back to today's first persisted OI
    (survives worker restarts, unlike a pure in-memory session open).

    ADV: mean of up to ADV_LOOKBACK_DAYS prior session volumes.
    """

    def __init__(self):
        self._rows: Dict[str, List[DaySnap]] = {}

    def hydrate(self, tradingsymbol: str, rows: List[dict]) -> None:
        out: List[DaySnap] = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            date = str(row.get("date") or "").strip()
            try:
                oi = float(row.get("oi") or 0.0)
                vol = float(row.get("vol") or 0.0)
                open_oi = float(row.get("open_oi") or oi or 0.0)
            except (TypeError, ValueError):
                continue
            if date:
                out.append(DaySnap(date=date, oi=oi, vol=vol, open_oi=open_oi))
        out.sort(key=lambda x: x.date)
        self._rows[tradingsymbol] = out[-HIST_MAX_DAYS:]

    def dump(self, tradingsymbol: str) -> List[dict]:
        return [
            {"date": s.date, "oi": s.oi, "vol": s.vol, "open_oi": s.open_oi}
            for s in self._rows.get(tradingsymbol, [])
        ]

    def observe(self, tradingsymbol: str, oi: float, vol: float, day: Optional[str] = None) -> DaySnap:
        day = day or _today_str()
        oi = max(float(oi or 0.0), 0.0)
        vol = max(float(vol or 0.0), 0.0)
        rows = self._rows.setdefault(tradingsymbol, [])
        if rows and rows[-1].date == day:
            prev = rows[-1]
            open_oi = prev.open_oi if prev.open_oi > 0 else (oi if oi > 0 else 0.0)
            snap = DaySnap(date=day, oi=oi, vol=vol, open_oi=open_oi)
            rows[-1] = snap
        else:
            open_oi = oi if oi > 0 else 0.0
            snap = DaySnap(date=day, oi=oi, vol=vol, open_oi=open_oi)
            rows.append(snap)
            self._rows[tradingsymbol] = rows[-HIST_MAX_DAYS:]
        return snap

    def overnight_jump_pct(
        self, tradingsymbol: str, current_oi: float, day: Optional[str] = None,
    ) -> Tuple[Optional[float], str]:
        day = day or _today_str()
        rows = self._rows.get(tradingsymbol) or []
        prior = [x for x in rows if x.date < day and x.oi > 0]
        if prior and current_oi >= 0:
            base = prior[-1].oi
            if base > 0:
                return round((current_oi - base) / base * 100.0, 2), "prev_day"
        today = next((x for x in rows if x.date == day), None)
        if today and today.open_oi > 0:
            return round((current_oi - today.open_oi) / today.open_oi * 100.0, 2), "session_open"
        return None, "no_data"

    def adv(self, tradingsymbol: str, day: Optional[str] = None) -> Optional[float]:
        day = day or _today_str()
        rows = self._rows.get(tradingsymbol) or []
        vols = [x.vol for x in rows if x.date < day and x.vol > 0][-ADV_LOOKBACK_DAYS:]
        if not vols:
            return None
        return round(sum(vols) / len(vols), 4)


# ── Module 2: Scale-Out Levels ───────────────────────────────────────────

GAMMA_JUMP_PCT = 20.0            # overnight OI jump -> "gamma building here"
OI_WALL_MULT = 2.0               # next strike OI > 2x current -> "wall ahead"
SPREAD_SCALE_TRIGGER = 1.4       # spread% > 1.4x session avg -> scale-out check
PRICE_NEAR_STRIKE_PCT = 0.5      # price within 0.5% of a target strike -> "approaching"
VOL_OI_DROP_PCT = 5.0            # Vol/OI must fall at least 5% vs prior eval
DEGRADATION_SCALE_TRIGGER = 1.5  # Method C: modeled spread >= 1.5x entry

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
    """Method B proxy. Strikes whose overnight (or session-open fallback) OI jump exceeds threshold."""
    return sorted(
        strike for strike, jump in strikes_oi_jump_pct.items()
        if jump is not None and jump > threshold_pct
    )


def approaching_strike(spot: Optional[float], strike: float, pct: float = PRICE_NEAR_STRIKE_PCT) -> bool:
    if not spot or strike <= 0:
        return False
    return abs(spot - strike) / strike * 100.0 <= pct


def degradation_multiplier(underlying_move_pct: float) -> float:
    """Method C. underlying_move_pct is the ABS % move since entry (or session open)."""
    mult = _DEGRADATION_BANDS[0][1]
    for band_pct, band_mult in _DEGRADATION_BANDS:
        if underlying_move_pct >= band_pct:
            mult = band_mult
    return mult


def degradation_exit_floor(deg_mult: float) -> int:
    """
    Spec Method C: begin exiting when modeled spread crosses 1.5x entry.
    Map the degradation curve onto the same tranche grid as the 2-of-5 matrix.
    """
    if deg_mult >= 2.5:      # +30% underlying → 2.75x
        return 65
    if deg_mult >= 1.8:      # +20% → 1.8x
        return 40
    if deg_mult >= DEGRADATION_SCALE_TRIGGER:
        return 25
    return 0


def vol_oi_is_dropping(
    current: Optional[float],
    previous: Optional[float],
    min_drop_pct: float = VOL_OI_DROP_PCT,
) -> bool:
    """Spec condition 4: Vol/OI on the current strike is falling."""
    if current is None or previous is None or previous <= 0:
        return False
    drop_pct = (previous - current) / previous * 100.0
    return drop_pct >= min_drop_pct


def net_delta_is_flattening(current_bias: str, previous_bias: Optional[str]) -> bool:
    """
    Spec condition 5: net delta flattening = momentum was directional
    and has now gone NEUTRAL. Always-NEUTRAL is not 'flattening'.
    """
    cur = (current_bias or "").upper()
    prev = (previous_bias or "").upper()
    return cur == "NEUTRAL" and prev in ("UP", "DOWN")


@dataclass(frozen=True)
class ScaleOutCheck:
    conditions: Dict[str, bool]
    conditions_met: int
    exit_pct: int  # 0 / 25 / 40 / 65 / 100
    reason: str
    advisory: bool = False


def scale_out_decision(
    price_near_top_oi_strike: bool,
    spread_ratio_vs_avg: Optional[float],
    next_strike_oi_is_wall: bool,
    vol_oi_dropping: bool,
    net_delta_flattening: bool,
    in_position: bool = False,
    approaching_gamma_bump: bool = False,
    band: str = "GREEN",
    degradation_mult: float = 1.0,
) -> ScaleOutCheck:
    """
    Decision matrix: 2-of-5 met -> 25%, 3-of-5 -> 40%, 4+/5 -> 65%.
    Method C independently floors the tranche when modeled spread >= 1.5x.
    RED band: 100% immediate exit (spec: if already in, exit regardless of P&L).
    YELLOW band: cap at 40% (2 tranches only).

    This is a signal layer, not an OMS: the recommended tranche is always
    computed. `advisory=True` when no live fill is open.
    """
    conditions = {
        "near_top_oi_strike": bool(price_near_top_oi_strike or approaching_gamma_bump),
        "spread_widened": spread_ratio_vs_avg is not None and spread_ratio_vs_avg > SPREAD_SCALE_TRIGGER,
        "oi_wall_ahead": next_strike_oi_is_wall,
        "vol_oi_dropping": vol_oi_dropping,
        "delta_flattening": net_delta_flattening,
        "degradation_stretched": degradation_mult >= DEGRADATION_SCALE_TRIGGER,
    }
    five = (
        conditions["near_top_oi_strike"],
        conditions["spread_widened"],
        conditions["oi_wall_ahead"],
        conditions["vol_oi_dropping"],
        conditions["delta_flattening"],
    )
    n = sum(1 for v in five if v)
    advisory = not in_position

    if (band or "").upper() == "RED":
        return ScaleOutCheck(
            conditions=conditions, conditions_met=n, exit_pct=100,
            reason="red_force_exit", advisory=advisory,
        )

    if n >= 4:
        exit_pct = 65
    elif n == 3:
        exit_pct = 40
    elif n == 2:
        exit_pct = 25
    else:
        exit_pct = 0

    floor = degradation_exit_floor(degradation_mult)
    if floor > exit_pct:
        exit_pct = floor
        reason = f"method_c_degradation_{degradation_mult}x"
    else:
        reason = f"{n}_of_5_conditions_met"

    if (band or "").upper() == "YELLOW" and exit_pct > YELLOW_EXIT_CAP:
        exit_pct = YELLOW_EXIT_CAP
        reason = f"{reason}_yellow_2_tranche_cap"

    if advisory and exit_pct > 0:
        reason = f"{reason}_advisory"

    return ScaleOutCheck(
        conditions=conditions, conditions_met=n, exit_pct=exit_pct,
        reason=reason, advisory=advisory,
    )


# ── Composite Liquidity Score (0-100) ────────────────────────────────────

SCORE_GREEN = 75
SCORE_YELLOW = 50
SCORE_ORANGE = 25


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def vol_oi_component(ratio: Optional[float], max_pts: float = 30.0) -> float:
    """
    Vol/OI score peaks in the Active band (0.5–1.5). Stale is low; Hot
    decays; Unusual (>3.0) is capped well below Green-making points.
    """
    if ratio is None or ratio <= 0:
        return 0.0
    if ratio < VOL_OI_NORMAL_LO:
        pts = (ratio / VOL_OI_NORMAL_LO) * 8.0
    elif ratio < VOL_OI_NORMAL_HI:
        pts = 8.0 + (ratio - VOL_OI_NORMAL_LO) / (VOL_OI_NORMAL_HI - VOL_OI_NORMAL_LO) * 14.0
    elif ratio <= 1.0:
        pts = 22.0 + (ratio - VOL_OI_NORMAL_HI) / 0.5 * 8.0
    elif ratio <= VOL_OI_ACTIVE:
        pts = 30.0 - (ratio - 1.0) / (VOL_OI_ACTIVE - 1.0) * 4.0
    elif ratio <= VOL_OI_HOT:
        pts = 26.0 - (ratio - VOL_OI_ACTIVE) / (VOL_OI_HOT - VOL_OI_ACTIVE) * 14.0
    else:
        pts = max(6.0, 12.0 - (ratio - VOL_OI_HOT) * 2.0)
    return round(_clamp(pts, 0.0, max_pts), 2)


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


def oi_expansion_component(
    current_oi: float,
    expected_oi: float,
    vol_oi: Optional[float] = None,
    max_pts: float = 15.0,
) -> float:
    if current_oi <= 0:
        return 0.0
    growth_pct = (expected_oi - current_oi) / current_oi * 100.0
    pts = _clamp(growth_pct / 20.0, 0.0, 1.0) * max_pts
    if vol_oi is not None and vol_oi > VOL_OI_HOT:
        pts *= 0.25
    elif vol_oi is not None and vol_oi > VOL_OI_ACTIVE:
        pts *= 0.6
    return round(pts, 2)


def depth_component(
    bid_top3: Optional[float],
    chain_max_bid_top3: Optional[float],
    max_pts: float = 10.0,
) -> float:
    """
    Spec 10 pts: book depth at top 3 bid levels, scored vs the deepest
    top-3 book on the same side of the chain (full points = thickest book).
    """
    if not bid_top3 or bid_top3 <= 0 or not chain_max_bid_top3 or chain_max_bid_top3 <= 0:
        return 0.0
    return round(_clamp(bid_top3 / chain_max_bid_top3, 0.0, 1.0) * max_pts, 2)


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
    bid_top3: Optional[float],
    chain_max_bid_top3: Optional[float],
    entry: EntrySizeResult,
) -> LiquidityScoreResult:
    components = {
        "vol_oi": vol_oi_component(vol_oi),
        "spread": spread_component(spread_ratio),
        "oi_rank": oi_rank_component(oi_rank, chain_size),
        "oi_expansion": oi_expansion_component(current_oi, expected_oi, vol_oi),
        "depth": depth_component(bid_top3, chain_max_bid_top3),
    }
    score = round(sum(components.values()), 2)
    band = classify_score_band(score)
    sized = apply_band_to_entry(entry, band)
    return LiquidityScoreResult(score=score, band=band, components=components, entry_size=sized)

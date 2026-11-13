"""
app/probability_engine.py
───────────────────────
Module 12 — Probability Engine (DECISION.md D7/D8).

Answers one question: "given everything we know right now, what is the
probability this trade will be successful?" as a 0-100 score.

  Inputs -> Normalize -> Apply Weights -> Probability -> Hard Filters
         -> Grade -> JSON

  probability = confluence x 0.25 + direction x 0.20 + intensity x 0.15
              + amd x 0.10 + historical x 0.10 + oi x 0.10
              + liquidity x 0.05 + greeks x 0.05

  Hard filters (reject whatever the score): liquidity < 40, option
  spread% > threshold, regime sideways, option liquidity band RED/POOR,
  historical samples < 30 (flag-only unless enforce_min_samples, D8).

  Bands:  <50 REJECT | 50-65 WATCHLIST | 65-75 SMALL_POSITION
          | 75-85 TRADE | >=85 HIGH_CONVICTION
  Grades: >=85 A+ | >=75 A | >=65 B | >=50 C | else D

The output is a weighted SCORE, not a calibrated probability, until the
trade journal exists to calibrate it against observed win rates.

Bands and grades classify the ROUNDED probability so the published number
and its label never disagree (84.6 -> 85 -> HIGH_CONVICTION).

Missing inputs contribute 0 and are listed in `missing` — never
fabricated. The one exception is `historical`, which is neutral (50) until
the journal has `min_samples` trades (see historical_score).

Never places orders, picks strikes or sets stops. No I/O here — Redis
wiring lives in run_probability.py.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Sequence

from app.freshness import stream_id_ms, ts_field_ms

WEIGHTS = {
    "confluence": 0.25,
    "direction": 0.20,
    "intensity": 0.15,
    "amd": 0.10,
    "historical": 0.10,
    "oi": 0.10,
    "liquidity": 0.05,
    "greeks": 0.05,
}

# ── Hard filters ────────────────────────────────────────────────────────
MIN_LIQUIDITY = 40.0
# Option spread% (ask-bid)/mid*100, same units as app/bidask_analyzer.py.
# The design gives no number ("> Threshold"); 3% is a conservative default
# for stock options — calibrate per symbol.
MAX_SPREAD_PCT = 3.0
MIN_HISTORICAL_SAMPLES = 30
SIDEWAYS_REGIMES = frozenset({"SIDEWAYS", "RANGE", "RANGEBOUND", "CHOPPY"})
POOR_LIQUIDITY_BANDS = frozenset({"RED", "POOR"})

# ── Bands / grades (lower bound inclusive) ──────────────────────────────
BANDS = [
    (85.0, "HIGH_CONVICTION"),
    (75.0, "TRADE"),
    (65.0, "SMALL_POSITION"),
    (50.0, "WATCHLIST"),
]
GRADES = [
    (85.0, "A+"),
    (75.0, "A"),
    (65.0, "B"),
    (50.0, "C"),
]

HISTORICAL_NEUTRAL = 50.0

# Greeks-phase (per trade-side contract) -> AMD score. The phase is computed
# on the contract itself (CE for BUY CALL, PE for BUY PUT), so MARKUP is
# favourable for either side.
_AMD_MAP = {
    "MARKUP": 100.0,
    "ACCUMULATION": 70.0,
    "NEUTRAL": 40.0,
    "DISTRIBUTION": 10.0,
}

_OI_MAP = {
    "BULLISH_POSITIONING": 1.0,
    "NEUTRAL": 0.0,
    "BEARISH_POSITIONING": -1.0,
}

# Same scale as app/expected_move.py's _VOLUME_SIGNAL_MAP (-2..+2).
_VOLUME_MAP = {
    "STRONG BULLISH VOLUME": 2.0,
    "BULLISH VOLUME": 1.0,
    "POSSIBLE WRONG ENTRY": 0.0,
    "BEARISH VOLUME": -1.0,
    "STRONG BEARISH VOLUME": -2.0,
}
VOLUME_SURGE_BONUS = 10.0
# Same threshold as app/volume_analyzer.py ("Strong" = volume / avg > 2.0).
VOLUME_SURGE_RATIO = 2.0


def volume_surge_flag(raw) -> bool:
    """
    entry_volume_surge -> surge? Upstream publishes the ratio volume / avg
    ("2.35"); a ratio above VOLUME_SURGE_RATIO is a surge. Legacy boolean
    strings ("1" / "true" / "yes") are still accepted.
    """
    v = "" if raw is None else str(raw).strip().lower()
    if v in ("1", "true", "yes"):          # legacy flag ("1" is never read as a ratio of 1.0)
        return True
    try:
        x = float(v)
    except ValueError:
        return False
    return math.isfinite(x) and x > VOLUME_SURGE_RATIO


def signal_origin_ms(msg_id, payload: Optional[Mapping] = None) -> Optional[int]:
    """
    Source time of a consumed decision-chain message: the EARLIER of its Redis
    stream-id ms (XADD time, never re-stamped) and the payload's `signal_ts_ms`
    (origin of the chain upstream). None when neither is known (treat as stale).
    """
    times = [t for t in (stream_id_ms(msg_id), ts_field_ms(payload, "signal_ts_ms")) if t is not None and t > 0]
    return min(times) if times else None


def signal_is_stale(origin_ms: Optional[int], now_ms: int, max_age_ms: int) -> bool:
    """Unknown origin is stale (fail closed); max_age_ms <= 0 disables the check."""
    if max_age_ms <= 0:
        return False
    return origin_ms is None or now_ms - origin_ms > max_age_ms


# ── Normalizers (upstream payload -> 0..100, aligned to trade side) ─────

def _clip(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def normalize_side(side: Optional[str]) -> str:
    """'BUY CALL' / 'CE' -> 'CE'; 'BUY PUT' / 'PE' -> 'PE'; else ''."""
    s = (side or "").strip().upper()
    if s in ("CE", "BUY CALL", "CALL"):
        return "CE"
    if s in ("PE", "BUY PUT", "PUT"):
        return "PE"
    return ""


def signed_to_score(value: Optional[float], side: str, scale: float = 1.0) -> Optional[float]:
    """
    Map a bullish(+)/bearish(-) value in [-scale, +scale] onto 0..100 in
    favour of the trade side: CE -> +scale = 100; PE -> -scale = 100.
    """
    if value is None or scale <= 0:
        return None
    side_n = normalize_side(side)
    if not side_n:
        return None
    x = max(-1.0, min(1.0, float(value) / scale))
    if side_n == "PE":
        x = -x
    return round((x + 1.0) * 50.0, 4)


def confluence_score(votes: Sequence[Optional[bool]]) -> Optional[float]:
    """% of known sub-signals agreeing with the trade side (None = unknown)."""
    known = [v for v in votes if v is not None]
    if not known:
        return None
    return round(100.0 * sum(1 for v in known if v) / len(known), 4)


def amd_score(phase: Optional[str]) -> Optional[float]:
    return _AMD_MAP.get((phase or "").strip().upper())


def oi_score(positioning: Optional[str], side: str) -> Optional[float]:
    v = _OI_MAP.get((positioning or "").strip().upper())
    return signed_to_score(v, side)


def intensity_score(
    volume_signal: Optional[str],
    side: str,
    volume_surge: Optional[bool] = None,
    em_confidence: Optional[float] = None,
) -> Optional[float]:
    """
    Mean of the side-aligned volume signal (-2..+2) and Module 8's
    confidence (0..100), whichever are known; +10 on a volume surge.
    """
    parts: List[float] = []
    vol = _VOLUME_MAP.get((volume_signal or "").strip().upper())
    vol_s = signed_to_score(vol, side, scale=2.0)
    if vol_s is not None:
        parts.append(vol_s)
    if em_confidence is not None:
        parts.append(_clip(float(em_confidence)))
    if not parts:
        return None
    score = sum(parts) / len(parts)
    if volume_surge:
        score += VOLUME_SURGE_BONUS
    return round(_clip(score), 4)


def historical_score(
    win_rate: Optional[float],
    samples: Optional[int],
    min_samples: int = MIN_HISTORICAL_SAMPLES,
) -> float:
    """
    Journal win rate (0..1 or 0..100) once `min_samples` trades exist;
    neutral 50 before that so an empty journal neither helps nor hurts.
    """
    if win_rate is None or not samples or samples < min_samples:
        return HISTORICAL_NEUTRAL
    wr = float(win_rate)
    if wr <= 1.0:
        wr *= 100.0
    return round(_clip(wr), 4)


# ── Core ────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ProbabilityInputs:
    symbol: str
    # Weighted components, each already 0..100 (None = missing)
    confluence: Optional[float] = None
    direction: Optional[float] = None
    intensity: Optional[float] = None
    amd: Optional[float] = None
    historical: Optional[float] = None
    oi: Optional[float] = None
    liquidity: Optional[float] = None
    greeks: Optional[float] = None
    # Hard-filter inputs
    spread_pct: Optional[float] = None
    regime: Optional[str] = None
    historical_samples: Optional[int] = None
    option_liquidity_band: Optional[str] = None


@dataclass(frozen=True)
class FilterConfig:
    min_liquidity: float = MIN_LIQUIDITY
    max_spread_pct: float = MAX_SPREAD_PCT
    min_historical_samples: int = MIN_HISTORICAL_SAMPLES
    enforce_min_samples: bool = False  # D8: flag-only during shadow mode


@dataclass(frozen=True)
class ProbabilityResult:
    symbol: str
    probability: int
    raw_probability: float
    grade: str
    decision: str
    components: Dict[str, Optional[float]]
    reject_reasons: List[str] = field(default_factory=list)
    flags: List[str] = field(default_factory=list)
    missing: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """The design's Step 7 JSON object, plus diagnostics."""
        out = {"symbol": self.symbol, "probability": self.probability, "grade": self.grade}
        out.update({k: (None if v is None else round(v, 2)) for k, v in self.components.items()})
        out.update({
            "decision": self.decision,
            "raw_probability": self.raw_probability,
            "reject_reasons": list(self.reject_reasons),
            "flags": list(self.flags),
            "missing": list(self.missing),
        })
        return out


def calculate_raw_probability(components: Dict[str, Optional[float]]) -> float:
    total = 0.0
    for name, w in WEIGHTS.items():
        v = components.get(name)
        if v is not None:
            total += _clip(float(v)) * w
    return round(total, 4)


def _classify(value: float, table) -> Optional[str]:
    for lo, label in table:
        if value >= lo:
            return label
    return None


def assign_grade(probability: float) -> str:
    return _classify(probability, GRADES) or "D"


def assign_band(probability: float) -> str:
    return _classify(probability, BANDS) or "REJECT"


def apply_hard_filters(inp: ProbabilityInputs, cfg: FilterConfig) -> tuple[List[str], List[str]]:
    """Returns (reject_reasons, flags)."""
    reasons: List[str] = []
    flags: List[str] = []

    if inp.liquidity is None:
        reasons.append("liquidity_missing")
    elif inp.liquidity < cfg.min_liquidity:
        reasons.append(f"liquidity_below_{cfg.min_liquidity:g}")

    if inp.spread_pct is not None and inp.spread_pct > cfg.max_spread_pct:
        reasons.append(f"spread_above_{cfg.max_spread_pct:g}pct")

    if (inp.regime or "").strip().upper() in SIDEWAYS_REGIMES:
        reasons.append("regime_sideways")

    if (inp.option_liquidity_band or "").strip().upper() in POOR_LIQUIDITY_BANDS:
        reasons.append("option_liquidity_poor")

    samples = inp.historical_samples or 0
    if samples < cfg.min_historical_samples:
        if cfg.enforce_min_samples:
            reasons.append(f"historical_samples_below_{cfg.min_historical_samples}")
        else:
            flags.append("LOW_SAMPLES")

    return reasons, flags


def compute_probability(
    inp: ProbabilityInputs,
    cfg: Optional[FilterConfig] = None,
) -> ProbabilityResult:
    cfg = cfg or FilterConfig()
    components = {name: getattr(inp, name) for name in WEIGHTS}
    missing = [name for name, v in components.items() if v is None]

    raw = calculate_raw_probability(components)
    prob = int(math.floor(raw + 0.5))  # half-up; round() is banker's
    grade = assign_grade(prob)

    reasons, flags = apply_hard_filters(inp, cfg)
    decision = "REJECT" if reasons else assign_band(prob)

    return ProbabilityResult(
        symbol=(inp.symbol or "").strip().upper(),
        probability=prob,
        raw_probability=raw,
        grade=grade,
        decision=decision,
        components=components,
        reject_reasons=reasons,
        flags=flags,
        missing=missing,
    )

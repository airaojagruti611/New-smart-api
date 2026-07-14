"""
app/composite_score.py
───────────────────────
Integration Architecture — combines six upstream signal modules into one
composite score per underlying symbol:

  Composite = imbalance_score        x 0.25
            + net_delta_direction    x 0.20
            + smart_money_flag       x 0.20
            + spread_health          x 0.15
            + sr_proximity           x 0.10
            + option_flow_alignment  x 0.10

  > +0.60  -> ENTRY
  < -0.40  -> EXIT
  else     -> NEUTRAL

The option-liquidity-exit module (Section 6) is NOT a weighted input — per
the brief it "runs independently and overrides the composite: liquidity
loss always wins." That override is applied by the caller (run_composite.py)
using classify_composite()'s `override_exit` flag, since it requires
knowing which contract(s) are relevant to the underlying — cross-referencing
that is a Redis-lookup concern, not core logic.

ASSUMPTION: the brief's thresholds (+0.60 entry / -0.40 exit) are
asymmetric — this reads as a long-only system (buy calls on bullish
composite, exit on deteriorating momentum), consistent with every prior
module in this series being written in bullish/exit terms rather than
symmetric long/short. No negative-side entry threshold is implemented;
add one (e.g. < -0.60 -> ENTRY_PUT) if bidirectional entries are wanted.

No I/O here — pure functions. Redis wiring lives in run_composite.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

WEIGHTS = {
    "imbalance": 0.25,
    "net_delta": 0.20,
    "smart_money": 0.20,
    "spread_health": 0.15,
    "sr_proximity": 0.10,
    "option_flow": 0.10,
}

ENTRY_THRESHOLD = 0.60
EXIT_THRESHOLD = -0.40

_SMART_MONEY_MAP = {
    "SMART_MONEY_BUY": 1.0,
    "WATCH_ACCUMULATION": 0.5,
    "WATCH": 0.0,
    "NEUTRAL": 0.0,
    "WATCH_DISTRIBUTION": -0.5,
    "SMART_MONEY_SELL": -1.0,
}

_SPREAD_HEALTH_MAP = {
    "HIGH_LIQUIDITY": 1.0,
    "MODERATE_LIQUIDITY": 0.0,
    "THIN_AVOID": -1.0,
    "NO_DATA": 0.0,
}


def imbalance_component(final_score: Optional[float]) -> float:
    if final_score is None:
        return 0.0
    return max(-1.0, min(1.0, final_score))


def net_delta_component(bias: Optional[str]) -> float:
    b = (bias or "").strip().upper()
    if b == "UP":
        return 1.0
    if b == "DOWN":
        return -1.0
    return 0.0


def smart_money_component(composite: Optional[str]) -> float:
    return _SMART_MONEY_MAP.get((composite or "").strip().upper(), 0.0)


def spread_health_component(signal: Optional[str]) -> float:
    return _SPREAD_HEALTH_MAP.get((signal or "").strip().upper(), 0.0)


def sr_proximity_component(
    spot: Optional[float],
    supports: List[dict],
    resistances: List[dict],
) -> float:
    """
    Positive when price sits closer to a defended support (bounce-likely,
    bullish tilt); negative when closer to defended resistance. 0 if
    neither side has a flagged level or spot is unknown.
    """
    if not spot:
        return 0.0
    sup_dists = [abs(spot - s.get("price", spot)) for s in supports if s]
    res_dists = [abs(spot - r.get("price", spot)) for r in resistances if r]
    if not sup_dists and not res_dists:
        return 0.0
    d_sup = min(sup_dists) if sup_dists else float("inf")
    d_res = min(res_dists) if res_dists else float("inf")
    if d_sup == float("inf") and d_res == float("inf"):
        return 0.0
    if d_sup == float("inf"):
        return -1.0
    if d_res == float("inf"):
        return 1.0
    total = d_sup + d_res
    if total <= 0:
        return 0.0
    return round((d_res - d_sup) / total, 4)


def option_flow_component(strikeflow_status: Optional[str], strikeflow_bias: Optional[str]) -> float:
    if (strikeflow_status or "").strip().upper() != "OK":
        return 0.0
    return net_delta_component(strikeflow_bias)


@dataclass(frozen=True)
class CompositeResult:
    components: Dict[str, float]
    score: float
    status: str  # "ENTRY" / "EXIT" / "NEUTRAL" / "FORCE_EXIT"
    override_reason: str


def compute_composite(components: Dict[str, float]) -> float:
    score = sum(components.get(k, 0.0) * w for k, w in WEIGHTS.items())
    return round(max(-1.0, min(1.0, score)), 4)


def classify_composite(
    score: float,
    override_exit: bool = False,
    override_reason: str = "",
) -> Tuple[str, str]:
    if override_exit:
        return "FORCE_EXIT", override_reason or "option_liquidity_exit"
    if score > ENTRY_THRESHOLD:
        return "ENTRY", "score_above_threshold"
    if score < EXIT_THRESHOLD:
        return "EXIT", "score_below_threshold"
    return "NEUTRAL", ""

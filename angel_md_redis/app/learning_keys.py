"""
Shared contract between the Strike Intelligence Engine (strike_select.py),
the Dynamic Capital Engine (capital_alloc.py) and Adaptive Learning
(trade_outcome.py / strike_learning.py). See DECISION.MD.

Pure helpers only -- no I/O. Every module that buckets a trade for
learning MUST use these functions so the producer (strike_learning) and
the consumer (strike_select's historical-performance term) agree on keys.

Redis keys / streams owned by the new modules:
  md:strike:select                 (stream, existing; new fields added)
  md:capital:alloc                 (stream, existing; new fields added)
  md:journal:outcome               (stream, one row per closed paper/real trade)
  md:paper:open:{TRADINGSYMBOL}    (key, JSON of an open paper trade)
  md:learning:strike_stats         (key, JSON {bucket_key: BucketStats-dict})
"""

from __future__ import annotations

from typing import Optional

STREAM_JOURNAL_OUTCOME = "md:journal:outcome"
PAPER_OPEN_PREFIX = "md:paper:open:"
LEARNING_STATS_KEY = "md:learning:strike_stats"


def strike_bucket(strike: float, atm: float, step: float, side: str) -> str:
    """
    Moneyness bucket relative to ATM, in strike steps, from the BUYER's view.
      CE: strike above ATM is OTM;  PE: strike below ATM is OTM.
    Returns "ATM", "OTM1".."OTM3", "ITM1".."ITM3" (|n| > 3 clipped to 3, suffix "+").
    """
    if step <= 0:
        return "ATM"
    n = int(round((float(strike) - float(atm)) / float(step)))
    if (side or "").upper() == "PE":
        n = -n
    if n == 0:
        return "ATM"
    kind = "OTM" if n > 0 else "ITM"
    k = abs(n)
    return f"{kind}{k}" if k <= 3 else f"{kind}3+"


def em_bucket(expected_move_pct: Optional[float]) -> str:
    """Expected move as % of spot -> coarse bucket."""
    if expected_move_pct is None:
        return "EM_NA"
    v = abs(float(expected_move_pct))
    if v < 0.5:
        return "EM_LT0.5"
    if v < 1.0:
        return "EM_0.5-1"
    if v < 2.0:
        return "EM_1-2"
    return "EM_GE2"


def delta_band(delta: Optional[float]) -> str:
    """|delta| -> 0.15-wide band label."""
    if delta is None:
        return "D_NA"
    d = abs(float(delta))
    edges = [0.0, 0.20, 0.35, 0.50, 0.65, 0.80, 1.01]
    for lo, hi in zip(edges, edges[1:]):
        if lo <= d < hi:
            return f"D_{lo:.2f}-{min(hi, 1.0):.2f}"
    return "D_NA"


def bucket_key(regime: str, em_b: str, strike_b: str, delta_b: str) -> str:
    """Canonical learning bucket key: 'REGIME|EM_BUCKET|STRIKE_BUCKET|DELTA_BAND'."""
    return "|".join([(regime or "NO_DATA").upper(), em_b, strike_b, delta_b])

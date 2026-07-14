"""
app/strike_flow.py
───────────────────────
Order Flow for Strike Price Selection — core logic.

Step 1 (stock direction) is read from md:orderflow:latest:{SYMBOL}.bias
(run_order_flow.py) by the runner, not recomputed here.

Step 2 (unusual option activity) — pure functions:
  - Vol/OI ratio + classification (UNUSUAL > 2.0, STRONG > 3.0)
  - Put/call ratio on VOLUME, rolling, with a "dropping sharply" flag
    (call demand rising relative to puts)

Step 3 (strike selection) — select_strike():
  1. bias must resolve to CE (UP) or PE (DOWN); NEUTRAL -> no selection
  2. reject strikes where only one side of the book is being refreshed
     (no real market-maker interest)
  3. reject strikes whose spread_ratio (current/avg spread%, from the
     bid-ask module) is >= 1.5x
  4. among survivors: prefer a CONFIRMED sweep matching direction; tightest
     spread_ratio breaks ties
  5. else prefer ATM
  6. else allow one strike OTM only if Vol/OI > 3.0
  7. else no qualifying strike

No I/O here — pure dataclasses + functions. Redis/cross-module wiring lives
in run_strike_flow.py.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, List, Optional

VOL_OI_UNUSUAL = 2.0
VOL_OI_STRONG_OTM = 3.0

SPREAD_RATIO_OK = 1.5

PCR_WINDOW = 10
PCR_DROP_PCT = 15.0  # PCR drop >= 15% over the window -> "call demand rising"


def vol_oi_ratio(vol: float, oi: float) -> Optional[float]:
    if not oi or oi <= 0:
        return None
    return round(vol / oi, 3)


def classify_vol_oi(ratio: Optional[float]) -> str:
    if ratio is None:
        return "NO_DATA"
    if ratio > VOL_OI_STRONG_OTM:
        return "STRONG"
    if ratio > VOL_OI_UNUSUAL:
        return "UNUSUAL"
    return "NORMAL"


@dataclass
class PutCallRatioTracker:
    """Rolling put/call VOLUME ratio per underlying; flags a sharp drop."""

    window: int = PCR_WINDOW
    _buf: Deque[float] = field(default_factory=deque, repr=False)

    def __post_init__(self) -> None:
        self._buf = deque(maxlen=self.window)

    def update(self, call_vol: float, put_vol: float) -> Optional[float]:
        if call_vol <= 0:
            return None
        pcr = put_vol / call_vol
        self._buf.append(pcr)
        return pcr

    @property
    def dropping_sharply(self) -> bool:
        if len(self._buf) < max(3, self.window // 2):
            return False
        first, last = self._buf[0], self._buf[-1]
        if first <= 0:
            return False
        drop_pct = ((first - last) / first) * 100.0
        return drop_pct >= PCR_DROP_PCT


def moneyness(strike: float, atm: float, step: float, cp: str) -> str:
    """
    ATM: strike == atm.
    CE: OTM = strike > atm, ITM = strike < atm.
    PE: OTM = strike < atm, ITM = strike > atm.
    OTM1/ITM1 = one step out, OTM2+/ITM2+ = two or more.
    """
    if step <= 0:
        return "ATM" if abs(strike - atm) < 1e-6 else "OTM1"
    diff_steps = round((strike - atm) / step)
    if diff_steps == 0:
        return "ATM"
    is_otm = (diff_steps > 0) if cp == "CE" else (diff_steps < 0)
    dist = abs(diff_steps)
    if is_otm:
        return "OTM1" if dist == 1 else "OTM2+"
    return "ITM1" if dist == 1 else "ITM2+"


@dataclass(frozen=True)
class StrikeCandidate:
    tradingsymbol: str
    strike: float
    cp: str  # "CE" / "PE"
    moneyness: str
    vol: float
    oi: float
    vol_oi_ratio: Optional[float]
    vol_oi_class: str
    sweep_signal: str        # from smart_money composite, e.g. "SMART_MONEY_BUY"
    sweep_confirmed: bool
    spread_pct: float
    spread_ratio: Optional[float]  # from bidask module: current/avg spread%
    one_sided_refresh: bool
    bid: float
    ask: float


@dataclass(frozen=True)
class StrikeSelection:
    status: str  # "OK" / "NO_CANDIDATE"
    underlying: str
    bias: str
    chosen: Optional[StrikeCandidate]
    reason: str
    rejected: List[str]  # "{tradingsymbol}:{why}" — observability, not gating


def select_strike(
    candidates: List[StrikeCandidate],
    bias: str,
    underlying: str = "",
) -> StrikeSelection:
    bias = (bias or "").strip().upper()
    if bias == "UP":
        want_cp = "CE"
    elif bias == "DOWN":
        want_cp = "PE"
    else:
        return StrikeSelection("NO_CANDIDATE", underlying, bias, None, "neutral_bias", [])

    rejected: List[str] = []
    survivors: List[StrikeCandidate] = []

    for c in candidates:
        if c.cp != want_cp:
            continue
        if c.one_sided_refresh:
            rejected.append(f"{c.tradingsymbol}:one_sided_refresh")
            continue
        if c.spread_ratio is not None and c.spread_ratio >= SPREAD_RATIO_OK:
            rejected.append(f"{c.tradingsymbol}:spread_wide({c.spread_ratio})")
            continue
        survivors.append(c)

    if not survivors:
        return StrikeSelection("NO_CANDIDATE", underlying, bias, None, "no_survivors", rejected)

    sweeps = [
        c for c in survivors
        if c.sweep_confirmed and (
            (want_cp == "CE" and c.sweep_signal == "SMART_MONEY_BUY")
            or (want_cp == "PE" and c.sweep_signal == "SMART_MONEY_SELL")
        )
    ]
    if sweeps:
        chosen = min(sweeps, key=lambda c: c.spread_ratio if c.spread_ratio is not None else 999.0)
        return StrikeSelection("OK", underlying, bias, chosen, "confirmed_sweep", rejected)

    atm = [c for c in survivors if c.moneyness == "ATM"]
    if atm:
        chosen = min(atm, key=lambda c: c.spread_ratio if c.spread_ratio is not None else 999.0)
        return StrikeSelection("OK", underlying, bias, chosen, "atm_default", rejected)

    otm1_strong = [
        c for c in survivors
        if c.moneyness == "OTM1" and c.vol_oi_ratio is not None and c.vol_oi_ratio > VOL_OI_STRONG_OTM
    ]
    if otm1_strong:
        chosen = max(otm1_strong, key=lambda c: c.vol_oi_ratio or 0.0)
        return StrikeSelection("OK", underlying, bias, chosen, "otm1_strong_vol_oi", rejected)

    return StrikeSelection("NO_CANDIDATE", underlying, bias, None, "no_qualifying_strike", rejected)

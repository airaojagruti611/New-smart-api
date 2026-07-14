from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .candle_types import PivotLevels
from .logging_setup import setup_logger

log = setup_logger("level_entry")


@dataclass(frozen=True)
class LevelEntryResult:
    signal: str  # "BUY CALL" / "BUY PUT" / "NEUTRAL"
    level: str  # "R1" / "R2" / "P" / "S1" / "S2" / ""
    side: str  # "break_up" / "break_down" / ""
    price: float
    strength: str  # "strong" / "base" / ""
    reason: str = ""


def level_entry(
    prev_close: float,
    close: float,
    pivots: PivotLevels,
) -> LevelEntryResult:
    """
    Classic pivot break entry zones.

    BUY CALL: close breaks above R1 (strong), P (base), or R2
    BUY PUT:  close breaks below S1 (strong), P (base), or S2
    Else:     NEUTRAL

    A break requires prior close on/at the level side and current close beyond it
    so the same level does not re-fire every bar while price stays through it.
    """
    log.debug(
        "LOGIC_IN prev_close=%.4f close=%.4f P=%.2f R1=%.2f R2=%.2f S1=%.2f S2=%.2f",
        prev_close,
        close,
        pivots.P,
        pivots.R1,
        pivots.R2,
        pivots.S1,
        pivots.S2,
    )

    # Prefer nearer/stronger levels first (R1/S1), then Pivot (P), then outer (R2/S2).
    if prev_close <= pivots.R1 < close:
        result = LevelEntryResult("BUY CALL", "R1", "break_up", close, "strong", "break_above_R1")
        log.debug("LOGIC_OUT %s", result)
        return result
    if prev_close <= pivots.P < close:
        result = LevelEntryResult("BUY CALL", "P", "break_up", close, "base", "break_above_P")
        log.debug("LOGIC_OUT %s", result)
        return result
    if prev_close <= pivots.R2 < close:
        result = LevelEntryResult("BUY CALL", "R2", "break_up", close, "strong", "break_above_R2")
        log.debug("LOGIC_OUT %s", result)
        return result
    if prev_close >= pivots.S1 > close:
        result = LevelEntryResult("BUY PUT", "S1", "break_down", close, "strong", "break_below_S1")
        log.debug("LOGIC_OUT %s", result)
        return result
    if prev_close >= pivots.P > close:
        result = LevelEntryResult("BUY PUT", "P", "break_down", close, "base", "break_below_P")
        log.debug("LOGIC_OUT %s", result)
        return result
    if prev_close >= pivots.S2 > close:
        result = LevelEntryResult("BUY PUT", "S2", "break_down", close, "strong", "break_below_S2")
        log.debug("LOGIC_OUT %s", result)
        return result

    result = LevelEntryResult("NEUTRAL", "", "", close, "", "no_break")
    log.debug("LOGIC_OUT %s", result)
    return result


def parse_pivots_payload(data: dict) -> Optional[PivotLevels]:
    """Parse JSON stored at md:pivots:prevday:{SYMBOL}."""
    try:
        date = str(data.get("date") or "")
        P = float(data["P"])
        R1 = float(data["R1"])
        S1 = float(data["S1"])
        R2 = float(data["R2"])
        S2 = float(data["S2"])
    except (KeyError, TypeError, ValueError):
        return None
    return PivotLevels(date=date, P=P, R1=R1, S1=S1, R2=R2, S2=S2)

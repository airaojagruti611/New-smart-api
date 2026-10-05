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


@dataclass
class LevelBarDecision:
    """Outcome of feeding one 1m bar into :class:`LevelBreakTracker`."""

    emit: bool  # True -> publish (bar is recent and had a prev close to compare)
    result: Optional[LevelEntryResult]
    prev_close: Optional[float]
    skip_reason: str = ""


class LevelBreakTracker:
    """Per-symbol prev-close state with a bar-age guard.

    * Bars older than ``max_bar_age_ms`` (by their own bar timestamp) only warm
      up the prev-close state; they never produce a signal. This stops history /
      bootstrap / replayed 1m bars from becoming live "breaks".
    * Bars whose timestamp is not newer than the last seen bar (duplicates,
      out-of-order replay) are ignored entirely, so a replayed old bar is never
      compared against a newer close.
    * A bar without a timestamp is not evaluated (fail closed).
    """

    def __init__(self, max_bar_age_ms: int) -> None:
        self.max_bar_age_ms = int(max_bar_age_ms)
        self._prev_close: dict = {}
        self._prev_ts: dict = {}

    def seed(self, symbol: str, close: float, bar_ts_ms: Optional[int]) -> None:
        self._prev_close[symbol] = float(close)
        if bar_ts_ms is not None:
            self._prev_ts[symbol] = int(bar_ts_ms)

    def prev_close(self, symbol: str) -> Optional[float]:
        return self._prev_close.get(symbol)

    def on_bar(
        self,
        symbol: str,
        close: float,
        bar_ts_ms: Optional[int],
        now_ms: int,
        pivots,
    ) -> LevelBarDecision:
        """``pivots`` is a PivotLevels, None, or a zero-arg callable returning
        one (only called for bars that are actually evaluated)."""
        if bar_ts_ms is None:
            return LevelBarDecision(False, None, self._prev_close.get(symbol), "missing_bar_ts")

        last_ts = self._prev_ts.get(symbol)
        if last_ts is not None and bar_ts_ms <= last_ts:
            return LevelBarDecision(False, None, self._prev_close.get(symbol), "out_of_order_or_duplicate")

        prev = self._prev_close.get(symbol)
        self._prev_close[symbol] = float(close)
        self._prev_ts[symbol] = int(bar_ts_ms)

        if prev is None:
            return LevelBarDecision(False, None, None, "seed_prev_close")
        if self.max_bar_age_ms > 0 and now_ms - bar_ts_ms > self.max_bar_age_ms:
            return LevelBarDecision(False, None, prev, "stale_bar_warmup")
        if callable(pivots):
            pivots = pivots()
        if pivots is None:
            return LevelBarDecision(False, None, prev, "no_pivots")
        return LevelBarDecision(True, level_entry(prev, close, pivots), prev, "")

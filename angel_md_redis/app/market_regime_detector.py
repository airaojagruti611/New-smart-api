from dataclasses import dataclass
from typing import Any, Dict
import math


def _safe_float(v):
    try:
        return float(v)
    except Exception:
        return None


@dataclass
class RegimeResult:
    total: int
    advance: int
    decline: int
    neutral: int
    breadth_ratio: float
    advance_pct: float
    decline_pct: float
    neutral_pct: float
    regime: str
    call_alloc_pct: int
    put_alloc_pct: int


class MarketRegimeDetector:
    """
    Maintains latest price state per symbol and computes market breadth regime.
    Uses:
      - ltp: current traded price
      - c: previous close
    """

    def __init__(self, neutral_eps: float = 0.0):
        self.neutral_eps = neutral_eps
        self.latest_by_symbol: Dict[str, Dict[str, float]] = {}

    def ingest_tick(self, fields: Dict[str, Any]) -> None:
        symbol = str(fields.get("symbol") or "").strip().upper()
        if not symbol:
            return

        ltp = _safe_float(fields.get("ltp"))
        prev_close = _safe_float(fields.get("c"))

        if ltp is None or prev_close is None:
            return

        self.latest_by_symbol[symbol] = {
            "ltp": ltp,
            "prev_close": prev_close,
        }

    def compute(self) -> RegimeResult:
        advance = 0
        decline = 0
        neutral = 0

        for row in self.latest_by_symbol.values():
            diff = row["ltp"] - row["prev_close"]
            if diff > self.neutral_eps:
                advance += 1
            elif diff < -self.neutral_eps:
                decline += 1
            else:
                neutral += 1

        total = advance + decline + neutral
        if total == 0:
            return RegimeResult(
                total=0,
                advance=0,
                decline=0,
                neutral=0,
                breadth_ratio=1.0,
                advance_pct=0.0,
                decline_pct=0.0,
                neutral_pct=0.0,
                regime="NO_DATA",
                call_alloc_pct=50,
                put_alloc_pct=50,
            )

        if decline == 0:
            breadth_ratio = math.inf if advance > 0 else 1.0
        else:
            breadth_ratio = advance / decline

        advance_pct = (advance / total) * 100.0
        decline_pct = (decline / total) * 100.0
        neutral_pct = (neutral / total) * 100.0

        if advance_pct >= 60.0 or breadth_ratio >= 1.5:
            regime = "BULLISH"
            call_alloc, put_alloc = 70, 30
        elif decline_pct >= 60.0 or breadth_ratio <= 0.7:
            regime = "BEARISH"
            call_alloc, put_alloc = 30, 70
        else:
            regime = "NEUTRAL"
            call_alloc, put_alloc = 50, 50

        return RegimeResult(
            total=total,
            advance=advance,
            decline=decline,
            neutral=neutral,
            breadth_ratio=breadth_ratio,
            advance_pct=advance_pct,
            decline_pct=decline_pct,
            neutral_pct=neutral_pct,
            regime=regime,
            call_alloc_pct=call_alloc,
            put_alloc_pct=put_alloc,
        )

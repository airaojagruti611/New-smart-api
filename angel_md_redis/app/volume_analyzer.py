from collections import deque
from dataclasses import dataclass
from typing import Optional


@dataclass
class VolumeResult:
    high: float
    low: float
    close: float
    volume: float
    buy_pct: float
    sell_pct: float
    volume_surge: Optional[float]   # None when avg_volume not available
    signal: str                     # "Strong Bullish Volume" | "Bullish Volume" |
                                    # "Strong Bearish Volume" | "Bearish Volume" |
                                    # "Possible Wrong Entry" | "NO_DATA"


class VolumeAnalyzer:
    """
    Estimates buyer vs seller dominance for a single 1-minute OHLCV candle.

    Volume is partitioned by where price closed within the candle range:
      buy_volume  = volume * (close - low)  / range
      sell_volume = volume * (high - close) / range

    Maintains a rolling window of completed candle volumes to compute
    avg_volume for the volume surge filter.
    """

    def __init__(self, avg_window: int = 20):
        self._avg_window = avg_window
        self._vol_history: deque = deque(maxlen=avg_window)

    def record_candle_volume(self, volume: float) -> None:
        """Call after each completed candle so avg_volume accumulates over time."""
        if volume > 0:
            self._vol_history.append(volume)

    @property
    def avg_volume(self) -> Optional[float]:
        if not self._vol_history:
            return None
        return sum(self._vol_history) / len(self._vol_history)

    def analyze(
        self,
        high: float,
        low: float,
        close: float,
        volume: float,
        avg_volume: Optional[float] = None,
    ) -> VolumeResult:
        if avg_volume is None:
            avg_volume = self.avg_volume

        if volume <= 0:
            return VolumeResult(
                high=high, low=low, close=close, volume=volume,
                buy_pct=0.0, sell_pct=0.0,
                volume_surge=None,
                signal="NO_DATA",
            )

        candle_range = high - low

        if candle_range <= 0:
            # Doji: no directional information
            buy_pct = 50.0
            sell_pct = 50.0
        else:
            buy_pct  = ((close - low)  / candle_range) * 100.0
            sell_pct = ((high  - close) / candle_range) * 100.0

        volume_surge: Optional[float] = None
        if avg_volume and avg_volume > 0:
            volume_surge = round(volume / avg_volume, 2)

        surge_confirmed = volume_surge is not None and volume_surge > 2.0

        if buy_pct >= 60.0:
            signal = "Strong Bullish Volume" if surge_confirmed else "Bullish Volume"
        elif sell_pct >= 60.0:
            signal = "Strong Bearish Volume" if surge_confirmed else "Bearish Volume"
        else:
            signal = "Possible Wrong Entry"

        return VolumeResult(
            high=high,
            low=low,
            close=close,
            volume=volume,
            buy_pct=round(buy_pct, 2),
            sell_pct=round(sell_pct, 2),
            volume_surge=volume_surge,
            signal=signal,
        )

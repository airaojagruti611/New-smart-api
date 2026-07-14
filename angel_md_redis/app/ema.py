from __future__ import annotations

from typing import List, Optional, Sequence


def ema(values: Sequence[float], period: int) -> List[Optional[float]]:
    """
    Exponential moving average.

    Seeds with SMA of the first `period` values, then applies:
      EMA_t = alpha * value_t + (1 - alpha) * EMA_{t-1}
    where alpha = 2 / (period + 1).

    Returns a list aligned to `values` with None until seeded.
    """
    n = len(values)
    out: List[Optional[float]] = [None] * n
    if period <= 0 or n < period:
        return out

    alpha = 2.0 / (period + 1)
    seed = sum(values[:period]) / period
    out[period - 1] = seed

    for i in range(period, n):
        prev = out[i - 1]
        assert prev is not None
        out[i] = alpha * values[i] + (1.0 - alpha) * prev

    return out


def ema_on_closes(closes: Sequence[float], period: int) -> List[Optional[float]]:
    return ema(closes, period)

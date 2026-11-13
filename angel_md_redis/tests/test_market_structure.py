"""Tests for app/market_structure.py (Module 18 helpers, DECISION.md §5 P9)."""

from __future__ import annotations

import unittest

from app.candle_types import Candle
from app.market_structure import (
    atr_pct,
    bars_to_candles,
    completed_bars,
    fib_levels,
    last_swing_high,
    last_swing_low,
    swing_points,
    update_bars,
)


def candles(highs, lows=None):
    lows = lows or [h - 1 for h in highs]
    return [Candle(ts_ms=i * 60_000, o=l, h=h, l=l, c=(h + l) / 2, v=0) for i, (h, l) in enumerate(zip(highs, lows))]


class SwingTest(unittest.TestCase):
    def test_fractal_high_and_low(self):
        cs = candles([10, 11, 13, 12, 11, 12, 14, 15], [9, 8, 12, 9, 7, 10, 13, 14])
        highs, lows = swing_points(cs)
        self.assertEqual([h for _, h in highs], [13])
        self.assertEqual([l for _, l in lows], [7])

    def test_last_bars_unconfirmed(self):
        cs = candles([10, 11, 12, 13, 14])        # rising: no confirmed swing
        self.assertIsNone(last_swing_high(cs))
        self.assertIsNone(last_swing_low(cs))

    def test_equal_highs_take_the_first(self):
        cs = candles([10, 11, 13, 13, 12, 11])
        highs, _ = swing_points(cs)
        self.assertEqual(len(highs), 1)
        self.assertEqual(highs[0][0], 2 * 60_000)


class FibAtrTest(unittest.TestCase):
    def test_fib_levels(self):
        f = fib_levels(110.0, 100.0)
        self.assertEqual(f["0.500"], 105.0)
        self.assertEqual(f["0.618"], 103.82)
        self.assertEqual(fib_levels(100.0, 110.0), {})
        self.assertEqual(fib_levels(None, 100.0), {})

    def test_atr_pct(self):
        cs = [Candle(ts_ms=i, o=100, h=101, l=99, c=100, v=0) for i in range(20)]
        self.assertAlmostEqual(atr_pct(cs, 14), 2.0)
        self.assertIsNone(atr_pct(cs[:10], 14))


class PremiumBarsTest(unittest.TestCase):
    def test_build_and_complete(self):
        bars = []
        for ts, px in [(0, 100), (20_000, 103), (40_000, 99), (61_000, 101), (125_000, 104)]:
            bars = update_bars(bars, ts, px)
        self.assertEqual(len(bars), 3)
        self.assertEqual(bars[0], {"t": 0, "o": 100, "h": 103, "l": 99, "c": 99})
        self.assertEqual(len(completed_bars(bars, 125_000)), 2)
        self.assertEqual(bars_to_candles(bars)[0].ts_ms, 60_000)

    def test_ignores_bad_and_old_samples(self):
        bars = update_bars([], 120_000, 100)
        self.assertEqual(update_bars(bars, 130_000, None), bars)
        self.assertEqual(update_bars(bars, 10_000, 90), bars)   # older than the forming bar


if __name__ == "__main__":
    unittest.main()

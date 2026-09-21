"""Greeks Analyzer phase rules: Markup must fire without gamma>=0.02."""

from __future__ import annotations

import unittest

from app.greeks_phase import GreeksPhaseTracker


def _tick(tracker: GreeksPhaseTracker, delta: float, **kwargs):
    kwargs.setdefault("cp", "CE")
    kwargs.setdefault("gamma", 0.011)
    kwargs.setdefault("theta", -1.8)
    kwargs.setdefault("vega", 0.12)
    kwargs.setdefault("iv", 0.22)
    kwargs.setdefault("price_change_pct", 0.04)
    return tracker.analyze(delta=delta, **kwargs)


class NoDataTest(unittest.TestCase):
    def test_missing_greeks_are_no_data_not_accumulation(self):
        t = GreeksPhaseTracker()
        res = t.analyze(
            cp="CE", delta=None, gamma=None, theta=None, vega=None, iv=None,
            price_change_pct=0.01,
        )
        self.assertEqual(res.phase, "NO_DATA")
        self.assertEqual(res.action, "HOLD")
        self.assertIn("missing_greeks", res.reason)


class AccumulationGuardTest(unittest.TestCase):
    def test_atm_band_low_gamma_is_not_accumulation(self):
        t = GreeksPhaseTracker()
        first = _tick(t, 0.55)
        second = _tick(t, 0.55)
        self.assertNotEqual(first.phase, "ACCUMULATION")
        self.assertNotEqual(second.phase, "ACCUMULATION")

    def test_quiet_otm_is_accumulation(self):
        t = GreeksPhaseTracker()
        _tick(t, 0.40)
        res = _tick(t, 0.40)
        self.assertEqual(res.phase, "ACCUMULATION")
        self.assertEqual(res.action, "NO_TRADE")


class MarkupFromDeltaTrendTest(unittest.TestCase):
    def test_rising_call_delta_enters_markup(self):
        t = GreeksPhaseTracker()
        phases = [_tick(t, d).phase for d in (0.50, 0.54, 0.58)]
        self.assertEqual(phases[-1], "MARKUP")
        t2 = GreeksPhaseTracker()
        last = None
        for d in (0.50, 0.54, 0.58):
            last = _tick(t2, d)
        self.assertEqual(last.action, "BUY CALL")
        self.assertIn("delta_rising", last.reason)

    def test_rising_put_delta_magnitude_buys_put(self):
        t = GreeksPhaseTracker()
        last = None
        for d in (-0.50, -0.54, -0.58):
            last = _tick(t, d, cp="PE")
        self.assertEqual(last.phase, "MARKUP")
        self.assertEqual(last.action, "BUY PUT")

    def test_markup_holds_then_exits_on_delta_drop(self):
        t = GreeksPhaseTracker()
        for d in (0.50, 0.54, 0.58):
            _tick(t, d)
        hold = _tick(t, 0.58)
        self.assertEqual(hold.phase, "MARKUP")
        self.assertEqual(hold.action, "HOLD")
        exit_res = _tick(t, 0.52)
        self.assertEqual(exit_res.phase, "DISTRIBUTION")
        self.assertEqual(exit_res.action, "EXIT")


if __name__ == "__main__":
    unittest.main()

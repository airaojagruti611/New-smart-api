"""Module 17: volatility-scaled risk budget in ICARE."""

from __future__ import annotations

import unittest

from app.icare import ICAREConfig, evaluate, max_risk_allowed, vol_risk_factor
from run_icare import build_inputs, expected_move_pct
from tests.test_icare import _inp, _pf


class VolFactorTest(unittest.TestCase):
    def test_disabled_by_default(self):
        cfg = ICAREConfig()
        self.assertEqual(vol_risk_factor(5.0, cfg), 1.0)
        self.assertEqual(vol_risk_factor(None, cfg), 1.0)

    def test_scales_down_clamped(self):
        cfg = ICAREConfig(vol_risk_baseline_pct=1.0, vol_risk_min_factor=0.5)
        self.assertEqual(vol_risk_factor(2.0, cfg), 0.5)
        self.assertEqual(vol_risk_factor(1.25, cfg), 0.8)
        self.assertEqual(vol_risk_factor(10.0, cfg), 0.5)       # clamped at min

    def test_never_above_one_by_default(self):
        cfg = ICAREConfig(vol_risk_baseline_pct=1.0)
        self.assertEqual(vol_risk_factor(0.2, cfg), 1.0)
        self.assertEqual(vol_risk_factor(0.0, cfg), 1.0)
        self.assertEqual(vol_risk_factor(None, cfg), 1.0)

    def test_opt_in_raise(self):
        cfg = ICAREConfig(vol_risk_baseline_pct=1.0, vol_risk_max_factor=1.5)
        self.assertEqual(vol_risk_factor(0.5, cfg), 1.5)

    def test_max_risk_allowed_factor(self):
        cfg = ICAREConfig()
        self.assertEqual(max_risk_allowed(750_000.0, cfg), 7500.0)
        self.assertEqual(max_risk_allowed(750_000.0, cfg, 0.5), 3750.0)


class EvaluateVolTest(unittest.TestCase):
    def test_default_unchanged(self):
        base = evaluate(_inp(), _pf())
        same = evaluate(_inp(expected_move_pct=4.0), _pf())
        self.assertEqual(base.recommended_lots, same.recommended_lots)
        self.assertEqual(same.vol_risk_factor, 1.0)
        self.assertNotIn("VOL_RISK_SCALED", same.flags)

    def test_high_vol_cuts_risk_lots(self):
        cfg = ICAREConfig(vol_risk_baseline_pct=1.0)
        base = evaluate(_inp(), _pf(), cfg)
        res = evaluate(_inp(expected_move_pct=2.0), _pf(), cfg)
        self.assertEqual(res.vol_risk_factor, 0.5)
        self.assertIn("VOL_RISK_SCALED", res.flags)
        self.assertEqual(res.max_risk_allowed, base.max_risk_allowed * 0.5)
        self.assertLess(res.lots_by_risk, base.lots_by_risk)


class AdapterTest(unittest.TestCase):
    def test_expected_move_pct(self):
        self.assertAlmostEqual(expected_move_pct({"expected_move": "-20", "spot": "1000"}), 2.0)
        self.assertIsNone(expected_move_pct({"expected_move": "5"}))
        self.assertIsNone(expected_move_pct({"expected_move": "x", "spot": "100"}))

    def test_build_inputs_carries_it(self):
        inp = build_inputs({"symbol": "SBIN", "expected_move": "10", "spot": "500"}, {}, {})
        self.assertAlmostEqual(inp.expected_move_pct, 2.0)


if __name__ == "__main__":
    unittest.main()

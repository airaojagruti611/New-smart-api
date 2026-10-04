"""Spec-faithful tests for Module 12 Probability Engine."""

from __future__ import annotations

import unittest

from app.probability_engine import (
    WEIGHTS,
    FilterConfig,
    ProbabilityInputs,
    amd_score,
    assign_band,
    assign_grade,
    calculate_raw_probability,
    compute_probability,
    confluence_score,
    historical_score,
    intensity_score,
    normalize_side,
    oi_score,
    signed_to_score,
)


def _inputs(**over) -> ProbabilityInputs:
    """Design's Step 3 worked example; liquid, trending, enough history."""
    base = dict(
        symbol="sbin",
        confluence=90, direction=85, intensity=80, amd=75,
        historical=78, oi=88, liquidity=92, greeks=70,
        spread_pct=0.8, regime="BULLISH", historical_samples=120,
        option_liquidity_band="GREEN",
    )
    base.update(over)
    return ProbabilityInputs(**base)


class WeightsTest(unittest.TestCase):
    def test_weights_sum_to_one(self):
        self.assertAlmostEqual(sum(WEIGHTS.values()), 1.0)

    def test_spec_weights(self):
        self.assertEqual(WEIGHTS, {
            "confluence": 0.25, "direction": 0.20, "intensity": 0.15, "amd": 0.10,
            "historical": 0.10, "oi": 0.10, "liquidity": 0.05, "greeks": 0.05,
        })


class WorkedExampleTest(unittest.TestCase):
    def test_step3_example_is_83_7_published_84(self):
        res = compute_probability(_inputs())
        self.assertAlmostEqual(res.raw_probability, 83.7)
        self.assertEqual(res.probability, 84)
        self.assertEqual(res.grade, "A")
        self.assertEqual(res.decision, "TRADE")
        self.assertEqual(res.reject_reasons, [])
        self.assertEqual(res.missing, [])

    def test_step7_output_shape(self):
        d = compute_probability(_inputs()).to_dict()
        for k in ("symbol", "probability", "grade", "confluence", "direction", "intensity",
                  "amd", "historical", "oi", "liquidity", "greeks", "decision"):
            self.assertIn(k, d)
        self.assertEqual(d["symbol"], "SBIN")

    def test_half_rounds_up_and_label_follows_published_number(self):
        # 84.5 raw -> 85 published -> HIGH_CONVICTION / A+ (not banker's 84)
        comps = dict(confluence=96, direction=100, intensity=100, amd=10,
                     historical=100, oi=100, liquidity=90, greeks=0)
        self.assertAlmostEqual(calculate_raw_probability(comps), 84.5)
        res = compute_probability(_inputs(**comps))
        self.assertEqual(res.probability, 85)
        self.assertEqual(res.decision, "HIGH_CONVICTION")
        self.assertEqual(res.grade, "A+")


class BandsAndGradesTest(unittest.TestCase):
    def test_step5_examples(self):
        self.assertEqual(assign_band(43), "REJECT")
        self.assertEqual(assign_band(72), "SMALL_POSITION")
        self.assertEqual(assign_band(89), "HIGH_CONVICTION")

    def test_band_boundaries_lower_inclusive(self):
        self.assertEqual(assign_band(49.99), "REJECT")
        self.assertEqual(assign_band(50), "WATCHLIST")
        self.assertEqual(assign_band(65), "SMALL_POSITION")
        self.assertEqual(assign_band(75), "TRADE")
        self.assertEqual(assign_band(85), "HIGH_CONVICTION")

    def test_step6_grades(self):
        self.assertEqual(assign_grade(89), "A+")
        self.assertEqual(assign_grade(85), "A+")
        self.assertEqual(assign_grade(75), "A")
        self.assertEqual(assign_grade(65), "B")
        self.assertEqual(assign_grade(50), "C")
        self.assertEqual(assign_grade(49), "D")


class HardFilterTest(unittest.TestCase):
    def test_step4_example_high_score_low_liquidity_rejects(self):
        res = compute_probability(_inputs(
            confluence=100, direction=100, intensity=100, amd=100,
            historical=100, oi=100, greeks=100, liquidity=25,
        ))
        self.assertGreaterEqual(res.probability, 92)
        self.assertEqual(res.decision, "REJECT")
        self.assertIn("liquidity_below_40", res.reject_reasons)
        self.assertEqual(res.grade, "A+")  # score kept for diagnostics

    def test_each_filter(self):
        cases = [
            (dict(spread_pct=3.5), "spread_above_3pct"),
            (dict(regime="sideways"), "regime_sideways"),
            (dict(option_liquidity_band="RED"), "option_liquidity_poor"),
            (dict(liquidity=None), "liquidity_missing"),
        ]
        for over, reason in cases:
            res = compute_probability(_inputs(**over))
            self.assertEqual(res.decision, "REJECT", over)
            self.assertIn(reason, res.reject_reasons, over)

    def test_spread_at_threshold_passes(self):
        self.assertEqual(compute_probability(_inputs(spread_pct=3.0)).reject_reasons, [])

    def test_low_samples_flag_only_by_default(self):
        res = compute_probability(_inputs(historical_samples=5))
        self.assertEqual(res.decision, "TRADE")
        self.assertIn("LOW_SAMPLES", res.flags)

    def test_low_samples_rejects_when_enforced(self):
        res = compute_probability(
            _inputs(historical_samples=5),
            FilterConfig(enforce_min_samples=True),
        )
        self.assertEqual(res.decision, "REJECT")
        self.assertIn("historical_samples_below_30", res.reject_reasons)


class MissingInputsTest(unittest.TestCase):
    def test_missing_contribute_zero_and_are_listed(self):
        res = compute_probability(_inputs(greeks=None, amd=None))
        self.assertAlmostEqual(res.raw_probability, 83.7 - 3.5 - 7.5)
        self.assertEqual(res.missing, ["amd", "greeks"])

    def test_out_of_range_components_clipped(self):
        self.assertAlmostEqual(calculate_raw_probability({"confluence": 150}), 25.0)
        self.assertAlmostEqual(calculate_raw_probability({"confluence": -20}), 0.0)


class NormalizerTest(unittest.TestCase):
    def test_side(self):
        self.assertEqual(normalize_side("BUY CALL"), "CE")
        self.assertEqual(normalize_side("pe"), "PE")
        self.assertEqual(normalize_side("NEUTRAL"), "")

    def test_signed_to_score_is_side_aligned(self):
        self.assertEqual(signed_to_score(1.0, "CE"), 100.0)
        self.assertEqual(signed_to_score(1.0, "PE"), 0.0)
        self.assertEqual(signed_to_score(-0.5, "BUY PUT"), 75.0)
        self.assertEqual(signed_to_score(0.0, "CE"), 50.0)
        self.assertEqual(signed_to_score(3.0, "CE", scale=2.0), 100.0)
        self.assertIsNone(signed_to_score(None, "CE"))
        self.assertIsNone(signed_to_score(0.5, ""))

    def test_confluence(self):
        self.assertEqual(confluence_score([True, True, False, None]), round(200 / 3, 4))
        self.assertIsNone(confluence_score([None, None]))

    def test_amd(self):
        self.assertEqual(amd_score("markup"), 100.0)
        self.assertEqual(amd_score("DISTRIBUTION"), 10.0)
        self.assertIsNone(amd_score("NO_DATA"))

    def test_oi(self):
        self.assertEqual(oi_score("BULLISH_POSITIONING", "CE"), 100.0)
        self.assertEqual(oi_score("BULLISH_POSITIONING", "PE"), 0.0)
        self.assertEqual(oi_score("NEUTRAL", "PE"), 50.0)
        self.assertIsNone(oi_score(None, "CE"))

    def test_intensity(self):
        # strong bullish volume (100) and EM confidence 80 -> 90, +10 surge
        self.assertEqual(intensity_score("STRONG BULLISH VOLUME", "CE", False, 80), 90.0)
        self.assertEqual(intensity_score("STRONG BULLISH VOLUME", "CE", True, 80), 100.0)
        self.assertEqual(intensity_score("BEARISH VOLUME", "PE"), 75.0)
        self.assertIsNone(intensity_score(None, "CE"))

    def test_historical_neutral_until_min_samples(self):
        self.assertEqual(historical_score(0.9, 10), 50.0)
        self.assertEqual(historical_score(None, 100), 50.0)
        self.assertEqual(historical_score(0.62, 30), 62.0)
        self.assertEqual(historical_score(71, 45), 71.0)


if __name__ == "__main__":
    unittest.main()

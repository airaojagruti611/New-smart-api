"""Spec-faithful tests for Module 8 Expected Move Engine."""

from __future__ import annotations

import unittest

from app.expected_move import (
    as_annualized_decimal,
    calculate_confidence,
    calculate_direction_score,
    calculate_expected_move_pct,
    calculate_multiplier,
    calculate_total_score,
    classify_move_quality,
    classify_pct_trend,
    combine_trend,
    compute_expected_move,
    MOVE_PCT_SCALE,
    pick_atm_delta,
    resistance_is_nearby,
)


class ScoreStepsTest(unittest.TestCase):
    def test_total_and_normalize_match_spec(self):
        total = calculate_total_score(2, 2, 1, 2)
        self.assertEqual(total, 7.0)
        score, label = calculate_direction_score(2, 2, 1, 2)
        self.assertEqual(score, 1.0)
        self.assertEqual(label, "STRONG_BULLISH")

        total_b = calculate_total_score(-2, -2, -1, -2)
        self.assertEqual(total_b, -7.0)
        score_b, label_b = calculate_direction_score(-2, -2, -1, -2)
        self.assertEqual(score_b, -1.0)
        self.assertEqual(label_b, "STRONG_BEARISH")

    def test_missing_scores_are_zero_not_fabricated(self):
        total = calculate_total_score(None, None, None, None)
        self.assertEqual(total, 0.0)
        score, label = calculate_direction_score(None, None, None, None)
        self.assertEqual(score, 0.0)
        self.assertEqual(label, "NEUTRAL")

    def test_live_reliance_direction(self):
        score, label = calculate_direction_score(2.0, 1.0, -0.3628, 2.0)
        self.assertEqual(score, 0.6625)
        self.assertEqual(label, "STRONG_BULLISH")


class MultiplierTest(unittest.TestCase):
    def test_base_is_one(self):
        self.assertEqual(calculate_multiplier(), 1.0)

    def test_spec_bonuses_and_resistance(self):
        self.assertEqual(calculate_multiplier(gamma_trend="up"), 1.4)
        self.assertEqual(calculate_multiplier(gamma_trend="up", iv_trend="up"), 1.7)
        self.assertEqual(
            calculate_multiplier(gamma_trend="up", iv_trend="up", delta=0.6),
            2.0,
        )
        self.assertEqual(
            calculate_multiplier(
                gamma_trend="up", iv_trend="up", delta=0.6, vacuum_zone=True
            ),
            2.4,
        )
        self.assertEqual(
            calculate_multiplier(
                gamma_trend="up",
                iv_trend="up",
                delta=0.6,
                vacuum_zone=True,
                strong_resistance_nearby=True,
            ),
            2.0,
        )

    def test_pe_delta_uses_magnitude(self):
        self.assertEqual(calculate_multiplier(delta=-0.62), 1.3)
        self.assertEqual(calculate_multiplier(delta=0.3), 1.0)
        self.assertEqual(calculate_multiplier(delta=0.9), 1.0)


class MovePctTest(unittest.TestCase):
    def test_scale_matches_spec_example_shape(self):
        # normalized 0.6 * multiplier 1.5 * 0.02 = 0.018 (spec JSON example)
        self.assertEqual(calculate_expected_move_pct(0.6, 1.5), 0.018)
        self.assertEqual(MOVE_PCT_SCALE, 0.02)

    def test_neutral_is_zero_move(self):
        r = compute_expected_move(
            spot_price=1244.4,
            indicator_score=0,
            volume_score=0,
            bidask_score=0,
            oi_score=0,
        )
        self.assertEqual(r.expected_move_pct, 0.0)
        self.assertEqual(r.expected_move, 0.0)
        self.assertEqual(r.target_price, 1244.4)
        self.assertEqual(r.final_expected_move, 0.0)
        self.assertEqual(r.direction, "NEUTRAL")
        self.assertEqual(r.confidence, 0)
        self.assertEqual(r.move_quality, "weak")

    def test_max_bull_without_greeks(self):
        r = compute_expected_move(
            spot_price=10000.0,
            indicator_score=2,
            volume_score=2,
            bidask_score=1,
            oi_score=2,
        )
        self.assertEqual(r.expected_move_pct, 0.02)
        self.assertEqual(r.expected_move, 200.0)
        self.assertEqual(r.target_price, 10200.0)
        self.assertEqual(r.final_expected_move, 200.0)
        self.assertEqual(r.direction, "STRONG_BULLISH")
        self.assertEqual(r.confidence, 80)  # abs(7)*10 + volume_strong*10
        self.assertEqual(r.move_quality, "strong")

    def test_bearish_is_one_sided_below_spot(self):
        r = compute_expected_move(
            spot_price=10000.0,
            indicator_score=-2,
            volume_score=-2,
            bidask_score=-1,
            oi_score=-2,
        )
        self.assertEqual(r.expected_move_pct, -0.02)
        self.assertEqual(r.expected_move, -200.0)
        self.assertEqual(r.target_price, 9800.0)
        self.assertEqual(r.final_expected_move, 200.0)
        self.assertEqual(r.lower_range, 9800.0)
        self.assertEqual(r.upper_range, 10000.0)
        self.assertEqual(r.direction, "STRONG_BEARISH")

    def test_straddle_does_not_drive_magnitude(self):
        r = compute_expected_move(
            spot_price=1035.8,
            implied_volatility=None,
            atm_call_mid=15.25,
            atm_put_mid=182.675,
            indicator_score=0,
            volume_score=0,
            bidask_score=0,
            oi_score=0,
        )
        self.assertEqual(r.expected_move, 0.0)
        self.assertEqual(r.straddle_reference, 197.93)
        self.assertIn("missing_implied_volatility", r.data_quality_flags)

    def test_delta_band_boosts_live_reliance_style(self):
        r = compute_expected_move(
            spot_price=1244.4,
            indicator_score=2.0,
            volume_score=1.0,
            bidask_score=-0.3628,
            oi_score=2.0,
            gamma_trend="flat",
            iv_trend="flat",
            delta=0.5783,
            vacuum_zone=None,
            strong_resistance_nearby=False,
        )
        # total 4.6372 / 7 = 0.6625; * 1.3 * 0.02
        self.assertEqual(r.direction_score, 0.6625)
        self.assertEqual(r.multiplier, 1.3)
        self.assertAlmostEqual(r.expected_move_pct, 0.6625 * 1.3 * 0.02, places=6)
        self.assertEqual(r.target_price, round(1244.4 + r.expected_move, 2))
        self.assertNotIn("missing_delta", r.data_quality_flags)
        self.assertIn("missing_vacuum_zone", r.data_quality_flags)


class ConfidenceTest(unittest.TestCase):
    def test_spec_step_7_example_78(self):
        # abs(5.8)*10 + gamma_up*10 + volume_strong*10 = 78
        self.assertEqual(calculate_confidence(5.8, gamma_trend="up", volume_score=2.0), 78)
        self.assertEqual(classify_move_quality(78), "strong")

    def test_clip_to_100(self):
        self.assertEqual(calculate_confidence(20.0, gamma_trend="up", volume_score=2.0), 100)


class AdapterTest(unittest.TestCase):
    def test_iv_percent_and_decimal(self):
        self.assertEqual(as_annualized_decimal(18.5), 0.185)
        self.assertEqual(as_annualized_decimal(0.185), 0.185)

    def test_trend_combine(self):
        self.assertEqual(classify_pct_trend(12.0, 10.0), "up")
        self.assertEqual(classify_pct_trend(0.0, 10.0), "flat")
        self.assertIsNone(classify_pct_trend(None, 10.0))
        self.assertEqual(combine_trend("flat", "up"), "up")
        self.assertIsNone(combine_trend(None, None))

    def test_pick_delta_and_resistance(self):
        self.assertEqual(pick_atm_delta(0.57, -0.43), 0.57)
        self.assertEqual(pick_atm_delta(0.49, -0.62), -0.62)
        self.assertEqual(pick_atm_delta(None, -0.62), -0.62)
        self.assertTrue(resistance_is_nearby(1244.4, 1240.0))
        self.assertFalse(resistance_is_nearby(1244.4, 1300.0))
        self.assertIsNone(resistance_is_nearby(1244.4, None))


if __name__ == "__main__":
    unittest.main()

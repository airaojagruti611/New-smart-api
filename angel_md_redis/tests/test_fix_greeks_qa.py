"""QA HIGH/MEDIUM fixes: IV solver no-solution, IV units, Markup gate,
greeks-change premium basis + stress side, SIE calendar-time theta."""

from __future__ import annotations

import unittest

from app.expected_move import (
    as_annualized_decimal,
    compute_expected_move,
    iv_from_decimal,
    iv_from_percent,
)
from app.greeks_change import (
    build_scenario_matrix,
    build_summary,
    generate_iv_scenarios,
    generate_spot_scenarios,
    generate_time_scenarios,
    resolve_current_state,
)
from app.greeks_phase import GreeksPhaseTracker
from app.option_pricing import bs_price_greeks, greeks_from_market_premium, implied_vol
from app.strike_intel import theta_risk

R = 0.065


class IVSolverNoSolutionTest(unittest.TestCase):
    def test_deep_itm_below_lower_bound_is_none(self):
        # S - K e^-rT = 1500 - 1300*e^(-0.065*7/365) = 201.62 > 200.5
        t = 7 / 365
        self.assertIsNone(implied_vol(1500, 1300, "CE", t, 200.5, R))
        self.assertIsNone(greeks_from_market_premium(1500, 1300, "CE", t, 200.5, R))

    def test_premium_above_max_vol_price_is_none(self):
        self.assertIsNone(implied_vol(1000, 1000, "CE", 7 / 365, 600.0, R))

    def test_round_trip_still_solves(self):
        t = 7 / 365
        p = bs_price_greeks(1000, 1000, "CE", t, 0.25, R).premium
        self.assertAlmostEqual(implied_vol(1000, 1000, "CE", t, p, R), 0.25, places=4)
        g = greeks_from_market_premium(1000, 1000, "CE", t, p, R)
        self.assertAlmostEqual(g["iv"], 25.0, places=2)


class IVUnitsTest(unittest.TestCase):
    def test_percent_source_tiny_reading_is_missing_not_1pct(self):
        # 0.01 in PERCENT units = 0.01% -> implausible -> None (was 0.01 = 1%).
        self.assertIsNone(iv_from_percent(0.01))
        self.assertEqual(iv_from_percent(18.5), 0.185)
        self.assertEqual(iv_from_percent(0.5 * 100), 0.5)

    def test_implausible_bounds(self):
        self.assertIsNone(iv_from_percent(0.9))      # 0.9% < 1%
        self.assertIsNone(iv_from_percent(350.0))    # 350% > 300%
        self.assertIsNone(iv_from_decimal(0.005))
        self.assertIsNone(iv_from_decimal(3.5))
        self.assertEqual(iv_from_decimal(0.185), 0.185)
        self.assertIsNone(iv_from_decimal(18.5))     # percent fed as decimal -> rejected, not /100

    def test_units_required(self):
        with self.assertRaises(ValueError):
            as_annualized_decimal(18.5, "")

    def test_compute_expected_move_flags_implausible_iv(self):
        r = compute_expected_move(spot_price=1000.0, implied_volatility=0.0001)
        self.assertIn("missing_implied_volatility", r.data_quality_flags)
        self.assertIsNone(r.implied_volatility)
        self.assertIsNone(r.iv_move)


def _tick(t, delta, gamma, iv, breakout):
    return t.analyze(cp="CE", delta=delta, gamma=gamma, theta=-1.8, vega=0.12, iv=iv,
                     price_change_pct=0.04, breakout=breakout)


class MarkupGateTest(unittest.TestCase):
    def test_qa_probe_falling_gamma_iv_never_buys(self):
        t = GreeksPhaseTracker()
        gamma, iv = 0.0200, 20.0
        deltas = (0.6000, 0.60005, 0.6001, 0.60015, 0.6002)
        for d in deltas:
            res = _tick(t, d, gamma, iv, breakout=None)
            self.assertNotIn("BUY", res.action, res.reason)
            gamma *= 0.95
            iv *= 0.975
        t2 = GreeksPhaseTracker()
        gamma, iv = 0.0200, 20.0
        for d in deltas:
            res = _tick(t2, d, gamma, iv, breakout=True)
            self.assertNotIn("BUY", res.action, res.reason)
            gamma *= 0.95
            iv *= 0.975

    def _rising(self, breakout):
        t = GreeksPhaseTracker()
        _tick(t, 0.55, 0.020, 20.0, breakout)
        return _tick(t, 0.57, 0.023, 21.2, breakout)  # gamma +15%, iv +6%

    def test_breakout_true_buys(self):
        res = self._rising(True)
        self.assertEqual(res.action, "BUY CALL")
        self.assertIn("breakout", res.reason)

    def test_breakout_missing_is_not_breakout(self):
        res = self._rising(None)
        self.assertEqual(res.action, "HOLD")
        self.assertEqual(res.phase, "NEUTRAL")
        self.assertIn("breakout_unavailable", res.reason)
        self.assertNotIn("+breakout|", res.reason + "|")

    def test_breakout_false_no_buy(self):
        self.assertEqual(self._rising(False).action, "HOLD")


class GreeksChangeTest(unittest.TestCase):
    S, K, T, IV = 1000.0, 1000.0, 7 / 365, 0.20

    def _matrix(self, option_type, mid_offset=3.0):
        model = bs_price_greeks(self.S, self.K, option_type, self.T, self.IV, R)
        cur = resolve_current_state(
            spot=self.S, strike=self.K, option_type=option_type, time_to_expiry_years=self.T,
            current_iv=self.IV, risk_free_rate=R, dividend_or_carry=0.0,
            observed_premium=model.premium + mid_offset,
            observed_delta=model.delta, observed_gamma=model.gamma,
            observed_theta_per_day=model.theta_per_day, observed_vega_per_point=model.vega_per_point,
        )
        m = build_scenario_matrix(
            current=cur, strike=self.K, option_type=option_type, risk_free_rate=R, dividend_or_carry=0.0,
            spot_scenarios=generate_spot_scenarios(self.S, 20.0),
            iv_scenarios=generate_iv_scenarios(self.IV),
            time_scenarios=generate_time_scenarios(self.T),
        )
        return cur, m

    def test_base_premium_change_is_zero_despite_mid_basis(self):
        cur, m = self._matrix("CE", mid_offset=3.0)
        self.assertAlmostEqual(cur.premium - cur.model_premium, 3.0, places=3)
        base = build_summary(m, "BULLISH", option_type="CE").base
        self.assertAlmostEqual(base.comparison.premium_change, 0.0, places=4)  # was -3.00

    def test_neutral_stress_is_adverse_for_option_type(self):
        _, m_ce = self._matrix("CE")
        _, m_pe = self._matrix("PE")
        s_ce = build_summary(m_ce, "NEUTRAL", option_type="CE").stress
        s_pe = build_summary(m_pe, "NEUTRAL", option_type="PE").stress
        self.assertEqual(s_ce.spot_scenario.multiplier, -2.0)
        self.assertEqual(s_pe.spot_scenario.multiplier, 2.0)
        self.assertLess(s_pe.comparison.premium_change, 0.0)

    def test_bullish_pe_stress_still_adverse_for_put(self):
        _, m = self._matrix("PE")
        self.assertEqual(build_summary(m, "BULLISH", option_type="PE").stress.spot_scenario.multiplier, 1.0)


class CalendarThetaTest(unittest.TestCase):
    def test_theta_risk_uses_calendar_minutes(self):
        pts, pct = theta_risk(-14.4, 100.0, 60.0)
        self.assertAlmostEqual(pts, 0.6, places=4)   # 14.4 * 60/1440 (was 2.304 with 375)
        self.assertAlmostEqual(pct, 0.6, places=4)

    def test_theta_risk_matches_bs_reprice_over_hold(self):
        t = 7 / 365
        now = bs_price_greeks(1000, 1000, "CE", t, 0.20, R)
        later = bs_price_greeks(1000, 1000, "CE", t - 60.0 / (365 * 1440), 0.20, R)
        pts, _ = theta_risk(now.theta_per_day, now.premium, 60.0)
        self.assertAlmostEqual(now.premium - later.premium, pts, delta=0.01)


if __name__ == "__main__":
    unittest.main()

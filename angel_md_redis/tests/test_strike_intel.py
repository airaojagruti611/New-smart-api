"""Spec-faithful tests for Module 10 Strike Intelligence Engine (+ runner adapters)."""

from __future__ import annotations

import json
import unittest
from dataclasses import replace

import pandas as pd

from app.option_pricing import bs_price_greeks
from app.strike_intel import (
    DELTA_BANDS,
    WEIGHTS,
    SIEConfig,
    SIEContext,
    StrikeCandidate,
    classify_market_phase,
    delta_suitability_score,
    em_fit_score,
    execution_quality_score,
    expected_range,
    gamma_opportunity_score,
    intrinsic_extrinsic,
    project_greeks,
    rank_strikes,
    select_window,
    theta_efficiency_score,
    theta_risk,
    vega_stability_score,
    weighted_score,
)
from run_strike_intel import build_lot_sizes, build_payload, load_candidate, trend_aligned

SPOT = 24500.0
T = 5 / 365
IV = 0.14
R = 0.065


def _ctx(**over) -> SIEContext:
    base = dict(
        symbol="NIFTY", side="CE", spot=SPOT, expected_move=220.0, em_direction="BULLISH",
        em_pct=0.9, iv_trend="flat", trend_aligned=True, time_to_expiry_years=T,
        expiry_iso="2026-10-09", today_iso="2026-10-04", hold_minutes=60.0, risk_free_rate=R,
    )
    base.update(over)
    return SIEContext(**base)


def _cand(strike: float, cp: str = "CE", liquidity: float = 95.0, **over) -> StrikeCandidate:
    p = bs_price_greeks(SPOT, strike, cp, T, IV, R)
    base = dict(
        tradingsymbol=f"NIFTY{int(strike)}{cp}", strike=strike, cp=cp, token=str(int(strike)),
        premium=p.premium, spread_pct=0.4, depth=2000.0, delta=p.delta, gamma=p.gamma,
        theta_per_day=p.theta_per_day, vega_per_point=p.vega_per_point, iv=IV,
        greeks_source="theoretical_black_scholes", liquidity_score=liquidity,
        liquidity_band="GREEN", lot_size=75.0,
    )
    base.update(over)
    return StrikeCandidate(**base)


def _chain(cp: str = "CE"):
    return [_cand(k, cp, liquidity=95.0 if k < 24800 else 70.0) for k in range(24300, 24900, 100)]


class WeightsTest(unittest.TestCase):
    def test_spec_weights(self):
        self.assertEqual(WEIGHTS, {"liquidity": 0.25, "em_fit": 0.25, "delta": 0.20,
                                   "theta": 0.15, "gamma": 0.10, "vega": 0.05})
        self.assertAlmostEqual(sum(WEIGHTS.values()), 1.0)

    def test_missing_components_renormalized_and_completeness(self):
        score, comp = weighted_score({"liquidity": 100, "em_fit": 50, "delta": None,
                                      "theta": None, "gamma": None, "vega": None})
        self.assertAlmostEqual(score, 75.0)
        self.assertAlmostEqual(comp, 0.5)
        self.assertEqual(weighted_score({k: None for k in WEIGHTS}), (0.0, 0.0))


class ExpectedMoveFitTest(unittest.TestCase):
    def test_design_range_example(self):
        self.assertEqual(expected_range(24500, 220), (24280, 24720))
        for k, inside in [(24500, True), (24600, True), (24700, True), (24800, False)]:
            self.assertEqual(em_fit_score(k, 24500, 220) == 100.0, inside, k)

    def test_linear_decay_beyond_range(self):
        self.assertAlmostEqual(em_fit_score(24830, 24500, 220), 50.0)   # 110 beyond = half an EM
        self.assertEqual(em_fit_score(25000, 24500, 220), 0.0)
        self.assertAlmostEqual(em_fit_score(24170, 24500, 220), 50.0)   # ITM side penalized too

    def test_missing_em(self):
        self.assertIsNone(em_fit_score(24500, 24500, None))
        self.assertIsNone(em_fit_score(24500, 24500, 0))


class MarketPhaseTest(unittest.TestCase):
    def test_design_delta_table(self):
        self.assertEqual(DELTA_BANDS["STRONG_TREND"], (0.60, 0.75))
        self.assertEqual(DELTA_BANDS["NORMAL_TREND"], (0.45, 0.60))
        self.assertEqual(DELTA_BANDS["HIGH_VOLATILITY"], (0.35, 0.50))
        self.assertEqual(DELTA_BANDS["EXPIRY_DAY"], (0.55, 0.70))

    def test_phase_precedence(self):
        self.assertEqual(classify_market_phase(_ctx(today_iso="2026-10-09", iv_trend="up"))[0], "EXPIRY_DAY")
        self.assertEqual(classify_market_phase(_ctx(iv_trend="up"))[0], "HIGH_VOLATILITY")
        self.assertEqual(classify_market_phase(_ctx(em_pct=2.5))[0], "HIGH_VOLATILITY")
        self.assertEqual(classify_market_phase(_ctx(em_direction="STRONG_BULLISH"))[0], "STRONG_TREND")
        self.assertEqual(classify_market_phase(_ctx(em_direction="STRONG_BEARISH", side="PE"))[0], "STRONG_TREND")

    def test_strong_trend_needs_side_and_trend_alignment(self):
        self.assertEqual(classify_market_phase(_ctx(em_direction="STRONG_BEARISH"))[0], "NORMAL_TREND")
        self.assertEqual(classify_market_phase(_ctx(em_direction="STRONG_BULLISH", trend_aligned=False))[0], "NORMAL_TREND")
        self.assertEqual(classify_market_phase(_ctx(em_direction="STRONG_BULLISH", trend_aligned=None))[0], "NORMAL_TREND")


class ComponentTest(unittest.TestCase):
    def test_delta_band_and_decay(self):
        band = (0.45, 0.60)
        self.assertEqual(delta_suitability_score(0.53, band), 100.0)
        self.assertEqual(delta_suitability_score(-0.50, band), 100.0)   # PE uses |delta|
        self.assertAlmostEqual(delta_suitability_score(0.725, band), 50.0)
        self.assertEqual(delta_suitability_score(0.15, band), 0.0)
        self.assertIsNone(delta_suitability_score(None, band))

    def test_theta_scales_with_hold_time(self):
        # design: 5-minute hold -> theta matters little; 3 hours -> much more
        _, short = theta_risk(-6.0, 100.0, 5)
        _, long = theta_risk(-6.0, 100.0, 180)
        # calendar-day theta x hold/1440 (QA fix: was /375 trading minutes)
        self.assertAlmostEqual(short, 0.0208)
        self.assertAlmostEqual(long, 0.75)
        self.assertGreater(theta_efficiency_score(short), theta_efficiency_score(long))
        self.assertEqual(theta_efficiency_score(10.0), 0.0)
        self.assertEqual(theta_risk(None, 100, 60), (None, None))

    def test_gamma_inverted_when_choppy(self):
        self.assertEqual(gamma_opportunity_score(0.02, 0.04, False), 50.0)
        self.assertEqual(gamma_opportunity_score(0.03, 0.04, True), 25.0)
        self.assertIsNone(gamma_opportunity_score(0.02, None, False))

    def test_vega_penalized_on_iv_contraction(self):
        flat = vega_stability_score(2.0, 100.0, "flat")
        down = vega_stability_score(2.0, 100.0, "down")
        self.assertEqual(flat, 80.0)
        self.assertEqual(down, 60.0)

    def test_intrinsic_extrinsic(self):
        self.assertEqual(intrinsic_extrinsic(24500, 24400, "CE", 180.0), (100.0, 80.0))
        self.assertEqual(intrinsic_extrinsic(24500, 24600, "PE", 150.0), (100.0, 50.0))
        self.assertEqual(intrinsic_extrinsic(24500, 24600, "CE", 60.0), (0.0, 60.0))

    def test_execution_quality(self):
        self.assertAlmostEqual(execution_quality_score(0.0, 75 * 20, 75), 100.0)
        self.assertAlmostEqual(execution_quality_score(1.5, 75 * 10, 75), 0.7 * 50 + 0.3 * 50)
        self.assertAlmostEqual(execution_quality_score(1.5, None, None), 50.0)
        self.assertIsNone(execution_quality_score(None, None, None))


class ProjectionTest(unittest.TestCase):
    def test_design_module4_direction_of_change(self):
        # OTM call, +EM move: delta rises, premium gains
        c = _cand(24700)
        p = project_greeks(c, _ctx())
        self.assertEqual(p.target_spot, 24720)
        self.assertGreater(p.delta, c.delta)
        self.assertGreater(p.premium_gain, 0)
        self.assertLess(p.premium_gain_iv_down, p.premium_gain)
        self.assertLess(p.premium_change_adverse, 0)

    def test_put_moves_down(self):
        p = project_greeks(_cand(24400, "PE"), _ctx(side="PE"))
        self.assertEqual(p.target_spot, 24280)
        self.assertGreater(p.premium_gain, 0)

    def test_needs_iv_time_and_em(self):
        self.assertIsNone(project_greeks(_cand(24500, iv=None), _ctx()))
        self.assertIsNone(project_greeks(_cand(24500), _ctx(time_to_expiry_years=None)))
        self.assertIsNone(project_greeks(_cand(24500), _ctx(expected_move=None)))


class RankTest(unittest.TestCase):
    def test_design_example_atm_ranks_first_and_far_otm_last(self):
        res = rank_strikes(_chain(), _ctx())
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.market_phase, "NORMAL_TREND")
        self.assertEqual(res.best.strike, 24500)
        self.assertEqual(len(res.top), 3)
        self.assertEqual(res.ranked[-1].strike, 24800)
        self.assertEqual([x.rank for x in res.ranked], list(range(1, 7)))
        self.assertTrue(any(s.startswith("✓ Inside expected move") for s in res.best.reasons))
        self.assertGreaterEqual(res.best.strike_score, 90)

    def test_low_liquidity_rejected_and_excluded_from_top(self):
        chain = [c if c.strike != 24500 else replace(c, liquidity_score=60.0) for c in _chain()]
        res = rank_strikes(chain, _ctx())
        self.assertNotIn(24500, [x.strike for x in res.top])
        rej = next(x for x in res.ranked if x.strike == 24500)
        self.assertEqual(rej.status, "REJECTED")
        self.assertIn("liquidity_below_70", rej.reject_reasons)
        self.assertEqual(res.ranked[-1].strike, 24500)   # rejected sorted after all OK

    def test_other_rejects(self):
        cases = [
            (dict(premium=None), "no_premium"),
            (dict(delta=None), "no_greeks"),
            (dict(liquidity_band="RED"), "liquidity_band_red"),
            (dict(spread_pct=4.0), "spread_above_3pct"),
            (dict(liquidity_score=None), "liquidity_missing"),
        ]
        for over, reason in cases:
            res = rank_strikes([_cand(24500, **over)], _ctx())
            self.assertEqual(res.status, "SKIP", over)
            self.assertEqual(res.reason, "all_strikes_rejected")
            self.assertIn(reason, res.ranked[0].reject_reasons, over)

    def test_partial_data_lowers_confidence_not_score(self):
        full = rank_strikes([_cand(24500)], _ctx()).best
        no_em = rank_strikes([_cand(24500)], _ctx(expected_move=None)).best
        self.assertLess(no_em.completeness, 1.0)
        self.assertLess(no_em.confidence, no_em.strike_score)
        self.assertAlmostEqual(full.confidence, full.strike_score, places=1)

    def test_strong_trend_prefers_higher_delta(self):
        normal = rank_strikes(_chain(), _ctx()).best
        strong = rank_strikes(_chain(), _ctx(em_direction="STRONG_BULLISH")).best
        self.assertLess(strong.strike, normal.strike)   # deeper ITM call

    def test_em_conflict_flag(self):
        self.assertTrue(rank_strikes(_chain(), _ctx(em_direction="BEARISH")).em_conflict)
        self.assertFalse(rank_strikes(_chain(), _ctx()).em_conflict)

    def test_only_trade_side_and_skips(self):
        mixed = _chain("CE") + _chain("PE")
        res = rank_strikes(mixed, _ctx(side="PE", em_direction="BEARISH"))
        self.assertTrue(all(x.cp == "PE" for x in res.ranked))
        self.assertEqual(rank_strikes([], _ctx()).reason, "no_CE_candidates")
        self.assertEqual(rank_strikes(_chain(), _ctx(side="")).reason, "invalid_side")
        self.assertEqual(rank_strikes(_chain(), _ctx(spot=0)).reason, "invalid_spot")

    def test_top_n_config(self):
        self.assertEqual(len(rank_strikes(_chain(), _ctx(), SIEConfig(top_n=1)).top), 1)

    def test_select_window(self):
        chain = [_cand(k) for k in range(23500, 25600, 100)] + [_cand(24500, "PE")]
        win = select_window(chain, "CE", 24520, 2)
        self.assertEqual([c.strike for c in win], [24300, 24400, 24500, 24600, 24700])
        self.assertEqual(len(select_window(chain + [_cand(24500)], "CE", 24500, 0)), 1)  # dedupe


class FakeRedis:
    def __init__(self, kv):
        self.kv = {k: json.dumps(v) for k, v in kv.items()}

    def get(self, key):
        return self.kv.get(key)


class RunnerAdapterTest(unittest.TestCase):
    def test_load_candidate_maps_live_payloads(self):
        tsym = "TCS29SEP262140CE"
        r = FakeRedis({
            f"md:greeks:phase:latest:{tsym}": {"delta": "0.5", "gamma": "0.004", "theta": "-1.77",
                                               "vega": "0.96", "iv": "28.16", "greeks_source": "broker_api"},
            f"md:bidask:latest:{tsym}": {"bid": "28.6", "ask": "28.9", "mid": "28.75",
                                         "spread_pct": "1.0435", "depth": "7200"},
            f"md:liquidity:score:latest:{tsym}": {"liquidity_score": "76.63", "liquidity_band": "GREEN",
                                                  "lot_size": "175"},
        })
        c = load_candidate(r, {"tradingsymbol": tsym, "strike": 2140.0, "cp": "CE", "token": "1"}, {})
        self.assertAlmostEqual(c.iv, 0.2816)          # percent -> decimal
        self.assertEqual(c.premium, 28.75)
        self.assertEqual(c.lot_size, 175.0)
        self.assertEqual(c.theta_per_day, -1.77)
        self.assertEqual(c.greeks_source, "broker_api")

    def test_load_candidate_missing_keys_and_lot_fallback(self):
        c = load_candidate(FakeRedis({}), {"tradingsymbol": "X", "strike": 100, "cp": "PE"}, {"X": 300.0})
        self.assertIsNone(c.premium)
        self.assertIsNone(c.delta)
        self.assertEqual(c.lot_size, 300.0)

    def test_trend_aligned(self):
        self.assertTrue(trend_aligned({"htf_bias": "CALL", "st_bias": "CALL"}, "CE"))
        self.assertFalse(trend_aligned({"htf_bias": "CALL", "st_bias": "PUT"}, "CE"))
        self.assertTrue(trend_aligned({"htf_bias": "PUT", "st_bias": "put"}, "PE"))
        self.assertIsNone(trend_aligned({"htf_bias": "CALL"}, "CE"))

    def test_build_lot_sizes(self):
        df = pd.DataFrame({
            "exch_seg": ["NFO", "NFO", "NSE"], "instrumenttype": ["OPTSTK", "FUTSTK", "EQ"],
            "symbol": ["A26OCT100CE", "A26OCTFUT", "A"], "lotsize": ["250", "250", "1"],
        })
        self.assertEqual(build_lot_sizes(df), {"A26OCT100CE": 250.0})

    def test_payload_is_flat_strings_and_echoes_entry(self):
        res = rank_strikes(_chain(), _ctx())
        p = build_payload(res, {"signal": "BUY CALL", "strength": "strong", "htf_bias": "CALL"},
                          "2026-10-09", 1, {"direction": "BULLISH", "confidence": 60})
        self.assertTrue(all(isinstance(v, str) for v in p.values()))
        self.assertEqual(p["tradingsymbol"], "NIFTY24500CE")
        self.assertEqual(p["entry_signal"], "BUY CALL")
        self.assertEqual(len(json.loads(p["top"])), 3)
        self.assertEqual(p["em_confidence"], "60")

    def test_payload_for_skip(self):
        res = rank_strikes([], _ctx())
        p = build_payload(res, {}, "2026-10-09", 1, {})
        self.assertEqual(p["status"], "SKIP")
        self.assertEqual(p["tradingsymbol"], "")
        self.assertEqual(json.loads(p["top"]), [])


if __name__ == "__main__":
    unittest.main()

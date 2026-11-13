"""Tests for the Trade Ranking Engine (Module 13, DECISION.md §6) — brief worked examples."""

from __future__ import annotations

import unittest
from dataclasses import replace

from app.adaptive_tsl import Snapshot, TSLConfig
from app.icare import ICAREConfig, ICAREInputs, PortfolioState, evaluate, presize_lots
from app.trade_ranking import PROFILES, WEIGHTS, Candidate, Context, RankConfig, TradeRankingEngine, get_profile
from app.trade_ranking.direction import analyze
from app.trade_ranking.economics import evaluate as economics
from app.trade_ranking.normalizer import label_alignment, normalize, signed_range_to_score
from app.trade_ranking.score import confidence_band, decide
from app.trade_ranking.sl_tsl import propose_tsl, tsl_quality

ICFG = ICAREConfig(margin_buffer_pct=0.0)
CTX = Context(total_capital=1_000_000, available_margin=1_000_000, max_risk_per_trade=7500, max_risk_pct=2.0)


def _cand(**over) -> Candidate:
    base = dict(
        symbol="SBIN", side="CE", tradingsymbol="SBIN26OCT800CE", candidate_id="SBIN-1",
        probability=88.0, probability_decision="HIGH_CONVICTION", p_oi=86.0,
        indicator_score=1.6, volume_signal="Strong Bullish Volume", volume_surge=False,
        regime="BULLISH", market_phase="STRONG_TREND", execution_quality=94.0, imbalance="BULLISH",
        oi_positioning="BULLISH_POSITIONING", greeks_score=79.0, liquidity_score=93.0, liquidity_band="GREEN",
        spread_pct=1.0, strike_score=89.0, em_fit=90.0, em_direction="BULLISH", em_confidence=85.0,
        htf_bias="CALL", st_bias="CALL", amd_phase="MARKUP",
        premium=100.0, lot_size=50.0, projected_gain=25.0, projected_gain_iv_down=20.0,
        adverse_change=-12.0, tsl_pct=8.0, sector="BANK",
    )
    base.update(over)
    return Candidate(**base)


ENGINE = TradeRankingEngine(RankConfig(), ICFG)


class TestNormalisation(unittest.TestCase):
    def test_weights_sum_100(self):
        self.assertAlmostEqual(sum(WEIGHTS.values()), 100.0)

    def test_brief_direction_formula(self):
        self.assertEqual(signed_range_to_score(100, -100, 100), 100.0)
        self.assertEqual(signed_range_to_score(0, -100, 100), 50.0)
        self.assertEqual(signed_range_to_score(-100, -100, 100), 0.0)

    def test_side_aligned_for_put(self):
        ce = normalize(_cand(indicator_score=-2.0), RankConfig())
        pe = normalize(_cand(side="PE", indicator_score=-2.0), RankConfig())
        self.assertEqual(ce["indicator"], 0.0)
        self.assertEqual(pe["indicator"], 100.0)

    def test_label_alignment(self):
        self.assertEqual(label_alignment("STRONG_BEARISH", "PE"), "FOR")
        self.assertEqual(label_alignment("PUT", "CE"), "AGAINST")
        self.assertEqual(label_alignment("NEUTRAL", "CE"), "NEUTRAL")
        self.assertIsNone(label_alignment("", "CE"))

    def test_contract_imbalance_not_side_flipped(self):
        """md:imbalance:latest:{TSYM} is the bought contract's book: BULLISH = FOR for CE and PE."""
        pe_bull = normalize(_cand(side="PE", imbalance="BULLISH"), RankConfig())
        pe_bear = normalize(_cand(side="PE", imbalance="BEARISH"), RankConfig())
        self.assertEqual(pe_bull["bidask"], round(0.7 * 94 + 0.3 * 100, 4))   # 95.8
        self.assertEqual(pe_bear["bidask"], round(0.7 * 94, 4))               # 65.8
        self.assertEqual(analyze(_cand(side="PE", imbalance="BULLISH"), RankConfig()).votes["bidask"], "FOR")
        self.assertEqual(analyze(_cand(side="PE", imbalance="BEARISH"), RankConfig()).votes["bidask"], "AGAINST")

    def test_em_opposing_zeroes_confidence(self):
        comps = normalize(_cand(em_direction="BEARISH", em_confidence=90.0, em_fit=80.0), RankConfig())
        self.assertEqual(comps["expected_move"], 40.0)


class TestValidation(unittest.TestCase):
    def test_missing_oi_is_not_zero(self):
        """Brief §6: missing OI != OI score 0."""
        missing = ENGINE.evaluate(_cand(p_oi=None, oi_positioning=None), CTX)
        zero = ENGINE.evaluate(_cand(p_oi=0.0), CTX)
        self.assertIn("oi", missing.missing)
        self.assertLess(missing.dq, 1.0)
        self.assertGreater(missing.trade_score, zero.trade_score)

    def test_missing_critical_is_data_insufficient(self):
        r = ENGINE.evaluate(_cand(liquidity_score=None), CTX)
        self.assertEqual(r.decision, "DATA_INSUFFICIENT")
        self.assertIsNone(r.trade_score)
        self.assertIn("MISSING_LIQUIDITY", r.reject_reasons)

    def test_stale_critical_is_data_insufficient(self):
        r = ENGINE.evaluate(_cand(stale=frozenset({"bidask"})), CTX)
        self.assertEqual(r.decision, "DATA_INSUFFICIENT")

    def test_stale_noncritical_is_excluded(self):
        r = ENGINE.evaluate(_cand(stale=frozenset({"volume"})), CTX)
        self.assertIsNone(r.components["volume"])
        self.assertEqual(r.status, "SCORED")


class TestDirection(unittest.TestCase):
    def test_agreement_ratio(self):
        """7 for / 2 against -> 77.8 %, factor 0.9722 (full at 80 %)."""
        c = _cand(regime="BEARISH", imbalance="BEARISH")
        d = analyze(c, RankConfig())
        self.assertEqual((d.n_for, d.n_against), (7, 2))
        self.assertAlmostEqual(d.agreement, 7 / 9, places=4)
        self.assertAlmostEqual(d.conflict_factor, round((7 / 9) / 0.8, 4), places=4)
        self.assertFalse(d.conflict)

    def test_brief_conflict_example_rejects(self):
        """Indicator + volume bullish, regime / bid-ask / OI bearish, greeks neutral -> 2 vs 3 -> reject."""
        c = _cand(regime="BEARISH", imbalance="BEARISH", oi_positioning="BEARISH_POSITIONING",
                  amd_phase="NEUTRAL", em_direction=None, htf_bias=None, st_bias=None)
        d = analyze(c, RankConfig())
        self.assertEqual((d.n_for, d.n_against), (2, 3))
        self.assertTrue(d.conflict)
        r = ENGINE.evaluate(c, CTX)
        self.assertEqual(r.decision, "REJECT")
        self.assertIn("DIRECTION_CONFLICT", r.reject_reasons)

    def test_low_evidence(self):
        c = _cand(indicator_score=0.0, volume_signal=None, regime=None, imbalance=None, oi_positioning=None,
                  amd_phase=None, em_direction=None, htf_bias="CALL", st_bias=None)
        d = analyze(c, RankConfig())
        self.assertIn("LOW_DIRECTIONAL_EVIDENCE", d.flags)
        self.assertEqual(d.conflict_factor, 0.9)


class TestEconomics(unittest.TestCase):
    def test_brief_ev_example(self):
        """p 85 %, profit 4,000, loss 2,000 -> gross EV 3,100; net of charges for the 3 feasible lots.

        Charges (charges.json), qty 300: buy @100 36.94; sell @140 104.01 (p .85), @80 69.55 (.15)
        -> 36.94 + 98.84 = 135.78 total = 45.26 / lot -> net 3,054.74 / lot.
        """
        c = _cand(probability=85.0, premium=100.0, lot_size=100.0, projected_gain=40.0, adverse_change=-20.0)
        e = economics(c, CTX, RankConfig(), ICFG)
        self.assertEqual(e.feasible_lots, 3)
        self.assertEqual(e.gross_ev_per_lot, 3100.0)
        self.assertEqual(e.charges, 135.78)
        self.assertEqual(e.ev_per_lot, 3054.74)
        self.assertEqual(e.ev_source, "model")
        self.assertEqual(e.reward_risk, 2.0)
        self.assertEqual(e.risk_factor, 1.0)

    def test_risk_factor_penalises_low_reward(self):
        e = economics(_cand(projected_gain=6.0), CTX, RankConfig(), ICFG)
        self.assertAlmostEqual(e.risk_factor, 0.775)

    def test_lot_not_feasible(self):
        """Required capital far above available -> 0 lots -> hard reject."""
        r = ENGINE.evaluate(_cand(), replace(CTX, available_margin=1000.0))
        self.assertEqual(r.feasible_lots, 0)
        self.assertIn("LOT_NOT_FEASIBLE", r.reject_reasons)

    def test_presize_matches_icare(self):
        inp = ICAREInputs(symbol="SBIN", side="CE", tradingsymbol="X", strike=800.0, premium=100.0, lot_size=100.0,
                          probability=92.0, probability_decision="TRADE", trend=92, expected_move_score=92,
                          strike_score=92, liquidity=92, greeks=92, bidask=92, projected_gain=40.0, adverse_change=-12.0)
        pf = PortfolioState(total_capital=1_000_000, available_margin=100_000, margin_utilization_pct=0,
                            day_pnl=0, open_positions=0, open_risk=0)
        res = evaluate(inp, pf, ICFG)
        lim = presize_lots(100.0, 100.0, res.sl_points, 100_000, 1_000_000, None, ICFG)
        self.assertEqual(lim["margin"], res.lots_by_margin)
        self.assertEqual(lim["risk"], res.lots_by_risk)


class TestRankingExamples(unittest.TestCase):
    def test_brief_section_1_profitability_beats_raw_probability(self):
        """92 % / +8 % / liq 95 is inferior to 87 % / +35 % / liq 90."""
        a = _cand(candidate_id="A", symbol="AAA", probability=92.0, projected_gain=8.0, liquidity_score=95.0, sector="")
        b = _cand(candidate_id="B", symbol="BBB", probability=87.0, projected_gain=35.0, liquidity_score=90.0, sector="")
        ranked, summary = ENGINE.rank([a, b], CTX)
        self.assertEqual(ranked[0].candidate_id, "B")
        self.assertEqual(ranked[0].rank, 1)
        self.assertEqual(summary.taken_ids[0], "B")

    def test_no_forced_trade(self):
        """Brief §20: nothing above the minimum -> NO_TRADE."""
        weak = dict(probability=66.0, greeks_score=40.0, strike_score=45.0, em_confidence=30.0, em_fit=30.0,
                    execution_quality=50.0, p_oi=40.0, projected_gain=13.0)
        cs = [_cand(candidate_id=f"C{i}", symbol=f"S{i}", sector="", **weak) for i in range(3)]
        ranked, summary = ENGINE.rank(cs, CTX)
        self.assertEqual(summary.outcome, "NO_TRADE")
        self.assertEqual(summary.taken, 0)
        self.assertTrue(all(r.decision != "TAKE_TRADE" for r in ranked))

    def test_one_side_per_underlying(self):
        ce = _cand(candidate_id="CE")
        pe = _cand(candidate_id="PE", side="PE", tradingsymbol="SBIN26OCT800PE", indicator_score=-1.6,
                   volume_signal="Strong Bearish Volume", regime="BEARISH", imbalance="BULLISH",   # put's own book
                   oi_positioning="BEARISH_POSITIONING", em_direction="BEARISH", htf_bias="PUT", st_bias="PUT",
                   probability=80.0, p_oi=80.0)
        ranked, summary = ENGINE.rank([pe, ce], CTX)
        self.assertEqual(summary.taken_ids, ["CE"])
        loser = next(r for r in ranked if r.candidate_id == "PE")
        self.assertEqual(loser.decision, "WATCH")
        self.assertIn("CORRELATED_SAME_UNDERLYING", loser.reject_reasons)

    def test_sector_and_slot_limits(self):
        a = _cand(candidate_id="A", symbol="TCS", sector="IT")
        b = _cand(candidate_id="B", symbol="INFY", sector="IT", probability=80.0)
        c = _cand(candidate_id="C", symbol="RELIANCE", sector="ENERGY", probability=79.0)
        ranked, summary = ENGINE.rank([a, b, c], replace(CTX, max_open_trades=5, open_positions=4))
        self.assertEqual(summary.taken_ids, ["A"])
        by_id = {r.candidate_id: r for r in ranked}
        self.assertIn("CORRELATED_SECTOR", by_id["B"].reject_reasons)
        self.assertIn("NO_SLOT", by_id["C"].reject_reasons)

    def test_open_position_sector_counts(self):
        r, s = ENGINE.rank([_cand(symbol="TCS", sector="IT")], replace(CTX, open_sectors={"IT": 1}, open_positions=1))
        self.assertEqual(s.outcome, "NO_TRADE")
        self.assertIn("CORRELATED_SECTOR", r[0].reject_reasons)


class TestHardGates(unittest.TestCase):
    def test_score_never_overrides_gates(self):
        cases = [
            (replace(CTX, kill_switch=True), {}, "KILL_SWITCH_ACTIVE"),
            (replace(CTX, open_symbols=frozenset({"SBIN"})), {}, "DUPLICATE_POSITION"),
            (replace(CTX, blocked=frozenset({"SBIN:CE"})), {}, "REENTRY_BLOCKED"),
            (replace(CTX, day_pnl=-30_000), {}, "DAILY_LOSS_LIMIT"),
            (replace(CTX, open_positions=5), {}, "PORTFOLIO_EXPOSURE_LIMIT"),
            (CTX, {"probability": 64.0}, "PROBABILITY_BELOW_PROFILE"),
            (CTX, {"liquidity_score": 60.0}, "LIQUIDITY_BELOW_MIN"),
            (CTX, {"liquidity_band": "RED"}, "LIQUIDITY_BELOW_MIN"),
            (CTX, {"spread_pct": 4.0}, "SPREAD_ABOVE_MAX"),
            (CTX, {"ltp": 100.5, "upper_circuit": 101.0, "lower_circuit": 80.0}, "CIRCUIT_LIMIT"),
        ]
        for ctx, over, reason in cases:
            with self.subTest(reason=reason):
                r = ENGINE.evaluate(_cand(**over), ctx)
                self.assertEqual(r.decision, "REJECT")
                self.assertIn(reason, r.reject_reasons)

    def test_negative_ev(self):
        r = ENGINE.evaluate(_cand(projected_gain=0.5, probability=66.0), CTX)
        self.assertIn("NEGATIVE_EV", r.reject_reasons)
        self.assertNotIn("LOW_EV", r.reject_reasons)

    def test_circuit_unchecked_flag(self):
        self.assertIn("CIRCUIT_UNCHECKED", ENGINE.evaluate(_cand(), CTX).flags)


class TestBandsAndProfiles(unittest.TestCase):
    def test_bands(self):
        self.assertEqual(confidence_band(49.9), "REJECT")
        self.assertEqual(confidence_band(55), "WATCH")
        self.assertEqual(confidence_band(65), "CONDITIONAL")
        self.assertEqual(confidence_band(75), "TAKE")
        self.assertEqual(confidence_band(85), "HIGH_CONVICTION")
        self.assertEqual(confidence_band(90), "EXCEPTIONAL")

    def test_conditional_depends_on_profile(self):
        self.assertEqual(decide(65, PROFILES["normal_intraday"])[0], "WATCH")
        self.assertEqual(decide(65, PROFILES["aggressive"])[0], "TAKE_TRADE")
        self.assertEqual(decide(72, PROFILES["conservative"])[0], "WATCH")

    def test_unknown_profile_defaults(self):
        self.assertEqual(get_profile("nope").name, "normal_intraday")

    def test_high_conviction_output_shape(self):
        r = ENGINE.evaluate(_cand(), CTX)
        self.assertEqual(r.decision, "TAKE_TRADE")
        self.assertIn(r.confidence, ("HIGH_CONVICTION", "EXCEPTIONAL"))
        d = r.to_dict()
        for k in ("trade_score", "probability", "expected_gain_pct", "expected_value", "initial_stop_loss_pct",
                  "trailing_stop_pct", "trailing_activation_pct", "capital_required", "risk_amount",
                  "reasons", "warnings"):
            self.assertIn(k, d)
        self.assertIn("Strong directional agreement", r.reasons)


class TestSlTsl(unittest.TestCase):
    def test_tsl_quality(self):
        cfg = RankConfig()
        self.assertEqual(tsl_quality(35.0, 9.0, 7.0, cfg), round((35 / 9 - 0.5) / 2.5 * 100, 4) if 35 / 9 < 3 else 100.0)
        self.assertEqual(tsl_quality(4.0, 10.0, 20.0, cfg), 0.0)
        self.assertIsNone(tsl_quality(None, 10.0, 5.0, cfg))

    def test_brief_wide_tsl_scores_lower(self):
        """Brief §13: 35 % gain with a 20 % TSL < 25 % gain with a 7 % TSL."""
        cfg = RankConfig()
        self.assertLess(tsl_quality(35.0, 10.0, 20.0, cfg), tsl_quality(25.0, 10.0, 7.0, cfg))

    def test_propose_tsl_expiry_day(self):
        pct, rule = propose_tsl(Snapshot(now_ms=0, dte=0, gamma=0.0001, spot=800.0), "CE", TSLConfig())
        self.assertEqual(rule, "EXPIRY_DAY")
        self.assertTrue(15.0 <= pct <= 20.0)


if __name__ == "__main__":
    unittest.main()

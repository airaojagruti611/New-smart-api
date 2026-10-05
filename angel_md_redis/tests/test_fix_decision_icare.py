"""QA fixes — ICARE / Trade Ranking pure logic (charges-net EV, sector lots, daily-loss room, NaN, kill switch)."""

from __future__ import annotations

import math
import unittest
from dataclasses import replace

from app.icare import (
    ICAREConfig,
    ICAREInputs,
    PortfolioState,
    daily_loss_room,
    ev_breakdown,
    evaluate,
    expected_value,
    round_trip_charges,
    trade_quality,
)
from app.order_executor.charges import load_rates
from app.trade_ranking import Candidate, Context, RankConfig, TradeRankingEngine
from app.trade_ranking.economics import evaluate as economics
from app.trade_ranking.portfolio import hard_gates

CFG = ICAREConfig(margin_buffer_pct=0.0, max_lots_per_trade=5)


def _inp(**over) -> ICAREInputs:
    base = dict(
        symbol="SBIN", side="CE", tradingsymbol="SBIN26OCT800CE", strike=800.0,
        premium=100.0, lot_size=100.0, probability=92.0, probability_decision="HIGH_CONVICTION",
        trend=92, expected_move_score=92, strike_score=92, liquidity=92, greeks=92, bidask=92,
        projected_gain=30.0, adverse_change=-12.5, sector="BANK",
    )
    base.update(over)
    return ICAREInputs(**base)


def _pf(**over) -> PortfolioState:
    base = dict(total_capital=750_000.0, available_margin=100_000.0, margin_utilization_pct=10.0,
                day_pnl=0.0, open_positions=0, open_risk=0.0)
    base.update(over)
    return PortfolioState(**base)


# The QA example: premium 4, lot 100, p 0.66, gain 1.2, SL 1.2.
CHEAP = dict(premium=4.0, lot_size=100.0, probability=66.0, projected_gain=1.2, adverse_change=-1.2)


class ChargesNetEVTest(unittest.TestCase):
    def test_rates_from_charges_json(self):
        r = load_rates()
        self.assertEqual((r.brokerage_per_order, r.stt_sell_pct, r.gst_pct), (20.0, 0.15, 18.0))

    def test_one_lot_round_trip_hand_computed(self):
        """Buy 400 -> 23.78; sell 520 -> 24.59 (p .66), sell 280 -> 24.14 (.34) -> 48.22."""
        self.assertEqual(round_trip_charges(4.0, 100.0, 1, 0.66, 1.2, 1.2, load_rates()), 48.22)

    def test_qa_example_rejected_net_negative(self):
        """gross 0.66 x 120 - 0.34 x 120 = +38.4; net 38.4 - 48.22 = -9.82 -> NOT APPROVED."""
        res = evaluate(_inp(**CHEAP, liquidity_max_lots=1), _pf(), CFG)
        self.assertEqual(res.recommended_lots, 0)
        self.assertEqual(res.status, "REJECTED")
        self.assertEqual(res.gross_ev, 38.4)
        self.assertEqual(res.charges, 48.22)
        self.assertEqual(res.charges_per_lot, 48.22)
        self.assertEqual(res.expected_value, -9.82)
        self.assertEqual(res.reasons, ["expected_value_not_positive"])
        # The gross helper alone would have passed it (the bug).
        self.assertEqual(expected_value(_inp(**CHEAP), 4.0, 100.0, 1.2, 1.2, CFG)[0], 38.4)

    def test_charges_for_the_sized_lots(self):
        """5 lots: buy 24.49 + .66 x 28.57 + .34 x 26.28 = 52.28 -> 10.46 / lot -> net 27.94 > 0."""
        res = evaluate(_inp(**CHEAP), _pf(), CFG)
        self.assertEqual(res.status, "APPROVED", res.reasons)
        self.assertEqual(res.recommended_lots, 5)
        self.assertEqual((res.gross_ev, res.charges, res.charges_per_lot, res.expected_value),
                         (38.4, 52.28, 10.46, 27.94))

    def test_charges_can_be_disabled(self):
        bd = ev_breakdown(_inp(**CHEAP), 4.0, 100.0, 1.2, 1.2, replace(CFG, include_charges=False))
        self.assertEqual((bd["gross_ev"], bd["charges"], bd["net_ev"]), (38.4, 0.0, 38.4))

    def test_ranking_uses_net_ev(self):
        c = Candidate(symbol="SBIN", side="CE", tradingsymbol="X", candidate_id="1", probability=66.0,
                      premium=4.0, lot_size=100.0, projected_gain=1.2, adverse_change=-1.2, liquidity_max_lots=1)
        e = economics(c, Context(total_capital=1_000_000, available_margin=1_000_000), RankConfig(), CFG)
        self.assertEqual((e.feasible_lots, e.gross_ev_per_lot, e.charges, e.ev_per_lot), (1, 38.4, 48.22, -9.82))
        r = TradeRankingEngine(RankConfig(), CFG).evaluate(c, Context(total_capital=1_000_000,
                                                                       available_margin=1_000_000))
        self.assertEqual((r.gross_ev, r.charges), (38.4, 48.22))
        self.assertFalse(r.gates["ev"])


class EVFallbackAndNaNTest(unittest.TestCase):
    def test_zero_wins_bucket_uses_empirical_rate(self):
        """40 samples, win rate 0.5, no avg win -> p 0.5 with model sizes: 0.5 x 3000 - 0.5 x 1250 = 875."""
        inp = _inp(history_samples=40, history_win_rate=0.5, history_avg_win_pct=None, history_avg_loss_pct=10.0)
        ev, src, p = expected_value(inp, 100.0, 100.0, 12.5, 30.0, CFG)
        self.assertEqual((ev, src, p), (875.0, "journal_rate", 0.5))

    def test_all_losses_bucket_rejects(self):
        inp = _inp(history_samples=40, history_win_rate=0.0, history_avg_win_pct=None, history_avg_loss_pct=12.0)
        res = evaluate(inp, _pf(), CFG)
        self.assertEqual(res.ev_source, "journal_rate")
        self.assertIn("expected_value_not_positive", res.reasons)

    def test_below_min_samples_still_model(self):
        inp = _inp(history_samples=10, history_win_rate=0.0, history_avg_win_pct=None)
        self.assertEqual(expected_value(inp, 100.0, 100.0, 12.5, 30.0, CFG)[1], "model")

    def test_nan_is_missing_not_100(self):
        self.assertEqual(trade_quality({"trend": math.nan, "probability": 100}), 20.0)
        res = evaluate(_inp(probability=math.nan), _pf(), CFG)
        self.assertIsNone(res.quality_components["probability"])
        self.assertEqual(res.ev_source, "none")
        self.assertIn("expected_value_not_positive", res.reasons)
        res = evaluate(_inp(premium=math.nan), _pf(), CFG)
        self.assertIn("no_premium", res.reasons)


class SectorExposureLotsTest(unittest.TestCase):
    """Capital 1L, IT cap 30% = 30,000; cost / lot 20 x 100 = 2,000; liquidity alone would allow 3."""

    def _res(self, existing):
        inp = _inp(symbol="TCS", sector="IT", premium=20.0, lot_size=100.0, projected_gain=6.0, adverse_change=-2.4,
                   liquidity_max_lots=3)
        return evaluate(inp, _pf(total_capital=100_000, sector_exposure={"IT": existing}), CFG)

    def test_lots_reduced_to_fit_cap(self):
        res = self._res(26_000)                     # 3 lots -> 32,000 = 32% > 30%
        self.assertEqual(res.status, "APPROVED", res.reasons)
        self.assertEqual((res.lots_by_sector, res.recommended_lots, res.limiting_factor), (2, 2, "sector_exposure"))
        self.assertLessEqual(26_000 + res.recommended_lots * 2_000, 30_000)

    def test_no_room_rejects(self):
        res = self._res(29_000)
        self.assertEqual(res.status, "REJECTED")
        self.assertEqual(res.lots_by_sector, 0)
        self.assertIn("sector_exposure_exceeded", res.reasons)
        self.assertNotIn("zero_lots_by_sector_exposure", res.reasons)


class DailyLossRoomTest(unittest.TestCase):
    """Capital 7.5L -> limit 2% = 15,000; risk / lot 12.5 x 100 = 1,250."""

    def test_room_caps_lots(self):
        res = evaluate(_inp(), _pf(day_pnl=-12_500), CFG)       # room 2,500 -> 2 lots (was 5)
        self.assertEqual(res.daily_loss_room, 2500.0)
        self.assertEqual((res.lots_by_daily_loss, res.recommended_lots, res.limiting_factor),
                         (2, 2, "daily_loss_room"))
        self.assertLessEqual(-12_500 - res.expected_max_loss, -0.0)
        self.assertGreaterEqual(-12_500 - res.expected_max_loss, -15_000)

    def test_profit_does_not_extend_room(self):
        self.assertEqual(daily_loss_room(750_000, 50_000, 2.0), 15_000.0)

    def test_boundary_consistent_with_ranking(self):
        at = evaluate(_inp(), _pf(day_pnl=-15_000), CFG)
        self.assertIn("daily_loss_limit_hit", at.reasons)            # exactly at the limit: rejected (<=)
        near = evaluate(_inp(), _pf(day_pnl=-14_999), CFG)          # room 1 < 1,250
        self.assertNotIn("daily_loss_limit_hit", near.reasons)
        self.assertIn("zero_lots_by_daily_loss_room", near.reasons)

        ctx = Context(total_capital=750_000, available_margin=1_000_000, day_pnl=-15_000)
        c = Candidate(symbol="SBIN", side="CE", tradingsymbol="X", candidate_id="1")
        g, _ = hard_gates(c, ctx, RankConfig(), feasible_lots=1, direction_conflict=False,
                          ev_per_lot=1.0, ev_per_risk=1.0)
        self.assertFalse(g["daily_loss"])
        g, _ = hard_gates(c, replace(ctx, day_pnl=-14_999), RankConfig(), feasible_lots=1,
                          direction_conflict=False, ev_per_lot=1.0, ev_per_risk=1.0)
        self.assertTrue(g["daily_loss"])
        # Ranking pre-sizing applies the same room: 1 < 1,250 risk / lot -> 0 lots.
        cand = Candidate(symbol="SBIN", side="CE", tradingsymbol="X", candidate_id="1", probability=92.0,
                         premium=100.0, lot_size=100.0, projected_gain=30.0, adverse_change=-12.5)
        e = economics(cand, replace(ctx, day_pnl=-14_999), RankConfig(), CFG)
        self.assertEqual((e.lot_limits["daily_loss_room"], e.feasible_lots), (0, 0))

    def test_unknown_day_pnl_fails_closed(self):
        res = evaluate(_inp(), _pf(day_pnl_known=False), CFG)
        self.assertEqual(res.status, "REJECTED")
        self.assertIn("daily_loss_unknown", res.reasons)
        c = Candidate(symbol="SBIN", side="CE", tradingsymbol="X", candidate_id="1")
        g, _ = hard_gates(c, Context(total_capital=750_000, day_pnl_known=False), RankConfig(), feasible_lots=1,
                          direction_conflict=False, ev_per_lot=1.0, ev_per_risk=1.0)
        self.assertFalse(g["daily_loss"])


class KillSwitchAndReentryCapTest(unittest.TestCase):
    def test_kill_switch_rejects(self):
        res = evaluate(_inp(), _pf(kill_switch=True), CFG)
        self.assertEqual(res.status, "REJECTED")
        self.assertIn("kill_switch_active", res.reasons)

    def test_reentry_cap_zero_means_zero(self):
        res = evaluate(_inp(reentry_max_lots=0.0), _pf(), CFG)
        self.assertIn("zero_lots_by_reentry_cap", res.reasons)


if __name__ == "__main__":
    unittest.main()

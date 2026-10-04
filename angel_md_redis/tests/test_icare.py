"""Spec-faithful tests for ICARE (Modules 11 + 20) and its runner adapters."""

from __future__ import annotations

import json
import unittest
from dataclasses import replace

from app.icare import (
    QUALITY_WEIGHTS,
    ICAREConfig,
    ICAREInputs,
    PortfolioState,
    classify_risk,
    evaluate,
    expected_value,
    stop_loss_points,
    trade_quality,
)
from run_icare import build_inputs, liquidity_max_lots, portfolio_state

CFG = ICAREConfig(margin_buffer_pct=0.0, max_lots_per_trade=5)


def _inp(**over) -> ICAREInputs:
    """Design Step 7 shape: margin 10, risk 6, capital 7, portfolio 5 -> 5 lots."""
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


class QualityAndClassTest(unittest.TestCase):
    def test_weights(self):
        self.assertEqual(QUALITY_WEIGHTS, {"trend": 0.20, "expected_move": 0.20, "probability": 0.20,
                                           "strike": 0.15, "liquidity": 0.10, "greeks": 0.10, "bidask": 0.05})
        self.assertAlmostEqual(sum(QUALITY_WEIGHTS.values()), 1.0)

    def test_quality_missing_counts_zero(self):
        self.assertEqual(trade_quality({k: 100 for k in QUALITY_WEIGHTS}), 100.0)
        self.assertEqual(trade_quality({"trend": 100}), 20.0)

    def test_design_risk_classes(self):
        self.assertEqual(classify_risk(96), ("A+", 15.0))
        self.assertEqual(classify_risk(95), ("A", 10.0))   # ">95" is A+
        self.assertEqual(classify_risk(92), ("A", 10.0))   # design example: 92 -> 10%
        self.assertEqual(classify_risk(90), ("A", 10.0))
        self.assertEqual(classify_risk(85), ("B", 7.0))
        self.assertEqual(classify_risk(70), ("C", 3.0))
        self.assertEqual(classify_risk(69.9), ("REJECT", 0.0))


class StepsTest(unittest.TestCase):
    def test_stop_loss_clamped(self):
        self.assertEqual(stop_loss_points(100, -12.5, CFG), 12.5)
        self.assertEqual(stop_loss_points(100, -2.0, CFG), 10.0)    # min 10%
        self.assertEqual(stop_loss_points(100, -60.0, CFG), 30.0)   # max 30%
        self.assertEqual(stop_loss_points(100, None, CFG), 30.0)

    def test_design_ev_example(self):
        # 72% x 2400 - 28% x 1000 = 1448
        inp = _inp(history_samples=100, history_win_rate=0.72, history_avg_win_pct=24.0, history_avg_loss_pct=10.0)
        ev, src, p = expected_value(inp, premium=100.0, lot=100.0, sl_pts=12.5, gain=30.0, cfg=CFG)
        self.assertEqual((ev, src, p), (1448.0, "journal", 0.72))

    def test_model_ev_before_history(self):
        ev, src, p = expected_value(_inp(history_samples=5), 100.0, 100.0, 12.5, 30.0, CFG)
        self.assertEqual(src, "model")
        self.assertAlmostEqual(ev, 0.92 * 3000 - 0.08 * 1250)


class EvaluateTest(unittest.TestCase):
    def test_design_step7_min_of_limits(self):
        res = evaluate(_inp(), _pf(), CFG)
        self.assertEqual(res.status, "APPROVED", res.reasons)
        self.assertEqual(res.risk_class, "A")
        self.assertEqual((res.lots_by_margin, res.lots_by_risk, res.lots_by_capital, res.lots_by_portfolio),
                         (10, 6, 7, 5))
        self.assertEqual(res.recommended_lots, 5)
        self.assertEqual(res.limiting_factor, "portfolio")
        self.assertEqual(res.max_capital, 75_000.0)
        self.assertEqual(res.stop_loss_premium, 87.5)
        self.assertEqual(res.target_premium, 130.0)
        self.assertEqual(res.expected_max_loss, 5 * 1250.0)
        self.assertLessEqual(res.expected_max_loss, CFG.max_risk_per_trade)
        self.assertEqual(res.expected_reward, 5 * 3000.0)
        self.assertEqual(res.reward_risk, 2.4)
        self.assertEqual(res.margin_used, 50_000.0)

    def test_liquidity_limit_joins_min(self):
        res = evaluate(_inp(liquidity_max_lots=3.7), _pf(), CFG)
        self.assertEqual(res.recommended_lots, 3)
        self.assertEqual(res.limiting_factor, "liquidity")

    def test_confidence_never_lifts_risk_cap(self):
        hi = evaluate(_inp(**{k: 100 for k in ("trend", "expected_move_score", "strike_score", "liquidity",
                                               "greeks", "bidask")}, probability=100),
                      _pf(available_margin=10_000_000), replace(CFG, max_lots_per_trade=1000))
        self.assertEqual(hi.risk_class, "A+")
        self.assertLessEqual(hi.expected_max_loss, CFG.max_risk_per_trade)
        self.assertEqual(hi.recommended_lots, 6)   # risk-limited

    def test_risk_cap_is_pct_of_small_capital(self):
        res = evaluate(_inp(), _pf(total_capital=100_000), CFG)
        self.assertEqual(res.max_risk_allowed, 2000.0)     # 2% of 1L < 7500
        self.assertEqual(res.lots_by_risk, 1)

    def test_small_position_caps_allocation(self):
        res = evaluate(_inp(probability_decision="SMALL_POSITION"), _pf(), CFG)
        self.assertEqual(res.allocation_pct, 3.0)
        self.assertIn("SMALL_POSITION_CAP", res.flags)
        self.assertEqual(res.lots_by_capital, 2)

    def test_rejects(self):
        cases = [
            (dict(), dict(margin_utilization_pct=85), "margin_utilization_exceeded"),
            (dict(), dict(day_pnl=-20_000), "daily_loss_limit_hit"),
            (dict(), dict(open_risk=40_000), "portfolio_risk_exceeded"),
            (dict(), dict(open_positions=5), "max_open_trades"),
            (dict(), dict(open_symbols=frozenset({"SBIN"})), "position_already_open"),
            (dict(), dict(sector_exposure={"BANK": 220_000}), "sector_exposure_exceeded"),
            (dict(probability_decision="WATCHLIST"), dict(), "probability_watchlist"),
            (dict(trend=0, expected_move_score=0), dict(), "trade_quality_below_70"),
            (dict(projected_gain=None), dict(), "no_projected_gain"),
            (dict(premium=None), dict(), "no_premium"),
            (dict(probability=10, history_samples=0), dict(), "expected_value_not_positive"),
            (dict(), dict(available_margin=5_000), "zero_lots_by_margin"),
        ]
        for i_over, p_over, reason in cases:
            res = evaluate(_inp(**i_over), _pf(**p_over), CFG)
            self.assertEqual(res.status, "REJECTED", reason)
            self.assertIn(reason, res.reasons)
            self.assertEqual(res.recommended_lots, 0)

    def test_probability_reject_reasons_forwarded(self):
        res = evaluate(_inp(probability_decision="REJECT", probability_reject_reasons=["liquidity_below_40"]), _pf(), CFG)
        self.assertIn("prob:liquidity_below_40", res.reasons)

    def test_sector_unchecked_flag(self):
        self.assertIn("SECTOR_UNCHECKED", evaluate(_inp(sector=""), _pf(), CFG).flags)


class RunnerAdapterTest(unittest.TestCase):
    def test_build_inputs_maps_probability_payload(self):
        prob = {"symbol": "tcs", "side": "PE", "tradingsymbol": "T", "strike": "2100", "premium": "25",
                "lot_size": "175", "probability": "78", "decision": "TRADE", "reject_reasons": "[]",
                "p_confluence": "80", "em_confidence": "60", "em_conflict": "0", "strike_score": "70",
                "liquidity_score": "75", "greeks_score": "66", "execution_quality": "90",
                "projected_premium_gain": "6.5", "projected_premium_change_adverse": "-4",
                "history_samples": "0", "history_win_rate": ""}
        inp = build_inputs(prob, {"size_unit": "lots", "final_entry_size": "23.3"}, {"TCS": "IT"})
        self.assertEqual(inp.symbol, "TCS")
        self.assertEqual(inp.expected_move_score, 60.0)
        self.assertEqual(inp.liquidity_max_lots, 23.3)
        self.assertEqual(inp.sector, "IT")
        self.assertIsNone(inp.history_win_rate)
        conflicted = build_inputs({**prob, "em_conflict": "1"}, {}, {})
        self.assertEqual(conflicted.expected_move_score, 0.0)
        self.assertIsNone(conflicted.liquidity_max_lots)

    def test_liquidity_lots_only_when_in_lots(self):
        self.assertIsNone(liquidity_max_lots({"size_unit": "shares", "final_entry_size": "500"}))

    def test_portfolio_state_falls_back_to_paper(self):
        pos = [{"symbol": "TCS", "qty": 175, "entry_premium": 20, "sl_premium": 15, "last_premium": 22}]
        pf, flags = portfolio_state({}, pos, {"TCS": "IT"}, now_ms=10**13)
        self.assertIn("ACCOUNT_FALLBACK_PAPER", flags)
        self.assertEqual(pf.open_positions, 1)
        self.assertEqual(pf.open_risk, 875.0)
        self.assertEqual(pf.sector_exposure, {"IT": 3500.0})
        self.assertEqual(pf.open_symbols, frozenset({"TCS"}))

    def test_portfolio_state_uses_fresh_account(self):
        acct = {"ts_ms": 10**13, "total_capital": 500000, "available_margin": 400000,
                "margin_utilization_pct": 20, "day_pnl": -100, "open_positions": 2, "open_risk": 3000}
        pf, flags = portfolio_state(acct, [], {}, now_ms=10**13 + 1000)
        self.assertEqual(flags, [])
        self.assertEqual(pf.total_capital, 500000)


if __name__ == "__main__":
    unittest.main()

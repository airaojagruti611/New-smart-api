"""Tests for the paper-trade journal, account snapshot and Probability runner inputs."""

from __future__ import annotations

import datetime as dt
import unittest

from app.broker_account import paper_snapshot, parse_rms, position_exposure
from app.trade_journal import (
    PaperPosition,
    close_position,
    exit_reason,
    mark,
    open_position,
    pick_bucket,
    summarize,
    update_stats,
)
from run_probability import build_inputs, confluence_votes, is_sideways
from run_trade_journal import is_eod

APPROVAL = {
    "symbol": "TCS", "tradingsymbol": "TCS26OCT2100CE", "side": "CE", "strike": "2100",
    "recommended_lots": "2", "lot_size": "175", "premium": "20", "stop_loss_premium": "17",
    "target_premium": "26", "hold_minutes": "60", "spot": "2110", "market_phase": "NORMAL_TREND",
    "delta": "0.52", "probability": "78", "trade_quality": "81", "risk_class": "B",
}


class PaperPositionTest(unittest.TestCase):
    def test_open_at_ask(self):
        pos = open_position(APPROVAL, 20.2, now_ms=0)
        self.assertEqual(pos.qty, 350)
        self.assertEqual((pos.sl_premium, pos.target_premium), (17.0, 26.0))
        self.assertEqual(pos.time_stop_ms, 60 * 60_000)
        self.assertEqual(pos.context["market_phase"], "NORMAL_TREND")
        self.assertEqual(PaperPosition.from_dict(pos.to_dict()), pos)

    def test_levels_reanchored_when_fill_beyond_them(self):
        pos = open_position(APPROVAL, 26.5, now_ms=0)   # fill above ICARE target
        self.assertAlmostEqual(pos.sl_premium, 17.0)     # E16: ICARE's SL stays a price (never widened)
        self.assertAlmostEqual(pos.target_premium, 32.5)  # only the passed target is re-anchored (+6)

    def test_cannot_open(self):
        self.assertIsNone(open_position({**APPROVAL, "recommended_lots": "0"}, 20, 0))
        self.assertIsNone(open_position(APPROVAL, 0, 0))

    def test_marks_mfe_mae_and_exits(self):
        pos = open_position(APPROVAL, 20.0, now_ms=0)
        pos = mark(mark(mark(pos, 24.0), 18.5), None)
        self.assertEqual((pos.max_premium, pos.min_premium, pos.last_premium), (24.0, 18.5, 18.5))
        self.assertEqual(exit_reason(pos, 16.9, 1, False), "SL")
        self.assertEqual(exit_reason(pos, 26.0, 1, False), "TARGET")
        self.assertEqual(exit_reason(pos, 21.0, 60 * 60_000, False), "TIME")
        self.assertEqual(exit_reason(pos, 21.0, 1, True), "EOD")
        self.assertIsNone(exit_reason(pos, 21.0, 1, False))

        rec = close_position(pos, 23.0, 30 * 60_000, "TIME")
        self.assertEqual(rec["pnl"], 3.0 * 350)
        self.assertEqual(rec["mfe"], 4.0 * 350)
        self.assertEqual(rec["mae"], -1.5 * 350)
        self.assertEqual(rec["drawdown"], -525.0)
        self.assertEqual(rec["holding_minutes"], 30.0)
        self.assertEqual(rec["win"], 1)
        self.assertEqual(rec["risk_class"], "B")

    def test_eod(self):
        self.assertTrue(is_eod(dt.datetime(2026, 10, 5, 15, 20)))
        self.assertFalse(is_eod(dt.datetime(2026, 10, 5, 15, 19)))


class StatsTest(unittest.TestCase):
    def test_buckets_and_pick(self):
        stats = {}
        for pct in (10, -5, 20):
            stats = update_stats(stats, {"symbol": "TCS", "market_phase": "NORMAL_TREND", "pnl_pct": pct, "pnl": pct})
        s = summarize(stats["SYM_PHASE:TCS|NORMAL_TREND"])
        self.assertEqual(s["samples"], 3)
        self.assertAlmostEqual(s["win_rate"], 2 / 3)
        self.assertAlmostEqual(s["avg_win_pct"], 15.0)
        self.assertAlmostEqual(s["avg_loss_pct"], 5.0)
        self.assertEqual(set(stats), {"ALL", "SYM:TCS", "SYM_PHASE:TCS|NORMAL_TREND"})

        self.assertEqual(pick_bucket(stats, "TCS", "NORMAL_TREND", 3)[0], "SYM_PHASE:TCS|NORMAL_TREND")
        stats = update_stats(stats, {"symbol": "INFY", "market_phase": "X", "pnl_pct": 1, "pnl": 1})
        self.assertEqual(pick_bucket(stats, "TCS", "NORMAL_TREND", 4)[0], "ALL")
        name, s = pick_bucket(stats, "TCS", "NORMAL_TREND", 30)   # none qualifies: largest
        self.assertEqual((name, s["samples"]), ("ALL", 4))
        self.assertEqual(pick_bucket({}, "TCS", "", 30)[1]["samples"], 0)


class AccountTest(unittest.TestCase):
    POS = [{"qty": 100, "entry_premium": 50, "last_premium": 55, "sl_premium": 40},
           {"qty": 0, "entry_premium": 10}]

    def test_exposure(self):
        self.assertEqual(position_exposure(self.POS), (1, 5000.0, 500.0, 1000.0))

    def test_paper(self):
        s = paper_snapshot(100_000, self.POS, -2000, ts_ms=1)
        self.assertEqual(s.total_capital, 98_000)
        self.assertEqual(s.available_margin, 93_000)
        self.assertAlmostEqual(s.margin_utilization_pct, 5000 / 98000 * 100, places=3)
        self.assertEqual(s.day_pnl, -1500)

    def test_rms(self):
        s = parse_rms({"net": "80000", "availablecash": "82000", "utiliseddebits": "20000",
                       "m2mrealized": "-500", "m2munrealized": "200"}, self.POS, 1)
        self.assertEqual((s.mode, s.total_capital, s.available_margin), ("live", 100_000, 80_000))
        self.assertEqual(s.margin_utilization_pct, 20.0)
        self.assertEqual(s.day_pnl, -300)
        self.assertIsNone(parse_rms(None, [], 1))


class ProbabilityRunnerTest(unittest.TestCase):
    SIE = {
        "symbol": "TCS", "side": "CE", "market_phase": "NORMAL_TREND", "em_direction": "BULLISH",
        "em_direction_score": "0.5", "em_confidence": "60", "liquidity_score": "80",
        "greeks_score": "70", "spread_pct": "1.0", "liquidity_band": "GREEN",
        "entry_htf_bias": "CALL", "entry_st_bias": "CALL", "entry_ema_state": "bullish",
        "entry_volume_signal": "BULLISH VOLUME", "entry_aligned": "1", "entry_volume_surge": "False",
        "entry_oi_positioning": "BULLISH_POSITIONING",
    }

    def test_votes(self):
        self.assertEqual(confluence_votes(self.SIE, {"score": "0.3"}, "CE"), [True] * 6)
        self.assertEqual(confluence_votes(self.SIE, {"score": "-0.3"}, "PE")[:3], [False, False, False])
        self.assertIsNone(confluence_votes(self.SIE, {}, "CE")[-1])

    def test_sideways_needs_both(self):
        self.assertTrue(is_sideways("NEUTRAL", {"regime": "NEUTRAL"}))
        self.assertFalse(is_sideways("NEUTRAL", {"regime": "BULLISH"}))
        self.assertFalse(is_sideways("BULLISH", {"regime": "NEUTRAL"}))

    def test_build_inputs(self):
        inp, bucket, summary = build_inputs(self.SIE, {"score": "0.3"}, {"phase": "MARKUP"}, {"regime": "BULLISH"}, {}, 30)
        self.assertEqual(inp.confluence, 100.0)
        self.assertEqual(inp.direction, 75.0)
        self.assertEqual(inp.intensity, 67.5)        # mean(volume 75, EM conf 60)
        self.assertEqual(inp.amd, 100.0)
        self.assertEqual(inp.historical, 50.0)
        self.assertEqual(inp.oi, 100.0)
        self.assertEqual((inp.liquidity, inp.greeks, inp.spread_pct), (80.0, 70.0, 1.0))
        self.assertEqual(inp.historical_samples, 0)
        self.assertEqual(inp.regime, "BULLISH")
        sideways, _, _ = build_inputs({**self.SIE, "em_direction": "NEUTRAL"}, {}, {}, {"regime": "NEUTRAL"}, {}, 30)
        self.assertEqual(sideways.regime, "SIDEWAYS")


if __name__ == "__main__":
    unittest.main()

"""Module 18 integration hooks: ICARE re-entry/block, journal TSL exits, runner helpers."""

from __future__ import annotations

import datetime as dt
import unittest

from app.adaptive_tsl import Snapshot, TradeState, new_trade
from app.icare import evaluate
from app.trade_journal import PaperPosition, close_position, exit_reason, open_position, tsl_exit_decision, tsl_journal_fields
from run_adaptive_tsl import days_to_expiry, flat, fresh, reentry_payload, st_counts, trade_event
from run_icare import build_inputs, is_reentry, reentry_shadow
from run_probability import live_sie_fields
from tests.test_icare import CFG, _inp, _pf
from tests.test_trade_journal import APPROVAL


class ICAREReentryTest(unittest.TestCase):
    def test_reentry_cap_limits_lots(self):
        res = evaluate(_inp(reentry_max_lots=2), _pf(), CFG)
        self.assertEqual((res.status, res.recommended_lots, res.limiting_factor), ("APPROVED", 2, "reentry_cap"))

    def test_shadow_and_block_reject(self):
        self.assertIn("tsl_shadow_mode", evaluate(_inp(reentry_shadow=True), _pf(), CFG).reasons)
        res = evaluate(_inp(blocked=True), _pf(), CFG)
        self.assertEqual(res.status, "REJECTED")
        self.assertIn("reentry_blocked", res.reasons)

    def test_runner_reentry_flags(self):
        msg = {"reentry": "1", "tsl_mode": "active", "max_lots": "3", "symbol": "SBIN"}
        self.assertTrue(is_reentry(msg))
        self.assertFalse(reentry_shadow(msg, "active"))
        self.assertTrue(reentry_shadow(msg, "shadow"))
        self.assertTrue(reentry_shadow(dict(msg, tsl_mode="shadow"), "active"))
        self.assertFalse(reentry_shadow({"symbol": "SBIN"}, "shadow"))   # normal entries unaffected
        inp = build_inputs(msg, {}, {}, blocked=True)
        self.assertEqual((inp.reentry_max_lots, inp.blocked), (3.0, True))
        self.assertIsNone(build_inputs({"symbol": "SBIN", "max_lots": "3"}, {}, {}).reentry_max_lots)


class JournalTSLTest(unittest.TestCase):
    def setUp(self):
        self.pos = open_position(dict(APPROVAL, chain_id="C1", reentry_no="1"), 20.0, now_ms=0)

    def test_chain_fields_in_context(self):
        self.assertEqual((self.pos.context["chain_id"], self.pos.context["reentry_no"]), ("C1", "1"))

    def test_decision_shadow_never_changes_exits(self):
        tsl = {"trade_id": self.pos.trade_id, "status": "EXIT", "updated_ms": 1000}
        self.assertEqual(tsl_exit_decision(tsl, self.pos.trade_id, 1000, "shadow", 60_000), (None, True))

    def test_decision_active(self):
        tid = self.pos.trade_id
        live = {"trade_id": tid, "status": "ACTIVE", "updated_ms": 1000}
        self.assertEqual(tsl_exit_decision(live, tid, 2000, "active", 60_000), (None, False))      # target off
        self.assertEqual(tsl_exit_decision(dict(live, status="EXIT"), tid, 2000, "active", 60_000),
                         ("TRAILING_STOP", False))
        self.assertEqual(tsl_exit_decision(live, tid, 70_000, "active", 60_000), (None, True))     # engine stale
        self.assertEqual(tsl_exit_decision(dict(live, trade_id="other"), tid, 2000, "active", 60_000), (None, True))

    def test_exit_reason_with_tsl(self):
        p = self.pos      # sl 17, target 26
        self.assertIsNone(exit_reason(p, 27.0, 1, False, use_target=False))
        self.assertEqual(exit_reason(p, 27.0, 1, False), "TARGET")
        self.assertEqual(exit_reason(p, 22.0, 1, False, force="TRAILING_STOP"), "TRAILING_STOP")
        self.assertEqual(exit_reason(p, 16.0, 1, False, force="TRAILING_STOP"), "SL")

    def test_counterfactual_fields(self):
        tsl = {"trade_id": self.pos.trade_id, "status": "EXIT", "exit_price": 24.0, "exit_ts_ms": 5,
               "activated": True, "stop": 24.2, "tsl_pct": 8.0, "tsl_rule": "STRONG_HIGH_VOL",
               "exit_trigger": "CONFIRMED", "mode": "shadow"}
        f = tsl_journal_fields(tsl, self.pos)
        self.assertEqual(f["tsl_shadow_pnl"], round(4.0 * self.pos.qty, 2))
        self.assertEqual((f["tsl_would_exit_px"], f["tsl_activated"], f["tsl_exit_trigger"]), (24.0, 1, "CONFIRMED"))
        rec = close_position(self.pos, 18.0, 10, "SL")
        rec.update(f)
        self.assertEqual(rec["exit_reason"], "SL")
        self.assertEqual(tsl_journal_fields({}, self.pos), {"tsl_status": ""})


class ProbabilityRescoreTest(unittest.TestCase):
    def test_live_fields_replace_entry_votes(self):
        base = {"symbol": "SBIN", "side": "CE", "entry_aligned": "1", "entry_st_bias": "PUT", "greeks_score": "70"}
        out = live_sie_fields(
            base, htf={"bias": "CALL"}, st={"bias": "CALL"}, ema={"state": "bullish"},
            volume={"signal": "Strong Bullish Volume"}, oi_und={"positioning": "BULLISH_POSITIONING"},
            em={"direction": "BULLISH", "direction_score": 0.4, "confidence": 70},
            liq={"liquidity_score": "82", "liquidity_band": "GREEN"}, bidask={"spread_pct": "0.8"},
        )
        self.assertEqual((out["entry_st_bias"], out["entry_aligned"], out["entry_volume_surge"]), ("CALL", "", "1"))
        self.assertEqual((out["liquidity_score"], out["spread_pct"], out["greeks_score"]), ("82", "0.8", "70"))
        self.assertEqual(base["entry_st_bias"], "PUT")     # input not mutated


class RunnerHelpersTest(unittest.TestCase):
    def test_fresh_and_counts(self):
        self.assertEqual(fresh({"ts_ms": "1000"}, 2000, 5000), {"ts_ms": "1000"})
        self.assertEqual(fresh({"ts_ms": "1000"}, 9000, 5000), {})
        self.assertEqual(fresh({}, 9000, 5000), {})
        doc = {"bullish": "3", "bearish": "1", "st_1m": "bullish", "st_5m": "bullish", "st_10m": "na", "st_30m": "bearish"}
        self.assertEqual(st_counts(doc), (3, 1, 3))
        self.assertEqual(days_to_expiry("2026-10-27", dt.date(2026, 10, 27)), 0)
        self.assertIsNone(days_to_expiry("bad", dt.date(2026, 10, 27)))

    def test_flat(self):
        self.assertEqual(flat({"a": True, "b": None, "c": {"x": 1}, "d": 2.5}),
                         {"a": "1", "b": "", "c": '{"x":1}', "d": "2.5"})

    def test_trade_events(self):
        st = new_trade("T", "T", 0, "SBIN", "X", "CE", 1, 100, 70, 0)
        self.assertEqual(trade_event(None, st), "TRACK")
        from dataclasses import replace
        self.assertEqual(trade_event(st, replace(st, activated=True, stop=95)), "ACTIVATED")
        self.assertEqual(trade_event(st, replace(st, stop=95)), "STOP_MOVED")
        self.assertEqual(trade_event(st, replace(st, status="EXIT")), "EXIT")
        self.assertEqual(trade_event(st, st), "")

    def test_reentry_payload_rescales_projection(self):
        origin = {"premium": "100", "projected_premium_gain": "20", "projected_premium_change_adverse": "-10"}
        signal = {"chain_id": "C", "parent_trade_id": "T", "reentry_no": 1, "max_lots": 2, "watch_price": 124.2,
                  "confidence": 91.0, "reason": "SWING_HIGH_BREAK_WITH_CONFLUENCE", "entry_price": 124.3}
        snap = Snapshot(now_ms=0, bid=124.0, ask=126.0)
        out = reentry_payload({"probability": "91", "decision": "HIGH_CONVICTION"}, signal, snap, origin)
        self.assertEqual((out["premium"], out["projected_premium_gain"], out["projected_premium_change_adverse"]),
                         ("125.0000", "25.0000", "-12.5000"))
        self.assertEqual((out["reentry"], out["max_lots"], out["reentry_no"], out["entry_signal"]), ("1", "2", "1", "REENTER"))


if __name__ == "__main__":
    unittest.main()

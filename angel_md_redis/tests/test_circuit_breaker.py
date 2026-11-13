"""Module 17 pure logic: loss caps, mismatch, watchdog, abnormal, state machine, emergency exits."""

from __future__ import annotations

import unittest

from app import circuit_breaker as C

NOW = 1_800_000_000_000
CFG = C.CircuitConfig(daily_loss_cap=10000, max_loss_per_trade=4000, max_total_exposure=100000,
                      mismatch_grace_sec=60, exit_unfilled_sec=100, cmd_timeout_sec=60,
                      exec_fill_timeout_sec=120, exit_fill_timeout_sec=100, max_orders_per_min=5,
                      max_rejects_window=3, reject_window_sec=300, heartbeat_max_age_sec=60, pnl_jump=5000,
                      warn_clear_sec=300)


def pos(tid="T1", qty=100.0, entry=100.0, last=100.0, **kw):
    return dict(trade_id=tid, tradingsymbol="X" + tid, symbol="X", qty=qty, orig_qty=qty, entry_premium=entry,
                last_premium=last, lot_size=50, **kw)


def codes(fs):
    return sorted(f.code for f in fs)


class LossCaps(unittest.TestCase):
    def test_no_findings_when_flat(self):
        self.assertEqual(C.check_loss_caps(CFG, 0, [pos()]), [])

    def test_daily_loss_levels(self):
        self.assertEqual(C.worst(C.check_loss_caps(CFG, -8000, [])), C.SEV_WARNING)
        self.assertEqual(C.worst(C.check_loss_caps(CFG, -10000, [])), C.SEV_CRITICAL)
        self.assertEqual(C.worst(C.check_loss_caps(CFG, -15000, [])), C.SEV_EMERGENCY)

    def test_daily_includes_unrealized(self):
        fs = C.check_loss_caps(CFG, -5000, [pos(qty=100, entry=100, last=45)])   # -5000 -5500 = -10500
        self.assertIn("DAILY_LOSS_CAP", codes(fs))

    def test_per_trade(self):
        fs = C.check_loss_caps(CFG, 0, [pos("A", last=60)])                       # -4000
        self.assertEqual([(f.code, f.severity, f.key) for f in fs], [("TRADE_LOSS_CAP", C.SEV_CRITICAL, "A")])

    def test_exposure(self):
        fs = C.check_loss_caps(CFG, 0, [pos("A", qty=1000, entry=100, last=100)])  # 100000
        self.assertIn("EXPOSURE_CAP", codes(fs))

    def test_missing_last_premium_uses_entry(self):
        p = pos(); p["last_premium"] = None
        self.assertEqual(C.position_pnl(p), 0.0)

    def test_profit_is_fine(self):
        self.assertEqual(C.check_loss_caps(CFG, 50000, [pos(last=150)]), [])


class Mismatch(unittest.TestCase):
    def ex(self, tid="T1", status="FILLED", units=100.0, fin=NOW - 200_000):
        return dict(trade_id=tid, status=status, finished_ms=fin, fills=[[100.0, units, fin, "o1"]])

    def test_filled_without_position(self):
        fs = C.check_mismatch(CFG, NOW, [], [self.ex()], [])
        self.assertEqual(codes(fs), ["ENTRY_FILLED_NO_POSITION"])

    def test_closed_trade_is_fine(self):
        self.assertEqual(C.check_mismatch(CFG, NOW, [], [self.ex()], [], {"T1"}), [])

    def test_grace_period(self):
        self.assertEqual(C.check_mismatch(CFG, NOW, [], [self.ex(fin=NOW - 10_000)], []), [])

    def test_shadow_mode_skips_entry_check(self):
        cfg = C.CircuitConfig(check_mismatch_entries=False)
        self.assertEqual(C.check_mismatch(cfg, NOW, [], [self.ex()], []), [])

    def test_qty_mismatch(self):
        fs = C.check_mismatch(CFG, NOW, [pos(qty=50)], [self.ex(units=100)], [])
        self.assertEqual(codes(fs), ["ENTRY_QTY_MISMATCH"])

    def test_qty_match_and_unfilled_ignored(self):
        self.assertEqual(C.check_mismatch(CFG, NOW, [pos(qty=100)], [self.ex(), self.ex("T2", "CANCELLED_TIMEOUT", 0)], []), [])

    def test_exit_commanded_not_filled(self):
        p = pos(exit_pending="SL", exit_requested_ms=NOW - 150_000)
        self.assertEqual(codes(C.check_mismatch(CFG, NOW, [p], [], [])), ["EXIT_COMMANDED_NOT_FILLED"])
        p = pos(exit_pending="SL", exit_requested_ms=NOW - 10_000)
        self.assertEqual(C.check_mismatch(CFG, NOW, [p], [], []), [])

    def test_exit_filled_position_still_open(self):
        xs = dict(position_trade_id="T1", status="EXIT_FILLED", finished_ms=NOW - 200_000, fills=[[90, 100, 0, "o"]])
        self.assertEqual(codes(C.check_mismatch(CFG, NOW, [pos()], [], [xs])), ["EXIT_FILLED_POSITION_OPEN"])

    def test_partial_exit_fill_is_not_mismatch(self):
        xs = dict(position_trade_id="T1", status="EXIT_FILLED", finished_ms=NOW - 200_000, fills=[[90, 40, 0, "o"]])
        self.assertEqual(C.check_mismatch(CFG, NOW, [pos(qty=60)], [], [xs]), [])


class Watchdog(unittest.TestCase):
    def test_approval_not_acted(self):
        a = [{"tsym": "S", "ts_ms": NOW - 90_000}]
        self.assertEqual(codes(C.check_watchdog(CFG, NOW, a, [], [], [], [])), ["APPROVAL_NOT_ACTED"])

    def test_approval_acted_or_young(self):
        a = [{"tsym": "S", "ts_ms": NOW - 90_000}]
        self.assertEqual(C.check_watchdog(CFG, NOW, a, [{"tsym": "S", "ts_ms": NOW - 89_000}], [], [], []), [])
        self.assertEqual(C.check_watchdog(CFG, NOW, [{"tsym": "S", "ts_ms": NOW - 5_000}], [], [], [], []), [])

    def test_very_old_approval_ignored(self):
        self.assertEqual(C.check_watchdog(CFG, NOW, [{"tsym": "S", "ts_ms": NOW - 900_000}], [], [], [], []), [])

    def test_entry_stuck(self):
        st = dict(trade_id="T", status="WORKING", received_ms=NOW - 200_000)
        self.assertEqual(codes(C.check_watchdog(CFG, NOW, [], [], [st], [], [])), ["ENTRY_NOT_FILLED"])
        st["status"] = "FILLED"
        self.assertEqual(C.check_watchdog(CFG, NOW, [], [], [st], [], []), [])

    def test_exit_stuck_escalates(self):
        xs = dict(position_trade_id="T", status="EXIT_WORKING", received_ms=NOW - 150_000)
        self.assertEqual(C.worst(C.check_watchdog(CFG, NOW, [], [], [], [xs], [])), C.SEV_WARNING)
        xs["received_ms"] = NOW - 400_000
        self.assertEqual(C.worst(C.check_watchdog(CFG, NOW, [], [], [], [xs], [])), C.SEV_CRITICAL)

    def test_exit_request_not_acted(self):
        p = pos(exit_pending="SL", exit_requested_ms=NOW - 90_000)
        self.assertEqual(codes(C.check_watchdog(CFG, NOW, [], [], [], [], [p])), ["EXIT_REQUEST_NOT_ACTED"])
        xs = dict(position_trade_id="T1", status="EXIT_WORKING", received_ms=NOW - 5_000)
        self.assertEqual(C.check_watchdog(CFG, NOW, [], [], [], [xs], [p]), [])


class Abnormal(unittest.TestCase):
    def test_order_rate(self):
        ts = [NOW - i * 1000 for i in range(6)]
        self.assertEqual(codes(C.check_abnormal(CFG, NOW, ts, [], {}, None, None)), ["ORDER_RATE"])
        self.assertEqual(C.check_abnormal(CFG, NOW, ts[:5], [], {}, None, None), [])
        self.assertEqual(C.check_abnormal(CFG, NOW, [NOW - 120_000] * 20, [], {}, None, None), [])

    def test_rejections(self):
        rj = [NOW - 1000 * i for i in range(3)]
        self.assertEqual(C.worst(C.check_abnormal(CFG, NOW, [], rj, {}, None, None)), C.SEV_WARNING)
        self.assertEqual(C.worst(C.check_abnormal(CFG, NOW, [], rj * 2, {}, None, None)), C.SEV_CRITICAL)
        self.assertEqual(C.check_abnormal(CFG, NOW, [], rj[:2], {}, None, None), [])

    def test_heartbeats(self):
        fs = C.check_abnormal(CFG, NOW, [], [], {"a": NOW - 1000, "b": NOW - 90_000, "c": None}, None, None)
        self.assertEqual([(f.key, f.severity) for f in fs], [("b", C.SEV_WARNING), ("c", C.SEV_WARNING)])

    def test_pnl_jump(self):
        self.assertEqual(codes(C.check_abnormal(CFG, NOW, [], [], {}, 0, -6000)), ["PNL_JUMP"])
        self.assertEqual(C.check_abnormal(CFG, NOW, [], [], {}, 0, -1000), [])
        self.assertEqual(C.check_abnormal(CFG, NOW, [], [], {}, None, -99999), [])


class StateMachine(unittest.TestCase):
    W = [C.Finding("X", C.SEV_WARNING)]
    K = [C.Finding("DAILY_LOSS_CAP", C.SEV_CRITICAL)]
    E = [C.Finding("DAILY_LOSS_CAP", C.SEV_EMERGENCY)]

    def test_armed_stays_armed(self):
        st, ev = C.advance(C.BreakerState(), [], NOW, CFG)
        self.assertEqual((st.state, ev), (C.ARMED, []))

    def test_warning_then_clear(self):
        st, ev = C.advance(C.BreakerState(), self.W, NOW, CFG)
        self.assertEqual((st.state, ev[0]["event"]), (C.WARNING, "WARNING"))
        st, ev = C.advance(st, [], NOW + 100_000, CFG)
        self.assertEqual((st.state, ev), (C.WARNING, []))
        st, ev = C.advance(st, [], NOW + 301_000, CFG)
        self.assertEqual((st.state, ev[0]["event"]), (C.ARMED, "CLEARED"))

    def test_warning_does_not_repeat_event(self):
        st, _ = C.advance(C.BreakerState(), self.W, NOW, CFG)
        st2, ev = C.advance(st, self.W, NOW + 1000, CFG)
        self.assertEqual((st2.state, ev, st2.since_ms), (C.WARNING, [], NOW))

    def test_trip_from_armed_and_warning(self):
        st, ev = C.advance(C.BreakerState(), self.K, NOW, CFG)
        self.assertEqual((st.state, ev[0]["event"], st.severity), (C.TRIPPED, "TRIPPED", C.SEV_CRITICAL))
        w, _ = C.advance(C.BreakerState(), self.W, NOW, CFG)
        self.assertEqual(C.advance(w, self.K, NOW + 1, CFG)[0].state, C.TRIPPED)

    def test_tripped_is_sticky(self):
        st, _ = C.advance(C.BreakerState(), self.K, NOW, CFG)
        st2, ev = C.advance(st, [], NOW + 10 ** 9, CFG)
        self.assertEqual((st2, ev), (st, []))

    def test_escalation_while_tripped(self):
        st, _ = C.advance(C.BreakerState(), self.K, NOW, CFG)
        st2, ev = C.advance(st, self.E, NOW + 1, CFG)
        self.assertEqual((st2.severity, ev[0]["event"]), (C.SEV_EMERGENCY, "ESCALATED"))
        self.assertTrue(C.should_emergency_exit(st2))

    def test_reset(self):
        st, _ = C.advance(C.BreakerState(), self.K, NOW, CFG)
        st2, ev = C.reset(st, NOW + 5)
        self.assertEqual((st2.state, ev["event"], ev["from"]), (C.ARMED, "RESET", C.TRIPPED))

    def test_roundtrip_dict(self):
        st, _ = C.advance(C.BreakerState(), self.E, NOW, CFG)
        self.assertEqual(C.BreakerState.from_dict(st.to_dict()), st)
        self.assertEqual(C.BreakerState.from_dict(None), C.BreakerState())
        self.assertEqual(C.BreakerState.from_dict({"state": "garbage"}).state, C.ARMED)


class Emergency(unittest.TestCase):
    def test_not_for_critical_or_handled(self):
        st, _ = C.advance(C.BreakerState(), StateMachine.K, NOW, CFG)
        self.assertFalse(C.should_emergency_exit(st))
        st, _ = C.advance(C.BreakerState(), StateMachine.E, NOW, CFG)
        self.assertTrue(C.should_emergency_exit(st))
        from dataclasses import replace
        self.assertFalse(C.should_emergency_exit(replace(st, emergency_handled=True)))

    def test_requests_format_and_skip_pending(self):
        ps = [pos("A", exec_mode="paper"), pos("B", exit_pending="SL"), pos("C", qty=0)]
        reqs = C.emergency_exit_requests(ps, NOW, "live")
        self.assertEqual(len(reqs), 1)
        q = reqs[0]
        self.assertEqual((q["trade_id"], q["exec_mode"], q["reason"], q["qty"], q["ts_ms"]),
                         ("A", "paper", "CIRCUIT_BREAKER", "100.0", str(NOW)))
        self.assertEqual(set(q), {"trade_id", "tradingsymbol", "symbol", "qty", "orig_qty", "lot_size", "reason",
                                  "limit_floor", "bid", "exec_mode", "ts_ms"})
        self.assertEqual(C.emergency_exit_requests([pos("D")], NOW, "live")[0]["exec_mode"], "live")


class Config(unittest.TestCase):
    def test_env(self):
        import os
        from unittest import mock
        with mock.patch.dict(os.environ, {"CB_DAILY_LOSS_CAP": "123", "CB_MAX_ORDERS_PER_MIN": "7", "EXEC_MODE": "paper",
                                          "CB_PNL_JUMP": "bad"}):
            c = C.CircuitConfig.from_env()
        self.assertEqual((c.daily_loss_cap, c.max_orders_per_min, c.check_mismatch_entries, c.pnl_jump),
                         (123.0, 7, True, 15000.0))


if __name__ == "__main__":
    unittest.main()

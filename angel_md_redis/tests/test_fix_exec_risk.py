"""QA fix 6 (risk vs ICARE SL) + cheap fixes (price-band re-quote, modify / cancel failures)."""

from __future__ import annotations

import os
import sys
import unittest
from dataclasses import replace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.order_executor import state as S
from app.order_executor.charges import ChargeRates
from app.order_executor.command import Context, build_command
from app.order_executor.config import ExecConfig
from app.order_executor.engine import action_failed, new_state, step
from app.order_executor.market import Quote
from app.order_executor.quantity import executable_lots
from app.trade_journal import open_position
from test_order_executor_runner import LOT, icare_msg

CFG = ExecConfig()
RATES = ChargeRates()
CTX = Context(hhmm="10:15", available_margin=10_000_000.0)


def cmd(**over):
    return build_command(icare_msg(now_ms=0, **over), "TRD_1", "1-0", {"tick": 0.05, "kind": "OPTSTK"}, CFG)


class RiskTest(unittest.TestCase):
    def test_ask_below_icare_sl_rejected(self):
        # SL 80, ask 79.30: previously 4 lots, risk Rs 61.5k after the journal re-anchored the SL
        c = cmd(premium="79.4", max_risk_allowed="30000")
        st, actions, _ = step(new_state(c, 0), Quote(0, 79.2, 79.3, ask_levels=((79.3, 4 * LOT),)), CTX, [], 0, CFG, RATES)
        self.assertEqual(st.status, S.REJECTED_BEFORE_EXECUTION)
        self.assertIn("PRICE_BELOW_STOP", st.reject_reasons)
        self.assertEqual(actions, [])

    def test_cap_at_or_below_sl_gives_zero_lots(self):
        lots, limiting, _r, limits = executable_lots(4, LOT, 79.6, None, 30_000, -0.4, None, None, CFG)
        self.assertEqual((lots, limiting, limits["risk"]), (0, "risk", 0))

    def test_risk_at_cap_never_exceeds_icare_max(self):
        c = cmd(max_risk_allowed="30000")                            # SL 80
        st, actions, _ = step(new_state(c, 0), Quote(0, 100.7, 100.8, ask_levels=((100.8, 4 * LOT),)), CTX, [], 0,
                              CFG, RATES)
        self.assertEqual(st.cap, 101.3)                              # 100.8 x 1.005 = 101.304
        self.assertEqual(st.target_lots, 1)                          # 30,000 / (21.3 x 750) = 1.88
        self.assertLessEqual((st.cap - 80.0) * st.target_lots * LOT, 30_000)

    def test_journal_keeps_icare_sl_and_caps_lots(self):
        self.assertIsNone(open_position(icare_msg(), 79.3, 0))      # entry below SL 80: refused
        pos = open_position(icare_msg(max_risk_allowed="30000"), 100.8, 0)
        self.assertEqual((pos.sl_premium, pos.lots), (80.0, 1))      # 20.8 x 750 = 15,600 per lot
        self.assertLessEqual((pos.entry_premium - pos.sl_premium) * pos.qty, 30_000)


class BandAndFailureTest(unittest.TestCase):
    def _started(self):
        q0 = Quote(0, 100.0, 101.0, ltp=100.2, ask_levels=((101.0, 4 * LOT),))
        st, a, _ = step(new_state(cmd(), 0), q0, CTX, [], 0, CFG, RATES)
        return st, a[0]

    def test_band_rejection_requotes_inside_band(self):
        st, place = self._started()
        self.assertEqual(place["price"], 100.5)
        q1 = Quote(500, 100.0, 101.0, ltp=100.2, ask_levels=((101.0, 4 * LOT),))
        st, a, _ = step(st, q1, CTX, [S.OrderUpdate(place["order_id"], "REJECTED", 0, None, "LPP band")], 500,
                        CFG, RATES)
        self.assertEqual((a[0]["type"], a[0]["price"]), ("PLACE", 100.2))   # LTP, not the refused 100.50

    def test_modify_failure_keeps_old_price(self):
        st, place = self._started()
        st, a, _ = step(st, Quote(2000, 100.0, 101.0, ask_levels=((101.0, 4 * LOT),)), CTX, [], 2000, CFG, RATES)
        self.assertEqual((a[0]["type"], a[0]["price"]), ("MODIFY", 100.7))
        st, ev = action_failed(st, a[0], "MODIFY_ERROR:timeout", 2000)
        self.assertEqual(st.working["price"], 100.5)
        self.assertEqual(ev[0]["event"], "MODIFY_FAILED")

    def test_cancel_failure_resent(self):
        st, place = self._started()
        st, a, _ = step(st, Quote(500, 100.0, 101.0), replace(CTX, kill_switch=True), [], 500, CFG, RATES)
        self.assertEqual(a[0]["type"], "CANCEL")
        st, _ = action_failed(st, a[0], "CANCEL_ERROR:timeout", 500)
        st, a, _ = step(st, Quote(1500, 100.0, 101.0), replace(CTX, kill_switch=True), [], 1500, CFG, RATES)
        self.assertEqual([x["type"] for x in a], ["CANCEL"])        # re-sent ~1 s later, not after 5 s

    def test_rate_limited_place_is_retried(self):
        st, place = self._started()
        st, ev = action_failed(st, place, "RATE_LIMIT_LOCAL", 0)
        self.assertIsNone(st.working)
        st, a, _ = step(st, Quote(500, 100.0, 101.0, ask_levels=((101.0, 4 * LOT),)), CTX, [], 500, CFG, RATES)
        self.assertEqual(a[0]["type"], "PLACE")
        self.assertEqual(st.status, S.WORKING)


if __name__ == "__main__":
    unittest.main()

"""Integration-review fixes: fills never orphaned, total-qty modify, per-stream locks, receive-time quote age, gross EV."""

from __future__ import annotations

import json
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import run_order_executor as roe
import run_trade_journal as rtj
from app.order_executor import exit_engine as X
from app.order_executor import state as S
from app.order_executor.broker import PaperBroker
from app.order_executor.charges import ChargeRates
from app.order_executor.command import Context, build_command
from app.order_executor.config import ExecConfig
from app.order_executor.engine import new_state, step
from app.order_executor.market import Quote, quote_from_bidask
from app.trade_journal import open_position
from test_fix_exec_exit import REQ, FakeSmart, angel, bidbook
from test_order_executor_runner import LOT, T0, TSYM, FakeRedis, account, book, icare_msg, runner

CFG = ExecConfig()
RATES = ChargeRates()
CTX = Context(hhmm="10:15", available_margin=1e7)
POS_KEY = f"md:position:open:{TSYM}"


def cmd(**over):
    return build_command(icare_msg(now_ms=0, **over), "TRD_1", "1-0", {"tick": 0.05, "kind": "OPTSTK"}, CFG)


def fill_msg(price="80.0", lots="4", tid="TRD_X_001", mode="live"):
    return dict(icare_msg(), recommended_lots=lots, fill_price=price, exec_mode=mode, exec_trade_id=tid,
                entry_charges="50")


@mock.patch.object(rtj, "is_eod", return_value=False)
class FillsAreFactsTest(unittest.TestCase):
    def test_fill_at_sl_opened_forced_and_exit_requested(self, _eod):
        r = FakeRedis()
        rtj.handle_approval(r, fill_msg("80.0"), T0, msg_id=f"{T0}-0", mode="live")     # SL 80
        pos = json.loads(r.get(POS_KEY))
        self.assertEqual((pos["qty"], pos["opened_forced"], pos["exit_pending"]),
                         (4 * LOT, "OPENED_FORCED:PRICE_BELOW_STOP", "FORCED_PRICE_BELOW_STOP"))
        req = r.streams["md:exec:exit_request"][0]
        self.assertEqual((req["qty"], req["reason"]), (str(4 * LOT), "FORCED_PRICE_BELOW_STOP"))

    def test_fill_after_eod_opened_and_exited(self, eod):
        eod.return_value = True
        r = FakeRedis()
        rtj.handle_approval(r, fill_msg("100.8"), T0, msg_id=f"{T0}-0", mode="live")
        self.assertEqual(json.loads(r.get(POS_KEY))["exit_pending"], "FORCED_AFTER_EOD")

    def test_second_fill_merged_not_dropped(self, _eod):
        r = FakeRedis()
        rtj.handle_approval(r, fill_msg("100.0", "2", "T1"), T0, msg_id=f"{T0}-0", mode="live")
        rtj.handle_approval(r, fill_msg("101.0", "2", "T2"), T0 + 1000, msg_id=f"{T0 + 1000}-0", mode="live")
        pos = json.loads(r.get(POS_KEY))
        self.assertEqual((pos["lots"], pos["qty"], pos["entry_premium"], pos["orig_qty"]), (4, 3000.0, 100.5, 3000.0))
        self.assertEqual(pos["context"]["entry_charges"], "100.0")

    def test_real_fill_not_resized_by_risk(self, _eod):
        r = FakeRedis()
        rtj.handle_approval(r, dict(fill_msg("100.8"), max_risk_allowed="30000"), T0, msg_id=f"{T0}-0", mode="live")
        self.assertEqual(json.loads(r.get(POS_KEY))["lots"], 4)      # the broker holds 4 lots

    def test_shadow_still_refuses_entry_below_sl(self, _eod):
        self.assertIsNone(open_position(icare_msg(), 79.9, 0))


class EntryStopTest(unittest.TestCase):
    def test_start_price_at_sl_rejected(self):
        # SL 80, bid 79.70 / ask 80.30: ask > SL but the mid start 80.00 is AT the stop
        st, actions, _ = step(new_state(cmd(premium="80.2"), 0), Quote(0, 79.7, 80.3, ask_levels=((80.3, 4 * LOT),)),
                              CTX, [], 0, CFG, RATES)
        self.assertEqual((st.status, st.reject_reasons, actions), (S.REJECTED_BEFORE_EXECUTION, ["PRICE_BELOW_STOP"], []))

    def test_later_slice_at_sl_stops(self):
        st, a, _ = step(new_state(cmd(premium="81"), 0), Quote(0, 80.9, 81.0, ask_levels=((81.0, LOT),)), CTX, [], 0,
                        CFG, RATES)
        oid = a[0]["order_id"]
        st, a, _ = step(st, Quote(500, 79.7, 80.3, ask_levels=((80.3, 4 * LOT),)), CTX,
                        [S.OrderUpdate(oid, "COMPLETE", LOT, 81.0)], 500, CFG, RATES)
        self.assertEqual(a, [])
        self.assertEqual((st.status, st.closing), (S.PARTIAL_FILL_STOPPED, "PRICE_BELOW_STOP"))


class ModifyTotalQtyTest(unittest.TestCase):
    def test_entry_modify_after_partial_sends_total(self):
        broker = PaperBroker(latency_ms=0)
        q0 = Quote(0, 100.0, 101.0, ask_levels=((101.0, 4 * LOT),))
        st, a, _ = step(new_state(cmd(), 0), q0, CTX, [], 0, CFG, RATES)
        broker.place(a[0], 0)
        oid = a[0]["order_id"]
        # 1 of 4 lots filled, then the ladder re-prices the resting 3
        st, a, _ = step(st, Quote(2000, 100.0, 101.0), CTX, [S.OrderUpdate(oid, "OPEN", LOT, 100.5)], 2000, CFG, RATES)
        self.assertEqual((a[0]["type"], a[0]["qty"]), ("MODIFY", 4 * LOT))
        self.assertEqual(broker.modify(oid, a[0]["price"], a[0]["qty"], 2000), (True, ""))
        self.assertEqual(broker.modify(oid, a[0]["price"], 3 * LOT, 2000), (False, "MODIFY_QTY_NOT_TOTAL"))

    def test_angel_modify_full_params(self):
        r, api = FakeRedis(), FakeSmart()
        bidbook(r, T0, 100.0, 100.5)
        rn = roe.ExecutorRunner(r, angel(api), {TSYM: {"tick": 0.05}}, mode="live")
        rn.on_exit_request(f"{T0}-0", dict(REQ, exec_mode="live"), T0)
        api.book = [{"orderid": "B1", "status": "open", "filledshares": "750", "averageprice": "100"}]
        bidbook(r, T0 + 1000, 99.0, 99.5)
        rn.tick(T0 + 1000)
        m = api.modified[0]
        self.assertEqual((m["quantity"], m["tradingsymbol"], m["symboltoken"], m["exchange"], m["ordertype"],
                          m["producttype"], m["duration"], m["variety"]),
                         ("1500", TSYM, "1", "NFO", "LIMIT", "INTRADAY", "DAY", "NORMAL"))


class LockTest(unittest.TestCase):
    def test_same_id_on_two_streams_both_processed(self):
        r = FakeRedis()
        book(r, T0, 100.70, 100.80, [(100.80, 4 * LOT)])
        account(r, T0)
        rn = runner(r)
        self.assertIsNotNone(rn.on_icare(f"{T0}-0", icare_msg(), T0))
        self.assertIsNotNone(rn.on_exit_request(f"{T0}-0", dict(REQ, tradingsymbol="OTHER"), T0))


class QuoteAgeTest(unittest.TestCase):
    def test_receive_time_is_the_age_basis(self):
        q = quote_from_bidask({"ts_ms": str(T0 - 10_000), "recv_ts_ms": str(T0 - 500), "bid": "100", "ask": "101"})
        self.assertEqual(q.age_ms(T0), 500)
        self.assertEqual(quote_from_bidask({"ts_ms": str(T0 - 10_000), "bid": "100", "ask": "101"}).age_ms(T0), 10_000)

    def test_entry_waits_for_fresh_quote_within_ttl(self):
        st = new_state(cmd(), 0)
        st, a, ev = step(st, Quote(-10_000, 100.7, 100.8), CTX, [], 0, CFG, RATES)       # stale
        self.assertEqual((st.status, a), (S.RECEIVED, []))
        self.assertIn("WAIT_QUOTE", [e["event"] for e in ev])
        st, a, _ = step(st, Quote(1000, 100.7, 100.8, ask_levels=((100.8, 4 * LOT),)), CTX, [], 1000, CFG, RATES)
        self.assertEqual(a[0]["type"], "PLACE")

    def test_entry_rejected_when_no_quote_by_ttl(self):
        st = new_state(cmd(), 0)
        for t in (0, 2500, 5001):
            st, a, _ = step(st, None, CTX, [], t, CFG, RATES)
        self.assertEqual(st.status, S.REJECTED_BEFORE_EXECUTION)
        self.assertIn("STALE_QUOTE", st.reject_reasons)

    def test_exit_uses_30s_window_then_last_known_bid(self):
        st = X.new_exit_state("EXT_1", dict(REQ, bid="99.0"), 0, {"tick": 0.05})
        st, a, _ = X.step_exit(st, Quote(-20_000, 99.5, 100.0), [], 0, CFG)              # 20 s old: still usable
        self.assertEqual((a[0]["type"], a[0]["price"]), ("PLACE", 99.5))
        st = X.new_exit_state("EXT_2", dict(REQ, bid="99.0"), 0, {"tick": 0.05})
        st, a, _ = X.step_exit(st, None, [], 0, CFG)
        self.assertEqual(a, [])                                                            # wait first
        st, a, _ = X.step_exit(st, None, [], 30_000, CFG)
        self.assertEqual((a[0]["type"], a[0]["price"]), ("PLACE", 99.0))                   # never blocked forever
        self.assertEqual(st.floor, 97.0)                                                    # 99 x 0.98 = 97.02


class GrossEvTest(unittest.TestCase):
    def test_gross_ev_preferred(self):
        self.assertEqual(cmd(gross_ev="2100", expected_value="2000").ev_per_lot, 2100.0)
        self.assertEqual(cmd(expected_value="2000").ev_per_lot, 2000.0)


if __name__ == "__main__":
    unittest.main()

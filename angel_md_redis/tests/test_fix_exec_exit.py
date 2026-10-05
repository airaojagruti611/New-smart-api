"""QA fix 3 (CRITICAL): live exit path (E17) — journal -> md:exec:exit_request -> SELL ladder -> md:exec:exit_fill."""

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
from app.order_executor.broker import AngelBroker, LiveGate, PaperBroker, RateBucket
from app.order_executor.config import ExecConfig
from app.order_executor.market import Quote
from app.trade_journal import open_position
from test_order_executor_runner import LOT, T0, TSYM, FakeRedis, book, icare_msg, runner

CFG = ExecConfig()
POS_KEY = f"md:position:open:{TSYM}"
REQ = {"trade_id": "SBIN-1", "tradingsymbol": TSYM, "symbol": "SBIN", "qty": str(2 * LOT), "orig_qty": str(2 * LOT),
       "lot_size": str(LOT), "reason": "SL", "limit_floor": "", "exec_mode": "paper", "ts_ms": str(T0)}


def bidbook(r, ts, bid, ask):
    r.set(f"md:bidask:latest:{TSYM}", json.dumps({"ts_ms": str(ts), "bid": str(bid), "ask": str(ask)}))


class FakeSmart:
    def __init__(self):
        self.placed, self.cancelled, self.modified, self.book = [], [], [], []

    def placeOrder(self, p):
        self.placed.append(p)
        return f"B{len(self.placed)}"

    def modifyOrder(self, p):
        self.modified.append(p)

    def cancelOrder(self, oid, variety):
        self.cancelled.append(oid)

    def orderBook(self):
        return {"data": self.book}


def angel(api):
    return AngelBroker(api=api, tokens={TSYM: "1"}, bucket=RateBucket(1000, lambda: 0), max_order_value=1e9)


class GateTest(unittest.TestCase):
    def test_live_refused_without_exit_path(self):
        self.assertEqual(LiveGate("live", True, "1.2.3.4", "1.2.3.4", True, 50_000).problems(),
                         ["LIVE_EXIT_PATH_MISSING"])


class ExitLadderTest(unittest.TestCase):
    def test_sell_ladder_bid_down_to_floor_then_reanchor(self):
        st = X.new_exit_state("EXT_1", REQ, 0, {"tick": 0.05})
        prices = []
        for t in range(0, 10_001, 1000):
            bid = 100.0 if t < 10_000 else 97.0
            st, actions, _ = X.step_exit(st, Quote(t, bid, 100.5), [], t, CFG)
            prices += [(a["type"], a["price"]) for a in actions]
        self.assertEqual(st.floor, 95.05)                     # re-anchored: 97 x 0.98 = 95.06 -> 95.05
        self.assertEqual(prices, [("PLACE", 100.0), ("MODIFY", 99.6), ("MODIFY", 99.2), ("MODIFY", 98.8),
                                  ("MODIFY", 98.4), ("MODIFY", 98.0), ("MODIFY", 97.0)])   # never below 98 floor
        self.assertEqual(st.reanchors, 1)
        self.assertEqual(st.status, X.EXIT_WORKING)            # never abandoned

    def test_paper_broker_sell_fills_at_bid(self):
        b = PaperBroker(latency_ms=0)
        b.place({"order_id": "x", "tradingsymbol": TSYM, "side": "SELL", "price": 99.0, "qty": 750, "lots": 1}, 0)
        self.assertEqual(b.poll({TSYM: Quote(1, 98.5, 99.5)}, 1), [])          # bid below limit: rests
        u = b.poll({TSYM: Quote(2, 99.2, 99.5)}, 2)[0]
        self.assertEqual((u.status, u.filled_qty, u.avg_price), ("COMPLETE", 750, 99.2))

    def test_band_reject_requotes_higher(self):
        st = X.new_exit_state("EXT_1", REQ, 0, {"tick": 0.05})
        st, a, _ = X.step_exit(st, Quote(0, 100.0, 100.5, ltp=100.3), [], 0, CFG)
        oid = a[0]["order_id"]
        st, a, _ = X.step_exit(st, Quote(500, 100.0, 100.5, ltp=100.3),
                               [S.OrderUpdate(oid, "REJECTED", 0, None, "LPP price band")], 500, CFG)
        self.assertEqual((a[0]["type"], a[0]["price"]), ("PLACE", 100.3))      # inside the band, > 100.00


class RunnerExitTest(unittest.TestCase):
    def test_paper_exit_request_fills_and_reports(self):
        r = FakeRedis()
        bidbook(r, T0, 100.0, 100.5)
        rn = runner(r, mode="paper")
        xid = rn.on_exit_request(f"{T0}-0", REQ, T0)
        self.assertEqual(r.hgetall("md:exec:exit:active"), {"SBIN-1": xid})
        bidbook(r, T0 + 500, 100.0, 100.5)
        rn.tick(T0 + 500)
        fill = r.streams["md:exec:exit_fill"][0]
        self.assertEqual((fill["status"], fill["filled_qty"], fill["avg_price"], fill["exec_mode"]),
                         (X.EXIT_FILLED, "1500.0", "100.0", "paper"))
        self.assertEqual(r.hgetall("md:exec:exit:active"), {})
        # a re-request for the same position never sells again
        self.assertIsNone(rn.on_exit_request(f"{T0 + 1000}-0", REQ, T0 + 1000))

    def test_other_mode_and_stale_requests_ignored(self):
        r = FakeRedis()
        bidbook(r, T0, 100.0, 100.5)
        rn = runner(r, mode="paper")
        self.assertIsNone(rn.on_exit_request(f"{T0}-0", dict(REQ, exec_mode="live"), T0))
        self.assertIsNone(rn.on_exit_request(f"{T0 - 121_000}-0", REQ, T0))
        self.assertEqual(rn.exits, {})

    def test_duplicate_while_active_ignored(self):
        r = FakeRedis()
        bidbook(r, T0, 100.0, 100.5)
        rn = runner(r, mode="paper")
        self.assertIsNotNone(rn.on_exit_request(f"{T0}-0", REQ, T0))
        self.assertIsNone(rn.on_exit_request(f"{T0}-1", REQ, T0))


@mock.patch.object(rtj, "is_eod", return_value=False)
class LiveEndToEndTest(unittest.TestCase):
    def test_sl_exit_closes_only_on_confirmed_sell(self, _eod):
        r = FakeRedis()
        pos = open_position(icare_msg(), 100.8, T0, exec_mode="live")          # 4 lots, SL 80
        r.set(POS_KEY, json.dumps(pos.to_dict()))
        bidbook(r, T0 + 1000, 79.0, 79.5)
        rtj.mark_all(r, T0 + 1000, mode="live")
        req = r.streams["md:exec:exit_request"][0]
        self.assertEqual((req["reason"], req["qty"], req["exec_mode"]), ("SL", "3000.0", "live"))
        self.assertEqual(json.loads(r.get(POS_KEY))["exit_pending"], "SL")      # still open
        self.assertNotIn("md:journal", r.streams)

        api = FakeSmart()
        rn = roe.ExecutorRunner(r, angel(api), {TSYM: {"tick": 0.05}}, mode="live")
        rn.on_exit_request(f"{T0 + 1000}-0", req, T0 + 1000)
        p = api.placed[0]
        self.assertEqual((p["transactiontype"], p["ordertype"], p["price"], p["quantity"]), ("SELL", "LIMIT", "79.00", "3000"))
        api.book = [{"orderid": "B1", "status": "complete", "filledshares": "3000", "averageprice": "78.95"}]
        bidbook(r, T0 + 1500, 79.0, 79.5)
        rn.tick(T0 + 1500)
        fill = r.streams["md:exec:exit_fill"][0]

        rtj.mark_all(r, T0 + 1600, mode="live")                                 # no duplicate request
        self.assertEqual(len(r.streams["md:exec:exit_request"]), 1)
        rec = rtj.handle_exit_fill(r, fill, T0 + 2000, mode="live")
        self.assertEqual((rec["exit_reason"], rec["exit_premium"], rec["qty"], rec["mode"]), ("SL", 78.95, 3000.0, "live"))
        self.assertEqual(rec["gross_pnl"], round((78.95 - 100.8) * 3000, 2))
        self.assertIsNone(r.get(POS_KEY))

    def test_partial_sell_keeps_rest_open_and_rerequests(self, _eod):
        r = FakeRedis()
        pos = open_position(icare_msg(), 100.8, T0, exec_mode="live")
        r.set(POS_KEY, json.dumps(pos.to_dict()))
        bidbook(r, T0 + 1000, 79.0, 79.5)
        rtj.mark_all(r, T0 + 1000, mode="live")
        fill = {"trade_id": pos.trade_id, "tradingsymbol": TSYM, "exec_mode": "live", "status": X.EXIT_FAILED,
                "filled_qty": "1500.0", "avg_price": "78.9", "charges": "100"}
        rec = rtj.handle_exit_fill(r, fill, T0 + 2000, mode="live")
        self.assertEqual(rec["qty"], 1500.0)
        left = json.loads(r.get(POS_KEY))
        self.assertEqual((left["qty"], left["lots"], left["exit_pending"]), (1500.0, 2, "SL"))
        rtj.mark_all(r, T0 + 2100, mode="live")
        req = r.streams["md:exec:exit_request"][-1]
        self.assertEqual((req["qty"], req["orig_qty"]), ("1500.0", "3000.0"))

    def test_paper_and_shadow_ignore_exit_fills(self, _eod):
        self.assertIsNone(rtj.handle_exit_fill(FakeRedis(), {"exec_mode": "paper"}, T0, mode="paper"))


if __name__ == "__main__":
    unittest.main()

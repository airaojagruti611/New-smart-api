"""QA fixes 4 + 5 (HIGH): live restart reconcile / CANCEL; unknown place outcome; local rate limit retry."""

from __future__ import annotations

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import run_order_executor as roe
from app.order_executor import state as S
from app.order_executor.broker import AngelBroker, RateBucket
from test_fix_exec_exit import FakeSmart
from test_order_executor_runner import LOT, T0, TSYM, FakeRedis, account, book, icare_msg

SPECS = {TSYM: {"tick": 0.05, "kind": "OPTSTK", "freeze_qty": 15001, "expiry": "2026-10-27"}}


def live_runner(r, api, bucket=None):
    b = AngelBroker(api=api, tokens={TSYM: "1"}, bucket=bucket or RateBucket(1000, lambda: 0), max_order_value=1e9)
    return roe.ExecutorRunner(r, b, SPECS, mode="live"), b


def market(r, t):
    book(r, t, 100.0, 101.0, [(101.0, 4 * LOT)])
    account(r, t)


class RestartTest(unittest.TestCase):
    def test_restart_cancels_resting_live_order_and_keeps_fill(self):
        r, api = FakeRedis(), FakeSmart()
        market(r, T0)
        rn, _ = live_runner(r, api)
        tid = rn.on_icare(f"{T0}-0", icare_msg(), T0)          # BUY 4 lots at mid 100.50, resting
        self.assertEqual(api.placed[0]["price"], "100.50")
        st = json.loads(r.get(f"md:exec:state:{tid}"))
        self.assertEqual(st["working"]["broker_id"], "B1")      # persisted for a restart

        # crash: new process, new adapter (ids empty); exchange filled 1 lot meanwhile
        api.book = [{"orderid": "B1", "status": "open", "filledshares": "750", "averageprice": "100.5"}]
        rn2, b2 = live_runner(r, api)
        market(r, T0 + 3000)
        rn2.recover(T0 + 3000)
        self.assertEqual(api.cancelled, ["B1"])                 # a REAL cancel, not a local assumption
        api.book = [{"orderid": "B1", "status": "cancelled", "filledshares": "750", "averageprice": "100.5"}]
        market(r, T0 + 3500)
        rn2.tick(T0 + 3500)
        rep = json.loads(r.get(f"md:exec:latest:{tid}"))
        self.assertEqual((rep["execution_status"], rep["filled_lots"]), (S.PARTIAL_FILL_STOPPED, 1))
        self.assertEqual(r.streams["md:exec:fill"][0]["recommended_lots"], "1")
        self.assertEqual(len(api.placed), 1)


class UnknownPlaceTest(unittest.TestCase):
    def test_timeout_reconciled_by_tag_never_replaced(self):
        r, api = FakeRedis(), FakeSmart()

        def timeout(p):
            api.placed.append(p)
            raise TimeoutError("read timed out")
        api.placeOrder = timeout
        market(r, T0)
        rn, b = live_runner(r, api)
        tid = rn.on_icare(f"{T0}-0", icare_msg(), T0)
        st = rn.states[tid]
        self.assertTrue(st.working["pending_reconcile"])
        self.assertEqual(st.status, S.WORKING)                  # not REJECTED_BY_BROKER
        # the order DID reach the exchange: found by our ordertag, filled
        api.book = [{"orderid": "B9", "ordertag": "TRD_20261005_001-1", "status": "complete",
                     "filledshares": "3000", "averageprice": "100.5"}]
        for t in (T0 + 500, T0 + 1000):
            market(r, t)
            rn.tick(t)
        rep = json.loads(r.get(f"md:exec:latest:{tid}"))
        self.assertEqual((rep["execution_status"], rep["filled_lots"]), (S.FILLED, 4))
        self.assertEqual(len(api.placed), 1)

    def test_not_in_book_after_window_ends_without_fill(self):
        r, api = FakeRedis(), FakeSmart()

        def timeout(p):
            api.placed.append(p)
            raise ConnectionError("reset")
        api.placeOrder = timeout
        market(r, T0)
        rn, _ = live_runner(r, api)
        tid = rn.on_icare(f"{T0}-0", icare_msg(), T0)
        t = T0
        while rn.states and t < T0 + 30_000:
            t += 500
            market(r, t)
            rn.tick(t)
        rep = json.loads(r.get(f"md:exec:latest:{tid}"))
        self.assertEqual(rep["filled_lots"], 0)
        self.assertIn("PLACE_UNKNOWN_NOT_IN_BOOK", rep["reject_reasons"])
        self.assertGreaterEqual(t, T0 + 15_000)                  # decided only after the 15 s search
        self.assertEqual(len(api.placed), 1)                     # never re-placed blindly

    def test_definite_rejection(self):
        class InputException(Exception):
            pass
        r, api = FakeRedis(), FakeSmart()

        def reject(p):
            raise InputException("Invalid price")
        api.placeOrder = reject
        market(r, T0)
        rn, _ = live_runner(r, api)
        tid = rn.on_icare(f"{T0}-0", icare_msg(), T0)
        market(r, T0 + 500)
        rn.tick(T0 + 500)
        rep = json.loads(r.get(f"md:exec:latest:{tid}"))
        self.assertEqual(rep["execution_status"], S.REJECTED_BY_BROKER)


class RateLimitTest(unittest.TestCase):
    def test_local_rate_limit_waits_and_retries(self):
        r, api = FakeRedis(), FakeSmart()
        clock = [0]
        bucket = RateBucket(1, lambda: clock[0])
        bucket.tokens = 0.0
        market(r, T0)
        rn, _ = live_runner(r, api, bucket)
        tid = rn.on_icare(f"{T0}-0", icare_msg(), T0)
        self.assertEqual(api.placed, [])
        self.assertEqual(rn.states[tid].status, S.WORKING)       # not aborted
        clock[0] = 1000
        market(r, T0 + 500)
        rn.tick(T0 + 500)
        self.assertEqual(len(api.placed), 1)
        self.assertIsNotNone(rn.states[tid].working)


if __name__ == "__main__":
    unittest.main()

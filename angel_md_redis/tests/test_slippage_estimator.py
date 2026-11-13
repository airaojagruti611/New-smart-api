"""Module 19: slippage estimator (pure) + runner on a fake Redis."""

from __future__ import annotations

import json
import unittest

import run_slippage_estimator as rse
from app.order_executor.market import Quote, quote_from_bidask
from app.slippage_estimator import estimate_slippage, round_trip_cost, walk_book

QUOTE = Quote(ts_ms=1, bid=99.0, ask=100.0, bid_qty=100, ask_qty=100,
              bid_levels=((99.0, 100), (98.0, 100), (97.0, 200)),
              ask_levels=((100.0, 100), (101.0, 100), (102.0, 200)))


class WalkBookTest(unittest.TestCase):
    def test_vwap_across_levels(self):
        w = walk_book(QUOTE.ask_levels, 150, "BUY")
        self.assertEqual(w["filled_qty"], 150)
        self.assertEqual(w["unfilled_qty"], 0)
        self.assertAlmostEqual(w["vwap"], (100 * 100 + 50 * 101) / 150, places=4)
        self.assertEqual((w["levels_used"], w["worst_price"]), (2, 101.0))

    def test_unfilled_remainder(self):
        w = walk_book(QUOTE.ask_levels, 500, "BUY")
        self.assertEqual((w["filled_qty"], w["unfilled_qty"]), (400, 100))

    def test_zero_qty(self):
        w = walk_book(QUOTE.ask_levels, 0, "BUY")
        self.assertIsNone(w["vwap"])


class EstimateTest(unittest.TestCase):
    def test_top_of_book_fill_has_zero_depth_slippage(self):
        e = estimate_slippage(QUOTE, 1, 50, "BUY")
        self.assertEqual(e["status"], "OK")
        self.assertEqual(e["slippage_per_unit"], 0.0)
        self.assertEqual(e["spread"], 1.0)
        self.assertAlmostEqual(e["cost_vs_mid_per_unit"], 0.5)
        self.assertAlmostEqual(e["cost_vs_mid"], 25.0)

    def test_depth_impact_buy(self):
        e = estimate_slippage(QUOTE, 2, 100, "BUY", reference=100.0, max_slippage_pct=0.1)
        self.assertEqual(e["qty"], 200)
        self.assertAlmostEqual(e["vwap"], 100.5)
        self.assertAlmostEqual(e["slippage_cost"], 100.0)
        self.assertAlmostEqual(e["slippage_vs_ref_pct"], 0.5)
        self.assertFalse(e["within_limit"])
        self.assertIn("SLIPPAGE_ABOVE_LIMIT", e["flags"])

    def test_sell_walks_bids(self):
        e = estimate_slippage(QUOTE, 2, 100, "SELL")
        self.assertAlmostEqual(e["vwap"], 98.5)
        self.assertAlmostEqual(e["slippage_per_unit"], 0.5)   # worse than best bid
        self.assertAlmostEqual(e["cost_vs_mid_per_unit"], 1.0)  # mid 99.5 - vwap 98.5

    def test_partial(self):
        e = estimate_slippage(QUOTE, 10, 100, "BUY")
        self.assertEqual(e["status"], "PARTIAL")
        self.assertEqual(e["unfilled_qty"], 600)
        self.assertIn("INSUFFICIENT_DEPTH", e["flags"])

    def test_top_of_book_fallback(self):
        q = Quote(ts_ms=1, bid=99.0, ask=100.0, bid_qty=50, ask_qty=50)
        e = estimate_slippage(q, 1, 100, "BUY")
        self.assertEqual(e["status"], "PARTIAL")
        self.assertIn("TOP_OF_BOOK_ONLY", e["flags"])
        self.assertEqual(e["filled_qty"], 50)

    def test_degenerate(self):
        self.assertEqual(estimate_slippage(None, 1, 50)["status"], "NO_QUOTE")
        self.assertEqual(estimate_slippage(Quote(1, 100.0, 99.0), 1, 50)["status"], "NO_QUOTE")
        self.assertEqual(estimate_slippage(QUOTE, 0, 50)["status"], "BAD_QTY")
        self.assertEqual(estimate_slippage(Quote(1, 99.0, 100.0), 1, 50)["status"], "NO_DEPTH")

    def test_round_trip(self):
        a, b = estimate_slippage(QUOTE, 1, 50, "BUY"), estimate_slippage(QUOTE, 1, 50, "SELL")
        self.assertAlmostEqual(round_trip_cost(a, b), 50.0)
        self.assertIsNone(round_trip_cost(a, {}))


class FakeRedis:
    def __init__(self):
        self.kv, self.streams = {}, []

    def get(self, k):
        return self.kv.get(k)

    def set(self, k, v, ex=None):
        self.kv[k] = v

    def xadd(self, name, fields, maxlen=None, approximate=None):
        self.streams.append((name, fields))


BIDASK_DOC = {"ts_ms": 1, "bid": "99", "ask": "100", "bid_qty": "100", "ask_qty": "100",
              "bid_depth5_px": "99,98", "bid_depth5": "100,100", "ask_depth5_px": "100,101", "ask_depth5": "100,100"}
APPROVAL = {"status": "APPROVED", "symbol": "TCS", "tradingsymbol": "TCS26OCT2100CE", "side": "CE",
            "recommended_lots": "2", "lot_size": "75", "premium": "100"}


class RunnerTest(unittest.TestCase):
    def test_parses_depth(self):
        q = quote_from_bidask(BIDASK_DOC)
        self.assertEqual(q.ask_levels, ((100.0, 100.0), (101.0, 100.0)))

    def test_publishes_for_approved(self):
        r = FakeRedis()
        r.kv["md:bidask:latest:TCS26OCT2100CE"] = json.dumps(BIDASK_DOC)
        est = rse.handle_message(r, APPROVAL, 1000)
        self.assertEqual(est["status"], "OK")
        self.assertAlmostEqual(est["entry"]["vwap"], (100 * 100 + 50 * 101) / 150, places=4)
        saved = json.loads(r.kv["md:slippage:latest:TCS26OCT2100CE"])
        self.assertEqual(saved["tradingsymbol"], "TCS26OCT2100CE")
        self.assertEqual(r.streams[0][0], "md:slippage:estimate")

    def test_skips_rejected_and_zero_lots(self):
        r = FakeRedis()
        self.assertIsNone(rse.handle_message(r, {**APPROVAL, "status": "REJECTED"}, 1))
        self.assertIsNone(rse.handle_message(r, {**APPROVAL, "recommended_lots": "0"}, 1))
        self.assertEqual(r.streams, [])

    def test_missing_quote_still_published(self):
        r = FakeRedis()
        est = rse.handle_message(r, APPROVAL, 1)
        self.assertEqual(est["status"], "NO_QUOTE")
        self.assertEqual(len(r.streams), 1)


if __name__ == "__main__":
    unittest.main()

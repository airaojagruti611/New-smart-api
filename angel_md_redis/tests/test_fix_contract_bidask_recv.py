"""Integration-review fix: md:bidask:latest carries recv_ts_ms (producer's local
receive time) and the executor ages quotes by it, not by exchange tick time."""

from __future__ import annotations

import unittest

import run_bidask_analyzer as rba
from app.order_executor.market import quote_from_bidask

NOW = 1_791_000_000_000


class BidAskRecvTsTest(unittest.TestCase):
    def _payload(self, ts_exch, ts_recv):
        tick = {"ts_exch": str(ts_exch), "ts_recv": str(ts_recv), "bid": "100", "ask": "100.5",
                "bid_sz": "500", "ask_sz": "400", "ltp": "100.2"}
        return rba._crossed_payload("SBIN26OCT800CE", "opt", 100.6, 100.5, ts_exch, tick)

    def test_payload_has_recv_ts(self):
        p = self._payload(NOW - 2_500, NOW - 200)
        self.assertEqual(p["ts_ms"], str(NOW - 2_500))
        self.assertEqual(p["recv_ts_ms"], str(NOW - 200))

    def test_executor_ages_quote_by_receive_time(self):
        # exchange clock 2.5 s behind the host; the tick arrived 0.2 s ago
        doc = {"ts_ms": str(NOW - 2_500), "recv_ts_ms": str(NOW - 200), "bid": "100", "ask": "100.5"}
        q = quote_from_bidask(doc)
        self.assertIsNotNone(q)
        self.assertEqual(q.age_ms(NOW), 200)


if __name__ == "__main__":
    unittest.main()

"""Equity tick change filter (app/ws_producer.eq_core_changed).

Before: ask-only / size-only book updates were dropped, so equity bid-ask
consumers missed them and — now that quote age is judged by tick time — a live
book would look stale. A heartbeat re-emits an unchanged book.
"""

from __future__ import annotations

import unittest

try:
    from app.ws_producer import eq_core_changed
except ImportError as exc:  # SmartApi SDK missing
    raise unittest.SkipTest(f"ws_producer import failed: {exc}")

EPS = 0.005
HB = 5000
NOW = 1_791_000_000_000
BASE = {"ltp": 100.0, "c": 99.0, "vol": 1000.0, "bid": 99.95, "ask": 100.05,
        "bid_sz": 500.0, "ask_sz": 400.0}


def _last(**over):
    d = dict(BASE, emitted_ms=NOW - 1000)
    d.update(over)
    return d


class EqCoreChangedTests(unittest.TestCase):
    def test_first_tick_emits(self):
        self.assertTrue(eq_core_changed(None, BASE, EPS, NOW, HB))

    def test_unchanged_within_heartbeat_is_dropped(self):
        self.assertFalse(eq_core_changed(_last(), dict(BASE), EPS, NOW, HB))

    def test_ask_only_change_emits(self):
        self.assertTrue(eq_core_changed(_last(), dict(BASE, ask=100.10), EPS, NOW, HB))

    def test_size_only_change_emits(self):
        self.assertTrue(eq_core_changed(_last(), dict(BASE, ask_sz=50.0), EPS, NOW, HB))
        self.assertTrue(eq_core_changed(_last(), dict(BASE, bid_sz=900.0), EPS, NOW, HB))

    def test_sub_eps_price_noise_dropped(self):
        self.assertFalse(eq_core_changed(_last(), dict(BASE, ltp=100.001), EPS, NOW, HB))

    def test_bid_vanishing_emits(self):
        self.assertTrue(eq_core_changed(_last(), dict(BASE, bid=None), EPS, NOW, HB))

    def test_heartbeat_reemits_unchanged_book(self):
        self.assertTrue(eq_core_changed(_last(emitted_ms=NOW - 5000), dict(BASE), EPS, NOW, HB))
        self.assertFalse(eq_core_changed(_last(emitted_ms=NOW - 5000), dict(BASE), EPS, NOW, 0))


if __name__ == "__main__":
    unittest.main()

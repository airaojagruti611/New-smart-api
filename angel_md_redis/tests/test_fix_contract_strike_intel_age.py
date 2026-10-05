"""Integration-review fix: strike_intel must not turn an entry-trigger backlog
into fresh strike picks, and must start the chain clock (signal_ts_ms) at the
break so probability / ranking / ICARE / executor age it correctly."""

from __future__ import annotations

import unittest

from app.probability_engine import signal_is_stale, signal_origin_ms
from run_strike_intel import trigger_is_stale, trigger_origin_ms

NOW = 1_791_000_000_000
MAX = 120_000


class StrikeIntelAgeTest(unittest.TestCase):
    def test_fresh_trigger_passes_and_origin_is_bar_time(self):
        fields = {"bar_ts_ms": str(NOW - 4_000)}
        self.assertFalse(trigger_is_stale(f"{NOW - 1_000}-0", fields, NOW, MAX))
        self.assertEqual(trigger_origin_ms(f"{NOW - 1_000}-0", fields), NOW - 4_000)

    def test_backlog_trigger_dropped_by_stream_id(self):
        self.assertTrue(trigger_is_stale(f"{NOW - 3 * 3_600_000}-0", {"bar_ts_ms": str(NOW - 3 * 3_600_000)}, NOW, MAX))

    def test_old_bar_dropped_even_if_message_is_new(self):
        self.assertTrue(trigger_is_stale(f"{NOW - 500}-0", {"bar_ts_ms": str(NOW - 600_000)}, NOW, MAX))

    def test_unknown_origin_fails_closed(self):
        self.assertTrue(trigger_is_stale("junk", {}, NOW, MAX))

    def test_probability_inherits_origin_not_intel_xadd_time(self):
        # strike_intel republishes a 3h-old break *now*: before the fix probability
        # saw origin age 0 (the md:strike:intel stream id) and passed it.
        old_bar = NOW - 3 * 3_600_000
        intel_payload = {"signal_ts_ms": str(trigger_origin_ms(f"{NOW - 100}-0", {"bar_ts_ms": str(old_bar)}))}
        origin = signal_origin_ms(f"{NOW - 50}-0", intel_payload)
        self.assertEqual(origin, old_bar)
        self.assertTrue(signal_is_stale(origin, NOW, 60_000))


if __name__ == "__main__":
    unittest.main()

"""Shared freshness guards (app/freshness.py)."""

from __future__ import annotations

import os
import unittest
from unittest import mock

from app.freshness import (
    env_ms,
    is_fresh_payload,
    is_fresh_ts,
    is_stale_message,
    message_age_ms,
    stream_id_ms,
    ts_field_ms,
)

NOW = 1_791_000_000_000


class StreamIdTests(unittest.TestCase):
    def test_parses_str_and_bytes(self):
        self.assertEqual(stream_id_ms("1791000000000-3"), NOW)
        self.assertEqual(stream_id_ms(b"1791000000000-0"), NOW)

    def test_bad_id(self):
        self.assertIsNone(stream_id_ms("abc"))
        self.assertIsNone(stream_id_ms(None))

    def test_age_and_stale(self):
        self.assertEqual(message_age_ms(f"{NOW - 5000}-0", NOW), 5000)
        self.assertFalse(is_stale_message(f"{NOW - 5000}-0", NOW, 10_000))
        self.assertTrue(is_stale_message(f"{NOW - 15000}-0", NOW, 10_000))

    def test_unparseable_is_stale_and_zero_disables(self):
        self.assertTrue(is_stale_message("junk", NOW, 10_000))
        self.assertFalse(is_stale_message(f"{NOW - 10**9}-0", NOW, 0))


class PayloadTests(unittest.TestCase):
    def test_ts_field(self):
        self.assertEqual(ts_field_ms({"ts_ms": "123"}), 123)
        self.assertEqual(ts_field_ms({"ts_ms": b"123.0"}), 123)
        self.assertIsNone(ts_field_ms({"ts_ms": ""}))
        self.assertIsNone(ts_field_ms({"ts_ms": "0"}))
        self.assertIsNone(ts_field_ms(None))

    def test_missing_ts_is_not_fresh(self):
        self.assertFalse(is_fresh_ts(None, NOW, 60_000))
        self.assertFalse(is_fresh_payload({}, NOW, 60_000))

    def test_fresh_boundary(self):
        self.assertTrue(is_fresh_payload({"ts_ms": NOW - 60_000}, NOW, 60_000))
        self.assertFalse(is_fresh_payload({"ts_ms": NOW - 60_001}, NOW, 60_000))
        self.assertFalse(is_fresh_payload({"bar_ts_ms": "1"}, NOW, 60_000, field="bar_ts_ms"))

    def test_env_ms(self):
        with mock.patch.dict(os.environ, {"X_AGE": "2.5"}):
            self.assertEqual(env_ms("X_AGE", 9), 2500)
        with mock.patch.dict(os.environ, {"X_AGE": "bad"}):
            self.assertEqual(env_ms("X_AGE", 9), 9000)
        self.assertEqual(env_ms("X_AGE_UNSET_XYZ", 3), 3000)


if __name__ == "__main__":
    unittest.main()

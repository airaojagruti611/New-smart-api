"""Module 9 runner: candidate order and strike parse from tradingsymbol."""

from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from run_greeks_change import (
    _candidate_from_strike_select,
    _strike_from_tsym,
    resolve_candidate,
)


class StrikeParseTest(unittest.TestCase):
    def test_nse_equity_option_suffix(self):
        self.assertEqual(_strike_from_tsym("RELIANCE25SEP1260CE"), 1260.0)
        self.assertEqual(_strike_from_tsym("TCS25SEP3000PE"), 3000.0)

    def test_missing_suffix(self):
        self.assertIsNone(_strike_from_tsym("RELIANCE"))
        self.assertIsNone(_strike_from_tsym(""))


class StrikeSelectCandidateTest(unittest.TestCase):
    def test_requires_ok_status(self):
        self.assertIsNone(_candidate_from_strike_select(
            {"status": "EMPTY", "tradingsymbol": "RELIANCE25SEP1260CE", "strike": 1260, "side": "CE"},
            "2026-09-25",
        ))

    def test_ok_payload(self):
        cand = _candidate_from_strike_select(
            {"status": "OK", "tradingsymbol": "RELIANCE25SEP1260CE", "strike": 1260, "side": "CE", "spot": 1265},
            "2026-09-25",
        )
        self.assertEqual(cand["origin"], "strike_select")
        self.assertEqual(cand["option_type"], "CE")


class FakeRedis:
    def __init__(self, kv=None, hashes=None):
        self.kv = kv or {}
        self.hashes = hashes or {}

    def get(self, key):
        return self.kv.get(key)

    def hget(self, name, key):
        return (self.hashes.get(name) or {}).get(key)


class ResolveCandidateTest(unittest.TestCase):
    def test_falls_back_to_atm_phase_from_tradingsymbol_strike(self):
        r = FakeRedis(
            kv={
                "md:greeks:phase:underlying:latest:RELIANCE:CE": json.dumps({
                    "tradingsymbol": "RELIANCE25SEP1260CE",
                    "cp": "CE",
                }),
            },
            hashes={"md:active_expiry": {"RELIANCE": "2026-09-25"}},
        )
        cand = resolve_candidate(r, "RELIANCE", "STRONG_BULLISH", 1265.0)
        self.assertIsNotNone(cand)
        self.assertEqual(cand["strike"], 1260.0)
        self.assertEqual(cand["origin"], "atm_phase_CE")

    def test_strike_select_wins_over_atm(self):
        r = FakeRedis(
            kv={
                "md:strike:select:latest:RELIANCE": json.dumps({
                    "status": "OK",
                    "tradingsymbol": "RELIANCE25SEP1280CE",
                    "strike": 1280,
                    "side": "CE",
                }),
                "md:greeks:phase:underlying:latest:RELIANCE:CE": json.dumps({
                    "tradingsymbol": "RELIANCE25SEP1260CE",
                    "strike": 1260,
                    "cp": "CE",
                }),
            },
            hashes={"md:active_expiry": {"RELIANCE": "2026-09-25"}},
        )
        cand = resolve_candidate(r, "RELIANCE", "BULLISH", 1265.0)
        self.assertEqual(cand["tradingsymbol"], "RELIANCE25SEP1280CE")
        self.assertEqual(cand["origin"], "strike_select")


class PrivateIpTest(unittest.TestCase):
    def test_rfc1918_and_loopback(self):
        from app.utils import is_private_ip, is_unusable_ip
        self.assertTrue(is_private_ip("10.0.0.8"))
        self.assertTrue(is_private_ip("192.168.1.10"))
        self.assertTrue(is_private_ip("172.16.0.1"))
        self.assertFalse(is_private_ip("8.8.8.8"))
        self.assertTrue(is_unusable_ip("127.0.0.1"))

    def test_headers_prefer_public_over_private_lan(self):
        from app import angel_rest
        with patch.object(angel_rest, "X_CLIENT_LOCAL_IP", "192.168.1.10"), \
             patch.object(angel_rest, "X_CLIENT_PUBLIC_IP", ""), \
             patch.object(angel_rest, "X_MAC_ADDRESS", "aa:bb:cc:dd:ee:ff"), \
             patch.object(angel_rest, "get_local_ip", return_value="192.168.1.10"), \
             patch.object(angel_rest, "get_public_ip", return_value="203.0.113.9"):
            h = angel_rest.build_headers("token", force_public=False)
        self.assertEqual(h["X-ClientLocalIP"], "203.0.113.9")
        self.assertEqual(h["X-ClientPublicIP"], "203.0.113.9")


if __name__ == "__main__":
    unittest.main()

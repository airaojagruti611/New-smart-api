"""Module 21: portfolio exposure (pure) + runner on a fake Redis."""

from __future__ import annotations

import fnmatch
import json
import unittest

import run_portfolio_exposure as rpe
from app.icare import ICAREConfig
from app.portfolio_exposure import compute_exposure, position_exposure

CE = {"trade_id": "t1", "symbol": "TCS", "tradingsymbol": "TCS26OCTCE", "side": "CE", "qty": 100,
      "entry_premium": 20.0, "sl_premium": 17.0, "context": {"delta": "0.5"}}
PE = {"trade_id": "t2", "symbol": "INFY", "tradingsymbol": "INFY26OCTPE", "side": "PE", "qty": 200,
      "entry_premium": 10.0, "sl_premium": 8.0, "last_premium": 9.0, "context": {"delta": "-0.4"}}
SECTORS = {"TCS": "IT", "INFY": "IT"}


class PositionTest(unittest.TestCase):
    def test_marked_at_bid(self):
        p = position_exposure(CE, 22.0)
        self.assertEqual(p["mark_source"], "bid")
        self.assertEqual(p["deployed"], 2000.0)
        self.assertEqual(p["unrealized_pnl"], 200.0)
        self.assertEqual(p["risk_at_entry"], 300.0)
        self.assertEqual(p["risk_to_sl"], 500.0)
        self.assertEqual(p["delta_exposure"], 50.0)

    def test_fallbacks_and_pe_sign(self):
        p = position_exposure(PE, None)
        self.assertEqual((p["mark_source"], p["mark"]), ("last_premium", 9.0))
        self.assertEqual(p["delta_exposure"], -80.0)
        self.assertEqual(position_exposure({**PE, "last_premium": None}, 0)["mark_source"], "entry")

    def test_below_sl_no_negative_risk(self):
        self.assertEqual(position_exposure(CE, 15.0)["risk_to_sl"], 0.0)

    def test_no_delta(self):
        self.assertIsNone(position_exposure({**CE, "context": {}}, 20.0)["delta_exposure"])


class ComputeTest(unittest.TestCase):
    def test_totals_groups(self):
        s = compute_exposure([CE, PE], {"TCS26OCTCE": 22.0, "INFY26OCTPE": 9.5}, {"total_capital": 100000, "ts_ms": 1},
                             SECTORS, ICAREConfig(), 100000, 5)
        self.assertEqual(s["open_positions"], 2)
        self.assertEqual(s["deployed"], 4000.0)
        self.assertEqual(s["utilization_pct"], 4.0)
        self.assertEqual(s["unrealized_pnl"], 200.0 - 100.0)
        self.assertEqual(s["risk_at_entry"], 300.0 + 400.0)
        self.assertEqual(s["by_side"]["CE"]["count"], 1)
        self.assertEqual(s["by_side"]["PE"]["net_delta"], -80.0)
        self.assertEqual(s["net_delta"], -30.0)
        self.assertEqual(s["by_sector"]["IT"]["deployed"], 4000.0)
        self.assertEqual(s["by_sector_pct"]["IT"], 4.0)
        self.assertEqual(sorted(s["by_underlying"]), ["INFY", "TCS"])
        self.assertEqual(s["flags"], [])

    def test_flags(self):
        cfg = ICAREConfig(max_open_trades=2, max_sector_exposure_pct=3.0, max_portfolio_risk_pct=0.5)
        s = compute_exposure([CE, PE], {}, {"total_capital": 100000, "day_pnl": -3000, "margin_utilization_pct": 90},
                             SECTORS, cfg, 100000, 5)
        for f in ("MAX_OPEN_TRADES_REACHED", "PORTFOLIO_RISK_LIMIT", "SECTOR_LIMIT:IT", "MARGIN_UTILIZATION_LIMIT",
                  "DAILY_LOSS_LIMIT", "STALE_MARKS"):
            self.assertIn(f, s["flags"])

    def test_empty_and_no_account(self):
        s = compute_exposure([], {}, None, {}, ICAREConfig(), 50000, 1)
        self.assertEqual((s["open_positions"], s["deployed"], s["total_capital"]), (0, 0.0, 50000))
        self.assertIn("NO_ACCOUNT", s["flags"])

    def test_unmapped_sector(self):
        s = compute_exposure([CE], {}, {}, {}, ICAREConfig(), 100000, 1)
        self.assertIn("SECTOR_UNMAPPED", s["flags"])

    def test_zero_capital(self):
        s = compute_exposure([CE], {}, {}, SECTORS, ICAREConfig(), 0, 1)
        self.assertIsNone(s["utilization_pct"])


class FakeRedis:
    def __init__(self):
        self.kv, self.streams = {}, []

    def get(self, k):
        return self.kv.get(k)

    def set(self, k, v, ex=None):
        self.kv[k] = v

    def scan_iter(self, match="*", count=None):
        return [k for k in list(self.kv) if fnmatch.fnmatch(k, match)]

    def xadd(self, name, fields, maxlen=None, approximate=None):
        self.streams.append((name, fields))


class RunnerTest(unittest.TestCase):
    def test_run_once_publishes_and_never_writes_positions(self):
        r = FakeRedis()
        r.kv["md:position:open:t1"] = json.dumps(CE)
        r.kv["md:bidask:latest:TCS26OCTCE"] = json.dumps({"bid": "22"})
        r.kv["md:account:latest"] = json.dumps({"total_capital": 100000})
        before = {k: v for k, v in r.kv.items() if k.startswith("md:position:open:")}
        snap = rpe.run_once(r, SECTORS, 123)
        self.assertEqual(snap["unrealized_pnl"], 200.0)
        latest = json.loads(r.kv["md:portfolio:exposure"])
        self.assertEqual(latest["open_positions"], 1)
        self.assertEqual(r.streams[0][0], "md:portfolio:exposure:stream")
        self.assertEqual(json.loads(r.streams[0][1]["data"])["ts_ms"], 123)
        self.assertEqual({k: v for k, v in r.kv.items() if k.startswith("md:position:open:")}, before)

    def test_bad_position_json_ignored(self):
        r = FakeRedis()
        r.kv["md:position:open:x"] = "{not json"
        self.assertEqual(rpe.run_once(r, {}, 1)["open_positions"], 0)


if __name__ == "__main__":
    unittest.main()

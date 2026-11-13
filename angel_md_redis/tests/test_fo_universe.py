from __future__ import annotations

import datetime as dt
import unittest

from app.fo_universe import ban_trade_date, build_universe, diff, parse_ban_csv, reconcile_symbols
from app.icare import evaluate
from run_icare import build_inputs
from tests.test_icare import CFG, _inp, _pf

TODAY = dt.date(2026, 10, 5)


def _row(name, expiry, it="OPTSTK", seg="NFO", lot="100"):
    return {"name": name, "expiry": expiry, "instrumenttype": it, "exch_seg": seg, "lotsize": lot}


ROWS = [
    *[_row("TCS", e) for e in ("27OCT2026", "24NOV2026", "29DEC2026")],
    *[_row("IEX", e) for e in ("27OCT2026", "24NOV2026")],            # no far month -> exiting
    _row("OLDCO", "29SEP2026"),                                        # all expired -> absent
    _row("NIFTY", "06OCT2026", it="OPTIDX"),
    _row("TCS", "", it="", seg="NSE"),                                 # cash row ignored
    _row("SBIN", "27OCT2026", it="FUTSTK"),                            # futures only ignored
]


class BuildUniverseTest(unittest.TestCase):
    def test_classifies_by_live_expiries(self):
        u = build_universe(ROWS, TODAY)
        self.assertEqual(list(u.active), ["TCS"])
        self.assertEqual(list(u.exiting), ["IEX"])
        self.assertEqual(u.indices, {"NIFTY"})
        self.assertEqual(u.active["TCS"]["expiries"], ["2026-10-27", "2026-11-24", "2026-12-29"])
        self.assertNotIn("OLDCO", u.live())
        self.assertNotIn("SBIN", u.live())

    def test_expiry_day_still_live(self):
        u = build_universe(ROWS, dt.date(2026, 10, 27))
        self.assertIn("TCS", u.active)         # Oct expires today but still counts: 3 live
        u = build_universe(ROWS, dt.date(2026, 10, 28))
        self.assertIn("TCS", u.exiting)        # Oct gone, no Jan listed in this fixture

    def test_sanity_floor(self):
        self.assertFalse(build_universe(ROWS, TODAY).sane())


class BanListTest(unittest.TestCase):
    TEXT = "Securities in Ban For Trade Date 05-OCT-2026:\n1,AMBUJACEM\n2,BANDHANBNK\n3,SAIL\n"

    def test_parse(self):
        self.assertEqual(parse_ban_csv(self.TEXT), ["AMBUJACEM", "BANDHANBNK", "SAIL"])
        self.assertEqual(ban_trade_date(self.TEXT), "2026-10-05")

    def test_no_bans(self):
        text = "Securities in Ban For Trade Date 06-OCT-2026: NIL\n"
        self.assertEqual(parse_ban_csv(text), [])
        self.assertEqual(parse_ban_csv(""), [])


class ReconcileTest(unittest.TestCase):
    def test_removes_only_symbols_without_live_contracts(self):
        u = build_universe(ROWS, TODAY)
        rec = reconcile_symbols(["TCS", "OLDCO", "IEX", "NIFTY"], u, banned=["TCS"])
        self.assertEqual(rec.keep, ["TCS", "IEX", "NIFTY"])
        self.assertEqual(rec.removed, ["OLDCO"])
        self.assertEqual(rec.exiting, ["IEX"])
        self.assertEqual(rec.banned, ["TCS"])

    def test_diff(self):
        self.assertEqual(diff({"A", "B"}, {"B", "C"}), (["C"], ["A"]))


class IcareBanTest(unittest.TestCase):
    def test_banned_stock_rejected(self):
        res = evaluate(_inp(fo_banned=True), _pf(), CFG)
        self.assertEqual(res.status, "REJECTED")
        self.assertIn("fo_ban_period", res.reasons)
        self.assertNotIn("fo_ban_period", evaluate(_inp(), _pf(), CFG).reasons)

    def test_build_inputs_passes_flag(self):
        self.assertTrue(build_inputs({"symbol": "SAIL"}, {}, {}, fo_banned=True).fo_banned)
        self.assertFalse(build_inputs({"symbol": "SAIL"}, {}, {}).fo_banned)


if __name__ == "__main__":
    unittest.main()

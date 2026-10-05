"""Fix tests: opt_type from CE/PE suffix; option chain filtered on exact `name`."""

import datetime as dt
import json
import os
import sys
import unittest

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app.scripmaster import build_atm_option_tokens, filter_underlying, option_type_of, prepare_scripmaster  # noqa: E402

MASTER = os.path.join(ROOT, "OpenAPIScripMaster.json")
WANT = {"PETRONET", "PERSISTENT", "HINDPETRO", "LT", "LTF", "LTM", "NIFTY", "NIFTYNXT50", "PNB", "PNBHOUSING"}


def _future_expiry(days=20):
    return (dt.date.today() + dt.timedelta(days=days)).strftime("%d%b%Y").upper()


def _row(sym, name, strike, exp, it="OPTSTK"):
    return {"token": sym, "symbol": sym, "name": name, "expiry": exp, "strike": f"{strike * 100:.6f}",
            "lotsize": "1", "instrumenttype": it, "exch_seg": "NFO", "tick_size": "5.0"}


class TestOptType(unittest.TestCase):
    def test_suffix(self):
        self.assertEqual(option_type_of("PETRONET27OCT26320CE", "OPTSTK"), "CE")
        self.assertEqual(option_type_of("PERSISTENT27OCT266000CE", "OPTSTK"), "CE")
        self.assertEqual(option_type_of("HINDPETRO27OCT26400CE", "OPTSTK"), "CE")
        self.assertEqual(option_type_of("HINDPETRO27OCT26400PE", "OPTSTK"), "PE")
        self.assertIsNone(option_type_of("PETRONET27OCT26FUT", "FUTSTK"))
        self.assertIsNone(option_type_of("PETRONET-EQ", ""))

    def test_synthetic_chain_lt_excludes_ltf_ltm(self):
        exp = _future_expiry()
        tag = dt.datetime.strptime(exp, "%d%b%Y").strftime("%d%b%y").upper()
        rows = []
        for name, strikes in (("LT", (3500, 3600, 3700)), ("LTF", (150, 160)), ("LTM", (5000, 5100))):
            for k in strikes:
                for cp in ("CE", "PE"):
                    rows.append(_row(f"{name}{tag}{k}{cp}", name, k, exp))
        df = prepare_scripmaster(pd.DataFrame(rows))
        contracts, expiry = build_atm_option_tokens(df, "LT", 3600.0, 1)
        self.assertTrue(contracts)
        self.assertTrue(all(c["tradingsymbol"].startswith(f"LT{tag}") for c in contracts))
        self.assertEqual(sorted({c["strike"] for c in contracts}), [3500.0, 3600.0, 3700.0])
        # a blank-name row falls back to the strict symbol pattern
        blank = pd.DataFrame([dict(_row(f"LT{tag}3600CE", "", 3600, exp)), dict(_row(f"LTF{tag}150CE", "", 150, exp))])
        got = filter_underlying(prepare_scripmaster(blank), "LT")
        self.assertEqual(got["symbol"].tolist(), [f"LT{tag}3600CE"])


@unittest.skipUnless(os.path.exists(MASTER), "cached OpenAPIScripMaster.json not present")
class TestRealScripMaster(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(MASTER, encoding="utf-8") as fh:
            data = json.load(fh)
        sub = [r for r in data if r.get("exch_seg") == "NFO" and str(r.get("name")) in WANT]
        cls.df = prepare_scripmaster(pd.DataFrame(sub))

    def _opts(self, name):
        d = self.df
        return d[(d["name"] == name) & d["instrumenttype"].str.startswith("OPT")]

    def test_petronet_persistent_hindpetro_calls_are_ce(self):
        for name in ("PETRONET", "PERSISTENT", "HINDPETRO"):
            d = self._opts(name)
            if d.empty:
                continue
            ce = d[d["symbol"].str.upper().str.endswith("CE")]
            pe = d[d["symbol"].str.upper().str.endswith("PE")]
            self.assertTrue((ce["opt_type"] == "CE").all(), name)
            self.assertTrue((pe["opt_type"] == "PE").all(), name)
            self.assertGreater(len(ce), 0)

    def test_chain_filters_exact_underlying(self):
        d = self.df[self.df["instrumenttype"].str.startswith("OPT")]
        for u, bad in (("LT", ("LTF", "LTM")), ("NIFTY", ("NIFTYNXT50",)), ("PNB", ("PNBHOUSING",))):
            got = filter_underlying(d, u)
            if got.empty:
                continue
            self.assertEqual(set(got["name"]), {u})
            for b in bad:
                self.assertFalse((got["name"] == b).any())

    def test_build_chain_lt_has_no_ltf(self):
        d = self._opts("LT")
        if d.empty:
            self.skipTest("no LT options in cache")
        spot = float(d["strike_f"].median()) / 100.0
        contracts, _exp = build_atm_option_tokens(self.df, "LT", spot, 2)
        for c in contracts:
            self.assertFalse(c["tradingsymbol"].startswith(("LTF", "LTM")), c["tradingsymbol"])
            self.assertEqual(c["cp"], c["tradingsymbol"][-2:])


if __name__ == "__main__":
    unittest.main()

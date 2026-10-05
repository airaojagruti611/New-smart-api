"""Fix tests: Angel history bar-end stamping, in-progress bar filter, greeks freshness, run_greeks API."""

import datetime as dt
import json
import os
import sys
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import history_bootstrap as hb  # noqa: E402
from app.candle_builder import IST  # noqa: E402
from app.candle_types import Candle  # noqa: E402


def ist(d, hh, mm, ss=0, ms=0):
    return int(dt.datetime(d.year, d.month, d.day, hh, mm, ss, ms * 1000, tzinfo=IST).timestamp() * 1000)


D1 = dt.date(2026, 10, 5)


class FakeStore:
    def __init__(self):
        self.rows = []

    def write_candle(self, stream, maxlen, sym, tf, c, date=""):
        self.rows.append((tf, c, date))


class TestAngelHistory(unittest.TestCase):
    def test_angel_1m_row_stamped_bar_end(self):
        row = ["2026-10-05T11:15:00+05:30", 100, 101, 99, 100.5, 1000]
        c = hb.normalize_bar_ts(hb._row_to_candle(row), "1m", source="angel")
        self.assertEqual(c.ts_ms, ist(D1, 11, 15, 59, 999))
        # Yahoo/Moneycontrol 1m are already bar-end -> unchanged
        y = Candle(ist(D1, 11, 15, 59, 999), 1, 1, 1, 1, 1)
        self.assertEqual(hb.normalize_bar_ts(y, "1m", source="yahoo").ts_ms, y.ts_ms)

    def test_angel_candles_via_fetch_are_normalized(self):
        body = {"status": True, "data": [["2026-10-05T09:15:00+05:30", 1, 2, 0.5, 1.5, 10]]}
        with mock.patch.object(hb, "fetch_candle_data", return_value=body), \
                mock.patch.object(hb, "candle_rows", return_value=body["data"]), \
                mock.patch.object(hb, "api_failed", return_value=False), \
                mock.patch.object(hb, "SLEEP_SEC", 0):
            got, err = hb._angel_candles("tok", {"exchange": "NSE", "token": "1"}, "ONE_MINUTE", "a", "b")
        self.assertEqual(err, "")
        self.assertEqual(got[0].ts_ms, ist(D1, 9, 15, 59, 999))

    def test_daily_rows_normalized_to_session_close(self):
        for raw in ("2026-10-05T00:00:00+05:30", "2026-10-05T09:15:00+05:30"):
            c = hb.normalize_bar_ts(hb._row_to_candle([raw, 1, 2, 0.5, 1.5, 10]), "1d", source="angel")
            self.assertEqual(c.ts_ms, ist(D1, 15, 29, 59, 999))

    def test_write_skips_in_progress_and_out_of_session(self):
        store = FakeStore()
        now = ist(D1, 11, 16, 30)
        bars = [
            Candle(ist(D1, 11, 15, 59, 999), 1, 2, 0.5, 1.5, 1),  # closed
            Candle(ist(D1, 11, 16, 59, 999), 1, 2, 0.5, 1.5, 1),  # in progress
            Candle(ist(D1, 15, 30, 59, 999), 1, 2, 0.5, 1.5, 1),  # after close
        ]
        with mock.patch.object(hb, "existing_ts_set", return_value=set()), \
                mock.patch.object(hb.time, "time", return_value=now / 1000.0):
            n = hb._write_candles(None, store, "s", 0, "TCS", "1m", bars, skip_today=False)
        self.assertEqual(n, 1)
        self.assertEqual(store.rows[0][1].ts_ms, ist(D1, 11, 15, 59, 999))

    def test_daily_today_only_after_close_and_not_degenerate(self):
        today = Candle(ist(D1, 15, 29, 59, 999), 1, 2, 0.5, 1.5, 1)
        flat = Candle(ist(D1, 15, 29, 59, 999) - 86_400_000, 1, 1, 1, 1, 0)
        for now, want in ((ist(D1, 14, 0), 0), (ist(D1, 15, 31), 1)):
            store = FakeStore()
            with mock.patch.object(hb, "existing_ts_set", return_value=set()), \
                    mock.patch.object(hb.time, "time", return_value=now / 1000.0):
                n = hb._write_candles(None, store, "s", 0, "TCS", "1d", [flat, today], skip_today=True)
            self.assertEqual(n, want)
            if want:
                self.assertEqual(store.rows[0][2], "2026-10-05")

    def test_pick_pivot_source(self):
        good = Candle(ist(D1, 15, 29, 59, 999) - 86_400_000, 1, 2, 0.5, 1.5, 1)
        flat = Candle(ist(D1, 15, 29, 59, 999), 1, 1, 1, 1, 0)
        self.assertEqual(hb.pick_pivot_source([good, flat], ist(D1, 16, 0)), good)


class TestGreeksFreshness(unittest.TestCase):
    def test_poller_stamps_ts_ms_and_keeps_list_format(self):
        from app.greeks_poller import stamp_greeks_items

        items = [{"strikePrice": "100", "optionType": "CE", "delta": "0.5"}]
        out = stamp_greeks_items(items, 1234)
        self.assertEqual(out[0]["ts_ms"], 1234)
        self.assertNotIn("ts_ms", items[0])
        self.assertIsInstance(json.loads(json.dumps(out)), list)

    def _joiner(self, payload, spot_rows=()):
        from app import joiner as jm

        class FakeR:
            def get(self, key):
                return payload

            def xrevrange(self, stream, count=40):
                return list(spot_rows)

        j = jm.OptionsGreeksJoiner.__new__(jm.OptionsGreeksJoiner)
        j.r = FakeR()
        j._cache, j._cache_t, j._spot, j._spot_ts, j._spot_t = {}, {}, {}, {}, 0.0
        return jm, j

    def test_stale_greeks_rejected_fresh_accepted(self):
        now = int(time.time() * 1000)
        stale = json.dumps([{"strikePrice": "100", "optionType": "CE", "delta": "0.5",
                             "impliedVolatility": "20", "gamma": "0.01", "ts_ms": now - 600_000}])
        jm, j = self._joiner(stale)
        self.assertEqual(j._get_greeks_for("TCS", "2026-10-27", "", strike="100", cp="CE"), {})
        fresh = stale.replace(str(now - 600_000), str(now - 5_000))
        jm, j = self._joiner(fresh)
        self.assertEqual(j._get_greeks_for("TCS", "2026-10-27", "", strike="100", cp="CE")["delta"], "0.5")
        legacy = json.dumps([{"strikePrice": "100", "optionType": "CE", "delta": "0.5"}])
        jm, j = self._joiner(legacy)
        self.assertEqual(j._get_greeks_for("TCS", "2026-10-27", "", strike="100", cp="CE"), {})
        self.assertFalse(jm.greeks_fresh({"ts_ms": now - 181_000}, now, 180_000))
        self.assertTrue(jm.greeks_fresh({"ts_ms": now - 179_000}, now, 180_000))

    def test_spot_ages_out(self):
        now = int(time.time() * 1000)
        rows = [(f"{now - 120_000}-0", {"symbol": "TCS", "ltp": "100"}),
                (f"{now - 1_000}-0", {"symbol": "INFY", "ltp": "200"})]
        jm, j = self._joiner(None, rows)
        j._refresh_spot()
        self.assertIsNone(j._fresh_spot("TCS"))
        self.assertEqual(j._fresh_spot("INFY"), 200.0)


class TestRunGreeks(unittest.TestCase):
    def test_uses_real_poller_api(self):
        import inspect

        import run_greeks

        src = inspect.getsource(run_greeks)
        self.assertNotIn("active_expiry_by_underlying=", src)
        self.assertNotIn("poller.run_forever", src)
        calls = []

        class P:
            def poll_once(self, active_expiry, per_request_sleep=0.12):
                calls.append(dict(active_expiry))
                return len(active_expiry)

        n = run_greeks.poll_loop(P(), lambda: {"TCS": "2026-10-27"}, poll_sec=0, max_cycles=2)
        self.assertEqual(n, 2)
        self.assertEqual(calls, [{"TCS": "2026-10-27"}] * 2)


if __name__ == "__main__":
    unittest.main()

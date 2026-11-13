"""Fix tests: session-anchored candles, 1d bar after close, time flush, out-of-order."""

import datetime as dt
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.candle_builder import (  # noqa: E402
    IST,
    CandleBuilder1d,
    CandleBuilder1m,
    resample_candles,
    session_date_ist,
    tick_is_stale,
)
from app.candle_types import Candle  # noqa: E402
import run_candles_resampler as rsm  # noqa: E402
import run_daily_pivots as rdp  # noqa: E402


def ist(d, hh, mm, ss=0, ms=0):
    return int(dt.datetime(d.year, d.month, d.day, hh, mm, ss, ms * 1000, tzinfo=IST).timestamp() * 1000)


D1 = dt.date(2026, 10, 5)  # Monday
D2 = dt.date(2026, 10, 6)


def one_min_bars(d, start_hm, end_hm, base=100.0):
    """1m bars (ts = bar end) from start_hm up to (excluding) end_hm."""
    out = []
    t = dt.datetime(d.year, d.month, d.day, *start_hm, tzinfo=IST)
    end = dt.datetime(d.year, d.month, d.day, *end_hm, tzinfo=IST)
    i = 0
    while t < end:
        ts = int(t.timestamp() * 1000) + 59_999
        p = base + i
        out.append(Candle(ts_ms=ts, o=p, h=p + 0.5, l=p - 0.5, c=p + 0.25, v=10))
        t += dt.timedelta(minutes=1)
        i += 1
    return out


class TestDailyBuilder(unittest.TestCase):
    def test_after_close_ticks_and_next_day_produce_no_extra_bar(self):
        b = CandleBuilder1d()
        self.assertIsNone(b.update_tick(ist(D1, 9, 15, 5), 100.0, 0, 100.0, 101.0, 99.0, 1000))
        self.assertIsNone(b.update_tick(ist(D1, 15, 29, 50), 105.0, 0, 100.0, 110.0, 95.0, 50000))
        bar = b.update_tick(ist(D1, 15, 30, 5), 105.5, 0)  # first after-close tick closes once
        self.assertIsNotNone(bar)
        self.assertEqual((bar.o, bar.h, bar.l, bar.c, bar.v), (100.0, 110.0, 95.0, 105.0, 50000.0))
        self.assertEqual(session_date_ist(bar.ts_ms), D1)
        self.assertEqual(bar.ts_ms, ist(D1, 15, 29, 59, 999))
        # later after-close ticks + next-day pre-open/open tick: no second bar
        self.assertIsNone(b.update_tick(ist(D1, 15, 45), 105.7, 0))
        self.assertIsNone(b.update_tick(ist(D1, 20, 0), 105.7, 0))
        self.assertIsNone(b.update_tick(ist(D2, 9, 0), 105.7, 0))
        self.assertIsNone(b.update_tick(ist(D2, 9, 15, 1), 106.0, 0, 106.0, 106.0, 106.0, 10))
        self.assertIsNone(b.flush(ist(D2, 12, 0)))
        bar2 = b.flush(ist(D2, 15, 30, 3))
        self.assertIsNotNone(bar2)
        self.assertEqual(session_date_ist(bar2.ts_ms), D2)

    def test_flush_at_close(self):
        b = CandleBuilder1d()
        b.update_tick(ist(D1, 10, 0), 100.0, 0, 99.0, 102.0, 98.0, 500)
        self.assertIsNone(b.flush(ist(D1, 15, 29, 59)))
        self.assertIsNone(b.flush(ist(D1, 15, 30, 1), grace_ms=2000))
        bar = b.flush(ist(D1, 15, 30, 2), grace_ms=2000)
        self.assertEqual((bar.o, bar.h, bar.l, bar.c), (99.0, 102.0, 98.0, 100.0))
        self.assertIsNone(b.flush(ist(D1, 15, 31)))  # once only

    def test_restart_midday_uses_exchange_day_fields(self):
        b = CandleBuilder1d()
        # started at 13:00: only sees ltp 104-105, but exchange day H/L/V are full-day
        b.update_tick(ist(D1, 13, 0), 104.0, 0, 100.0, 112.0, 96.0, 70000)
        b.update_tick(ist(D1, 14, 0), 105.0, 0, 100.0, 112.0, 96.0, 90000)
        bar = b.flush(ist(D1, 15, 31))
        self.assertEqual((bar.o, bar.h, bar.l, bar.c, bar.v), (100.0, 112.0, 96.0, 105.0, 90000.0))

    def test_ticks_only_after_close_never_emit(self):
        b = CandleBuilder1d()
        self.assertIsNone(b.update_tick(ist(D1, 15, 40), 100.0, 0))
        self.assertIsNone(b.update_tick(ist(D2, 9, 20), 101.0, 0))
        self.assertIsNone(b.flush(ist(D1, 23, 0)))

    def test_out_of_order_older_session_dropped(self):
        b = CandleBuilder1d()
        b.update_tick(ist(D2, 10, 0), 100.0, 0)
        self.assertIsNone(b.update_tick(ist(D1, 14, 0), 90.0, 0))
        bar = b.flush(ist(D2, 15, 31))
        self.assertEqual(bar.l, 100.0)

    def test_stale_snapshot_with_yesterdays_timestamp(self):
        # WS connect snapshot at 09:16 today carrying yesterday's 15:29:59 exchange ts
        self.assertTrue(tick_is_stale(ist(D1, 15, 29, 59), ist(D2, 9, 16), 300_000))
        self.assertFalse(tick_is_stale(ist(D2, 9, 15, 58), ist(D2, 9, 16), 300_000))
        self.assertTrue(tick_is_stale(ist(D2, 9, 15), ist(D2, 9, 30), 300_000))


class TestMinuteBuilder(unittest.TestCase):
    def test_no_bars_at_or_after_close_and_flush(self):
        b = CandleBuilder1m()
        b.update_tick(ist(D1, 15, 29, 10), 100.0, 5)
        b.update_tick(ist(D1, 15, 29, 40), 101.0, 5)
        closed, _ = b.update_tick(ist(D1, 15, 30, 5), 102.0, 5)  # after close: ignored
        self.assertIsNone(closed)
        self.assertIsNone(b.flush(ist(D1, 15, 30, 1), grace_ms=2000))
        bar = b.flush(ist(D1, 15, 30, 2), grace_ms=2000)
        self.assertEqual(bar.ts_ms, ist(D1, 15, 29, 59, 999))
        self.assertEqual((bar.o, bar.h, bar.c, bar.v), (100.0, 101.0, 101.0, 10.0))
        self.assertIsNone(b.update_tick(ist(D1, 15, 45), 103.0, 1)[0])
        self.assertIsNone(b.flush(ist(D2, 9, 0)))

    def test_out_of_order_tick_does_not_close_or_reopen(self):
        b = CandleBuilder1m()
        b.update_tick(ist(D1, 10, 0, 10), 100.0, 0)
        closed, _ = b.update_tick(ist(D1, 10, 1, 5), 101.0, 0)
        self.assertEqual(closed.c, 100.0)
        closed, _ = b.update_tick(ist(D1, 10, 0, 50), 50.0, 0)  # older minute
        self.assertIsNone(closed)
        closed, _ = b.update_tick(ist(D1, 10, 2, 0), 102.0, 0)
        self.assertEqual((closed.o, closed.l, closed.c), (101.0, 101.0, 101.0))

    def test_late_tick_after_flush_dropped(self):
        b = CandleBuilder1m()
        b.update_tick(ist(D1, 10, 0, 10), 100.0, 0)
        self.assertIsNotNone(b.flush(ist(D1, 10, 1, 3)))
        self.assertIsNone(b.update_tick(ist(D1, 10, 0, 59), 99.0, 0)[0])
        self.assertIsNone(b.bucket)

    def test_pre_open_ignored(self):
        b = CandleBuilder1m()
        b.update_tick(ist(D1, 9, 7), 100.0, 0)
        self.assertIsNone(b.bucket)


class TestResample(unittest.TestCase):
    def test_history_resample_anchored_at_0915(self):
        bars = one_min_bars(D1, (9, 15), (15, 30))
        now = ist(D1, 16, 0)
        r10 = resample_candles(bars, 10, now_ms=now)
        r30 = resample_candles(bars, 30, now_ms=now)
        r5 = resample_candles(bars, 5, now_ms=now)
        self.assertEqual(r10[0].ts_ms, ist(D1, 9, 24, 59, 999))  # 09:15-09:25
        self.assertEqual(r10[0].o, 100.0)
        self.assertEqual(r10[0].v, 100.0)
        self.assertEqual(r30[0].ts_ms, ist(D1, 9, 44, 59, 999))  # 09:15-09:45
        self.assertEqual(r30[-1].ts_ms, ist(D1, 15, 29, 59, 999))  # 15:15-15:30 truncated
        self.assertEqual(r30[-1].v, 150.0)
        self.assertEqual(len(r30), 13)
        self.assertEqual(len(r10), 38)
        self.assertEqual(len(r5), 75)
        self.assertEqual(r10[-1].ts_ms, ist(D1, 15, 29, 59, 999))

    def test_history_resample_skips_in_progress(self):
        bars = one_min_bars(D1, (9, 15), (9, 50))
        r30 = resample_candles(bars, 30, now_ms=ist(D1, 9, 50, 30))
        self.assertEqual([c.ts_ms for c in r30], [ist(D1, 9, 44, 59, 999)])

    def test_live_resampler_first_10m_and_close_on_last_minute(self):
        rs = rsm.Resampler(10)
        out = []
        for c in one_min_bars(D1, (9, 15), (9, 25)):
            out.extend(rs.ingest("TCS", c))
        self.assertEqual(len(out), 1)  # emitted on the 09:24 bar, not on 09:25
        self.assertEqual(out[0].ts_ms, ist(D1, 9, 24, 59, 999))
        self.assertEqual(out[0].o, 100.0)

    def test_live_resampler_primed_skips_replayed_history(self):
        rs = rsm.Resampler(10)
        rs.prime("TCS", ist(D1, 9, 34, 59, 999))  # 09:25-09:35 already in the stream
        out = []
        for c in one_min_bars(D1, (9, 15), (9, 45)):
            out.extend(rs.ingest("TCS", c))
        self.assertEqual([c.ts_ms for c in out], [ist(D1, 9, 44, 59, 999)])

    def test_live_resampler_30m_closes_at_1530(self):
        rs = rsm.Resampler(30)
        out = []
        for c in one_min_bars(D1, (15, 0), (15, 30)):
            out.extend(rs.ingest("TCS", c))
        # 15:00-15:15 closes on the 15:14 bar; 15:15-15:30 closes on the 15:29 bar
        self.assertEqual([c.ts_ms for c in out], [ist(D1, 15, 14, 59, 999), ist(D1, 15, 29, 59, 999)])

    def test_live_resampler_wall_clock_flush_when_last_minute_missing(self):
        rs = rsm.Resampler(30)
        for c in one_min_bars(D1, (15, 15), (15, 28)):
            self.assertEqual(rs.ingest("TCS", c), [])
        self.assertEqual(rs.flush(ist(D1, 15, 30, 2), grace_ms=5000), [])
        got = rs.flush(ist(D1, 15, 30, 5), grace_ms=5000)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0][1].ts_ms, ist(D1, 15, 29, 59, 999))
        # late bar for the flushed bucket is dropped
        late = one_min_bars(D1, (15, 28), (15, 29))[0]
        self.assertEqual(rs.ingest("TCS", late), [])

    def test_live_resampler_out_of_order_and_out_of_session(self):
        rs = rsm.Resampler(5)
        bars = one_min_bars(D1, (9, 15), (9, 22))
        out = []
        for c in bars[:6]:
            out.extend(rs.ingest("X", c))
        self.assertEqual(len(out), 1)
        self.assertEqual(rs.ingest("X", bars[2]), [])  # older bucket
        self.assertEqual(rs.ingest("X", Candle(ist(D1, 15, 30, 59, 999), 1, 1, 1, 1, 1)), [])

    def test_bar_rolls_previous_and_is_last_minute_of_new_bucket(self):
        rs = rsm.Resampler(5)
        a = Candle(ist(D1, 9, 15, 59, 999), 1, 2, 0.5, 1.5, 1)
        b = Candle(ist(D1, 9, 24, 59, 999), 3, 4, 2.5, 3.5, 1)
        self.assertEqual(rs.ingest("X", a), [])
        out = rs.ingest("X", b)
        self.assertEqual([c.ts_ms for c in out], [ist(D1, 9, 19, 59, 999), ist(D1, 9, 24, 59, 999)])


class TestPivotSanity(unittest.TestCase):
    def test_reject_degenerate_and_future_and_older(self):
        good = Candle(ist(D1, 15, 29, 59, 999), 100, 110, 95, 105, 1)
        degenerate = Candle(ist(D2, 15, 29, 59, 999), 105, 105, 105, 105, 0)
        # morning of D2: D2's bar is not a completed session
        ok, _d, why = rdp.validate_pivot_source(degenerate, "", "2026-10-05", ist(D2, 9, 20))
        self.assertFalse(ok)
        ok, _d, why = rdp.validate_pivot_source(degenerate, "", "2026-10-05", ist(D2, 16, 0))
        self.assertFalse(ok)
        self.assertIn("degenerate", why)
        # next-day-stamped legacy bar dated D2 with real range during D2 session
        legacy = Candle(ist(D2, 9, 16), 105, 105.2, 104.9, 105.1, 0)
        self.assertFalse(rdp.validate_pivot_source(legacy, "2026-10-06", "2026-10-05", ist(D2, 9, 20))[0])
        ok, d, _ = rdp.validate_pivot_source(good, "2026-10-05", "2026-10-02", ist(D1, 15, 31))
        self.assertTrue(ok)
        self.assertEqual(d, "2026-10-05")
        self.assertFalse(rdp.validate_pivot_source(good, "", "2026-10-06", ist(D2, 16, 0))[0])
        self.assertFalse(rdp.validate_pivot_source(good, "2026-10-04", "", ist(D2, 16, 0))[0])

    def test_existing_date_parse(self):
        self.assertEqual(rdp._existing_pivots_date('{"date":"2026-10-05","P":"1"}'), "2026-10-05")
        self.assertEqual(rdp._existing_pivots_date(None), "")
        self.assertEqual(rdp._existing_pivots_date("garbage"), "")


class FakeStore:
    def __init__(self):
        self.rows = []

    def write_candle(self, stream, maxlen, sym, tf, c, date=""):
        self.rows.append((stream, sym, tf, c, date))


class TestPublisherFlow(unittest.TestCase):
    def test_publisher_process_and_flush(self):
        import run_candles_publisher as pub

        store = FakeStore()
        cb1m, cb1d, prev = {}, {}, {}
        w1d = lambda sym, c: store.write_candle(pub.OUT_1D_STREAM, 0, sym, "1d", c,
                                                date=session_date_ist(c.ts_ms).isoformat())

        def tick(ts, ltp, vol, recv=None, o="100", h="110", lo="95"):
            f = {"symbol": "TCS", "ltp": str(ltp), "ts_exch": str(ts), "vol": str(vol), "o": o, "h": h, "l": lo,
                 "c": "99"}
            pub.process_tick(f, recv or ts + 50, {"TCS"}, cb1m, cb1d, prev, store, w1d)

        # stale connect snapshot carrying yesterday's ts creates nothing
        tick(ist(D1, 15, 29, 59) - 86_400_000, 98, 10, recv=ist(D1, 9, 15, 2))
        self.assertEqual(cb1d, {})
        tick(ist(D1, 15, 29, 1), 104, 1000)
        tick(ist(D1, 15, 29, 30), 105, 1200)
        tick(ist(D1, 15, 30, 5), 106, 1300)  # after close
        one_d = [r for r in store.rows if r[2] == "1d"]
        self.assertEqual(len(one_d), 1)
        self.assertEqual(one_d[0][4], "2026-10-05")
        self.assertEqual((one_d[0][3].h, one_d[0][3].l, one_d[0][3].c, one_d[0][3].v), (110, 95, 105, 1200))
        pub.flush_all(ist(D1, 15, 30, 3), cb1m, cb1d, store, w1d)
        one_m = [r for r in store.rows if r[2] == "1m"]
        self.assertEqual(len(one_m), 1)
        self.assertEqual(one_m[0][3].ts_ms, ist(D1, 15, 29, 59, 999))
        tick(ist(D1, 15, 45), 106, 1400)
        tick(ist(D2, 9, 14), 106, 0)
        pub.flush_all(ist(D2, 9, 14, 30), cb1m, cb1d, store, w1d)
        self.assertEqual(len([r for r in store.rows if r[2] == "1d"]), 1)


if __name__ == "__main__":
    unittest.main()

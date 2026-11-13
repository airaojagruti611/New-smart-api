"""History seed dedup across a shared multi-symbol stream, and 1m volume deltas."""

import unittest

from app.candle_io import read_symbol_candles, stream_symbol_ts


class StreamRedis:
    """Minimal XADD/XRANGE fake with Redis exclusive-start ("(id") semantics."""

    def __init__(self):
        self.streams = {}
        self.seq = 0

    def xadd(self, stream, fields, maxlen=None):
        self.seq += 1
        self.streams.setdefault(stream, []).append((f"{self.seq}-0", dict(fields)))

    def xrange(self, stream, min="-", max="+", count=None):
        rows = self.streams.get(stream, [])
        if min.startswith("("):
            lo = int(min[1:].split("-")[0])
            rows = [r for r in rows if int(r[0].split("-")[0]) > lo]
        return rows[:count] if count else rows


def bar(sym, ts):
    return {"symbol": sym, "ts_ms": str(ts), "o": "1", "h": "2", "l": "0.5", "c": "1.5", "v": "10"}


class TestStreamIndex(unittest.TestCase):
    def test_full_pass_sees_early_symbols_beyond_page(self):
        r = StreamRedis()
        for i in range(25):
            r.xadd("s", bar("AAA", 1000 + i))
        for i in range(30):  # later symbol pushes AAA out of any tail window
            r.xadd("s", bar("BBB", 1000 + i))
        r.xadd("s", bar("AAA", 1000))  # duplicate ts
        import app.candle_io as cio

        orig = cio._iter_stream
        cio._iter_stream = lambda rr, st, page=7: orig(rr, st, page=7)
        try:
            idx = stream_symbol_ts(r, "s")
            got = read_symbol_candles(r, "s", {"AAA", "CCC"}, limit=10)
        finally:
            cio._iter_stream = orig
        self.assertEqual(len(idx["AAA"]), 25)
        self.assertEqual(len(idx["BBB"]), 30)
        self.assertEqual(len(got["AAA"]), 10)
        self.assertEqual(got["AAA"][-1].ts_ms, 1024)
        self.assertEqual(got["CCC"], [])


class TestPublisherVolume(unittest.TestCase):
    def test_out_of_order_cum_vol_not_double_counted(self):
        import run_candles_publisher as pub

        prev = {}
        deltas = []

        class CB1m:
            def update_tick(self, ts_ms, ltp, vol_delta):
                deltas.append(vol_delta)
                return None, None

        class CB1d:
            def update_tick(self, **kw):
                return None

        cb1m, cb1d = {"TCS": CB1m()}, {"TCS": CB1d()}
        now = 1791184000000
        for cum in (1000, 1200, 1100, 1200, 1300, 50):
            f = {"symbol": "TCS", "ltp": "100", "ts_exch": str(now), "vol": str(cum)}
            pub.process_tick(f, now + 10, {"TCS"}, cb1m, cb1d, prev, None, None)
        # 1000->1200 +200, 1100 stale 0, 1200 repeat 0, 1300 +100, 50 new session 0
        self.assertEqual(deltas, [0, 200, 0, 0, 100, 0])
        self.assertEqual(prev["TCS"], 50)


if __name__ == "__main__":
    unittest.main()

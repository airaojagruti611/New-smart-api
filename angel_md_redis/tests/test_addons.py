"""Unit tests for app/addons.py and the run_addons engine (fake Redis)."""

from __future__ import annotations

import datetime as dt
import json
import unittest

import run_addons as ra
from app import addons as A
from app.candle_types import Candle

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


def ms(day, h, m):
    return int(dt.datetime(2026, 11, day, h, m, tzinfo=IST).timestamp() * 1000)


def bars(n, start=100.0, step=0.0, vol=1000.0, day=12, h=10, m0=0, spread=0.5):
    out = []
    for i in range(n):
        px = start + step * i
        t = ms(day, h, m0) + i * 60_000
        out.append(Candle(ts_ms=t, o=px, h=px + spread, l=px - spread, c=px, v=vol))
    return out


class TestCircuit(unittest.TestCase):
    def test_none_inputs(self):
        self.assertFalse(A.circuit_proximity(None, 110, 90)["available"])
        r = A.circuit_proximity(100, None, None)
        self.assertEqual(r["reason"], "no_circuit_limits")

    def test_near_and_hit(self):
        r = A.circuit_proximity(109.5, 110, 90, 1.0)
        self.assertEqual(r["signal"], "NEAR_UPPER_CIRCUIT")
        self.assertAlmostEqual(r["dist_upper_pct"], 0.5 / 109.5 * 100)
        self.assertEqual(A.circuit_proximity(110, 110, 90)["signal"], "UPPER_CIRCUIT_HIT")
        self.assertEqual(A.circuit_proximity(90, 110, 90)["signal"], "LOWER_CIRCUIT_HIT")
        self.assertEqual(A.circuit_proximity(100, 110, 90)["signal"], "NONE")
        self.assertEqual(A.circuit_proximity(90.5, 110, 90)["signal"], "NEAR_LOWER_CIRCUIT")


class TestDailyVolume(unittest.TestCase):
    def test_gain(self):
        r = A.daily_volume_gain(150, [100] * 10)
        self.assertAlmostEqual(r["gain_pct"], 50.0)
        self.assertEqual(r["signal"], "VOLUME_UP")

    def test_window_uses_last_20(self):
        r = A.daily_volume_gain(100, [1000] * 5 + [100] * 20)
        self.assertAlmostEqual(r["gain_pct"], 0.0)

    def test_insufficient(self):
        r = A.daily_volume_gain(100, [100, 100])
        self.assertFalse(r["available"])
        self.assertEqual(r["reason"], "insufficient_daily_history")
        self.assertEqual(A.daily_volume_gain(None, [1] * 9)["reason"], "no_today_volume")


class TestBox(unittest.TestCase):
    def test_breakout_breakdown_inside(self):
        base = bars(61)
        up = base[:-1] + [Candle(base[-1].ts_ms, 100, 103, 100, 102, 1000)]
        self.assertEqual(A.box_breakout(up)["signal"], "BOX_BREAKOUT")
        dn = base[:-1] + [Candle(base[-1].ts_ms, 100, 100, 97, 98, 1000)]
        self.assertEqual(A.box_breakout(dn)["signal"], "BOX_BREAKDOWN")
        self.assertEqual(A.box_breakout(base)["signal"], "INSIDE_BOX")

    def test_insufficient(self):
        r = A.box_breakout(bars(10))
        self.assertFalse(r["available"])
        self.assertFalse(A.box_breakout([])["available"])


class TestVolumeSpike(unittest.TestCase):
    def test_spike(self):
        hist = [1000.0 + (i % 3) * 10 for i in range(20)]
        r = A.volume_spike(5000, hist)
        self.assertEqual(r["signal"], "VOLUME_SPIKE")
        self.assertGreater(r["multiple"], 4)
        self.assertGreater(r["zscore"], 3)

    def test_normal_and_flat_history(self):
        self.assertEqual(A.volume_spike(1000, [1000.0] * 12)["signal"], "NORMAL")
        r = A.volume_spike(1000, [1000.0] * 12)
        self.assertIsNone(r["zscore"])

    def test_insufficient(self):
        self.assertEqual(A.volume_spike(1, [1, 2])["reason"], "insufficient_history")
        self.assertEqual(A.volume_spike(None, [])["reason"], "no_current_volume")

    def test_rolling_sums(self):
        self.assertEqual(A.rolling_sums([1, 2, 3, 4, 5, 6, 7], 3), [9, 18])


SECT = {"TCS": "IT", "INFY": "IT", "SBIN": "BANK"}


class TestSectors(unittest.TestCase):
    def test_heatmap(self):
        stats = {"TCS": {"buy_vol": 80, "sell_vol": 20}, "INFY": {"buy_vol": 70, "sell_vol": 30},
                 "SBIN": {"buy_vol": 10, "sell_vol": 90}, "XYZ": {"buy_vol": 1, "sell_vol": 1}}
        r = A.sector_heatmap(stats, SECT)
        self.assertEqual(r["sectors"]["IT"]["label"], "BUYING")
        self.assertAlmostEqual(r["sectors"]["IT"]["net_pct"], 50.0)
        self.assertEqual(r["sectors"]["BANK"]["label"], "SELLING")
        self.assertEqual(r["unmapped_symbols"], 1)

    def test_heatmap_empty(self):
        r = A.sector_heatmap({}, SECT)
        self.assertFalse(r["available"])

    def test_change(self):
        stats = {"TCS": {"chg_pct": 2.0}, "INFY": {"chg_pct": 1.0}, "SBIN": {"chg_pct": -1.0},
                 "XYZ": {"chg_pct": 5.0}, "NONE": {"chg_pct": None}}
        r = A.sector_change(stats, SECT)
        self.assertAlmostEqual(r["sectors"]["IT"]["avg_chg_pct"], 1.5)
        self.assertEqual(r["top"], "IT")
        self.assertEqual(r["bottom"], "BANK")
        self.assertEqual(r["sectors"]["IT"]["advancers"], 2)
        self.assertFalse(A.sector_change({}, SECT)["available"])


class TestPremarket(unittest.TestCase):
    def test_gap(self):
        r = A.premarket_analysis(100, 102, 5000)
        self.assertEqual(r["signal"], "GAP_UP")
        self.assertAlmostEqual(r["gap_pct"], 2.0)
        self.assertIsNone(r["reason"])
        self.assertEqual(A.premarket_analysis(100, 98, 1)["signal"], "GAP_DOWN")
        self.assertEqual(A.premarket_analysis(100, 100.2, 1)["signal"], "FLAT_OPEN")

    def test_fallbacks(self):
        r = A.premarket_analysis(100, None, None, open_price=103)
        self.assertEqual(r["price_source"], "open")
        self.assertEqual(r["reason"], "preopen_volume_unavailable")
        self.assertEqual(A.premarket_analysis(None, 1)["reason"], "no_prev_close")
        self.assertEqual(A.premarket_analysis(100, None)["reason"], "no_preopen_data")


class TestFlow(unittest.TestCase):
    def test_buying_selling(self):
        up = [Candle(i, 100, 101, 100, 101, 100) for i in range(5)]
        dn = [Candle(i, 101, 101, 100, 100, 100) for i in range(5)]
        self.assertEqual(A.heavy_flow(up)["signal"], "HEAVY_BUYING")
        self.assertEqual(A.heavy_flow(dn)["signal"], "HEAVY_SELLING")
        r = A.heavy_flow(up, tbq=300, tsq=100)
        self.assertAlmostEqual(r["qty_imbalance"], 0.5)

    def test_balanced_and_missing(self):
        doji = [Candle(i, 100, 100, 100, 100, 100) for i in range(5)]
        self.assertEqual(A.heavy_flow(doji)["signal"], "BALANCED")
        self.assertFalse(A.heavy_flow(doji[:2])["available"])
        zero = [Candle(i, 100, 101, 99, 100, 0) for i in range(5)]
        self.assertEqual(A.heavy_flow(zero)["reason"], "no_volume")


class TestFastMove(unittest.TestCase):
    def test_up_down_normal(self):
        up = bars(10, step=0.5)  # 4.5 up over 9 bars; last 5 bars => +2.5
        self.assertEqual(A.fast_move(up, 5, 1.0)["signal"], "FAST_UP")
        dn = bars(10, start=100, step=-0.5)
        self.assertEqual(A.fast_move(dn, 5, 1.0)["signal"], "FAST_DOWN")
        self.assertEqual(A.fast_move(bars(10), 5, 1.0)["signal"], "NORMAL")

    def test_insufficient(self):
        self.assertFalse(A.fast_move(bars(3), 5)["available"])


class TestVolumeBreaker(unittest.TestCase):
    def _with_last(self, o, h, l, c, v):
        b = bars(21)
        return b[:-1] + [Candle(b[-1].ts_ms, o, h, l, c, v)]

    def test_up_down(self):
        self.assertEqual(A.volume_breaker(self._with_last(100, 103, 100, 102, 5000))["signal"],
                         "VOLUME_BREAKER_UP")
        self.assertEqual(A.volume_breaker(self._with_last(100, 100, 97, 98, 5000))["signal"],
                         "VOLUME_BREAKER_DOWN")

    def test_needs_volume(self):
        self.assertEqual(A.volume_breaker(self._with_last(100, 103, 100, 102, 1000))["signal"], "NONE")
        self.assertFalse(A.volume_breaker(bars(5))["available"])


class TestIntradayVolume(unittest.TestCase):
    def test_same_time(self):
        r = A.intraday_volume_increase(150, [100, 100, 100])
        self.assertEqual(r["reference"], "same_time_avg")
        self.assertAlmostEqual(r["increase_pct"], 50.0)

    def test_prev_day_fallback(self):
        r = A.intraday_volume_increase(50, [100], prev_day_total=200)
        self.assertEqual(r["reference"], "prev_day_total")
        self.assertAlmostEqual(r["increase_pct"], -75.0)
        self.assertEqual(r["signal"], "VOLUME_DOWN")

    def test_unavailable(self):
        self.assertEqual(A.intraday_volume_increase(None, [])["reason"], "no_today_volume")
        self.assertEqual(A.intraday_volume_increase(5, [])["reason"], "no_reference_volume")


class FakeRedis:
    def __init__(self):
        self.kv, self.streams = {}, {}

    def set(self, k, v, ex=None):
        self.kv[k] = v

    def get(self, k):
        return self.kv.get(k)

    def xadd(self, k, fields, maxlen=None, approximate=True):
        s = self.streams.setdefault(k, [])
        s.append((f"{len(s)+1}-0", dict(fields)))

    def xrevrange(self, k, max="+", min="-", count=None):
        rows = list(reversed(self.streams.get(k, [])))
        return rows[:count] if count else rows


def cfields(sym, c: Candle):
    return {"symbol": sym, "ts_ms": str(c.ts_ms), "o": str(c.o), "h": str(c.h), "l": str(c.l),
            "c": str(c.c), "v": str(c.v)}


class TestRunner(unittest.TestCase):
    def setUp(self):
        self.r = FakeRedis()
        # prior day 1m history + 6 daily bars
        for c in bars(120, day=11, h=10):
            self.r.xadd(ra.IN_1M, cfields("TCS", c))
        for i in range(6):
            self.r.xadd(ra.IN_1D, cfields("TCS", Candle(ms(1 + i, 15, 29), 100, 101, 99, 100, 100000)))
        self.r.xadd(ra.EQ_STREAM, {"symbol": "TCS", "ltp": "100", "c": "99", "vol": "150000",
                                   "tbq": "300", "tsq": "100", "upper_circuit_limit": "100.5",
                                   "lower_circuit_limit": "90"})
        self.eng = ra.AddonsEngine(self.r, {"TCS", "INFY"}, {"TCS": "IT", "INFY": "IT"})
        self.eng.seed()

    def test_on_candle_publishes(self):
        b = bars(30, day=12, h=10)
        doc = None
        for c in b[:-1]:
            self.eng.on_candle("TCS", c)
        last = Candle(b[-1].ts_ms, 100, 103, 100, 102.9, 6000)
        doc = self.eng.on_candle("TCS", last)
        self.assertIsNotNone(doc)
        for k in ("circuit", "daily_volume_gain", "box", "volume_spike_1m", "volume_spike_5m", "premarket",
                  "heavy_flow", "fast_move", "volume_breaker", "intraday_volume"):
            self.assertIn(k, doc)
        self.assertEqual(doc["circuit"]["signal"], "NEAR_UPPER_CIRCUIT")
        self.assertEqual(doc["volume_spike_1m"]["signal"], "VOLUME_SPIKE")
        self.assertEqual(doc["volume_breaker"]["signal"], "VOLUME_BREAKER_UP")
        self.assertEqual(doc["intraday_volume"]["reference"], "prev_day_total")
        self.assertTrue(doc["daily_volume_gain"]["available"])
        self.assertEqual(doc["heavy_flow"]["signal"], "HEAVY_BUYING")
        latest = json.loads(self.r.get(ra.LATEST_PREFIX + "TCS"))
        self.assertEqual(latest["ts_ms"], last.ts_ms)
        entry = self.r.streams[ra.OUT_STREAM][-1][1]
        self.assertEqual(entry["symbol"], "TCS")
        self.assertIn("VOLUME_SPIKE", entry["flags"])

    def test_dedupe_and_unknown_symbol(self):
        c = bars(1, day=12)[0]
        self.assertIsNotNone(self.eng.on_candle("TCS", c))
        self.assertIsNone(self.eng.on_candle("TCS", c))
        self.assertIsNone(self.eng.on_candle("ZZZ", c))

    def test_missing_inputs_never_crash(self):
        doc = self.eng.on_candle("INFY", bars(1, day=12)[0])
        self.assertFalse(doc["box"]["available"])
        self.assertIsNotNone(doc["box"]["reason"])
        self.assertFalse(doc["circuit"]["available"])
        self.assertNotIn("error", doc)

    def test_market_keys(self):
        for c in bars(20, day=12):
            self.eng.on_candle("TCS", c)
        self.eng.publish_market(123)
        heat = json.loads(self.r.get(ra.HEATMAP_KEY))
        chg = json.loads(self.r.get(ra.SECTOR_CHANGE_KEY))
        self.assertIn("IT", heat["sectors"])
        self.assertIn("IT", chg["sectors"])
        self.assertEqual(heat["ts_ms"], 123)

    def test_parse_tick_optional(self):
        t = ra.parse_tick({"ltp": "5", "c": ""})
        self.assertIsNone(t["upper"])
        self.assertIsNone(t["prev_close"])


if __name__ == "__main__":
    unittest.main()

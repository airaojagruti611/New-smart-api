"""QA fixes: stale level breaks, entry-trigger null guards/ACK, volume freshness,
HTF weekly/monthly completed-bucket handling, indicator-score side check,
volume surge rounding, EMA equal -> neutral."""

from __future__ import annotations

import datetime as dt
import json
import time
import unittest

from app.candle_types import Candle, PivotLevels
from app.htf_trend_filter import IST, htf_trend_bias
from app.indicator_score import compute_indicator_score
from app.level_entry import LevelBreakTracker

import run_entry_trigger as ret

PIVOTS = PivotLevels(date="2026-10-02", P=100.0, R1=105.0, S1=95.0, R2=110.0, S2=90.0)
SYM = "NIFTY"


class FakeRedis:
    """Minimal in-memory stand-in for the methods run_entry_trigger uses."""

    def __init__(self, kv=None):
        self.kv = dict(kv or {})
        self.streams = {}
        self.acked = []

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None):
        self.kv[key] = value

    def xadd(self, stream, payload, maxlen=None, approximate=True):
        self.streams.setdefault(stream, []).append(dict(payload))
        return f"{int(time.time() * 1000)}-0"

    def xack(self, stream, group, *ids):
        self.acked.extend(ids)
        return len(ids)


# ── Fix 1: level_entry ─────────────────────────────────────────────────────
class LevelBreakTrackerTests(unittest.TestCase):
    def setUp(self):
        self.now = 1_791_000_000_000
        self.t = LevelBreakTracker(max_bar_age_ms=120_000)

    def test_old_bars_warm_up_but_never_signal(self):
        # Two history bars 1 hour old: 104 -> 106 crosses R1 but must NOT signal.
        old = self.now - 3_600_000
        d1 = self.t.on_bar(SYM, 104.0, old, self.now, PIVOTS)
        d2 = self.t.on_bar(SYM, 106.0, old + 60_000, self.now, PIVOTS)
        self.assertFalse(d1.emit)
        self.assertFalse(d2.emit)
        self.assertEqual(d2.skip_reason, "stale_bar_warmup")
        # State was still updated by the old bar.
        self.assertEqual(self.t.prev_close(SYM), 106.0)

    def test_fresh_bar_after_warmup_signals(self):
        self.t.on_bar(SYM, 104.0, self.now - 3_600_000, self.now, PIVOTS)
        d = self.t.on_bar(SYM, 106.0, self.now - 30_000, self.now, PIVOTS)
        self.assertTrue(d.emit)
        self.assertEqual(d.result.signal, "BUY CALL")
        self.assertEqual(d.result.level, "R1")
        self.assertEqual(d.prev_close, 104.0)

    def test_replayed_older_bar_not_compared_to_newer_seed(self):
        # Seeded at startup from the latest bar (close 106 at now-30s).
        self.t.seed(SYM, 106.0, self.now - 30_000)
        # A replayed older bar must not be compared against the seed.
        d = self.t.on_bar(SYM, 104.0, self.now - 90_000, self.now, PIVOTS)
        self.assertFalse(d.emit)
        self.assertEqual(d.skip_reason, "out_of_order_or_duplicate")
        self.assertEqual(self.t.prev_close(SYM), 106.0)

    def test_missing_bar_ts_not_evaluated(self):
        self.t.seed(SYM, 104.0, None)
        d = self.t.on_bar(SYM, 106.0, None, self.now, PIVOTS)
        self.assertFalse(d.emit)
        self.assertEqual(d.skip_reason, "missing_bar_ts")

    def test_pivots_loader_only_called_for_fresh_bars(self):
        calls = []

        def loader():
            calls.append(1)
            return PIVOTS

        self.t.on_bar(SYM, 104.0, self.now - 3_600_000, self.now, loader)
        self.t.on_bar(SYM, 106.0, self.now - 3_540_000, self.now, loader)
        self.assertEqual(calls, [])
        self.t.on_bar(SYM, 104.0, self.now - 10_000, self.now, loader)
        self.assertEqual(calls, [1])


# ── Fixes 1-3: entry_trigger runner ────────────────────────────────────────
class EntryTriggerRunnerTests(unittest.TestCase):
    def setUp(self):
        self.now = int(time.time() * 1000)
        self.msg_id = f"{self.now - 5_000}-0"
        self.fields = {
            "symbol": SYM,
            "signal": "BUY CALL",
            "level": "R1",
            "strength": "strong",
            "side": "break_up",
            "price": "106.00",
            "bar_ts_ms": str(self.now - 10_000),
        }

    def _kv(self, *, htf=True, oi=True, vol_ts=None, gp=True):
        now = self.now
        kv = {
            f"{ret.ST_LATEST_PREFIX}{SYM}": json.dumps({"ts_ms": now, "bias": "CALL"}),
            f"{ret.EMA_LATEST_PREFIX}{SYM}": json.dumps({"ts_ms": now, "state": "bullish"}),
        }
        if htf:
            kv[f"{ret.HTF_LATEST_PREFIX}{SYM}"] = json.dumps({"ts_ms": now, "bias": "CALL"})
        if oi:
            kv[f"{ret.OI_UNDERLYING_LATEST_PREFIX}{SYM}"] = json.dumps(
                {"ts_ms": now, "positioning": "BULLISH_POSITIONING", "primary_resistance": "24500"}
            )
        if gp:
            kv[f"{ret.GREEKS_PHASE_UNDERLYING_PREFIX}{SYM}:CE"] = json.dumps({"phase": "MARKUP"})
        kv[ret.VOLUME_LATEST_KEY] = json.dumps(
            {SYM: {"ts_ms": now if vol_ts is None else vol_ts, "signal": "Bullish Volume"}}
        )
        return kv

    def _run(self, r, msg_id=None, fields=None):
        resp = [(ret.IN_LEVEL, [(msg_id or self.msg_id, fields or self.fields)])]
        return ret.process_batch(r, resp, {SYM}, now_ms=self.now)

    def _out(self, r):
        return r.streams.get(ret.OUT_STREAM, [])

    def test_all_gates_pass_emits_buy(self):
        r = FakeRedis(self._kv())
        self._run(r)
        self.assertEqual(len(self._out(r)), 1)
        self.assertEqual(self._out(r)[0]["signal"], "BUY CALL")
        self.assertEqual(r.acked, [self.msg_id])

    def test_missing_htf_oi_volume_no_crash_acked_no_trigger(self):
        kv = self._kv(htf=False, oi=False, gp=False)
        del kv[ret.VOLUME_LATEST_KEY]
        r = FakeRedis(kv)
        self._run(r)
        self.assertEqual(r.acked, [self.msg_id])
        out = self._out(r)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["signal"], "NEUTRAL")
        self.assertIn("htf_fail", out[0]["reason"])

    def test_old_bar_ts_dropped_and_acked(self):
        r = FakeRedis(self._kv())
        fields = dict(self.fields, bar_ts_ms=str(self.now - 3_600_000))
        self._run(r, fields=fields)
        self.assertEqual(self._out(r), [])
        self.assertEqual(r.acked, [self.msg_id])

    def test_missing_bar_ts_dropped(self):
        r = FakeRedis(self._kv())
        fields = {k: v for k, v in self.fields.items() if k != "bar_ts_ms"}
        self._run(r, fields=fields)
        self.assertEqual(self._out(r), [])
        self.assertEqual(r.acked, [self.msg_id])

    def test_old_stream_id_dropped(self):
        r = FakeRedis(self._kv())
        old_id = f"{self.now - 600_000}-0"
        self._run(r, msg_id=old_id)
        self.assertEqual(self._out(r), [])
        self.assertEqual(r.acked, [old_id])

    def test_stale_volume_fails_gate(self):
        r = FakeRedis(self._kv(vol_ts=1000))  # 1970
        self._run(r)
        out = self._out(r)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["signal"], "NEUTRAL")
        self.assertIn("volume_fail", out[0]["reason"])
        self.assertEqual(out[0]["volume_fresh"], "0")
        self.assertEqual(out[0]["volume_signal"], "")
        self.assertEqual(out[0]["volume_signal_raw"], "Bullish Volume")
        # companion fields of stale volume are blanked too (probability scores them)
        self.assertEqual(out[0]["volume_surge"], "")
        self.assertEqual(out[0]["buy_pct"], "")
        self.assertEqual(out[0]["sell_pct"], "")

    def test_one_bad_message_does_not_kill_batch(self):
        r = FakeRedis(self._kv())
        bad_id = f"{self.now - 4_000}-0"
        good_id = f"{self.now - 3_000}-0"
        orig = ret._load_latest

        calls = {"n": 0}

        def boom(rr, key):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("boom")
            return orig(rr, key)

        ret._load_latest = boom
        try:
            resp = [(ret.IN_LEVEL, [(bad_id, self.fields), (good_id, self.fields)])]
            ret.process_batch(r, resp, {SYM}, now_ms=self.now)
        finally:
            ret._load_latest = orig
        self.assertEqual(r.acked, [bad_id, good_id])
        self.assertEqual(len(self._out(r)), 1)


# ── Fix 4: HTF weekly / monthly ────────────────────────────────────────────
def _daily(d: dt.date, close: float) -> Candle:
    ts = int(dt.datetime(d.year, d.month, d.day, 15, 30, tzinfo=IST).timestamp() * 1000)
    return Candle(ts_ms=ts, o=close, h=close, l=close, c=close, v=1.0)


def _sessions(start: dt.date, end: dt.date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += dt.timedelta(days=1)


class HtfTrendTests(unittest.TestCase):
    def _series(self):
        # Aug 3 .. Sep 25 2026: flat-ish decline 200 -> ~150; then the week
        # Sep 28 – Oct 2 rallies strongly to 230.
        bars = []
        price = 200.0
        for d in _sessions(dt.date(2026, 8, 3), dt.date(2026, 9, 25)):
            price -= 1.0
            bars.append(_daily(d, price))
        for i, d in enumerate(_sessions(dt.date(2026, 9, 28), dt.date(2026, 10, 2))):
            bars.append(_daily(d, 200.0 + 7.5 * (i + 1)))
        return bars

    def test_completed_week_kept_on_monday(self):
        res = htf_trend_bias(self._series(), today=dt.date(2026, 10, 5))
        self.assertEqual(res.weekly, "bullish")
        self.assertAlmostEqual(res.w_close, 237.5)
        # Sep closed on Sep 30 (215.0+7.5=222.5) vs Aug close (lower) -> bullish.
        self.assertEqual(res.monthly, "bullish")
        self.assertAlmostEqual(res.m_close, 222.5)

    def test_in_progress_week_dropped_midweek(self):
        bars = self._series() + [_daily(dt.date(2026, 10, 5), 100.0)]
        res = htf_trend_bias(bars, today=dt.date(2026, 10, 6))
        # Current week (only Oct 5 so far) is in progress -> ignored.
        self.assertEqual(res.weekly, "bullish")
        self.assertAlmostEqual(res.w_close, 237.5)
        # October is in progress -> monthly compares Sep vs Aug.
        self.assertAlmostEqual(res.m_close, 222.5)
        # Daily uses closed dailies directly.
        self.assertEqual(res.daily, "bearish")


# ── Fix 5: indicator score side check ──────────────────────────────────────
class IndicatorScoreTests(unittest.TestCase):
    def test_put_bearish_with_call_strong_not_upgraded(self):
        res = compute_indicator_score("PUT", "bearish", level_signal="BUY CALL", level="R1", strength="strong")
        self.assertEqual(res.score, -1.0)
        self.assertEqual(res.label, "bearish")

    def test_call_bullish_with_put_strong_not_upgraded(self):
        res = compute_indicator_score("CALL", "bullish", level_signal="BUY PUT", level="S1", strength="strong")
        self.assertEqual(res.score, 1.0)

    def test_matching_side_strong_upgrades(self):
        self.assertEqual(
            compute_indicator_score("PUT", "bearish", level_signal="BUY PUT", level="P", strength="strong").score,
            -2.0,
        )
        self.assertEqual(
            compute_indicator_score("CALL", "bullish", level_signal="BUY CALL", level="R1", strength="strong").score,
            2.0,
        )


# ── Mediums ────────────────────────────────────────────────────────────────
class VolumeSurgeTests(unittest.TestCase):
    def test_surge_compared_unrounded(self):
        from app.volume_analyzer import VolumeAnalyzer

        # 2004 / 1000 = 2.004 -> rounds to 2.0 but is > 2 -> Strong.
        res = VolumeAnalyzer().analyze(high=110.0, low=100.0, close=109.0, volume=2004.0, avg_volume=1000.0)
        self.assertEqual(res.volume_surge, 2.0)
        self.assertEqual(res.signal, "Strong Bullish Volume")
        res2 = VolumeAnalyzer().analyze(high=110.0, low=100.0, close=109.0, volume=2000.0, avg_volume=1000.0)
        self.assertEqual(res2.signal, "Bullish Volume")


class EmaNeutralTests(unittest.TestCase):
    def test_equal_emas_neutral(self):
        from app import ema_cross as ec

        closes = [100.0] * 40
        candles = [Candle(ts_ms=i * 60_000, o=c, h=c, l=c, c=c, v=1.0) for i, c in enumerate(closes)]
        pts = [p for p in ec.ema_cross(candles) if p is not None]
        self.assertTrue(pts)
        self.assertEqual(pts[-1].state, "neutral")


if __name__ == "__main__":
    unittest.main()

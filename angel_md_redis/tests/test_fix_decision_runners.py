"""QA fixes — runner wiring: stale-signal drop, signal_ts_ms chain, account fallback, surge ratio, kill switch."""

from __future__ import annotations

import datetime as dt
import fnmatch
import json
import time
import unittest

import run_icare
import run_probability
import run_trade_ranking as rtr
from app.option_pricing import IST
from app.probability_engine import signal_origin_ms, volume_surge_flag


class FakeRedis:
    def __init__(self):
        self.kv, self.hashes, self.zsets, self.streams = {}, {}, {}, {}

    def get(self, k):
        return self.kv.get(k)

    def set(self, k, v, ex=None):
        self.kv[k] = v

    def exists(self, k):
        return int(k in self.kv)

    def scan_iter(self, match="*", count=None):
        return [k for k in list(self.kv) if fnmatch.fnmatch(k, match)]

    def hget(self, k, f):
        return self.hashes.get(k, {}).get(f)

    def hset(self, k, f, v):
        self.hashes.setdefault(k, {})[f] = v

    def hgetall(self, k):
        return dict(self.hashes.get(k, {}))

    def hdel(self, k, *fs):
        for f in fs:
            self.hashes.get(k, {}).pop(f, None)

    def zadd(self, k, mapping):
        self.zsets.setdefault(k, {}).update(mapping)

    def zrem(self, k, *ms):
        for m in ms:
            self.zsets.get(k, {}).pop(m, None)

    def xadd(self, k, fields, maxlen=None, approximate=True):
        self.streams.setdefault(k, []).append(dict(fields))

    def xrevrange(self, *a, **kw):
        return []


NOW = int(time.time() * 1000)
MAX = 60_000


def sid(ms: int) -> str:
    return f"{ms}-0"


def sie_msg(**over):
    m = {"symbol": "TCS", "status": "OK", "side": "CE", "tradingsymbol": "TCS26OCT2100CE", "premium": "20",
         "lot_size": "175", "liquidity_score": "90", "greeks_score": "80", "spread_pct": "1",
         "entry_volume_signal": "Bullish Volume", "entry_volume_surge": "", "em_confidence": "50"}
    m.update(over)
    return m


def prob_msg(**over):
    m = {"symbol": "TCS", "side": "CE", "tradingsymbol": "TCS26OCT2100CE", "strike": "2100", "probability": "86",
         "decision": "HIGH_CONVICTION", "reject_reasons": "[]", "p_confluence": "90", "em_confidence": "80",
         "em_conflict": "0", "strike_score": "88", "liquidity_score": "90", "greeks_score": "80",
         "execution_quality": "92", "premium": "20", "lot_size": "175", "projected_premium_gain": "6",
         "projected_premium_change_adverse": "-2.4", "history_samples": "0", "entry_bar_ts_ms": "1000"}
    m.update(over)
    return m


class SignalOriginTest(unittest.TestCase):
    def test_earlier_of_stream_id_and_payload(self):
        self.assertEqual(signal_origin_ms(sid(NOW), {"signal_ts_ms": str(NOW - 5000)}), NOW - 5000)
        self.assertEqual(signal_origin_ms(sid(NOW - 5000), {"signal_ts_ms": str(NOW)}), NOW - 5000)
        self.assertEqual(signal_origin_ms(sid(NOW), {}), NOW)
        self.assertIsNone(signal_origin_ms("garbage", {}))


class ProbabilityRunnerTest(unittest.TestCase):
    def setUp(self):
        self.r = FakeRedis()

    def _handle(self, msg_id, fields):
        return run_probability.handle_message(self.r, msg_id, fields, {"TCS"}, {}, {}, NOW, max_signal_age_ms=MAX)

    def test_stale_message_dropped(self):
        self.assertIsNone(self._handle(sid(NOW - 3_600_000), sie_msg()))      # hour-old replay
        self.assertIsNone(self._handle(sid(NOW), sie_msg(signal_ts_ms=str(NOW - 61_000))))
        self.assertNotIn("md:probability", self.r.streams)

    def test_fresh_message_carries_signal_ts(self):
        p = self._handle(sid(NOW - 2000), sie_msg())
        self.assertEqual((p["signal_ts_ms"], p["ts_ms"]), (str(NOW - 2000), str(NOW)))
        p = self._handle(sid(NOW - 2000), sie_msg(signal_ts_ms=str(NOW - 9000)))
        self.assertEqual(p["signal_ts_ms"], str(NOW - 9000))

    def test_reentry_rescore_does_not_inherit_origin_signal_ts(self):
        """run_adaptive_tsl re-scores an old origin payload: stamped with its own time, not the origin's."""
        old = sie_msg(signal_ts_ms="1")
        inp, bucket, summary = run_probability.build_inputs(old, {}, {}, {}, {}, 30)
        res = run_probability.compute_probability(inp, run_probability.FILTERS)
        self.assertEqual(run_probability.build_payload(res, old, bucket, summary, NOW)["signal_ts_ms"], str(NOW))

    def test_volume_surge_ratio(self):
        self.assertTrue(volume_surge_flag("2.35"))
        self.assertFalse(volume_surge_flag("2.0"))       # volume_analyzer: Strong needs > 2.0
        self.assertFalse(volume_surge_flag("1.5"))
        self.assertTrue(volume_surge_flag("1"))          # legacy flag
        self.assertTrue(volume_surge_flag("true"))
        self.assertFalse(volume_surge_flag("nan"))
        self.assertFalse(volume_surge_flag(""))
        surge, _, _ = run_probability.build_inputs(sie_msg(entry_volume_surge="2.35"), {}, {}, {}, {}, 30)
        plain, _, _ = run_probability.build_inputs(sie_msg(entry_volume_surge="1.5"), {}, {}, {}, {}, 30)
        self.assertAlmostEqual(surge.intensity - plain.intensity, 10.0)


class RankingRunnerTest(unittest.TestCase):
    def setUp(self):
        self.r = FakeRedis()

    def test_stale_message_not_booked(self):
        self.assertFalse(rtr.ingest(self.r, sid(NOW - 3_600_000), prob_msg(), {"TCS"}, NOW, max_signal_age_ms=MAX))
        self.assertFalse(rtr.ingest(self.r, sid(NOW), prob_msg(signal_ts_ms=str(NOW - 61_000)), {"TCS"}, NOW,
                                    max_signal_age_ms=MAX))
        self.assertEqual(self.r.hgetall(rtr.BOOK_KEY), {})

    def test_book_carries_origin_and_expires_by_signal_time(self):
        self.assertTrue(rtr.ingest(self.r, sid(NOW - 1000), prob_msg(signal_ts_ms=str(NOW - 4000)), {"TCS"}, NOW,
                                   max_signal_age_ms=MAX))
        entry = json.loads(self.r.hget(rtr.BOOK_KEY, "TCS:CE"))
        self.assertEqual((entry["signal_ms"], entry["prob"]["signal_ts_ms"], entry["arrived_ms"]),
                         (NOW - 4000, str(NOW - 4000), NOW))
        self.assertIn("TCS:CE", rtr.load_book(self.r, NOW))
        # Arrived just now, but its signal is TTL + 1 s old -> expired (arrival time no longer counts).
        rtr.add_to_book(self.r, prob_msg(entry_bar_ts_ms="2000"), NOW, signal_ms=NOW - rtr.CANDIDATE_TTL_MS - 1000)
        self.assertEqual(rtr.load_book(self.r, NOW), {})
        # Legacy entry without any signal time is expired (fail closed).
        self.r.hset(rtr.BOOK_KEY, "X:CE", json.dumps({"prob": {}, "candidate_id": "x", "arrived_ms": NOW}))
        self.assertEqual(rtr.load_book(self.r, NOW), {})


class ICARERunnerTest(unittest.TestCase):
    def setUp(self):
        self.r = FakeRedis()

    def _handle(self, msg_id, fields, stream="md:probability"):
        return run_icare.handle_message(self.r, stream, msg_id, fields, {"TCS"}, {"TCS": "IT"}, NOW,
                                        max_signal_age_ms=MAX)

    def test_stale_signal_dropped(self):
        self.assertIsNone(self._handle(sid(NOW - 3_600_000), prob_msg()))
        self.assertIsNone(self._handle(sid(NOW), prob_msg(signal_ts_ms=str(NOW - 61_000))))
        self.assertIsNone(self._handle(sid(NOW - 61_000), prob_msg(reentry="1"), stream=run_icare.REENTRY_STREAM))
        self.assertNotIn("md:icare", self.r.streams)
        self.assertEqual([k for k in self.r.kv if k.startswith("md:icare")], [])

    def test_signal_ts_contract(self):
        """md:icare signal_ts_ms = EARLIER of consumed stream-id ms and upstream signal_ts_ms; ts_ms = publish."""
        p = self._handle(sid(NOW - 3000), prob_msg(signal_ts_ms=str(NOW - 7000)))
        self.assertEqual((p["signal_ts_ms"], p["ts_ms"]), (str(NOW - 7000), str(NOW)))
        p = self._handle(sid(NOW - 3000), prob_msg())
        self.assertEqual(p["signal_ts_ms"], str(NOW - 3000))
        ranked = prob_msg(rank_emit="1", rank_decision="TAKE_TRADE", signal_ts_ms=str(NOW - 8000))
        p = self._handle(sid(NOW - 500), ranked, stream=run_icare.OUT_RANKING_STREAM)
        self.assertEqual(p["signal_ts_ms"], str(NOW - 8000))
        for k in ("gross_ev", "charges", "charges_per_lot", "expected_value", "lots_by_sector", "daily_loss_room"):
            self.assertIn(k, p)

    def test_origin_doc_drops_signal_ts(self):
        p = self._handle(sid(NOW - 3000), prob_msg(signal_ts_ms=str(NOW - 7000)))
        self.assertEqual(p["status"], "APPROVED", p["reasons"])
        origin = json.loads(self.r.get(f"{run_icare.ORIGIN_PREFIX}TCS26OCT2100CE"))
        self.assertNotIn("signal_ts_ms", origin)

    def test_kill_switch(self):
        self.r.set("md:control:kill_switch", "1")
        p = self._handle(sid(NOW - 1000), prob_msg())
        self.assertEqual(p["status"], "REJECTED")
        self.assertIn("kill_switch_active", json.loads(p["reasons"]))

    def test_reentry_max_lots_zero(self):
        inp = run_icare.build_inputs(prob_msg(reentry="1", max_lots="0"), {}, {})
        self.assertEqual(inp.reentry_max_lots, 0.0)
        self.assertIsNone(run_icare.build_inputs(prob_msg(reentry="1", max_lots=""), {}, {}).reentry_max_lots)


class AccountFallbackTest(unittest.TestCase):
    """md:account:latest missing / stale: realized PnL must not be dropped (TOTAL_CAPITAL 1L default)."""

    POS = [{"symbol": "INFY", "qty": 100, "entry_premium": 10, "sl_premium": 8, "last_premium": 10}]

    def test_journal_realized_loss_kept(self):
        pf, flags = run_icare.portfolio_state({}, self.POS, {}, NOW, journal_daily={"realized_pnl": -3000},
                                              exec_mode="paper")
        self.assertIn("ACCOUNT_FALLBACK_PAPER", flags)
        self.assertIn("REALIZED_PNL_FROM_JOURNAL", flags)
        self.assertEqual((pf.day_pnl, pf.total_capital), (-3000.0, 97_000.0))
        # 2% of 97,000 = 1,940 < 3,000 lost -> daily-loss gate holds (was bypassed with day_pnl 0).
        res = run_icare.evaluate(run_icare.build_inputs(prob_msg(), {}, {}), pf, run_icare.CFG)
        self.assertIn("daily_loss_limit_hit", res.reasons)

    def test_worse_of_journal_and_same_day_stale_snapshot(self):
        stale = {"ts_ms": NOW - 10 * 60_000, "realized_pnl": -2500, "total_capital": 1}
        if dt.datetime.fromtimestamp(stale["ts_ms"] / 1000, IST).date() != dt.datetime.fromtimestamp(NOW / 1000, IST).date():
            self.skipTest("crosses IST midnight")
        pf, flags = run_icare.portfolio_state(stale, [], {}, NOW, journal_daily={"realized_pnl": -1000})
        self.assertEqual(pf.day_pnl, -2500.0)
        self.assertIn("REALIZED_PNL_FROM_STALE_ACCOUNT", flags)

    def test_live_unknown_fails_closed(self):
        pf, flags = run_icare.portfolio_state({}, [], {}, NOW, journal_daily=None, exec_mode="live")
        self.assertFalse(pf.day_pnl_known)
        self.assertIn("DAILY_PNL_UNKNOWN", flags)
        res = run_icare.evaluate(run_icare.build_inputs(prob_msg(), {}, {}), pf, run_icare.CFG)
        self.assertIn("daily_loss_unknown", res.reasons)

    def test_paper_no_journal_key_is_zero(self):
        pf, flags = run_icare.portfolio_state({}, [], {}, NOW, journal_daily=None, exec_mode="shadow")
        self.assertTrue(pf.day_pnl_known)
        self.assertEqual(pf.day_pnl, 0.0)

    def test_load_journal_daily_ist_key(self):
        r = FakeRedis()
        today = dt.datetime.fromtimestamp(NOW / 1000, IST).date().isoformat()
        r.set(f"md:journal:daily:{today}", json.dumps({"realized_pnl": -1234.5, "trades": 2, "wins": 0}))
        self.assertEqual(run_icare.load_journal_daily(r, NOW)["realized_pnl"], -1234.5)
        pf, _, _ = run_icare.load_portfolio(r, {}, NOW)
        self.assertEqual(pf.day_pnl, -1234.5)


if __name__ == "__main__":
    unittest.main()

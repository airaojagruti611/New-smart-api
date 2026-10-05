"""
QA fixes for the microstructure layer (optexit, imbalance, strike flow,
order flow / smart money / stock entry-exit, bid-ask, liquidity / OI books,
composite override). Pure functions + a tiny in-memory fake Redis.
"""

from __future__ import annotations

import datetime as dt
import fnmatch
import json
import unittest

import run_bidask_analyzer as rba
import run_composite as rc
import run_oi_analysis as roi
import run_option_liquidity_exit as role
from app.bidask_imbalance import DepthLevel as IDL
from app.bidask_imbalance import ImbalanceDetector, weighted_filtered_imbalance
from app.bidask_analyzer import BidAskAnalyzer
from app.option_liquidity_exit import OptionLiquidityExitDetector
from app.order_flow import (
    CumVolTradeQty,
    DepthLevel as ODL,
    OrderFlowDetector,
    newest_data_ts,
    prune_stale_book,
    tick_ts_ms,
)
from app.smart_money import TimeSalesCluster
from app.strike_flow import StrikeCandidate, select_strike

NOW = 1_790_000_000_000  # fixed epoch ms


class FakeRedis:
    def __init__(self):
        self.kv, self.streams = {}, {}

    def get(self, k):
        return self.kv.get(k)

    def set(self, k, v, ex=None, nx=False):
        if nx and k in self.kv:
            return None
        self.kv[k] = v
        return True

    def exists(self, k):
        return int(k in self.kv)

    def scan_iter(self, match="*", count=None):
        return [k for k in list(self.kv) if fnmatch.fnmatch(k, match)]

    def xadd(self, stream, payload, maxlen=None, approximate=True):
        self.streams.setdefault(stream, []).append(dict(payload))
        return f"{NOW}-0"


# ── #1 option liquidity exit: one-sided book ─────────────────────────────

class TestOptexitBidsVanished(unittest.TestCase):
    def _tick(self, bid, ask, bid_sz="100", ask_sz="200", ts=NOW):
        return {"bid": bid, "ask": ask, "bid_sz": bid_sz, "ask_sz": ask_sz, "ts_exch": str(ts), "ts_recv": str(ts)}

    def test_bid_zero_with_ask_is_exit_now_stage5(self):
        det = OptionLiquidityExitDetector()
        res0 = role.evaluate_tick(det, self._tick("10.0", "10.5"), None, NOW)
        self.assertEqual(res0.exit_status, "NONE")
        res = role.evaluate_tick(det, self._tick("0", "11.0", bid_sz="0"), None, NOW + 500)
        self.assertIsNotNone(res)
        self.assertEqual(res.exit_status, "EXIT_NOW")
        self.assertTrue(res.stage5_one_sided)
        self.assertTrue(res.bids_vanished)
        self.assertEqual(res.bid_drop_pct, 100.0)
        p = role._to_payload("X26OCT100CE", "X", res, NOW + 500)
        self.assertEqual(p["bids_vanished"], "1")
        self.assertEqual(p["stage5_one_sided"], "1")
        self.assertEqual(p["ts_ms"], str(NOW + 500))

    def test_missing_bid_with_ask_is_exit_now(self):
        res = role.evaluate_tick(OptionLiquidityExitDetector(), self._tick("", "4.0", bid_sz=""), None, NOW)
        self.assertEqual(res.exit_status, "EXIT_NOW")

    def test_fully_empty_book_skipped(self):
        self.assertIsNone(role.evaluate_tick(OptionLiquidityExitDetector(), self._tick("0", "0"), None, NOW))
        self.assertIsNone(role.evaluate_tick(OptionLiquidityExitDetector(), self._tick("", ""), None, NOW))

    def test_latest_ttl_short(self):
        self.assertLessEqual(role.LATEST_TTL_SEC, 300)


# ── #2 imbalance spoof filter never flips sign ──────────────────────────

class TestImbalanceSignGuard(unittest.TestCase):
    def test_repriced_ask_side_does_not_turn_bullish(self):
        det = ImbalanceDetector()
        bids = [IDL(99.0, 100), IDL(98.9, 100), IDL(98.8, 100), IDL(98.7, 100), IDL(98.6, 100)]  # 500
        res = None
        for i in range(8):  # ask ladder steps down every tick -> never confirmed
            top = 101.0 - 0.1 * i
            asks = [IDL(round(top + 0.05 * k, 2), 1000) for k in range(5)]  # 5000
            res = det.analyze(bids, asks)
        self.assertEqual(res.raw, -0.8182)
        self.assertLess(res.weighted_filtered, 0)        # was +1.0 before the fix
        self.assertNotEqual(res.signal, "BULLISH")
        self.assertEqual(res.signal, "BEARISH")

    def test_filter_cannot_reverse_raw_sign(self):
        bids = [IDL(99.0, 5000), IDL(98.9, 10), IDL(98.8, 10)]
        asks = [IDL(101.0, 300), IDL(101.1, 300), IDL(101.2, 300)]
        # Only the asks are confirmed, the big bid is not: per-level filter
        # would read pure ask -> -1. raw is +0.69 -> result clamped to 0.
        out = weighted_filtered_imbalance(bids, asks, {98.9}, {101.0, 101.1, 101.2}, raw=0.6927)
        self.assertEqual(out, 0.0)

    def test_confirmed_levels_weighted_per_level(self):
        bids = [IDL(99.0, 200), IDL(98.9, 200)]
        asks = [IDL(101.0, 100), IDL(101.1, 100)]
        out = weighted_filtered_imbalance(bids, asks, {99.0, 98.9}, {101.0, 101.1}, raw=0.3333)
        # (200*5+200*3 - 100*5-100*3) / (1600+800)
        self.assertAlmostEqual(out, 0.3333, places=4)


# ── #3 strike flow sweep confirmation ───────────────────────────────────

def _cand(tsym, cp, sweep, conf=True, money="OTM1", spread_ratio=1.0):
    return StrikeCandidate(
        tradingsymbol=tsym, strike=100.0, cp=cp, moneyness=money, vol=10, oi=100,
        vol_oi_ratio=0.1, vol_oi_class="NORMAL", sweep_signal=sweep, sweep_confirmed=conf,
        spread_pct=1.0, spread_ratio=spread_ratio, one_sided_refresh=False, bid=1.0, ask=1.1,
    )


class TestStrikeSweep(unittest.TestCase):
    def test_up_bias_sweep_buy_on_ce(self):
        sel = select_strike([_cand("A100CE", "CE", "SWEEP_BUY"), _cand("A100PE", "PE", "SWEEP_BUY")], "UP")
        self.assertEqual(sel.reason, "confirmed_sweep")
        self.assertEqual(sel.chosen.tradingsymbol, "A100CE")

    def test_down_bias_needs_buy_sweep_on_pe(self):
        sel = select_strike([_cand("A100PE", "PE", "SWEEP_BUY")], "DOWN")
        self.assertEqual(sel.reason, "confirmed_sweep")
        self.assertEqual(sel.chosen.tradingsymbol, "A100PE")

    def test_down_bias_sell_sweep_on_pe_does_not_confirm(self):
        sel = select_strike([_cand("A100PE", "PE", "SWEEP_SELL")], "DOWN")
        self.assertNotEqual(sel.reason, "confirmed_sweep")

    def test_unconfirmed_sweep_ignored(self):
        sel = select_strike([_cand("A100CE", "CE", "SWEEP_BUY", conf=False)], "UP")
        self.assertNotEqual(sel.reason, "confirmed_sweep")


# ── #4 traded qty from cumulative vol (no ltq double counting) ──────────

class TestCumVolTradeQty(unittest.TestCase):
    def test_one_trade_plus_quote_updates(self):
        tq = CumVolTradeQty()
        vols = ["50000", "51000", "51000", "51000", "51000", "51000"]  # 1 trade of 1000 + 4 quote ticks
        qtys = [tq.update("T", v) for v in vols]
        self.assertEqual(qtys, [0.0, 1000.0, 0.0, 0.0, 0.0, 0.0])

    def test_reset_and_bad_values(self):
        tq = CumVolTradeQty()
        self.assertEqual(tq.update("T", "100"), 0.0)
        self.assertEqual(tq.update("T", ""), 0.0)
        self.assertEqual(tq.update("T", "150"), 50.0)
        self.assertEqual(tq.update("T", "20"), 0.0)   # reset -> reseed
        self.assertEqual(tq.update("T", "35"), 15.0)

    def test_buy_pressure_counts_trade_once(self):
        det = OrderFlowDetector(direction_window=50)
        tq = CumVolTradeQty()
        bids = [ODL(99.9, 100)]
        asks = [ODL(100.0, 100)]
        res = None
        for v in ["50000", "51000", "51000", "51000", "51000", "51000"]:
            # ltp=100.0 (hit the ask) repeated; ltq=1000 would have been x5
            res = det.analyze(ts_ms=NOW, trade_price=100.0, trade_qty=tq.update("T", v),
                              bid=99.9, ask=100.0, bid_levels=bids, ask_levels=asks)
        self.assertEqual(res.direction.buy_pressure, 1000.0)

    def test_tick_ts_prefers_exchange(self):
        self.assertEqual(tick_ts_ms({"ts_exch": str(NOW), "ts_recv": str(NOW + 900)}), NOW)
        self.assertEqual(tick_ts_ms({"ts_exch": "", "ts_recv": str(NOW + 900)}), NOW + 900)
        self.assertEqual(tick_ts_ms({"ts_exch": "1790000000"}), 1_790_000_000_000)
        self.assertEqual(tick_ts_ms({}, 7), 7)

    def test_cluster_uses_per_tick_time(self):
        # Two 1000-lot prints at the same price 5s apart: with one batch
        # now_ms they'd look simultaneous and cluster; per-tick ts must not.
        cl = TimeSalesCluster()
        self.assertIsNone(cl.update(NOW, 100.0, 1000))
        self.assertIsNone(cl.update(NOW + 5000, 100.0, 1000))
        cl2 = TimeSalesCluster()
        cl2.update(NOW, 100.0, 1000)
        self.assertIsNotNone(cl2.update(NOW + 100, 100.0, 1000))


# ── #5 bid-ask latest: newest tick, source ts, crossed book ─────────────

class TestBidAskPublish(unittest.TestCase):
    def test_throttle_keeps_newest(self):
        th = rba.LatestThrottle(1.0)
        th.offer("K", "first")
        th.offer("K", "newest")
        self.assertEqual(th.due(100.0), ["newest"])
        th.offer("K", "a")
        self.assertEqual(th.due(100.5), [])           # throttled, but kept pending
        th.offer("K", "b")
        self.assertEqual(th.due(101.0), ["b"])        # newest published later

    def test_payload_ts_is_tick_time_and_crossed_not_zero_spread(self):
        r = FakeRedis()
        th = rba.LatestThrottle(1.0)
        res = BidAskAnalyzer(is_option=True).analyze(10.0, 10.2, [100], [100])
        tick = {"ts_exch": str(NOW - 4000), "ltp": "10.1"}
        th.offer("OPT", ("ok", "OPT", "opt", rba._to_payload("OPT", "opt", res, tick_ts_ms(tick), tick)))
        rba._publish_due(r, th, 1000.0)
        doc = json.loads(r.kv[f"{rba.LATEST_KEY_PREFIX}OPT"])
        self.assertEqual(doc["ts_ms"], str(NOW - 4000))
        self.assertEqual(doc["crossed"], "0")

        cp = rba._crossed_payload("OPT", "opt", 10.5, 10.3, NOW, tick)
        th.offer("OPT", ("crossed", "OPT", "opt", cp))
        rba._publish_due(r, th, 1002.0)
        self.assertEqual(r.streams[rba.OUT_STREAM][-1]["signal"], "CROSSED")
        self.assertEqual(r.streams[rba.OUT_STREAM][-1]["spread_pct"], "")
        # latest key still the last good (older) quote -> ages out downstream
        self.assertEqual(json.loads(r.kv[f"{rba.LATEST_KEY_PREFIX}OPT"])["ts_ms"], str(NOW - 4000))


# ── #6 stale books pruned + old-session ticks ───────────────────────────

class _St:
    def __init__(self, ts):
        self.data_ts_ms = ts


class TestBookPrune(unittest.TestCase):
    def test_prune_and_newest(self):
        book = {"A": _St(NOW - 10_000), "B": _St(NOW - 130_000), "C": _St(0)}
        removed = prune_stale_book(book, NOW, 120_000)
        self.assertEqual(sorted(removed), ["B", "C"])
        self.assertEqual(list(book), ["A"])
        self.assertEqual(newest_data_ts(book.values()), NOW - 10_000)

    def test_old_session_tick_rejected(self):
        today = roi._ist_date_of_ms(NOW)
        yesterday_ms = NOW - 24 * 3600 * 1000
        self.assertTrue(roi.tick_is_current_session(NOW, today))
        self.assertFalse(roi.tick_is_current_session(yesterday_ms, today))
        self.assertFalse(roi.tick_is_current_session(None, today))


# ── #7 composite FORCE_EXIT only for held, fresh, same-side contracts ───

class TestCompositeOverride(unittest.TestCase):
    def setUp(self):
        self.r = FakeRedis()

    def _optexit(self, tsym, status, ts):
        self.r.set(f"{rc.OPTEXIT_LATEST_PREFIX}{tsym}", json.dumps({"ts_ms": str(ts), "exit_status": status}))

    def _hold(self, tsym, sym="TCS", side="CE"):
        self.r.set(f"{rc.POSITION_OPEN_PREFIX}{tsym}", json.dumps({"symbol": sym, "tradingsymbol": tsym, "side": side}))

    def _eval(self, sym="TCS"):
        pos = rc.held_positions_by_underlying(self.r)
        return rc.liquidity_override(self.r, pos.get(sym, []), NOW, 60_000)

    def test_not_held_no_force_exit(self):
        self._optexit("TCS26OCT4000CE", "EXIT_NOW", NOW)
        self.assertFalse(self._eval()[0])

    def test_held_fresh_force_exit(self):
        self._hold("TCS26OCT4000CE")
        self._optexit("TCS26OCT4000CE", "EXIT_NOW", NOW - 5_000)
        ok, reason, held = self._eval()
        self.assertTrue(ok)
        self.assertEqual(reason, "TCS26OCT4000CE:EXIT_NOW")
        self.assertEqual(held["side"], "CE")

    def test_held_but_stale_doc_ignored(self):
        self._hold("TCS26OCT4000CE")
        self._optexit("TCS26OCT4000CE", "EXIT_NOW", NOW - 61_000)
        self.assertFalse(self._eval()[0])

    def test_other_contract_on_underlying_ignored(self):
        self._hold("TCS26OCT4000CE")
        self._optexit("TCS26OCT4000PE", "EXIT_NOW", NOW)   # not held
        self.assertFalse(self._eval()[0])

    def test_greeks_distribution_fresh_held(self):
        self._hold("TCS26OCT3900PE", side="PE")
        self.r.set(f"{rc.GREEKS_PHASE_LATEST_PREFIX}TCS26OCT3900PE", json.dumps({"ts_ms": str(NOW - 1000), "phase": "DISTRIBUTION"}))
        ok, reason, _ = self._eval()
        self.assertTrue(ok)
        self.assertEqual(reason, "TCS26OCT3900PE:GREEKS_DISTRIBUTION")


if __name__ == "__main__":
    unittest.main()

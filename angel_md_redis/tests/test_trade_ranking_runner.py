"""
Module 13 runner + integration (DECISION.md §6 P18/P19/P22) through the real
runner / ICARE / journal functions on an in-memory Redis.
"""

from __future__ import annotations

import fnmatch
import json
import time
import unittest

import run_trade_ranking as rtr
from app.icare import PortfolioState, evaluate
from app.trade_journal import open_position
from app.trade_ranking import TradeRankingEngine
from run_icare import CFG as ICARE_CFG
from run_icare import accept_message, build_inputs
from run_trade_journal import with_ranking


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


SECTORS = {"TCS": "IT", "INFY": "IT", "RELIANCE": "ENERGY"}


def prob_msg(sym, side, tsym, **over):
    m = {
        "ts_ms": "1", "symbol": sym, "side": side, "tradingsymbol": tsym, "strike": "2100", "expiry": "2026-10-27",
        "probability": "86", "decision": "HIGH_CONVICTION", "grade": "A", "reject_reasons": "[]",
        "p_oi": "85", "p_confluence": "90", "market_phase": "STRONG_TREND", "execution_quality": "92",
        "greeks_score": "80", "strike_score": "88", "liquidity_score": "90", "liquidity_band": "GREEN",
        "spread_pct": "1", "premium": "20", "lot_size": "175", "projected_premium_gain": "6",
        "projected_premium_gain_iv_down": "5", "projected_premium_change_adverse": "-2.4",
        "em_direction": "BULLISH" if side == "CE" else "BEARISH", "em_confidence": "80", "em_conflict": "0",
        "hold_minutes": "60", "entry_bar_ts_ms": "1000", "history_samples": "0",
        "top": json.dumps([{"tradingsymbol": tsym, "components": {"em_fit": 90}}]),
    }
    m.update(over)
    return m


def seed_live(r, now_ms, sym, side, tsym, spread=1.0):
    bull = side == "CE"
    ts = str(now_ms)
    r.set(f"md:indicator:score:latest:{sym}", json.dumps({"ts_ms": ts, "score": "1.5" if bull else "-1.5"}))
    r.set(f"md:imbalance:latest:{tsym}", json.dumps({"ts_ms": ts, "signal": "BULLISH"}))
    r.set(f"md:bidask:latest:{tsym}", json.dumps({"ts_ms": ts, "bid": "19.9", "ask": "20.1", "mid": "20", "spread_pct": str(spread)}))
    r.set(f"md:liquidity:score:latest:{tsym}", json.dumps({"ts_ms": ts, "liquidity_score": "90", "liquidity_band": "GREEN",
                                                         "size_unit": "lots", "final_entry_size": "3"}))
    r.set(f"md:oi:underlying:latest:{sym}", json.dumps({"ts_ms": ts, "positioning": "BULLISH_POSITIONING" if bull else "BEARISH_POSITIONING"}))
    r.set(f"md:expected_move:latest:{sym}", json.dumps({"ts_ms": ts, "direction": "BULLISH" if bull else "BEARISH",
                                                       "direction_score": "0.6" if bull else "-0.6", "confidence": "80"}))
    r.set(f"md:greeks:phase:latest:{tsym}", json.dumps({"ts_ms": ts, "phase": "MARKUP", "gamma": "0.002", "iv_pct": "1"}))
    r.set(f"md:htf:trend:latest:{sym}", json.dumps({"ts_ms": ts, "bias": "CALL" if bull else "PUT"}))
    r.set(f"md:supertrend:bias:latest:{sym}", json.dumps({"ts_ms": ts, "bias": "CALL" if bull else "PUT",
                                                         "bullish": "3" if bull else "0", "bearish": "0" if bull else "3",
                                                         "st_1m": "x", "st_5m": "x", "st_10m": "x"}))
    vol = json.loads(r.get("md:volume:latest") or "{}")
    vol[sym] = {"ts_ms": ts, "signal": "Bullish Volume" if bull else "Bearish Volume"}
    r.set("md:volume:latest", json.dumps(vol))
    r.set("md:regime:latest", json.dumps({"ts_ms": ts, "regime": "BULLISH"}))


class RankingRunnerTest(unittest.TestCase):
    def setUp(self):
        rtr._last_cycle.update(sig=None, ts=0.0)
        self.r = FakeRedis()
        self.engine = TradeRankingEngine(rtr.CFG, ICARE_CFG)
        self.now = int(time.time() * 1000)
        for sym, side, tsym, spread in (("TCS", "CE", "TCS26OCT2100CE", 1.0), ("INFY", "CE", "INFY26OCT1500CE", 1.0),
                                        ("RELIANCE", "PE", "RELIANCE26OCT2800PE", 5.0)):
            seed_live(self.r, self.now, sym, side, tsym, spread)
        self.msgs = {
            "TCS": prob_msg("TCS", "CE", "TCS26OCT2100CE"),
            "INFY": prob_msg("INFY", "CE", "INFY26OCT1500CE", probability="78", decision="TRADE"),
            "RELIANCE": prob_msg("RELIANCE", "PE", "RELIANCE26OCT2800PE"),
        }

    def _add_all(self, age_ms):
        for m in self.msgs.values():
            rtr.add_to_book(self.r, m, self.now - age_ms)

    def _latest(self, field):
        return json.loads(self.r.get(f"md:ranking:latest:{field}"))

    def test_batch_window_then_single_emission(self):
        self._add_all(age_ms=0)
        rtr.run_cycle(self.r, self.engine, SECTORS)
        self.assertFalse([m for m in self.r.streams["md:ranking"] if m["rank_emit"] == "1"])   # still in window

        self.r.hashes.clear()
        self._add_all(age_ms=5000)
        rtr.run_cycle(self.r, self.engine, SECTORS)
        rtr.run_cycle(self.r, self.engine, SECTORS)                         # second cycle must not re-emit
        emitted = [m for m in self.r.streams["md:ranking"] if m["rank_emit"] == "1"]
        self.assertEqual([m["tradingsymbol"] for m in emitted], ["TCS26OCT2100CE"])

        tcs, infy, rel = self._latest("TCS:CE"), self._latest("INFY:CE"), self._latest("RELIANCE:PE")
        self.assertEqual((tcs["rank_decision"], tcs["rank"]), ("TAKE_TRADE", "1"))
        self.assertEqual(infy["rank_decision"], "WATCH")
        self.assertIn("CORRELATED_SECTOR", json.loads(infy["rank_reject_reasons"]))
        self.assertEqual(rel["rank_decision"], "REJECT")
        self.assertIn("SPREAD_ABOVE_MAX", json.loads(rel["rank_reject_reasons"]))
        brief = json.loads(tcs["ranking_json"])
        for k in ("trade_score", "probability_score", "trailing_stop_pct", "initial_stop_loss_pct", "confidence"):
            self.assertIn(k, brief)
        self.assertGreater(self.r.zsets["md:ranking:rank"]["TCS:CE"], self.r.zsets["md:ranking:rank"]["INFY:CE"])
        cyc = json.loads(self.r.get("md:ranking:cycle:latest"))
        self.assertEqual((cyc["outcome"], cyc["taken"], cyc["scanned"]), ("TRADE", 1, 3))

        # ICARE (active mode) sizes the emission; probability decision survives the forwarding.
        msg = emitted[0]
        self.assertTrue(accept_message("md:ranking", msg))
        self.assertFalse(accept_message("md:ranking", dict(msg, rank_emit="0")))
        inp = build_inputs(msg, {}, SECTORS)
        self.assertEqual(inp.probability_decision, "HIGH_CONVICTION")
        self.assertFalse(inp.rank_conditional)
        pf = PortfolioState(total_capital=100_000, available_margin=100_000, margin_utilization_pct=0,
                            day_pnl=0, open_positions=0, open_risk=0)
        self.assertEqual(evaluate(inp, pf, ICARE_CFG).status, "APPROVED")

        # Journal (shadow mode) attaches the ranking verdict for the same contract.
        icare_msg = {"symbol": "TCS", "side": "CE", "tradingsymbol": "TCS26OCT2100CE", "recommended_lots": "1",
                     "lot_size": "175", "premium": "20", "stop_loss_premium": "17", "target_premium": "26"}
        pos = open_position(with_ranking(self.r, icare_msg), 20.1, 0)
        self.assertEqual((pos.context["rank_decision"], pos.context["rank"]), ("TAKE_TRADE", "1"))
        other = with_ranking(self.r, dict(icare_msg, tradingsymbol="TCS26OCT2200CE"))
        self.assertNotIn("rank_decision", other)

    def test_no_trade_cycle_and_kill_switch(self):
        self.r.set("md:control:kill_switch", "1")
        self._add_all(age_ms=5000)
        rtr.run_cycle(self.r, self.engine, SECTORS)
        cyc = json.loads(self.r.get("md:ranking:cycle:latest"))
        self.assertEqual((cyc["outcome"], cyc["taken"]), ("NO_TRADE", 0))
        self.assertIn("KILL_SWITCH_ACTIVE", json.loads(self._latest("TCS:CE")["rank_reject_reasons"]))

    def test_stale_quote_is_data_insufficient(self):
        self.r.set("md:bidask:latest:TCS26OCT2100CE", json.dumps({"ts_ms": str(self.now - 60_000), "spread_pct": "1"}))
        self._add_all(age_ms=5000)
        rtr.run_cycle(self.r, self.engine, SECTORS)
        self.assertEqual(self._latest("TCS:CE")["rank_decision"], "DATA_INSUFFICIENT")
        self.assertNotIn("TCS:CE", self.r.zsets["md:ranking:rank"])

    def test_empty_book_still_publishes_heartbeat_cycle(self):
        rtr.run_cycle(self.r, self.engine, SECTORS)
        cyc = json.loads(self.r.get("md:ranking:cycle:latest"))
        self.assertEqual((cyc["outcome"], cyc["scanned"], cyc["ranked"]), ("NO_TRADE", 0, []))

    def test_expired_candidates_leave_the_book(self):
        self._add_all(age_ms=rtr.CANDIDATE_TTL_MS + 1000)
        rtr.run_cycle(self.r, self.engine, SECTORS)
        self.assertEqual(self.r.hgetall("md:ranking:book"), {})

    def test_probability_hard_reject_skipped(self):
        self.assertTrue(rtr.is_hard_rejected({"reject_reasons": '["sideways_regime"]'}))
        self.assertFalse(rtr.is_hard_rejected({"reject_reasons": "[]"}))

    def test_conditional_cap_in_icare(self):
        inp = build_inputs(dict(self.msgs["TCS"], rank_confidence="CONDITIONAL"), {}, SECTORS)
        pf = PortfolioState(total_capital=100_000, available_margin=100_000, margin_utilization_pct=0,
                            day_pnl=0, open_positions=0, open_risk=0)
        res = evaluate(inp, pf, ICARE_CFG)
        self.assertLessEqual(res.allocation_pct, 3.0)
        if res.risk_class not in ("C", "REJECT"):
            self.assertIn("CONDITIONAL_CAP", res.flags)


if __name__ == "__main__":
    unittest.main()

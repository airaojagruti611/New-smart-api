"""
Module 14 runner + integration (DECISION.md §7 P27/P28/P30) through the real
runner / ICARE exposure / journal functions on an in-memory Redis.
"""

from __future__ import annotations

import datetime as dt
import fnmatch
import json
import unittest
from unittest import mock

import run_icare
import run_order_executor as roe
import run_trade_journal as rtj
from app.option_pricing import IST
from app.order_executor import state as S
from app.order_executor.broker import PaperBroker
from app.trade_journal import PaperPosition, apply_charges, close_position, open_position

T0 = int(dt.datetime(2026, 10, 5, 10, 15, tzinfo=IST).timestamp() * 1000)
TSYM = "SBIN26OCT900CE"
LOT = 750.0


class FakeRedis:
    def __init__(self):
        self.kv, self.hashes, self.streams = {}, {}, {}

    def get(self, k):
        return self.kv.get(k)

    def set(self, k, v, ex=None, nx=False):
        if nx and k in self.kv:
            return None
        self.kv[k] = v
        return True

    def delete(self, k):
        self.kv.pop(k, None)

    def exists(self, k):
        return int(k in self.kv)

    def incr(self, k):
        self.kv[k] = int(self.kv.get(k, 0)) + 1
        return self.kv[k]

    def expire(self, k, sec):
        return True

    def scan_iter(self, match="*", count=None):
        return [k for k in list(self.kv) if fnmatch.fnmatch(k, match)]

    def hset(self, k, f, v):
        self.hashes.setdefault(k, {})[f] = v

    def hgetall(self, k):
        return dict(self.hashes.get(k, {}))

    def hdel(self, k, *fs):
        for f in fs:
            self.hashes.get(k, {}).pop(f, None)

    def xadd(self, k, fields, maxlen=None, approximate=True):
        self.streams.setdefault(k, []).append(dict(fields))


def icare_msg(now_ms=T0, **over):
    d = {"status": "APPROVED", "ts_ms": str(now_ms), "symbol": "SBIN", "tradingsymbol": TSYM, "side": "CE",
         "strike": "900", "recommended_lots": "4", "lot_size": str(LOT), "premium": "100.5",
         "stop_loss_premium": "80", "target_premium": "130", "max_risk_allowed": "100000",
         "max_capital": "42000", "expected_value": "2000", "hold_minutes": "30", "spot": "905"}
    d.update(over)
    return d


def book(r, ts, bid, ask, levels):
    r.set(f"md:bidask:latest:{TSYM}", json.dumps({
        "ts_ms": str(ts), "bid": str(bid), "ask": str(ask),
        "ask_depth5": ",".join(str(q) for _p, q in levels), "ask_depth5_px": ",".join(str(p) for p, _q in levels)}))


def account(r, ts, margin=1_000_000):
    r.set("md:account:latest", json.dumps({"ts_ms": ts, "available_margin": margin, "total_capital": margin}))


def runner(r, mode="paper"):
    return roe.ExecutorRunner(r, PaperBroker(latency_ms=0), {TSYM: {"tick": 0.05, "kind": "OPTSTK",
                                                                   "freeze_qty": 15001, "expiry": "2026-10-27"}},
                              mode=mode)


def run_until_done(rn, r, t_start, quotes, max_ms=15_000):
    """quotes: list of (from_ms_offset, bid, ask, levels)."""
    t = t_start
    while t <= t_start + max_ms and rn.states:
        for off, bid, ask, lv in quotes:
            if t - t_start >= off:
                cur = (bid, ask, lv)
        book(r, t, *cur)
        account(r, t)
        rn.tick(t)
        t += 500
    return t


class RunnerTest(unittest.TestCase):
    def test_paper_fill_reaches_journal_at_actual_price(self):
        r = FakeRedis()
        book(r, T0, 100.0, 101.0, [(101.0, 3 * LOT)])
        account(r, T0)
        rn = runner(r)
        tid = rn.on_icare("1-0", icare_msg(), T0)
        self.assertEqual(tid, "TRD_20261005_001")
        self.assertEqual(r.hgetall("md:exec:active"), {tid: "SBIN"})
        run_until_done(rn, r, T0 + 500, [(0, 100.60, 100.80, [(100.80, 3 * LOT)]),
                                         (6000, 100.90, 101.10, [(101.10, LOT)])])
        rep = json.loads(r.get(f"md:exec:latest:{tid}"))
        self.assertEqual((rep["execution_status"], rep["filled_lots"]), (S.FILLED, 4))
        self.assertAlmostEqual(rep["average_fill_price"], 100.875)
        self.assertEqual(r.hgetall("md:exec:active"), {})
        events = [e["event"] for e in r.streams["md:exec"]]
        self.assertEqual(events[:3], ["COMMAND", "VALIDATED", "ORDER_PLACED"])
        self.assertEqual(events[-2:], ["FINAL", "REPORT"])

        fill = r.streams["md:exec:fill"][0]
        self.assertEqual((fill["recommended_lots"], fill["icare_recommended_lots"]), ("4", "4"))
        with mock.patch.object(rtj, "is_eod", return_value=False):
            rtj.handle_approval(r, fill, T0 + 9000)
        pos = json.loads(r.get(f"md:position:open:{TSYM}"))
        self.assertAlmostEqual(pos["entry_premium"], 100.875)
        self.assertEqual(pos["lots"], 4)
        self.assertEqual(pos["context"]["exec_execution_status"], S.FILLED)
        self.assertEqual(pos["sl_premium"], 80.0)           # ICARE levels kept as prices (E16)

        # offline execution report (learn_weights.py) reads the REPORT events
        import pandas as pd
        from learn_weights import execution_report
        out = execution_report(pd.DataFrame(r.streams["md:exec"]))
        self.assertEqual(int(out["by status"].loc[S.FILLED, "trades"]), 1)
        self.assertAlmostEqual(float(out["by symbol"].loc["SBIN", "fill_ratio"]), 1.0)
        self.assertIn("by spread at entry", out)

    def test_partial_fill_opens_partial_position(self):
        r = FakeRedis()
        book(r, T0, 100.70, 100.80, [(100.80, 2 * LOT)])
        account(r, T0)
        rn = runner(r)
        tid = rn.on_icare("1-0", icare_msg(), T0)
        run_until_done(rn, r, T0 + 500, [(0, 100.70, 100.80, [(100.80, 2 * LOT)]), (500, 101.40, 101.90, [])])
        rep = json.loads(r.get(f"md:exec:latest:{tid}"))
        self.assertEqual((rep["execution_status"], rep["filled_lots"]), (S.PARTIAL_FILL_TIMEOUT, 2))
        self.assertEqual(r.streams["md:exec:fill"][0]["recommended_lots"], "2")
        self.assertIn(tid, r.hgetall("md:exec:missed"))      # opportunity cost of the 2 missing lots

    def test_duplicate_message_executes_once(self):
        r = FakeRedis()
        book(r, T0, 100.70, 100.80, [(100.80, 4 * LOT)])
        account(r, T0)
        rn = runner(r)
        self.assertIsNotNone(rn.on_icare("1-0", icare_msg(), T0))
        self.assertIsNone(rn.on_icare("1-0", icare_msg(), T0))
        self.assertIsNone(rn.on_icare("2-0", icare_msg(status="REJECTED"), T0))

    def test_kill_switch_rejects_before_any_order(self):
        r = FakeRedis()
        book(r, T0, 100.70, 100.80, [(100.80, 4 * LOT)])
        account(r, T0)
        r.set("md:control:kill_switch", "1")
        rn = runner(r)
        tid = rn.on_icare("1-0", icare_msg(), T0)
        rep = json.loads(r.get(f"md:exec:latest:{tid}"))
        self.assertEqual(rep["execution_status"], S.REJECTED_BEFORE_EXECUTION)
        self.assertIn("KILL_SWITCH", rep["reject_reasons"])
        self.assertNotIn("md:exec:fill", r.streams)

    def test_same_underlying_executing_is_blocked(self):
        r = FakeRedis()
        book(r, T0, 100.0, 101.0, [(101.0, LOT)])       # first order rests at mid, stays executing
        account(r, T0)
        rn = runner(r)
        rn.on_icare("1-0", icare_msg(), T0)
        tid2 = rn.on_icare("2-0", icare_msg(tradingsymbol=TSYM), T0)
        rep = json.loads(r.get(f"md:exec:latest:{tid2}"))
        self.assertIn("DUPLICATE_UNDERLYING", rep["reject_reasons"])

    def test_shadow_ignores_the_journal_mirror_position(self):
        r = FakeRedis()
        book(r, T0, 100.70, 100.80, [(100.80, 4 * LOT)])
        account(r, T0)
        pos = open_position(icare_msg(), 100.8, T0 + 50)
        r.set(f"md:position:open:{TSYM}", json.dumps(pos.to_dict()))
        rn = runner(r, mode="shadow")
        tid = rn.on_icare("1-0", icare_msg(), T0 + 100)
        run_until_done(rn, r, T0 + 600, [(0, 100.70, 100.80, [(100.80, 4 * LOT)])])
        rep = json.loads(r.get(f"md:exec:latest:{tid}"))
        self.assertEqual(rep["execution_status"], S.FILLED)
        self.assertEqual(rep["mode"], "shadow")

        # the journal closes its own (ask-filled) position and attaches the simulated execution
        rec = close_position(PaperPosition.from_dict(pos.to_dict()), 110.0, T0 + 60_000, "TARGET")
        extra = rtj.shadow_exec_fields(r, PaperPosition.from_dict(pos.to_dict()), rec)
        self.assertEqual(extra["exec_execution_status"], S.FILLED)

    def test_restart_recovers_state_and_publishes_fill(self):
        r = FakeRedis()
        book(r, T0, 100.70, 100.80, [(100.80, LOT)])
        account(r, T0)
        rn = runner(r)
        tid = rn.on_icare("1-0", icare_msg(), T0)
        book(r, T0 + 500, 100.70, 100.80, [(100.80, LOT)])
        rn.tick(T0 + 500)                       # 1 lot filled, next slice working
        rn2 = runner(r)                         # process restart: memory and paper orders lost
        rn2.recover(T0 + 2000)
        rep = json.loads(r.get(f"md:exec:latest:{tid}"))
        self.assertEqual(rep["execution_status"], S.PARTIAL_FILL_STOPPED)
        self.assertGreaterEqual(rep["filled_lots"], 1)
        self.assertEqual(r.streams["md:exec:fill"][0]["symbol"], "SBIN")

    def test_missed_move_checks(self):
        r = FakeRedis()
        book(r, T0, 100.0, 103.0, [])
        account(r, T0)
        rn = runner(r)
        tid = rn.on_icare("1-0", icare_msg(), T0)     # spread too wide -> rejected, no reference
        self.assertNotIn(tid, r.hgetall("md:exec:missed"))
        r.hset("md:exec:missed", "X", json.dumps({"tsym": TSYM, "reference": 100.0, "done_ms": T0, "checked": []}))
        book(r, T0 + 5 * 60_000, 109.0, 111.0, [])
        rn.check_missed(T0 + 5 * 60_000)
        ev = [e for e in r.streams["md:exec"] if e["event"] == "MISSED_MOVE"]
        self.assertEqual((ev[0]["minutes"], ev[0]["missed_move_pct"]), ("5", "10.0"))


class IntegrationTest(unittest.TestCase):
    def test_icare_counts_executing_trades_in_paper_mode(self):
        r = FakeRedis()
        r.hset("md:exec:active", "TRD_1", "SBIN")
        self.assertEqual(run_icare.load_positions(r, exec_mode="shadow"), [])
        pos = run_icare.load_positions(r, exec_mode="paper")
        pf, _ = run_icare.portfolio_state({"ts_ms": T0, "available_margin": 1e6, "total_capital": 1e6,
                                           "open_positions": 0}, pos, {}, T0)
        self.assertEqual(pf.open_positions, 1)
        self.assertIn("SBIN", pf.open_symbols)

    def test_journal_pnl_net_of_charges(self):
        pos = open_position(icare_msg(), 100.0, T0)
        rec = close_position(pos, 101.0, T0 + 60_000, "TARGET")     # +Rs 3,000 gross on 4 lots
        net = apply_charges(rec, 60.0, 500.0)
        self.assertEqual((net["gross_pnl"], net["charges"], net["pnl"], net["win"]), (3000.0, 560.0, 2440.0, 1))
        loss = apply_charges(close_position(pos, 100.1, T0 + 60_000, "TIME"), 60.0, 500.0)
        self.assertEqual(loss["win"], 0)                             # gross +300, net -260


if __name__ == "__main__":
    unittest.main()

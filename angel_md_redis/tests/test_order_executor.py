"""Tests for Module 14 — Order Executor (DECISION.md §7). Brief examples are pinned here."""

from __future__ import annotations

import unittest
from dataclasses import replace

import pandas as pd

from app.order_executor import state as S
from app.order_executor.broker import LiveGate, PaperBroker, RateBucket
from app.order_executor.charges import ChargeRates, charges, per_order_cost
from app.order_executor.command import Context, build_command, validate
from app.order_executor.config import ExecConfig
from app.order_executor.engine import build_report, new_state, recover_after_restart, step
from app.order_executor.fills import continue_decision, fill_delta
from app.order_executor.market import Quote, quote_from_bidask
from app.order_executor.pricing import ladder_step, price_cap, start_price
from app.order_executor.quantity import executable_lots, max_lots_per_order, slice_lots
from app.scripmaster import option_specs

CFG = ExecConfig()
RATES = ChargeRates()
LOT = 750.0
CTX = Context(hhmm="10:15", available_margin=1_000_000.0)

ICARE = {
    "status": "APPROVED", "ts_ms": "0", "symbol": "SBIN", "tradingsymbol": "SBIN26OCT900CE", "side": "CE",
    "strike": "900", "recommended_lots": "4", "lot_size": str(LOT), "premium": "100.5",
    "stop_loss_premium": "80", "target_premium": "130", "max_risk_allowed": "100000",
    "max_capital": "42000", "expected_value": "2000", "rank": "1", "trade_score": "89.4", "probability": "88.7",
}


def cmd(**over):
    return build_command(dict(ICARE, **over), "TRD_1", "1-0", {"tick": 0.05, "kind": "OPTSTK"}, CFG)


def q(ts, bid, ask, levels=None, ask_qty=None):
    return Quote(ts_ms=ts, bid=bid, ask=ask, ask_qty=ask_qty, ask_levels=tuple(levels or ()))


def simulate(st, quotes, ctx=CTX, cfg=CFG, until_ms=20_000, tick_ms=500, broker=None, ctx_at=None):
    """Drive step() + PaperBroker on a quote timeline [(from_ms, Quote-maker(now))]."""
    broker = broker or PaperBroker(latency_ms=0)
    actions_log = []
    now = 0
    updates = []
    while now <= until_ms and st.status not in S.TERMINAL:
        cur = None
        for t0, mk in quotes:
            if now >= t0:
                cur = mk(now)
        c = ctx_at(now) if ctx_at else ctx
        st, actions, _ev = step(st, cur, c, updates, now, cfg, RATES)
        for a in actions:
            actions_log.append(a)
            if a["type"] == "PLACE":
                broker.place(a, now)
            elif a["type"] == "MODIFY":
                broker.modify(a["order_id"], a["price"], a["qty"], now)
            elif a["type"] == "CANCEL":
                broker.cancel(a["order_id"], now)
        updates = broker.poll({ICARE["tradingsymbol"]: cur}, now + 1)
        now += tick_ms
    return st, actions_log


class PricingTest(unittest.TestCase):
    def test_spread_pct_brief_example(self):
        self.assertAlmostEqual(q(0, 100, 102).spread_pct, 1.98, places=2)

    def test_cap_and_ladder(self):
        quote = q(0, 100.0, 101.0)
        cap = price_cap(101.0, 0.05, CFG)
        self.assertEqual(cap, 101.50)                       # 101 x 1.005 = 101.505 -> 101.50
        self.assertEqual(start_price(quote, 0.05, cap, CFG), 100.50)
        cfg10 = replace(CFG, ladder_steps=10)
        self.assertEqual(ladder_step(100.50, cap, 0.05, cfg10), 0.10)   # 100.50 -> 100.60 -> 100.70 ...

    def test_tight_spread_starts_at_ask_and_penny_tick(self):
        cap = price_cap(20.02, 0.01, CFG)
        self.assertEqual(cap, 20.12)
        self.assertEqual(start_price(q(0, 20.0, 20.02), 0.01, cap, CFG), 20.02)


class QuantityTest(unittest.TestCase):
    cfg = replace(CFG, margin_buffer_pct=0.0)

    def test_margin_limits_lots(self):
        # 15,000 per lot (lot 150 x Rs 100), 50,000 available -> 3; requested 4 -> 3
        lots, limiting, red, _ = executable_lots(4, 150, 100.0, 50_000, None, None, None, None, self.cfg)
        self.assertEqual((lots, limiting, red), (3, "margin", ["REDUCED_MARGIN"]))
        # brief §23: margin fell to 35,000 -> 2 lots
        self.assertEqual(executable_lots(4, 150, 100.0, 35_000, None, None, None, None, self.cfg)[0], 2)

    def test_never_above_requested(self):
        self.assertEqual(executable_lots(4, 150, 100.0, 10_000_000, None, None, None, None, self.cfg)[0], 4)

    def test_reduce_not_allowed(self):
        cfg = replace(self.cfg, allow_reduce=False)
        self.assertEqual(executable_lots(4, 150, 100.0, 50_000, None, None, None, None, cfg)[0], 0)

    def test_risk_and_depth(self):
        lots, limiting, _r, _l = executable_lots(4, 750, 101.5, None, 30_000, 20.0, None, None, self.cfg)
        self.assertEqual((lots, limiting), (2, "risk"))
        self.assertEqual(executable_lots(4, 750, 101.5, None, None, None, 300, None, self.cfg)[0], 0)

    def test_slicing_by_depth_and_freeze(self):
        self.assertEqual(slice_lots(10, 3 * LOT, LOT, None, CFG), 3)    # 10 requested, depth 3 lots
        self.assertEqual(slice_lots(7, 2 * LOT, LOT, None, CFG), 2)
        self.assertEqual(max_lots_per_order(1225, 36751, CFG), 30)       # HINDZINC freeze qty
        self.assertEqual(slice_lots(40, None, 1225, 36751, CFG), 30)


class ChargesTest(unittest.TestCase):
    def test_round_trip_one_lot(self):
        buy = charges("BUY", 75_000, 1, RATES)
        sell = charges("SELL", 75_000, 1, RATES)
        self.assertEqual(buy["stt"], 0.0)
        self.assertEqual(sell["stt"], 112.5)                 # 0.15 % on sell premium
        self.assertAlmostEqual(buy["total"] + sell["total"], 224, delta=1.5)

    def test_per_order_cost(self):
        self.assertEqual(per_order_cost(RATES), 23.6)


class PartialDecisionTest(unittest.TestCase):
    def test_brief_continue_and_cancel(self):
        ok, why, d = continue_decision(2, LOT, 101.0, 100.8, 101.0, 1250.0, 2, 23.6, CFG)
        self.assertTrue(ok)                                    # benefit 2,500 > cost 323.6 x 1.5
        self.assertEqual(d["benefit"], 2500.0)
        ok, why, d = continue_decision(2, LOT, 101.0, 100.8, 101.0, 200.0, 2, 23.6, CFG)
        self.assertEqual((ok, why), (False, "COST_EXCEEDS_BENEFIT"))

    def test_residual_too_small(self):
        ok, why, _ = continue_decision(1, 10, 50.0, 49.0, 50.0, 1000.0, 1, 23.6, CFG)
        self.assertEqual((ok, why), (False, "RESIDUAL_TOO_SMALL"))

    def test_fill_delta_cumulative(self):
        self.assertEqual(fill_delta(0, None, 1500, 100.8, 101), (100.8, 1500))
        self.assertEqual(fill_delta(1500, 100.8, 2250, 100.9, 101), (101.1, 750))
        self.assertIsNone(fill_delta(1500, 100.8, 1500, 100.8, 101))


class ValidationTest(unittest.TestCase):
    def test_each_gate(self):
        c = cmd()
        good = q(0, 100.0, 100.8)
        self.assertEqual(validate(c, good, CTX, 0, CFG), [])
        self.assertIn("COMMAND_EXPIRED", validate(cmd(ts_ms="1000"), replace(good, ts_ms=6500), CTX, 6500, CFG))
        self.assertIn("KILL_SWITCH", validate(c, good, replace(CTX, kill_switch=True), 0, CFG))
        self.assertIn("OPENING_WINDOW", validate(c, good, replace(CTX, hhmm="09:17"), 0, CFG))
        self.assertIn("MARKET_CLOSED", validate(c, good, replace(CTX, hhmm="15:20"), 0, CFG))
        self.assertIn("EXPIRY_CUTOFF", validate(c, good, replace(CTX, hhmm="13:05", expiry_today=True), 0, CFG))
        self.assertIn("DUPLICATE_UNDERLYING", validate(c, good, replace(CTX, underlying_busy=True), 0, CFG))
        self.assertIn("EXPOSURE_LIMIT", validate(c, good, replace(CTX, open_trades=5), 0, CFG))
        self.assertEqual(validate(c, replace(good, ts_ms=-4000), CTX, 0, CFG), ["STALE_QUOTE"])
        self.assertEqual(validate(c, None, CTX, 0, CFG), ["STALE_QUOTE"])
        self.assertIn("SPREAD_TOO_WIDE", validate(c, q(0, 100, 102), CTX, 0, CFG))
        self.assertIn("MARKET_CONDITION_CHANGED", validate(c, q(0, 102.6, 102.7), CTX, 0, CFG))

    def test_rejected_before_execution_sends_nothing(self):
        st, actions, ev = step(new_state(cmd(), 0), q(0, 100, 102), CTX, [], 0, CFG, RATES)
        self.assertEqual(st.status, S.REJECTED_BEFORE_EXECUTION)
        self.assertEqual(actions, [])
        rep = build_report(st, RATES, "paper")
        self.assertEqual((rep["execution_status"], rep["reject_reasons"][0]), (S.REJECTED_BEFORE_EXECUTION, "SPREAD_TOO_WIDE"))

    def test_insufficient_margin(self):
        st, actions, _ = step(new_state(cmd(), 0), q(0, 100.0, 100.8), replace(CTX, available_margin=10_000), [], 0, CFG, RATES)
        self.assertEqual((st.status, st.reject_reasons), (S.REJECTED_BEFORE_EXECUTION, ["INSUFFICIENT_MARGIN"]))


class FlowTest(unittest.TestCase):
    def test_brief_complete_trade_two_orders(self):
        """Brief §20–22: 3 lots fill, refresh, 1 more lot; avg 100.875, slippage -0.125 vs the 101 ask."""
        tl = [
            (0, lambda t: q(t, 100.00, 101.00, [(101.00, 3 * LOT)])),
            (1000, lambda t: q(t, 100.60, 100.80, [(100.80, 3 * LOT)])),
            (5000, lambda t: q(t, 100.90, 101.10, [(101.10, LOT)])),
        ]
        st, actions = simulate(new_state(cmd(), 0), tl)
        rep = build_report(st, RATES, "paper")
        self.assertEqual(rep["execution_status"], S.FILLED)
        self.assertEqual((rep["requested_lots"], rep["filled_lots"]), (4, 4))
        self.assertEqual(rep["orders_used"], 2)
        self.assertAlmostEqual(rep["average_fill_price"], 100.875)
        self.assertEqual(rep["reference_price"], 101.0)
        self.assertAlmostEqual(rep["slippage"], -0.125)
        self.assertEqual(rep["brokerage"], 40.0)
        self.assertEqual(rep["partial_decision"]["decision"], "CONTINUE")
        self.assertTrue(all(a["price"] <= 101.50 for a in actions if "price" in a))
        self.assertEqual(sum(a["lots"] for a in actions if a["type"] == "PLACE"), 4)

    def test_partial_fill_timeout(self):
        tl = [(0, lambda t: q(t, 100.70, 100.80, [(100.80, 2 * LOT)]))]

        def once(t):   # depth only on the first quote, then the offer is empty above the cap
            return q(t, 100.70, 100.80, [(100.80, 2 * LOT)]) if t == 0 else q(t, 101.40, 101.90)
        st, _ = simulate(new_state(cmd(), 0), [(0, once)])
        rep = build_report(st, RATES, "paper")
        self.assertEqual(rep["execution_status"], S.PARTIAL_FILL_TIMEOUT)
        self.assertEqual((rep["filled_lots"], rep["remaining_lots"]), (2, 2))
        self.assertEqual(rep["cancel_reason"], "TIMEOUT")

    def test_ask_above_cap_never_chased(self):
        tl = [(0, lambda t: q(t, 100.00, 101.00, [(101.0, LOT)])),
              (1000, lambda t: q(t, 101.80, 102.00, [(102.0, 4 * LOT)]))]
        st, actions = simulate(new_state(cmd(), 0), tl)
        self.assertEqual(st.status, S.ABORTED_SLIPPAGE_LIMIT)
        self.assertTrue(any(a["type"] == "CANCEL" for a in actions))
        self.assertTrue(all(a["price"] <= 101.50 for a in actions if "price" in a))
        self.assertEqual(st.filled_units, 0)

    def test_kill_switch_mid_fill_keeps_partial(self):
        def book(t):
            # after the first fill the next order rests below the ask when the kill switch flips
            return q(t, 100.70, 100.80, [(100.80, LOT)]) if t == 0 else q(t, 100.70, 101.20, [(101.2, LOT)])
        st, actions = simulate(new_state(cmd(), 0), [(0, book)],
                               ctx_at=lambda t: replace(CTX, kill_switch=t >= 1500))
        self.assertEqual(st.status, S.PARTIAL_KILLED)
        self.assertEqual(st.filled_units, LOT)
        self.assertEqual(actions[-1]["type"], "CANCEL")

    def test_stop_when_cost_exceeds_benefit(self):
        def book(t):
            return q(t, 100.70, 100.80, [(100.80, LOT)]) if t < 600 else q(t, 101.20, 101.30, [(101.3, 4 * LOT)])
        st, _ = simulate(new_state(cmd(expected_value="50"), 0), [(0, book)])
        self.assertEqual(st.status, S.PARTIAL_FILL_STOPPED)
        self.assertEqual(st.decision["why"], "COST_EXCEEDS_BENEFIT")

    def test_slices_follow_depth(self):
        c = cmd(recommended_lots="10", max_risk_allowed="1000000")
        st, actions = simulate(new_state(c, 0), [(0, lambda t: q(t, 100.70, 100.80, [(100.80, 3 * LOT)]))])
        self.assertEqual(st.status, S.FILLED)
        self.assertEqual([a["lots"] for a in actions if a["type"] == "PLACE"], [3, 3, 3, 1])

    def test_broker_price_band_rejects_abort(self):
        class LPPBroker(PaperBroker):
            def place(self, a, now_ms):
                ok, oid, _ = super().place(a, now_ms)
                self.orders[oid].status = "REJECTED"
                return ok, oid, ""

            def poll(self, quotes, now_ms):
                return [S.OrderUpdate(oid, "REJECTED", 0, None, "PRICE_OUT_OF_BAND (LPP)")
                        for oid in list(self.orders) if self.orders.pop(oid)]
        st, _ = simulate(new_state(cmd(), 0), [(0, lambda t: q(t, 100.70, 100.80, [(100.8, 4 * LOT)]))],
                         broker=LPPBroker(0))
        self.assertEqual(st.status, S.ABORTED_PRICE_BAND)

    def test_restart_recovery_keeps_fills(self):
        st = new_state(cmd(), 0)
        st, _a, _e = step(st, q(0, 100.0, 101.0, [(101.0, 4 * LOT)]), CTX, [], 0, CFG, RATES)
        oid = st.working["order_id"]
        st, _a, _e = step(st, q(500, 100.0, 101.0), CTX, [S.OrderUpdate(oid, "OPEN", LOT, 100.9)], 500, CFG, RATES)
        st = recover_after_restart(S.ExecState.from_dict(st.to_dict()))
        st, actions, _e = step(st, q(900, 100.0, 101.0), CTX, [], 900, CFG, RATES)
        self.assertEqual((st.status, st.filled_units, actions), (S.PARTIAL_FILL_STOPPED, LOT, []))


class AdaptersTest(unittest.TestCase):
    def test_quote_from_bidask_payload(self):
        quote = quote_from_bidask({"ts_ms": "5", "bid": "100", "ask": "101", "ask_qty": "750",
                                   "ask_depth5": "750,1500", "ask_depth5_px": "101,101.5"})
        self.assertEqual(quote.ask_units_within(101.2), 750)
        self.assertEqual(quote.ask_units_within(101.5), 2250)
        self.assertIsNone(quote_from_bidask({}))

    def test_option_specs_from_scripmaster(self):
        df = pd.DataFrame([
            {"exch_seg": "NFO", "instrumenttype": "OPTSTK", "name": "IDEA", "symbol": "IDEA26OCT10CE",
             "tick_size": "1.000000", "lotsize": "70000", "freeze_qty": "1000001", "expiry_date": None, "token": "1"},
            {"exch_seg": "NFO", "instrumenttype": "OPTSTK", "name": "TCS", "symbol": "TCS26OCT3000CE",
             "tick_size": "5.000000", "lotsize": "175", "freeze_qty": "10501", "expiry_date": None, "token": "2"},
        ])
        specs = option_specs(df, ["IDEA", "TCS"])
        self.assertEqual(specs["IDEA26OCT10CE"]["tick"], 0.01)
        self.assertEqual(specs["TCS26OCT3000CE"]["tick"], 0.05)
        self.assertEqual(specs["TCS26OCT3000CE"]["freeze_qty"], 10501)

    def test_live_gate_without_static_ip(self):
        gate = LiveGate("live", True, "", "1.2.3.4", True, 50_000, exit_path=True)
        self.assertEqual(gate.problems(), ["LIVE_BLOCKED_NO_STATIC_IP"])
        self.assertIn("EXEC_MODE_NOT_LIVE", LiveGate("paper", False, "", "", False, 0).problems())
        self.assertEqual(LiveGate("live", True, "1.2.3.4", "1.2.3.4", True, 50_000, exit_path=True).problems(), [])

    def test_angel_broker_limit_day_only_and_reconcile(self):
        from app.order_executor.broker import AngelBroker

        class FakeSmart:
            def __init__(self):
                self.placed, self.cancelled, self.book = [], [], []

            def placeOrder(self, p):
                self.placed.append(p)
                return "B1"

            def cancelOrder(self, oid, variety):
                self.cancelled.append(oid)

            def orderBook(self):
                return {"data": self.book}

        api = FakeSmart()
        b = AngelBroker(api=api, tokens={"X": "123"}, bucket=RateBucket(5, lambda: 0), max_order_value=100_000)
        ok, bid, _ = b.place({"order_id": "TRD_1-1", "tradingsymbol": "X", "side": "BUY", "price": 100.5,
                              "qty": 750, "lots": 1}, 0)
        self.assertTrue(ok)
        p = api.placed[0]
        self.assertEqual((p["ordertype"], p["duration"], p["variety"], p["price"]), ("LIMIT", "DAY", "NORMAL", "100.50"))
        self.assertEqual(b.place({"order_id": "big", "tradingsymbol": "X", "side": "BUY", "price": 200, "qty": 750,
                                  "lots": 1}, 0)[2], "MAX_ORDER_VALUE")
        # cancel sent, but the exchange filled first: the fill is read from the book, never assumed away
        b.cancel("TRD_1-1", 0)
        api.book = [{"orderid": "B1", "status": "complete", "filledshares": "750", "averageprice": "100.45"}]
        ups = b.poll({}, 0)
        self.assertEqual((ups[0].order_id, ups[0].status, ups[0].filled_qty, ups[0].avg_price),
                         ("TRD_1-1", "COMPLETE", 750.0, 100.45))
        self.assertEqual(b.poll({}, 0), [])

    def test_rate_bucket(self):
        t = [0]
        b = RateBucket(5, lambda: t[0])
        self.assertEqual(sum(b.take() for _ in range(10)), 5)
        t[0] = 1000
        self.assertTrue(b.take())


if __name__ == "__main__":
    unittest.main()

"""QA fix 1 (CRITICAL): signal expiry uses the SOURCE time (signal_ts_ms / md:icare stream id), never ts_ms."""

from __future__ import annotations

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.order_executor import state as S
from app.order_executor.command import build_command, validate
from app.order_executor.config import ExecConfig
from app.order_executor.market import Quote
from app.order_executor.command import Context
from test_order_executor_runner import LOT, T0, FakeRedis, account, book, icare_msg, runner

CFG = ExecConfig()
SPEC = {"tick": 0.05, "kind": "OPTSTK"}


class SignalTimeTest(unittest.TestCase):
    def test_signal_ts_ms_wins_over_ts_ms_and_stream_id(self):
        c = build_command(icare_msg(ts_ms=str(T0), signal_ts_ms=str(T0 - 2000)), "T", f"{T0}-0", SPEC, CFG)
        self.assertEqual(c.signal_ts_ms, T0 - 2000)
        self.assertEqual(c.source_ms, T0)

    def test_stream_id_when_no_signal_ts_ms_and_ts_ms_ignored(self):
        msg = icare_msg(ts_ms=str(T0))                  # ICARE re-stamped "now" on a replayed approval
        msg.pop("signal_ts_ms", None)
        c = build_command(msg, "T", f"{T0 - 60_000}-3", SPEC, CFG)
        self.assertEqual(c.signal_ts_ms, T0 - 60_000)
        good = Quote(ts_ms=T0, bid=100.0, ask=100.8)
        reasons = validate(c, good, Context(hhmm="10:15", available_margin=1e6), T0, CFG)
        self.assertEqual(reasons, ["COMMAND_EXPIRED"])     # 60 s old message > 5 s TTL; signal age 60 s ok
        c2 = build_command(msg, "T", f"{T0 - 61_000}-0", SPEC, CFG)
        self.assertEqual(validate(c2, good, Context(hhmm="10:15", available_margin=1e6), T0, CFG),
                         ["COMMAND_EXPIRED", "SIGNAL_EXPIRED"])

    def test_missing_signal_time_fails_closed(self):
        msg = icare_msg()
        msg.pop("signal_ts_ms", None)
        c = build_command(msg, "T", "not-an-id", SPEC, CFG)
        self.assertEqual(c.signal_ts_ms, 0)
        self.assertEqual(validate(c, Quote(T0, 100.0, 100.8), Context(hhmm="10:15"), T0, CFG)[:2],
                         ["COMMAND_EXPIRED", "SIGNAL_EXPIRED"])

    def test_signal_age_has_its_own_60s_limit(self):
        ctx = Context(hhmm="10:15", available_margin=1e6)
        good = Quote(T0, 100.0, 100.8)
        # chain took 8 s (> the 5 s command TTL) but the md:icare message itself is fresh: executes
        c = build_command(icare_msg(signal_ts_ms=str(T0 - 8000)), "T", f"{T0 - 500}-0", SPEC, CFG)
        self.assertEqual(validate(c, good, ctx, T0, CFG), [])
        c = build_command(icare_msg(signal_ts_ms=str(T0 - 61_000)), "T", f"{T0 - 500}-0", SPEC, CFG)
        self.assertEqual(validate(c, good, ctx, T0, CFG), ["SIGNAL_EXPIRED"])


class BacklogTest(unittest.TestCase):
    def test_backlog_message_rejected_without_orders(self):
        r = FakeRedis()
        book(r, T0, 100.70, 100.80, [(100.80, 4 * LOT)])
        account(r, T0)
        rn = runner(r)
        msg = icare_msg(ts_ms=str(T0))                 # ts_ms = now (ICARE publish time)
        msg.pop("signal_ts_ms", None)
        tid = rn.on_icare(f"{T0 - 3_600_000}-0", msg, T0)   # added an hour ago, replayed from "0"
        rep = json.loads(r.get(f"md:exec:latest:{tid}"))
        self.assertEqual(rep["execution_status"], S.REJECTED_BEFORE_EXECUTION)
        self.assertEqual(rep["reject_reasons"][:2], ["COMMAND_EXPIRED", "SIGNAL_EXPIRED"])
        self.assertEqual(rep["orders_used"], 0)
        self.assertNotIn("md:exec:fill", r.streams)
        self.assertEqual(rn.states, {})

    def test_fresh_message_executes_and_fill_carries_signal_ts(self):
        r = FakeRedis()
        book(r, T0, 100.70, 100.80, [(100.80, 4 * LOT)])
        account(r, T0)
        rn = runner(r)
        rn.on_icare(f"{T0 - 1000}-0", icare_msg(signal_ts_ms=str(T0 - 1500)), T0)
        book(r, T0 + 500, 100.70, 100.80, [(100.80, 4 * LOT)])
        account(r, T0 + 500)
        rn.tick(T0 + 500)
        rn.tick(T0 + 1000)
        fill = r.streams["md:exec:fill"][0]
        self.assertEqual(fill["signal_ts_ms"], str(T0 - 1500))
        self.assertEqual(fill["exec_mode"], "paper")


if __name__ == "__main__":
    unittest.main()

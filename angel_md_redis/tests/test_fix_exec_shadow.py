"""QA fix 2 (CRITICAL): shadow fills never become positions; journal age / mode / quote guards."""

from __future__ import annotations

import json
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import run_trade_journal as rtj
from test_order_executor_runner import LOT, T0, TSYM, FakeRedis, account, book, icare_msg, run_until_done, runner

POS_KEY = f"md:position:open:{TSYM}"


def executed_fill(mode):
    r = FakeRedis()
    book(r, T0, 100.70, 100.80, [(100.80, 4 * LOT)])
    account(r, T0)
    rn = runner(r, mode=mode)
    tid = rn.on_icare(f"{T0}-0", icare_msg(), T0)
    run_until_done(rn, r, T0 + 500, [(0, 100.70, 100.80, [(100.80, 4 * LOT)])])
    return r, tid


class ExecutorShadowTest(unittest.TestCase):
    def test_shadow_publishes_no_fill(self):
        r, tid = executed_fill("shadow")
        self.assertEqual(json.loads(r.get(f"md:exec:latest:{tid}"))["execution_status"], "FILLED")
        self.assertNotIn("md:exec:fill", r.streams)          # the simulated fill is report-only

    def test_paper_fill_is_tagged(self):
        r, _ = executed_fill("paper")
        self.assertEqual(r.streams["md:exec:fill"][0]["exec_mode"], "paper")


@mock.patch.object(rtj, "is_eod", return_value=False)
class JournalGuardTest(unittest.TestCase):
    def setUp(self):
        r, _ = executed_fill("paper")
        self.fill = r.streams["md:exec:fill"][0]
        self.r = FakeRedis()

    def test_paper_journal_opens_fresh_same_mode_fill(self, _eod):
        rtj.handle_approval(self.r, self.fill, T0 + 5000, msg_id=f"{T0 + 4000}-0", mode="paper")
        pos = json.loads(self.r.get(POS_KEY))
        self.assertEqual((pos["entry_premium"], pos["lots"], pos["exec_mode"]), (100.8, 4, "paper"))

    def test_fill_of_another_mode_ignored(self, _eod):
        rtj.handle_approval(self.r, dict(self.fill, exec_mode="shadow"), T0 + 5000, msg_id=f"{T0}-0", mode="paper")
        rtj.handle_approval(self.r, self.fill, T0 + 5000, msg_id=f"{T0}-0", mode="live")
        self.assertIsNone(self.r.get(POS_KEY))

    def test_fill_older_than_120s_opened_forced_and_exited(self, _eod):
        # a real fill is a fact: never dropped; opened, flagged and exited at once
        rtj.handle_approval(self.r, self.fill, T0 + 121_000, msg_id=f"{T0}-0", mode="paper")
        self.assertIsNone(self.r.get(POS_KEY))
        rec = self.r.streams["md:journal"][0]
        self.assertEqual((rec["exit_reason"], rec["opened_forced"]), ("FORCED_STALE_FILL", "OPENED_FORCED:STALE_FILL"))
        self.assertEqual(self.r.streams["md:exec:exit_request"][0]["reason"], "FORCED_STALE_FILL")
        # the same fill delivered again is not booked twice
        rtj.handle_approval(self.r, self.fill, T0 + 122_000, msg_id=f"{T0}-0", mode="paper")
        self.assertEqual(len(self.r.streams["md:journal"]), 1)

    def test_shadow_entry_needs_quote_within_30s(self, _eod):
        book(self.r, T0, 100.70, 100.80, [])
        rtj.handle_approval(self.r, icare_msg(), T0 + 31_000, msg_id=f"{T0 + 30_500}-0", mode="shadow")
        self.assertIsNone(self.r.get(POS_KEY))                # 31 s old quote: no entry at a stale ask
        rtj.handle_approval(self.r, icare_msg(), T0 + 29_000, msg_id=f"{T0 + 28_500}-0", mode="shadow")
        self.assertEqual(json.loads(self.r.get(POS_KEY))["entry_premium"], 100.8)

    def test_shadow_backlog_icare_ignored(self, _eod):
        book(self.r, T0 + 600_000, 100.70, 100.80, [])
        rtj.handle_approval(self.r, icare_msg(), T0 + 600_000, msg_id=f"{T0}-0", mode="shadow")
        self.assertIsNone(self.r.get(POS_KEY))

    def test_stale_bid_does_not_trigger_sl(self, _eod):
        rtj.handle_approval(self.r, self.fill, T0 + 5000, msg_id=f"{T0 + 4000}-0", mode="paper")
        book(self.r, T0, 70.0, 70.5, [])                      # below SL 80 but 60 s old
        rtj.mark_all(self.r, T0 + 60_000, mode="paper")
        self.assertIsNotNone(self.r.get(POS_KEY))
        self.assertNotIn("md:journal", self.r.streams)


if __name__ == "__main__":
    unittest.main()

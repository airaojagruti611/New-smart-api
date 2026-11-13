"""Cross-layer contract: the md:icare payload ICARE really builds -> the executor command.

The decision agent added `signal_ts_ms` to md:icare and the execution agent reads
it; each side was tested with its own fixtures. This test wires the real
producer (run_icare.build_payload on a real ICARE evaluation) into the real
consumer (order_executor.command.build_command / validate).
"""

from __future__ import annotations

import unittest

from app.icare import ICAREConfig, ICAREInputs, PortfolioState, evaluate
from app.order_executor.command import Context, build_command, validate
from app.order_executor.config import ExecConfig
from app.order_executor.market import Quote
from run_icare import build_payload

T0 = 1_791_000_000_000
ICFG = ICAREConfig(margin_buffer_pct=0.0, max_lots_per_trade=5)
XCFG = ExecConfig()
SPEC = {"tick": 0.05, "kind": "OPTSTK"}
CTX = Context(hhmm="10:15", available_margin=1e7)


def _approved():
    inp = ICAREInputs(
        symbol="SBIN", side="CE", tradingsymbol="SBIN26OCT800CE", strike=800.0,
        premium=100.0, lot_size=100.0, probability=92.0, probability_decision="HIGH_CONVICTION",
        trend=92, expected_move_score=92, strike_score=92, liquidity=92, greeks=92, bidask=92,
        projected_gain=30.0, adverse_change=-12.5, sector="BANK",
    )
    pf = PortfolioState(total_capital=750_000.0, available_margin=100_000.0, margin_utilization_pct=10.0,
                        day_pnl=0.0, open_positions=0, open_risk=0.0)
    res = evaluate(inp, pf, ICFG)
    assert res.status == "APPROVED", res.reasons
    return res, pf


class IcareToExecutorContractTest(unittest.TestCase):
    def setUp(self):
        self.res, self.pf = _approved()

    def _cmd(self, origin_ms, icare_xadd_ms, now_ms):
        payload = build_payload(self.res, {}, self.pf, [], now_ms, signal_ts_ms=origin_ms)
        self.assertEqual(payload["ts_ms"], str(now_ms))           # ICARE publish time
        self.assertEqual(payload["signal_ts_ms"], str(origin_ms))  # chain origin
        return build_command(payload, "TRD_1", f"{icare_xadd_ms}-0", SPEC, XCFG)

    def test_fields_map_through(self):
        cmd = self._cmd(T0 - 2000, T0, T0)
        self.assertEqual(cmd.signal_ts_ms, T0 - 2000)
        self.assertEqual(cmd.source_ms, T0)
        self.assertEqual(cmd.tradingsymbol, "SBIN26OCT800CE")
        self.assertEqual(cmd.requested_lots, self.res.recommended_lots)
        self.assertEqual(cmd.sl_premium, self.res.stop_loss_premium)
        self.assertEqual(cmd.max_risk_amount, self.res.max_risk_allowed)
        self.assertGreater(cmd.requested_lots, 0)

    def test_fresh_chain_executes(self):
        cmd = self._cmd(T0 - 8000, T0 - 300, T0)   # chain took 8 s, command 0.3 s old
        self.assertEqual(validate(cmd, Quote(T0, 100.0, 100.5), CTX, T0, XCFG), [])

    def test_replayed_backlog_rejected_even_though_icare_restamped_now(self):
        # ICARE replays an hour-old probability message: ts_ms = now, but the origin is old
        cmd = self._cmd(T0 - 3_600_000, T0 - 100, T0)
        self.assertIn("SIGNAL_EXPIRED", validate(cmd, Quote(T0, 100.0, 100.5), CTX, T0, XCFG))

    def test_executor_backlog_rejected(self):
        # md:icare message itself sat in the executor's backlog for 30 s
        cmd = self._cmd(T0 - 31_000, T0 - 30_000, T0)
        self.assertIn("COMMAND_EXPIRED", validate(cmd, Quote(T0, 100.0, 100.5), CTX, T0, XCFG))


if __name__ == "__main__":
    unittest.main()

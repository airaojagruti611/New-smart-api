"""Module 17 runner on an in-memory Redis: trip -> kill switch, restart protection, reset, emergency exits."""

from __future__ import annotations

import fnmatch
import json
import unittest

import run_circuit_breaker as rcb
from app import circuit_breaker as C
from app.trade_journal import PaperPosition

NOW = 1_800_000_000_000
CFG = C.CircuitConfig(daily_loss_cap=10000, max_loss_per_trade=4000, max_total_exposure=10 ** 9,
                      heartbeat_max_age_sec=60, pnl_jump=10 ** 9)


class FakeRedis:
    def __init__(self):
        self.kv, self.streams = {}, {}

    def get(self, k):
        return self.kv.get(k)

    def set(self, k, v, **kw):
        self.kv[k] = v
        return True

    def delete(self, k):
        self.kv.pop(k, None)

    def exists(self, k):
        return int(k in self.kv)

    def scan_iter(self, match="*", count=None):
        return [k for k in list(self.kv) if fnmatch.fnmatch(k, match)]

    def xadd(self, k, fields, maxlen=None, approximate=True):
        self.streams.setdefault(k, []).append((f"{NOW + len(self.streams.get(k, []))}-0", dict(fields)))

    def xrange(self, k, min="-", max="+", count=None):
        return list(self.streams.get(k, []))

    def xrevrange(self, k, count=None):
        return list(reversed(self.streams.get(k, [])))[:count]


def put_pos(r, tid="T1", qty=100.0, entry=100.0, last=100.0, **kw):
    p = PaperPosition(trade_id=tid, symbol="X", tradingsymbol="X" + tid, side="CE", strike=1, lots=2, lot_size=50,
                      qty=qty, entry_premium=entry, entry_ts_ms=NOW, sl_premium=1, target_premium=999,
                      time_stop_ms=0, entry_spot=1, last_premium=last, orig_qty=qty, exec_mode="paper", **kw)
    r.set(f"md:position:open:X{tid}", json.dumps(p.to_dict()))


def mk(r, emergency=False):
    return rcb.CircuitRunner(r, CFG, exec_mode="paper", emergency_exit=emergency)


def events(r):
    return [f["event"] for _, f in r.streams.get(rcb.EVENTS_STREAM, [])]


class RunnerTests(unittest.TestCase):
    def setUp(self):
        rcb.MARKET_HOURS_ONLY = True      # NOW is a Sunday/off-hours independent check below
        rcb.HEARTBEAT_KEYS = []

    def test_quiet_stays_armed_and_writes_latest(self):
        r = FakeRedis()
        put_pos(r)
        st = mk(r).tick(NOW)
        self.assertEqual(st.state, C.ARMED)
        self.assertIsNone(r.get(rcb.KILL_SWITCH_KEY))
        self.assertEqual(json.loads(r.get(rcb.LATEST_KEY))["state"], "ARMED")

    def test_trip_sets_kill_switch_in_executor_format(self):
        r = FakeRedis()
        put_pos(r, last=55)                                   # -4500 on one trade -> CRITICAL
        st = mk(r).tick(NOW)
        self.assertEqual(st.state, C.TRIPPED)
        self.assertEqual(r.get(rcb.KILL_SWITCH_KEY), "1")
        self.assertIn("TRADE_LOSS_CAP", r.get(rcb.KILL_REASON_KEY))
        self.assertEqual(events(r), ["TRIPPED"])
        self.assertEqual(json.loads(r.get(rcb.STATE_KEY))["state"], "TRIPPED")

    def test_daily_loss_from_journal(self):
        r = FakeRedis()
        r.set(f"md:journal:daily:{rcb.ist_date(NOW)}", json.dumps({"realized_pnl": -12000}))
        self.assertEqual(mk(r).tick(NOW).state, C.TRIPPED)

    def test_restart_keeps_tripped_and_reasserts_kill_switch(self):
        r = FakeRedis()
        put_pos(r, last=55)
        mk(r).tick(NOW)
        r.delete(rcb.KILL_SWITCH_KEY)                          # someone cleared it / redis restored partially
        put_pos(r, last=100)                                   # the loss is gone
        rn = mk(r)                                             # "restart"
        self.assertEqual(rn.recover(NOW + 1).state, C.TRIPPED)
        self.assertEqual(r.get(rcb.KILL_SWITCH_KEY), "1")
        self.assertEqual(rn.tick(NOW + 2).state, C.TRIPPED)    # no auto recovery
        self.assertIn("RESTART_STILL_TRIPPED", events(r))

    def test_manual_reset_clears_state_and_own_kill_switch(self):
        r = FakeRedis()
        put_pos(r, last=55)
        rn = mk(r)
        rn.tick(NOW)
        put_pos(r, last=100)
        self.assertEqual(rn.manual_reset(NOW + 5).state, C.ARMED)
        self.assertIsNone(r.get(rcb.KILL_SWITCH_KEY))
        self.assertEqual(rn.tick(NOW + 10).state, C.ARMED)
        self.assertEqual(events(r), ["TRIPPED", "RESET"])

    def test_reset_keeps_manually_set_kill_switch(self):
        r = FakeRedis()
        r.set(rcb.KILL_SWITCH_KEY, "1")                        # operator's own kill switch
        put_pos(r, last=55)
        rn = mk(r)
        rn.tick(NOW)
        rn.manual_reset(NOW + 5)
        self.assertEqual(r.get(rcb.KILL_SWITCH_KEY), "1")

    def test_emergency_shadow_publishes_alert_only(self):
        r = FakeRedis()
        put_pos(r, "A", last=40)                               # -6000 = 1.5x -> EMERGENCY
        mk(r, emergency=False).tick(NOW)
        self.assertEqual(events(r), ["TRIPPED", "EMERGENCY_EXIT_SHADOW"])
        self.assertNotIn(rcb.EXIT_REQ_STREAM, r.streams)

    def test_emergency_enabled_publishes_exit_requests_once(self):
        r = FakeRedis()
        put_pos(r, "A", last=40)
        put_pos(r, "B", last=100, exit_pending="SL")
        rn = mk(r, emergency=True)
        rn.tick(NOW)
        rn.tick(NOW + 5000)
        reqs = [f for _, f in r.streams[rcb.EXIT_REQ_STREAM]]
        self.assertEqual([(q["trade_id"], q["reason"], q["exec_mode"]) for q in reqs], [("A", "CIRCUIT_BREAKER", "paper")])
        self.assertEqual(events(r).count("EMERGENCY_EXIT_PUBLISHED"), 1)

    def test_order_rate_and_rejections_from_exec_stream(self):
        r = FakeRedis()
        for _ in range(40):
            r.xadd("md:exec", {"event": "ORDER_PLACED"})
        self.assertEqual(mk(r).tick(NOW + 1000).state, C.TRIPPED)

    def test_watchdog_unacted_approval_warns(self):
        r = FakeRedis()
        r.streams["md:icare"] = [(f"{NOW - 150_000}-0", {"status": "APPROVED", "tradingsymbol": "S1"})]
        st = mk(r).tick(NOW)
        self.assertEqual((st.state, st.reason), (C.WARNING, "APPROVAL_NOT_ACTED"))
        self.assertIsNone(r.get(rcb.KILL_SWITCH_KEY))

    def test_filled_entry_without_position_trips(self):
        r = FakeRedis()
        r.set("md:exec:state:T9", json.dumps({"trade_id": "T9", "status": "FILLED", "finished_ms": NOW - 600_000,
                                              "fills": [[100, 50, NOW, "o"]]}))
        self.assertEqual(mk(r).tick(NOW).state, C.TRIPPED)
        r.set("md:journal:closed:T9", "{}")
        r2 = FakeRedis()
        r2.kv.update(r.kv)
        r2.delete(rcb.STATE_KEY)
        r2.delete(rcb.KILL_SWITCH_KEY)
        self.assertEqual(mk(r2).tick(NOW).state, C.ARMED)

    def test_stale_heartbeat_key(self):
        rcb.HEARTBEAT_KEYS, rcb.MARKET_HOURS_ONLY = ["md:account:latest"], False
        r = FakeRedis()
        r.set("md:account:latest", json.dumps({"ts_ms": NOW - 300_000}))
        self.assertEqual(mk(r).tick(NOW).state, C.WARNING)
        r.set("md:account:latest", json.dumps({"ts_ms": NOW}))

    def test_corrupt_state_is_armed(self):
        r = FakeRedis()
        r.set(rcb.STATE_KEY, "{nope")
        self.assertEqual(mk(r).load_state().state, C.ARMED)


if __name__ == "__main__":
    unittest.main()

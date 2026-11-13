"""Dashboard loaders (app/dashboard_data.py) on an in-memory Redis seeded for every layer."""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))

from app import dashboard_data as dd  # noqa: E402
from test_fix_dashboard_fake import NOW_MS, SYM, TID, TSYM, TSYM_PE, FakeRedis, ago, seeded  # noqa: E402


class FreshnessHelpers(unittest.TestCase):
    def test_doc_ts_prefers_fields_then_stream_id(self):
        self.assertEqual(dd.doc_ts_ms({"ts_ms": "1700000000000"}), 1700000000000)
        self.assertEqual(dd.doc_ts_ms({"ts_recv": 1700000000}), 1700000000000)      # seconds → ms
        self.assertEqual(dd.doc_ts_ms({"updated_ms": 5_000_000_000_000}), 5_000_000_000_000)
        self.assertEqual(dd.doc_ts_ms({}, "1700000000123-0"), 1700000000123)
        self.assertIsNone(dd.doc_ts_ms({"x": 1}))
        self.assertIsNotNone(dd.doc_ts_ms({"date": "2026-10-02"}))

    def test_freshness_states(self):
        self.assertEqual(dd.freshness(10, 30), "FRESH")
        self.assertEqual(dd.freshness(31, 30), "STALE")
        self.assertEqual(dd.freshness(None, 30), "NO_TS")
        self.assertEqual(dd.freshness(1e9, None), "N/A")
        lay = dd.LAYER_BY_ID["ticks_eq"]
        self.assertEqual(dd.effective_threshold(lay), 30)
        self.assertEqual(dd.effective_threshold(lay, 900), 900)
        self.assertIsNone(dd.effective_threshold(dd.LAYER_BY_ID["position"], 900))

    def test_fmt_age(self):
        self.assertEqual(dd.fmt_age(None), "no ts")
        self.assertEqual(dd.fmt_age(5), "5s")
        self.assertEqual(dd.fmt_age(600), "10.0m")

    def test_flags(self):
        self.assertEqual(dd.doc_flags({"flags": '["CROSSED","STALE"]'}), "CROSSED, STALE")
        self.assertEqual(dd.doc_flags({"flags": "STALE"}), "STALE")
        self.assertEqual(dd.doc_flags({"crossed": "true"}), "CROSSED")
        self.assertEqual(dd.doc_flags(None), "")

    def test_greeks_latest_both_forms(self):
        old = dd.parse_greeks_latest('[{"strikePrice":"1400"}]')
        self.assertEqual((old["form"], old["n"], old["ts_ms"]), ("list", 1, None))
        new = dd.parse_greeks_latest('{"ts_ms": 1700000000000, "data": [{"a":1},{"a":2}], "expiry": "2026-10-27"}')
        self.assertEqual((new["form"], new["n"], new["ts_ms"], new["expiry"]), ("dict", 2, 1700000000000, "2026-10-27"))
        self.assertEqual(dd.parse_greeks_latest(None)["form"], "missing")
        stamped = dd.parse_greeks_latest('[{"ts_ms": 1700000000000}, {"ts_ms": 1700000005000}]')
        self.assertEqual((stamped["form"], stamped["ts_ms"]), ("list", 1700000005000))
        self.assertEqual(dd.parse_greeks_latest("not json")["n"], 0)


class Loaders(unittest.TestCase):
    def setUp(self):
        self.r = seeded()
        self.keys = dd.key_index(self.r)

    def tearDown(self):
        self.assertEqual(self.r.writes, [], "dashboard must never write to Redis")

    def test_key_index_scans_md_only(self):
        self.assertTrue(all(k.startswith("md:") for k in self.keys))
        self.assertEqual(len(dd.key_index(self.r, cap=5)), 5)

    def test_symbol_latest_docs_cover_every_layer(self):
        rows = dd.symbol_latest_docs(self.r, SYM, keys=self.keys, now=NOW_MS)
        layers = {x["layer"] for x in rows}
        for lid in ("greeks", "greeks_phase", "greeks_phase_und", "pivots", "ema", "supertrend", "htf", "level",
                    "momentum", "indicator_score", "volume", "regime", "entry", "bidask", "imbalance", "smartmoney",
                    "orderflow", "strikeflow", "oi", "oi_und", "liquidity", "optexit", "stockflow", "composite",
                    "expected", "greeks_change", "strike", "strike_intel", "capital", "probability", "ranking",
                    "ranking_cycle", "icare", "icare_origin", "account", "exec_latest", "tsl", "position"):
            self.assertIn(lid, layers, lid)
        by_key = {x["key"]: x for x in rows}
        self.assertEqual(by_key[f"md:bidask:latest:{TSYM}"]["scope"], "contract")
        self.assertEqual(by_key[f"md:bidask:latest:{TSYM}"]["fresh"], "FRESH")
        stale = by_key[f"md:bidask:latest:{TSYM_PE}"]
        self.assertEqual((stale["fresh"], stale["flags"]), ("STALE", "STALE"))
        self.assertGreater(stale["age_s"], 500)
        self.assertEqual(by_key[f"md:greeks:latest:{SYM}:2026-10-27"]["fresh"], "NO_TS")   # old list form
        self.assertEqual(by_key[f"md:greeks:latest:{SYM}:2026-11-24"]["fresh"], "FRESH")   # new form with ts_ms
        self.assertEqual(by_key[f"md:icare:latest:{SYM}"]["flags"], "CROSSED")
        self.assertEqual(by_key[f"md:position:open:{TSYM}"]["fresh"], "N/A")
        self.assertIn("md:volume:latest[RELIANCE]", by_key)
        # TCS docs are not mixed in
        self.assertFalse(any("TCS" in x["key"] for x in rows))

    def test_stale_symbol_and_override(self):
        rows = dd.symbol_latest_docs(self.r, "TCS", keys=self.keys, now=NOW_MS)
        entry = next(x for x in rows if x["layer"] == "entry")
        self.assertEqual(entry["fresh"], "STALE")
        rows = dd.symbol_latest_docs(self.r, "TCS", keys=self.keys, now=NOW_MS, override_sec=2 * 86400)
        self.assertEqual(next(x for x in rows if x["layer"] == "entry")["fresh"], "FRESH")

    def test_missing_symbol(self):
        self.assertEqual([x for x in dd.symbol_latest_docs(self.r, "NOPE", keys=self.keys) if x["scope"] != "global"], [])
        chain = dd.decision_chain(self.r, "NOPE", keys=self.keys)
        self.assertTrue(all(c["fresh"] == "MISSING" for c in chain))

    def test_decision_chain(self):
        chain = dd.decision_chain(self.r, SYM, keys=self.keys, now=NOW_MS)
        stages = [c["stage"] for c in chain]
        self.assertEqual(stages, ["level", "entry", "strike", "strike_intel", "probability", "ranking", "icare", "icare_origin",
                                  "exec", "tsl", "position", "journal"])
        by = {c["stage"]: c for c in chain}
        self.assertTrue(all(c["present"] for c in chain))
        self.assertEqual(by["icare"]["summary"]["net_ev"], "460")
        self.assertEqual(by["icare"]["summary"]["exec_mode"], "paper")
        self.assertEqual(by["ranking"]["key"], f"md:ranking:latest:{SYM}:CALL")
        self.assertEqual(by["exec"]["summary"]["execution_status"], "FILLED")
        self.assertEqual(by["entry"]["fresh"], "FRESH")
        self.assertEqual(by["journal"]["summary"]["exit_reason"], "SL")
        self.assertTrue(all(by[g]["verdict"] == "PASS" for g in dd.CHAIN_GATES))
        self.assertIsNone(dd.chain_blocker(chain))

    def test_chain_blocked_at_entry(self):
        # TCS: entry trigger is NEUTRAL (rejected) — downstream stages wait on it.
        chain = dd.decision_chain(self.r, "TCS", keys=self.keys, now=NOW_MS)
        by = {c["stage"]: c for c in chain}
        self.assertEqual(by["entry"]["verdict"], "REJECTED")
        blk = dd.chain_blocker(chain)
        self.assertEqual((blk["stage"], blk["verdict"]), ("entry", "REJECTED"))   # missing level doc is skipped
        self.assertIn("waiting", by["probability"]["note"])
        self.assertIn("key unknown", by["exec"]["note"])

    def test_chain_level_no_break(self):
        r = FakeRedis()
        r.seed_str("md:level:entry:latest:ABC", {"ts_ms": ago(5), "signal": "NEUTRAL", "reason": "no_break",
                                                 "price": "100", "P": "99", "R1": "102", "S1": "97"})
        chain = dd.decision_chain(r, "ABC", keys=dd.key_index(r), now=NOW_MS)
        blk = dd.chain_blocker(chain)
        self.assertEqual((blk["stage"], blk["verdict"]), ("level", "NO_SIGNAL"))
        self.assertIn("no_break", blk["reason"])
        self.assertIn("R1 102", blk["reason"])

    def test_stage_verdicts(self):
        self.assertEqual(dd.stage_verdict("probability", {"decision": "WATCHLIST"}), "REJECTED")
        self.assertEqual(dd.stage_verdict("probability", {"decision": "SMALL_POSITION"}), "PASS")
        self.assertEqual(dd.stage_verdict("icare", {"status": "REJECTED"}), "REJECTED")
        self.assertEqual(dd.stage_verdict("exec", {"execution_status": "PARTIAL_FILL_TIMEOUT", "filled_lots": 1}), "PASS")
        self.assertEqual(dd.stage_verdict("exec", {"execution_status": "PARTIAL_FILL_TIMEOUT", "filled_lots": 0}), "REJECTED")
        self.assertEqual(dd.stage_verdict("tsl", {"status": "TRAILING"}), "INFO")
        self.assertEqual(dd.stage_verdict("entry", {}), "MISSING")

    def test_stream_tail(self):
        rows = dd.stream_tail(self.r, "md:ticks:eq", n=10, now=NOW_MS, stale_sec=30)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["symbol"], "TCS")           # newest first
        self.assertEqual(rows[0]["_fresh"], "FRESH")
        only = dd.stream_tail(self.r, "md:ticks:eq", n=10, symbol=SYM)
        self.assertEqual([x["symbol"] for x in only], [SYM])
        self.assertEqual(dd.stream_tail(self.r, "md:missing", n=5), [])
        # contract rows match their underlying
        self.assertEqual(len(dd.stream_tail(self.r, "md:bidask:signal", n=5, symbol=SYM)), 1)

    def test_stream_age_falls_back_to_id(self):
        r = FakeRedis()
        r.seed_stream("s", [(ago(100), {"x": "1"})])
        row = dd.stream_tail(r, "s", n=1, now=NOW_MS, stale_sec=30)[0]
        self.assertAlmostEqual(row["_age_s"], 100, delta=1)
        self.assertEqual(row["_fresh"], "STALE")

    def test_streams_overview(self):
        ov = {x["stream"]: x for x in dd.streams_overview(self.r, now=NOW_MS)}
        self.assertEqual(ov["md:ticks:eq"]["fresh"], "FRESH")
        self.assertEqual(ov["md:exec:exit_request"]["fresh"], "EMPTY")
        self.assertEqual(ov["md:tsl:reentry"]["fresh"], "N/A")
        self.assertEqual(ov["md:candles:1m"]["length"], 1)

    def test_exec_overview(self):
        ex = dd.exec_overview(self.r, n=10, keys=self.keys, now=NOW_MS)
        self.assertFalse(ex["kill_switch"]["on"])
        self.assertEqual([x["trade_id"] for x in ex["reports"]], [TID])        # tsym:* not double counted
        self.assertEqual(len(ex["reports_by_tsym"]), 1)
        self.assertEqual(len(ex["states"]), 1)
        self.assertEqual(ex["events"][0]["event"], "REPORT")
        self.assertEqual(len(ex["fills"]), 1)
        self.assertFalse(ex["exit_request_exists"])
        self.assertIn("TRD_X", ex["missed"])
        self.assertIsInstance(ex["missed"]["TRD_X"], dict)

    def test_kill_switch_on(self):
        self.r.seed_str("md:control:kill_switch", "1")
        self.assertTrue(dd.kill_switch_state(self.r)["on"])

    def test_tsl_overview(self):
        t = dd.tsl_overview(self.r, n=10, keys=self.keys, now=NOW_MS)
        self.assertEqual(t["latest"][0]["_fresh"], "FRESH")
        self.assertEqual(len(t["states"]), 1)
        self.assertEqual(len(t["exit_contexts"]), 1)
        self.assertEqual(len(t["chains"]), 1)
        self.assertEqual(len(t["blocks"]), 1)
        self.assertEqual(t["sets"]["chains"], ["CH1"])
        self.assertEqual(len(t["reentry"]), 1)

    def test_journal_overview(self):
        j = dd.journal_overview(self.r, n=10, keys=self.keys, now=NOW_MS)
        self.assertEqual(j["positions"][0]["unrealized_pnl"], 1000.0)
        allb = next(x for x in j["stats"] if x["bucket"] == "ALL")
        self.assertEqual((allb["trades"], allb["wins"], allb["win_rate"]), (4, 3, 0.75))
        self.assertEqual(allb["avg_loss_pct"], 10.0)
        self.assertEqual(j["daily"][0]["trades"], 1)
        self.assertEqual(j["closed"][0]["trade_id"], "TRD_OLD")
        self.assertEqual(len(j["stream"]), 1)

    def test_ranking_and_account(self):
        rk = dd.ranking_overview(self.r, keys=self.keys, now=NOW_MS)
        self.assertEqual(rk["rank_zset"], [(f"{SYM}:CALL", 77.0)])
        self.assertEqual(rk["probability_zset"][0], (SYM, 71.0))
        self.assertTrue(rk["book"][f"{SYM}:CALL"]["emitted"])
        self.assertEqual(rk["cycle"]["outcome"], "TAKE")
        self.assertEqual(len(rk["latest"]), 1)
        acct = dd.account_snapshot(self.r, now=NOW_MS)
        self.assertEqual(acct["fresh"], "FRESH")
        self.assertEqual(dd.account_snapshot(FakeRedis())["fresh"], "MISSING")

    def test_collect_symbol_and_meta(self):
        d = dd.collect_symbol(self.r, SYM)
        self.assertEqual(d["entry"]["signal"], "BUY CALL")
        self.assertEqual(d["volume"]["signal"], "Bullish Volume")
        self.assertEqual(d["liquidity"]["tradingsymbol"], TSYM)
        self.assertEqual(d["expiry"], "2026-10-27")
        self.assertEqual(d["tick"]["ltp"], "1381.5")
        empty = dd.collect_symbol(FakeRedis(), SYM)
        self.assertEqual(empty["entry"], {})
        cs = dd.contracts_for_symbol(self.r, SYM)
        self.assertEqual([c["tradingsymbol"] for c in cs], [TSYM_PE, TSYM])
        self.assertEqual(dd.eq_meta(self.r, SYM)["token"], "2885")
        self.assertIn("TCS", dd.universe(self.r))

    def test_raw_explorer(self):
        found = {x["key"]: x for x in dd.explore(self.r, "md:ranking*", cap=50)}
        self.assertEqual(found["md:ranking:rank"]["type"], "zset")
        self.assertEqual(found["md:ranking:book"]["type"], "hash")
        self.assertEqual(len(dd.explore(self.r, "md:*", cap=3)), 3)
        s = dd.read_key(self.r, f"md:strike:intel:latest:{SYM}")
        self.assertEqual(s["type"], "string")
        self.assertEqual(s["ttl"], 3500)
        self.assertEqual(s["value"]["status"], "OK")
        st = dd.read_key(self.r, "md:exec", n=1)
        self.assertEqual((st["type"], len(st["value"])), ("stream", 1))
        z = dd.read_key(self.r, "md:probability:rank", n=1)
        self.assertEqual(z["value"], [{"member": SYM, "score": 71.0}])
        h = dd.read_key(self.r, "md:ranking:book")
        self.assertIsInstance(h["value"][f"{SYM}:CALL"], dict)
        self.assertEqual(dd.read_key(self.r, "md:nope")["type"], "none")
        self.assertEqual(dd.read_key(self.r, "md:tsl:chains")["value"], ["CH1"])

    def test_redis_health(self):
        h = dd.redis_health(self.r)
        self.assertTrue(h["ok"])
        self.assertEqual(h["eq"], 2)

        class Down(FakeRedis):
            def ping(self):
                raise ConnectionError("refused")
        self.assertFalse(dd.redis_health(Down())["ok"])


class ClientFactory(unittest.TestCase):
    def test_override(self):
        fake = FakeRedis()
        dd.set_client_factory(lambda: fake)
        try:
            self.assertIs(dd.connect(), fake)
        finally:
            dd.set_client_factory(None)

    def test_env_override(self):
        old = os.environ.get("DASHBOARD_REDIS_FACTORY")
        os.environ["DASHBOARD_REDIS_FACTORY"] = "test_fix_dashboard_fake:make_client"
        try:
            self.assertIsInstance(dd.connect(), FakeRedis)
        finally:
            if old is None:
                os.environ.pop("DASHBOARD_REDIS_FACTORY", None)
            else:
                os.environ["DASHBOARD_REDIS_FACTORY"] = old


class MissingFlowsSurfaced(unittest.TestCase):
    def test_contract_falls_back_to_strikeflow_pick_then_signal_side(self):
        r = FakeRedis()
        r.seed_str(f"md:strikeflow:latest:{SYM}", {"ts_ms": NOW_MS, "chosen_tradingsymbol": TSYM_PE})
        r.seed_str(f"md:optexit:latest:{TSYM_PE}", {"ts_ms": NOW_MS, "signal": "HOLD"})
        d = dd.collect_symbol(r, SYM)
        self.assertEqual(d["contract"], TSYM_PE)
        self.assertEqual(d["optexit"]["signal"], "HOLD")

        r = FakeRedis()
        r.seed_str(f"md:momentum:confirm:latest:{SYM}", {"ts_ms": NOW_MS, "signal": "BUY PUT"})
        r.seed_str(f"md:greeks:phase:underlying:latest:{SYM}:CE", {"tradingsymbol": TSYM})
        r.seed_str(f"md:greeks:phase:underlying:latest:{SYM}:PE", {"tradingsymbol": TSYM_PE})
        self.assertEqual(dd.collect_symbol(r, SYM)["contract"], TSYM_PE)

    def test_htf_candle_found_behind_long_single_symbol_run(self):
        r = FakeRedis()
        r.seed_stream("md:candles:30m", [(ago(60), {"symbol": SYM, "ts_ms": ago(60), "c": "1"})]
                      + [(ago(5), {"symbol": "INFY", "ts_ms": ago(5), "c": "2"}) for _ in range(400)])
        self.assertEqual(dd.collect_symbol(r, SYM)["c30m"].get("c"), "1")

    def test_exit_order_state_machines_listed(self):
        r = FakeRedis()
        r.seed_str("md:exec:exit_state:EX1", {"exit_id": "EX1", "status": "WORKING", "ts_ms": NOW_MS})
        r.seed_str(f"md:exec:state:{TID}", {"trade_id": TID, "updated_ms": NOW_MS})
        r.seed_hash("md:exec:exit:active", {TID: "EX1"})
        ex = dd.exec_overview(r, keys=sorted(r._all()))
        self.assertEqual([x["exit_id"] for x in ex["exit_states"]], ["EX1"])
        self.assertEqual([x.get("trade_id") for x in ex["states"]], [TID])
        self.assertEqual(ex["exit_active"], {TID: "EX1"})


if __name__ == "__main__":
    unittest.main()

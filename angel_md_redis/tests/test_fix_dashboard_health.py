"""pipeline_health.py freshness verdicts + Streamlit app smoke test on a fake Redis."""

from __future__ import annotations

import ast
import io
import os
import sys
import unittest
from contextlib import redirect_stdout

sys.path.insert(0, os.path.dirname(__file__))

import pipeline_health as ph  # noqa: E402
from app import dashboard_data as dd  # noqa: E402
from test_fix_dashboard_fake import NOW_MS, FakeRedis, ago, seeded  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _mod(worker):
    for layer in ph.LAYERS:
        for m in layer["modules"]:
            if m.get("worker") == worker:
                return m
    raise KeyError(worker)


class HealthFreshness(unittest.TestCase):
    def test_fresh_module_ok(self):
        r = seeded()
        v = ph.evaluate_module(r, _mod("run_ema_cross.py"), now_ms=NOW_MS)
        self.assertEqual(v["status"], "OK")
        self.assertLess(v["newest_age_sec"], 60)

    def test_yesterdays_keys_are_stale(self):
        r = FakeRedis()
        r.seed_str("md:ema:cross:latest:RELIANCE", {"ts_ms": ago(86400), "state": "bullish"})
        r.seed_stream("md:ema:cross", [(ago(86400), {"ts_ms": str(ago(86400)), "symbol": "RELIANCE"})])
        v = ph.evaluate_module(r, _mod("run_ema_cross.py"), now_ms=NOW_MS)
        self.assertEqual(v["status"], "STALE")
        self.assertGreater(v["newest_age_sec"], 80000)
        # a generous override makes it OK again
        self.assertEqual(ph.evaluate_module(r, _mod("run_ema_cross.py"), now_ms=NOW_MS, max_age_override=2 * 86400)["status"], "OK")

    def test_key_without_timestamp_is_stale(self):
        r = FakeRedis()
        r.seed_str("md:supertrend:bias:latest:RELIANCE", {"bias": "CALL"})
        self.assertEqual(ph.evaluate_module(r, _mod("run_supertrend_mtf_bias.py"), now_ms=NOW_MS)["status"], "STALE")

    def test_stream_id_age_used_when_no_ts_field(self):
        r = FakeRedis()
        r.seed_stream("md:ticks:eq", [(ago(5), {"symbol": "RELIANCE"})])
        self.assertEqual(ph.evaluate_module(r, _mod("run_producer.py"), now_ms=NOW_MS)["status"], "OK")
        r2 = FakeRedis()
        r2.seed_stream("md:ticks:eq", [(ago(600), {"symbol": "RELIANCE"})])
        self.assertEqual(ph.evaluate_module(r2, _mod("run_producer.py"), now_ms=NOW_MS)["status"], "STALE")

    def test_missing_is_fail_and_event_driven_not_aged(self):
        self.assertEqual(ph.evaluate_module(FakeRedis(), _mod("run_ema_cross.py"), now_ms=NOW_MS)["status"], "FAIL")
        r = FakeRedis()
        r.seed_stream("md:journal", [(ago(5 * 86400), {"trade_id": "T"})])
        v = ph.evaluate_module(r, _mod("run_trade_journal.py"), now_ms=NOW_MS)
        self.assertEqual((v["status"], v["max_age_sec"]), ("OK", None))

    def test_streams_filled_in(self):
        self.assertEqual(_mod("run_supertrend_mtf_bias.py")["streams"], ["md:supertrend:bias"])
        self.assertIn("md:oi:underlying:signal", _mod("run_oi_analysis.py")["streams"])
        empty = [m["name"] for L in ph.LAYERS for m in L["modules"]
                 if m.get("worker") and not m.get("streams") and m.get("worker") != "run_account.py"]
        self.assertEqual(empty, [])

    def test_greeks_list_form_and_hash(self):
        r = seeded()
        v = ph.evaluate_module(r, _mod("run_greeks_only.py"), now_ms=NOW_MS)
        self.assertEqual(v["status"], "OK")
        v = ph.evaluate_module(r, _mod("run_producer.py"), now_ms=NOW_MS)
        self.assertEqual(v["status"], "OK")

    def test_env_threshold(self):
        r = FakeRedis()
        r.seed_stream("md:ticks:eq", [(ago(100), {"symbol": "X"})])
        os.environ["HEALTH_MAX_AGE_RUN_PRODUCER"] = "1000"
        try:
            self.assertEqual(ph.evaluate_module(r, _mod("run_producer.py"), now_ms=NOW_MS)["status"], "OK")
        finally:
            os.environ.pop("HEALTH_MAX_AGE_RUN_PRODUCER")

    def test_evaluate_all_with_key_list_matches_scan(self):
        r = seeded()
        a = {x["name"]: x["status"] for x in ph.evaluate_all(r, now_ms=NOW_MS)}
        b = {x["name"]: x["status"] for x in ph.evaluate_all(r, now_ms=NOW_MS, keys=dd.key_index(r))}
        self.assertEqual(a, b)
        self.assertEqual(a["Module 1d: Pivot Levels"], "OK")
        self.assertEqual(r.writes, [])

    def test_print_report_counts_stale(self):
        r = FakeRedis()
        r.seed_str("md:ema:cross:latest:RELIANCE", {"ts_ms": ago(86400)})
        buf = io.StringIO()
        with redirect_stdout(buf):
            ph.print_report(r, filter_layer="1.1")
        out = buf.getvalue()
        self.assertIn("STALE", out)
        self.assertIn("Stale data", out)


class AppSmoke(unittest.TestCase):
    def test_parses(self):
        with open(os.path.join(ROOT, "streamlit_app.py")) as fh:
            ast.parse(fh.read())

    def test_apptest_with_fake_redis(self):
        try:
            from streamlit.testing.v1 import AppTest
        except Exception:  # pragma: no cover
            self.skipTest("streamlit AppTest unavailable")
        import streamlit as st

        st.cache_resource.clear()
        st.cache_data.clear()
        fake = seeded()
        os.environ["DASHBOARD_REFRESH_SEC"] = "0"
        dd.set_client_factory(lambda: fake)
        cwd = os.getcwd()
        os.chdir(ROOT)
        try:
            at = AppTest.from_file(os.path.join(ROOT, "streamlit_app.py"), default_timeout=120)
            at.run()
            self.assertEqual([e.value for e in at.exception], [])
            self.assertEqual([e.value for e in at.error], [])
            labels = [t.label for t in at.tabs]
            for name in ("Decision chain", "Symbol data", "Execution", "TSL", "Positions & journal", "Streams",
                         "Pipeline health", "Raw Redis explorer"):
                self.assertIn(name, labels)
            self.assertGreater(len(at.dataframe), 15)
            # change symbol + stream row count, rerun
            at.sidebar.selectbox[0].set_value("TCS").run()
            self.assertEqual([e.value for e in at.exception], [])
            self.assertEqual(fake.writes, [], "dashboard must be read-only")
        finally:
            os.chdir(cwd)
            dd.set_client_factory(None)
            os.environ.pop("DASHBOARD_REFRESH_SEC", None)

    def test_apptest_redis_down(self):
        try:
            from streamlit.testing.v1 import AppTest
        except Exception:  # pragma: no cover
            self.skipTest("streamlit AppTest unavailable")

        class Down(FakeRedis):
            def ping(self):
                raise ConnectionError("refused")

        os.environ["DASHBOARD_REFRESH_SEC"] = "0"
        dd.set_client_factory(lambda: Down())
        try:
            import streamlit as st
            st.cache_resource.clear()
            at = AppTest.from_file(os.path.join(ROOT, "streamlit_app.py"), default_timeout=60).run()
            self.assertEqual([e.value for e in at.exception], [])
            self.assertTrue(any("Cannot connect" in e.value for e in at.error))
        finally:
            dd.set_client_factory(None)
            os.environ.pop("DASHBOARD_REFRESH_SEC", None)
            import streamlit as st
            st.cache_resource.clear()


class WorkerLiveness(unittest.TestCase):
    def _env(self, pid):
        import tempfile

        d = tempfile.mkdtemp()
        run_all = os.path.join(d, "run_all.sh")
        with open(run_all, "w") as f:
            f.write('start "entry_trigger" python3 run_entry_trigger.py\n')
        os.makedirs(os.path.join(d, "logs", "2026-10-05", "pids"))
        with open(os.path.join(d, "logs", "2026-10-05", "pids", "entry_trigger.pid"), "w") as f:
            f.write(str(pid))
        return ph.worker_pid_names(run_all), os.path.join(d, "logs")

    def test_alive_and_dead_pid(self):
        names, logs = self._env(os.getpid())
        self.assertEqual(names, {"run_entry_trigger.py": "entry_trigger"})
        self.assertTrue(ph.worker_alive("run_entry_trigger.py", names, logs))
        names, logs = self._env(2 ** 22 + 12345)
        self.assertFalse(ph.worker_alive("run_entry_trigger.py", names, logs))
        self.assertIsNone(ph.worker_alive("run_unknown.py", names, logs))

    def test_running_worker_without_output_is_idle_not_fail(self):
        orig = ph.worker_alive
        try:
            ph.worker_alive = lambda w, names=None, log_dir=None: True
            rows = ph.evaluate_all(FakeRedis(), keys=[], check_workers=True)
            entry = next(x for x in rows if x["worker"] == "run_entry_trigger.py")
            self.assertEqual(entry["status"], "IDLE")
            ph.worker_alive = lambda w, names=None, log_dir=None: False
            rows = ph.evaluate_all(seeded(), check_workers=True)
            self.assertTrue(all(x["status"] == "DOWN" for x in rows if x.get("worker")))
        finally:
            ph.worker_alive = orig
        plain = ph.evaluate_all(FakeRedis(), keys=[])
        self.assertEqual(next(x for x in plain if x["worker"] == "run_entry_trigger.py")["status"], "FAIL")


if __name__ == "__main__":
    unittest.main()

"""
stop_all.sh / run_all.sh tests: syntax check plus a functional run of
stop_all.sh against dummy processes and fake pid files in a temp LOG_DIR.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STOP = ROOT / "stop_all.sh"
RUN = ROOT / "run_all.sh"


def _spawn_orphan(cwd: Path, cmd: str) -> int:
    """Start `cmd` detached (re-parented to init, so no zombie lingers) and return its pid."""
    out = subprocess.check_output(
        ["bash", "-c", f"nohup {cmd} >/dev/null 2>&1 & echo $!"], cwd=str(cwd)
    )
    return int(out.decode().strip())


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    # a zombie still answers kill -0; treat it as gone
    try:
        with open(f"/proc/{pid}/stat") as fh:
            return fh.read().split(")")[-1].split()[0] != "Z"
    except OSError:
        return True


@unittest.skipIf(shutil.which("bash") is None, "bash not available")
class ShellSyntaxTest(unittest.TestCase):
    def test_bash_n(self):
        for script in (STOP, RUN):
            r = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, f"{script.name}: {r.stderr}")

    def test_run_all_mentions_stop_all_and_keeps_workers(self):
        text = RUN.read_text()
        self.assertIn("./stop_all.sh", text)
        self.assertNotIn("kill \\$(cat", text)
        for runner in ("run_producer.py", "run_order_executor.py", "run_capital_alloc.py",
                       "run_archiver_layers.py all", "run_adaptive_tsl.py"):
            self.assertIn(runner, text)


@unittest.skipIf(shutil.which("bash") is None or not Path("/proc").is_dir(), "needs bash + /proc")
class StopAllFunctionalTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.base = self.tmp / "pipeline"
        self.base.mkdir()
        self.logs = self.base / "logs"
        self.other = self.tmp / "elsewhere"
        self.other.mkdir()
        self.pids = []

    def tearDown(self):
        for pid in self.pids:
            try:
                os.kill(pid, 9)
            except OSError:
                pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _pidfile(self, day: str, name: str, pid: int) -> Path:
        d = self.logs / day / "pids"
        d.mkdir(parents=True, exist_ok=True)
        f = d / f"{name}.pid"
        f.write_text(f"{pid}\n")
        return f

    def _run(self, *args, timeout_s="2"):
        env = dict(os.environ, LOG_DIR=str(self.logs), PIPELINE_DIR=str(self.base),
                   STOP_TIMEOUT=timeout_s)
        return subprocess.run(["bash", str(STOP), *args], capture_output=True, text=True,
                              env=env, timeout=60)

    def test_stops_workers_across_dates_term_then_kill(self):
        # yesterday's run (after midnight the old code found nothing)
        p_old = _spawn_orphan(self.base, "sleep 300")
        # today's run
        p_new = _spawn_orphan(self.base, "sleep 300")
        # ignores SIGTERM -> must be SIGKILLed after STOP_TIMEOUT
        stubborn = (f"{sys.executable} -c \"import signal,time; "
                    "signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(300)\"")
        p_stub = _spawn_orphan(self.base, stubborn)
        # recycled pid: a live process that is NOT ours (different cwd) -> never signalled
        p_foreign = _spawn_orphan(self.other, "sleep 300")
        self.pids += [p_old, p_new, p_stub, p_foreign]
        time.sleep(0.5)  # let the python process install its handler

        f_old = self._pidfile("2026-10-04", "producer", p_old)
        f_new = self._pidfile("2026-10-05", "joiner", p_new)
        f_stub = self._pidfile("2026-10-05", "order_executor", p_stub)
        f_foreign = self._pidfile("2026-10-03", "greeks", p_foreign)
        f_stale = self._pidfile("2026-10-05", "dead", 4194303)  # above default pid_max -> not running

        chk = self._run("--check")
        self.assertEqual(chk.returncode, 1, chk.stdout + chk.stderr)
        self.assertIn("producer", chk.stdout)
        self.assertTrue(f_stale.exists(), "--check must not modify pid files")

        t0 = time.time()
        r = self._run()
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out)
        self.assertGreaterEqual(time.time() - t0, 1.5, "should wait STOP_TIMEOUT before SIGKILL")

        for pid in (p_old, p_new, p_stub):
            deadline = time.time() + 3
            while _alive(pid) and time.time() < deadline:
                time.sleep(0.1)
            self.assertFalse(_alive(pid), f"pid {pid} still alive\n{out}")
        self.assertTrue(_alive(p_foreign), "foreign process must not be killed")
        self.assertIn("KILL order_executor", out)
        self.assertIn("Stopped 3 worker(s)", out)
        for f in (f_old, f_new, f_stub, f_stale, f_foreign):
            self.assertFalse(f.exists(), f"{f} should be removed")

        # nothing left -> --check passes, second stop is a no-op
        self.assertEqual(self._run("--check").returncode, 0)
        r2 = self._run()
        self.assertEqual(r2.returncode, 0)
        self.assertIn("No running pipeline workers", r2.stdout)

    def test_missing_log_dir_is_ok(self):
        r = self._run()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()

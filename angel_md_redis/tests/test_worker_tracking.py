"""Exercise the actual PowerShell worker guards without touching real workers."""
import os
from pathlib import Path
import shutil
import subprocess
import unittest


@unittest.skipUnless(os.name == 'nt' and shutil.which('powershell'), 'Windows PowerShell required')
class WorkerTrackingTests(unittest.TestCase):
    def test_stale_protected_and_exited_processes_are_never_workers_or_kill_targets(self):
        script = Path(__file__).with_suffix('.ps1')
        result = subprocess.run(
            ['powershell', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
             '-File', str(script)], capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('Worker identity regression checks passed', result.stdout)

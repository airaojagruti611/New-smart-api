"""Console-free Task Scheduler entry point for the market-hours controller."""
from __future__ import annotations

import argparse
from datetime import datetime
import os
from pathlib import Path
import subprocess


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    base = Path(__file__).resolve().parent
    logs = base / "logs" / "automation"
    logs.mkdir(parents=True, exist_ok=True)
    shell = Path(os.environ["SystemRoot"]) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
    command = [str(shell), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
               "-WindowStyle", "Hidden", "-File", str(base / "market_automation.ps1")]
    if args.status:
        command.append("-Status")
    # pythonw.exe has no console of its own. CREATE_NO_WINDOW prevents Windows
    # Terminal from creating a tab before PowerShell can apply WindowStyle.
    with (logs / (datetime.now().strftime("%Y-%m-%d") + "-launcher.log")).open("a", encoding="utf-8") as log:
        log.write(f"\n{datetime.now().astimezone().isoformat()} controller launch\n")
        log.flush()
        result = subprocess.run(command, cwd=base, stdin=subprocess.DEVNULL,
                                stdout=log, stderr=subprocess.STDOUT,
                                creationflags=subprocess.CREATE_NO_WINDOW, check=False)
        log.write(f"Controller exit: {result.returncode}\n")
        return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())

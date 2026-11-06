"""Windowless upload-only task; never starts collection or Docker."""
from datetime import datetime
from pathlib import Path
import subprocess
import sys


def main():
    base = Path(__file__).resolve().parent
    logs = base / 'logs' / 'automation'
    logs.mkdir(parents=True, exist_ok=True)
    python = Path(sys.executable).with_name('python.exe')
    stamp = datetime.now().strftime('%Y-%m-%d-%H%M%S')
    command = [str(python), str(base / 'market_automation.py'), 'upload', '--drain']
    with (logs / (stamp + '-drive-worker.log')).open('a', encoding='utf-8') as log:
        result = subprocess.run(command, cwd=base, stdin=subprocess.DEVNULL, stdout=log,
                                stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW)
    return result.returncode


if __name__ == '__main__':
    raise SystemExit(main())

"""Keep the local operations UI available independently of market workers."""
from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys


def main():
    base = Path(__file__).resolve().parent
    logs = base / 'logs' / 'automation'
    logs.mkdir(parents=True, exist_ok=True)
    for key in ('OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS'):
        os.environ.setdefault(key, '1')
    with (logs / (datetime.now().strftime('%Y-%m-%d') + '-operations.log')).open('a', encoding='utf-8') as log:
        return subprocess.run([str(Path(sys.executable).with_name('python.exe')), str(base / 'operations_server.py')],
                              cwd=base, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                              creationflags=subprocess.CREATE_NO_WINDOW).returncode


if __name__ == '__main__':
    raise SystemExit(main())

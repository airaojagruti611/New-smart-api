"""
run_history_bootstrap.py
────────────────────────
One-shot seed of 1d/1m/5m/10m/30m candles + prev-day pivots.
Tries Angel historical API, then Yahoo NSE if Angel returns AG8004 / empty.

Usage:
  python3 run_history_bootstrap.py
  python3 run_history_bootstrap.py --force
"""

import argparse

from app.history_bootstrap import seed_history


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force",
        action="store_true",
        help="Fetch even when streams already have the minimum bar count",
    )
    args = parser.parse_args()
    written = seed_history(force=args.force)
    if written.get("error"):
        raise SystemExit(1)
    print(f"[HISTORY] done {written}")


if __name__ == "__main__":
    main()

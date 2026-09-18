"""
run_history_bootstrap.py
────────────────────────
One-shot seed of 1d/1m/5m/10m/30m candles + prev-day pivots from Angel
historical API. Safe to re-run; skips symbols/intervals that already have
enough bars.
"""

from app.history_bootstrap import seed_history


def main() -> None:
    written = seed_history(force=False)
    if written.get("error"):
        raise SystemExit(1)
    print(f"[HISTORY] done {written}")


if __name__ == "__main__":
    main()

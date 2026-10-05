"""
run_archiver_layers.py
─────────────────────
Redis Streams → Parquet data_lake, same layout as Angel One ticks:

  data_lake/stream=<stream_with_underscores>/dt=YYYY-MM-DD/[symbol=...]/part-<ts>.parquet

Covers Angel One market data plus every stream produced by the pipeline
workers (signal layers, decision layer, executor, TSL, capital allocation).

Usage:
  python run_archiver_layers.py              # all streams
  python run_archiver_layers.py all
  python run_archiver_layers.py expected_move
  python run_archiver_layers.py list
"""

from __future__ import annotations

import sys
import time
import threading
from typing import Dict, Tuple

from app.archiver import StreamParquetArchiver

# name -> (redis stream, consumer, batch_size)
# Existing Angel One consumers keep the same names so a restart continues
# from the last ACK instead of replaying the whole stream.
STREAMS: Dict[str, Tuple[str, str, int]] = {
    # Angel One market data (already archived by the old per-stream scripts)
    "eq": ("md:ticks:eq", "arch-eq-1", 5000),
    "opt": ("md:ticks:opt", "arch-opt-1", 8000),
    "greeks": ("md:greeks:snap", "arch-greeks-1", 2000),
    "features": ("md:features:opt", "arch-features-1", 5000),
    "candles_1m": ("md:candles:1m", "arch-candles-1m-1", 5000),
    "candles_5m": ("md:candles:5m", "arch-candles-5m-1", 2000),
    "candles_10m": ("md:candles:10m", "arch-candles-10m-1", 2000),
    "candles_30m": ("md:candles:30m", "arch-candles-30m-1", 2000),
    "candles_1d": ("md:candles:1d", "arch-candles-1d-1", 2000),
    # Module 1 — Indicator Signals
    "supertrend": ("md:supertrend:bias", "arch-supertrend-1", 2000),
    "ema": ("md:ema:cross", "arch-ema-1", 2000),
    "htf": ("md:htf:trend", "arch-htf-1", 2000),
    "pivots": ("md:pivots:prevday", "arch-pivots-1", 2000),
    "level": ("md:level:entry", "arch-level-1", 2000),
    "momentum": ("md:momentum:confirm", "arch-momentum-1", 2000),
    # Module 2 — Volume
    "volume": ("md:volume:signal", "arch-volume-1", 2000),
    # Module 3 — Market regime
    "regime": ("md:regime", "arch-regime-1", 2000),
    # Module 4 — Bid-ask family
    "bidask": ("md:bidask:signal", "arch-bidask-1", 5000),
    "smartmoney": ("md:smartmoney:signal", "arch-smartmoney-1", 5000),
    "orderflow": ("md:orderflow:signal", "arch-orderflow-1", 5000),
    "imbalance": ("md:imbalance:signal", "arch-imbalance-1", 5000),
    "stockflow": ("md:stockflow:signal", "arch-stockflow-1", 2000),
    "composite": ("md:composite:signal", "arch-composite-1", 2000),
    # Module 5 — Open interest
    "oi": ("md:oi:signal", "arch-oi-1", 2000),
    "oi_und": ("md:oi:underlying:signal", "arch-oi-und-1", 2000),
    # Module 6 — Greeks phase (raw greeks snap is "greeks" above)
    "greeks_phase": ("md:greeks:phase:signal", "arch-greeks-phase-1", 5000),
    # Module 7 — Liquidity
    "liquidity": ("md:liquidity:score:signal", "arch-liquidity-1", 2000),
    "optexit": ("md:optexit:signal", "arch-optexit-1", 2000),
    "strikeflow": ("md:strikeflow:signal", "arch-strikeflow-1", 2000),
    # Module 8 — Expected move
    "expected_move": ("md:expected_move:signal", "arch-expected-move-1", 2000),
    # Inputs to Module 9
    "entry_trigger": ("md:entry:trigger", "arch-entry-trigger-1", 2000),
    "strike_select": ("md:strike:select", "arch-strike-select-1", 2000),
    # Module 10 — Strike Intelligence Engine
    "strike_intel": ("md:strike:intel", "arch-strike-intel-1", 2000),
    # Module 12 — Probability, Modules 11+20 — ICARE, Module 22 — journal
    "probability": ("md:probability", "arch-probability-1", 2000),
    # Module 13 — Trade Ranking decisions + cycle summaries
    "ranking": ("md:ranking", "arch-ranking-1", 2000),
    "ranking_cycle": ("md:ranking:cycle", "arch-ranking-cycle-1", 2000),
    "icare": ("md:icare", "arch-icare-1", 2000),
    "journal": ("md:journal", "arch-journal-1", 500),
    # Module 14 — Order Executor events (orders, fills, final reports, missed moves) + fills
    "exec": ("md:exec", "arch-exec-1", 2000),
    "exec_fill": ("md:exec:fill", "arch-exec-fill-1", 500),
    # Live exit path (order executor): exit requests. Archiving a stream that no
    # producer has written yet is harmless: the consumer group is created with
    # MKSTREAM and the reader just idles until the first entry arrives.
    "exec_exit_request": ("md:exec:exit_request", "arch-exec-exit-request-1", 500),
    # Module 18 — Adaptive trailing SL events + re-entry signals
    "tsl": ("md:tsl", "arch-tsl-1", 2000),
    "tsl_reentry": ("md:tsl:reentry", "arch-tsl-reentry-1", 200),
    # Module 9 — Greeks change
    "greeks_change": ("md:greeks_change:signal", "arch-greeks-change-1", 2000),
    # Capital allocation (run_capital_alloc.py) — sized CALL/PUT notional
    "capital_alloc": ("md:capital:alloc", "arch-capital-alloc-1", 2000),
    # To archive a NEW stream: add one line here
    #   "<name>": ("<redis stream>", "arch-<name>-1", <batch_size>),
    # (consumer names must stay stable so restarts resume from the last ACK).
}

GROUP = "archive"
OUT_DIR = "data_lake"
RESTART_SEC = 5


def _run_one(name: str, stream: str, consumer: str, batch: int) -> None:
    while True:
        try:
            print(f"[ARCH_LAYERS] starting {name} stream={stream} consumer={consumer}")
            StreamParquetArchiver(
                stream=stream,
                group=GROUP,
                consumer=consumer,
                out_dir=OUT_DIR,
                batch_size=batch,
                flush_sec=10,
                partition_by_symbol=True,
            ).run_forever()
        except Exception as e:
            print(f"[ARCH_LAYERS] {name} crashed: {e!r}; restarting in {RESTART_SEC}s")
            time.sleep(RESTART_SEC)


def _start_threads(names: list[str]) -> None:
    threads: list[threading.Thread] = []
    for name in names:
        stream, consumer, batch = STREAMS[name]
        t = threading.Thread(
            target=_run_one,
            args=(name, stream, consumer, batch),
            name=f"arch-{name}",
            daemon=False,
        )
        t.start()
        threads.append(t)

    print(f"[ARCH_LAYERS] {len(threads)} parquet archivers running -> {OUT_DIR}/")
    for t in threads:
        t.join()


def main() -> None:
    arg = (sys.argv[1] if len(sys.argv) > 1 else "all").strip().lower()

    if arg in ("-h", "--help", "help"):
        print("Usage: python run_archiver_layers.py [all|<name>|list]")
        print("Names:")
        for name, (stream, _c, _b) in STREAMS.items():
            print(f"  {name:18s}  {stream}")
        raise SystemExit(0)

    if arg == "list":
        for name, (stream, consumer, batch) in STREAMS.items():
            print(f"{name:18s}  {stream:32s}  {consumer}  batch={batch}")
        raise SystemExit(0)

    if arg == "all":
        _start_threads(list(STREAMS.keys()))
        return

    if arg not in STREAMS:
        print(f"Unknown stream {arg!r}. Use: python run_archiver_layers.py list")
        raise SystemExit(1)

    stream, consumer, batch = STREAMS[arg]
    _run_one(arg, stream, consumer, batch)


if __name__ == "__main__":
    main()

r"""Synthetic shutdown benchmark; writes only to temporary storage, never Redis/Drive.

Run from angel_md_redis:
  .\.venv\Scripts\python.exe scripts\benchmark_archiver.py --baseline runtime\archiver_before_optimization.py
"""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pyarrow.parquet as pq
from app.archiver import StreamParquetArchiver


def measure(cls, rows: int, symbols: int, optimized: bool) -> dict:
    pages = []
    expected = set()
    for start in range(0, rows, 1000):
        messages = []
        for index in range(start, min(rows, start + 1000)):
            stream_id = f"1793258100000-{index}"
            expected.add(stream_id)
            fields = {b"symbol": f"SYMBOL{index % symbols:03d}".encode(), b"ts_recv": b"1793258100000",
                      b"ltp": str(10000 + index % 1000).encode(), b"quantity": str(index % 500).encode()}
            messages.append((stream_id.encode(), fields))
        pages.append([(b"benchmark", messages)])
    stop = threading.Event()
    stop.set()
    with tempfile.TemporaryDirectory(prefix="archive-benchmark-") as directory:
        client = MagicMock()
        client.xautoclaim.return_value = [b"0-0", [], []]
        client.xpending.return_value = {"pending": 0}
        with patch.object(cls, "_ensure_group"), patch("redis.from_url", return_value=client):
            archiver = cls("benchmark", "archive", "bench", out_dir=directory,
                           batch_size=32000 if optimized else 8000,
                           flush_sec=60 if optimized else 10, partition_by_symbol=not optimized)
        archiver._xreadgroup = MagicMock(side_effect=[[], *pages, []])
        started = time.perf_counter()
        archiver.run_forever(stop_event=stop)
        elapsed = time.perf_counter() - started
        files = list(Path(directory).rglob("*.parquet"))
        actual = []
        for file in files:
            table = pq.ParquetFile(file, page_checksum_verification=True).read()
            actual.extend(table["_redis_id"].to_pylist())
        if len(actual) != rows or set(actual) != expected:
            raise RuntimeError("Benchmark lost or duplicated input records")
        return {"rows": rows, "symbols": symbols, "seconds": round(elapsed, 4),
                "rows_per_second": round(rows / elapsed), "parquet_files": len(files),
                "roundtrip_unique_ids_verified": True}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--symbols", type=int, default=100)
    args = parser.parse_args()
    result = {}
    if args.baseline:
        spec = importlib.util.spec_from_file_location("archiver_baseline", args.baseline)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        result["before"] = measure(module.StreamParquetArchiver, args.rows, args.symbols, False)
    result["after"] = measure(StreamParquetArchiver, args.rows, args.symbols, True)
    if "before" in result:
        result["speedup"] = round(result["before"]["seconds"] / result["after"]["seconds"], 2)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

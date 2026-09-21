"""
run_volume_analyzer.py
──────────────────────
Consumes closed 1-minute OHLCV candles from md:candles:1m — the same
clock-aligned bars Supertrend / EMA use — and emits buyer vs seller
volume imbalance per the Volume Analyzer spec.

Output:
  Stream : md:volume:signal  (one entry per symbol per closed 1m candle)
  Key    : md:volume:latest  (closed-candle JSON snapshot, TTL 3600 s)

md:volume:latest is the entry-gate input. It always reflects a completed
1-minute bar (candle_open=0), never a partial tick window.
"""

from __future__ import annotations

import json
import os
from typing import Dict, Set

import redis

from app.candle_io import parse_candle_fields, read_last_candles
from app.candle_types import Candle
from app.config import load_symbols
from app.volume_analyzer import VolumeAnalyzer, VolumeResult

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
IN_1M = os.getenv("STREAM_CANDLES_1M", "md:candles:1m")
OUT_STREAM = os.getenv("STREAM_VOLUME_SIGNAL", "md:volume:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_VOLUME", "20000"))
GROUP = os.getenv("VOLUME_GROUP", "volume")
CONSUMER = os.getenv("VOLUME_CONSUMER", "volume-1")
LATEST_KEY = os.getenv("VOLUME_LATEST_KEY", "md:volume:latest")
AVG_WINDOW = int(os.getenv("VOLUME_AVG_WINDOW", "20"))
LATEST_TTL_SEC = int(os.getenv("VOLUME_LATEST_TTL_SEC", "3600"))

_VOLUME_SYMBOLS_ENV = os.getenv("VOLUME_SYMBOLS", "").strip()


def _load_tracked_symbols() -> Set[str]:
    if _VOLUME_SYMBOLS_ENV:
        return {s.strip().upper() for s in _VOLUME_SYMBOLS_ENV.split(",") if s.strip()}
    return set(load_symbols())


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    # "$" = new messages only. History is seeded via read_last_candles so we
    # do not replay the whole 1m stream into md:volume:signal on first start.
    try:
        r.xgroup_create(stream, group, id="$", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def to_stream_payload(symbol: str, candle: Candle, result: VolumeResult) -> Dict[str, str]:
    return {
        "ts_ms": str(candle.ts_ms),
        "symbol": symbol,
        "tf": "1m",
        "open": f"{candle.o:.2f}",
        "high": f"{result.high:.2f}",
        "low": f"{result.low:.2f}",
        "close": f"{result.close:.2f}",
        "volume": f"{result.volume:.0f}",
        "buy_pct": f"{result.buy_pct:.2f}",
        "sell_pct": f"{result.sell_pct:.2f}",
        "volume_surge": f"{result.volume_surge:.2f}" if result.volume_surge is not None else "",
        "signal": result.signal,
        "candle_open": "0",
    }


def _snapshot_entry(payload: Dict[str, str]) -> Dict[str, str]:
    return {k: v for k, v in payload.items() if k != "symbol"}


def _publish_latest(r: redis.Redis, snapshot: Dict[str, dict]) -> None:
    if not snapshot:
        return
    ts_ms = "0"
    for entry in snapshot.values():
        ts = str(entry.get("ts_ms") or "0")
        if ts > ts_ms:
            ts_ms = ts
    blob = {**snapshot, "ts_ms": ts_ms}
    r.set(LATEST_KEY, json.dumps(blob), ex=LATEST_TTL_SEC)


def _analyze_closed_bar(
    analyzer: VolumeAnalyzer,
    candle: Candle,
) -> VolumeResult:
    result = analyzer.analyze(
        high=candle.h,
        low=candle.l,
        close=candle.c,
        volume=candle.v,
    )
    analyzer.record_candle_volume(candle.v)
    return result


def main() -> None:
    symbols = _load_tracked_symbols()
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_1M, GROUP)

    analyzers: Dict[str, VolumeAnalyzer] = {}
    last_bar_ts: Dict[str, int] = {}
    snapshot: Dict[str, dict] = {}

    print(
        f"[VOLUME] reading {IN_1M}, writing {OUT_STREAM} + {LATEST_KEY}, "
        f"avg_window={AVG_WINDOW}, symbols={sorted(symbols)}"
    )

    for sym in sorted(symbols):
        bars = read_last_candles(r, IN_1M, sym, AVG_WINDOW + 1)
        analyzer = VolumeAnalyzer(avg_window=AVG_WINDOW)
        analyzers[sym] = analyzer
        if not bars:
            print(f"[VOLUME] WARMUP symbol={sym} bars=0")
            continue

        last = bars[-1]
        for prior in bars[:-1]:
            analyzer.record_candle_volume(prior.v)

        result = _analyze_closed_bar(analyzer, last)
        last_bar_ts[sym] = last.ts_ms
        payload = to_stream_payload(sym, last, result)
        snapshot[sym] = _snapshot_entry(payload)
        print(
            "[VOLUME WARMUP]",
            sym,
            f"bar_ts={last.ts_ms}",
            f"O:{last.o:.2f}",
            f"H:{last.h:.2f}",
            f"L:{last.l:.2f}",
            f"C:{last.c:.2f}",
            f"V:{last.v:.0f}",
            f"Buy:{result.buy_pct:.1f}%",
            f"Sell:{result.sell_pct:.1f}%",
            f"Surge:{result.volume_surge}",
            f"hist={analyzer.history_len}/{AVG_WINDOW}",
            f"-> {result.signal}",
        )

    _publish_latest(r, snapshot)

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={IN_1M: ">"},
            count=2000,
            block=2000,
        )
        if not resp:
            continue

        changed = False
        for _stream, msgs in resp:
            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)
                parsed = parse_candle_fields(fields)
                if parsed is None:
                    continue
                sym, candle = parsed
                if symbols and sym not in symbols:
                    continue
                prev_ts = last_bar_ts.get(sym)
                if prev_ts is not None and candle.ts_ms <= prev_ts:
                    continue

                analyzer = analyzers.get(sym)
                if analyzer is None:
                    analyzer = VolumeAnalyzer(avg_window=AVG_WINDOW)
                    analyzers[sym] = analyzer

                result = _analyze_closed_bar(analyzer, candle)
                last_bar_ts[sym] = candle.ts_ms
                payload = to_stream_payload(sym, candle, result)
                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                snapshot[sym] = _snapshot_entry(payload)
                changed = True

                print(
                    "[VOLUME CLOSE]",
                    sym,
                    f"bar_ts={candle.ts_ms}",
                    f"O:{candle.o:.2f}",
                    f"H:{candle.h:.2f}",
                    f"L:{candle.l:.2f}",
                    f"C:{candle.c:.2f}",
                    f"V:{candle.v:.0f}",
                    f"Buy:{result.buy_pct:.1f}%",
                    f"Sell:{result.sell_pct:.1f}%",
                    f"Surge:{result.volume_surge}",
                    f"-> {result.signal}",
                )

            if ack_ids:
                r.xack(IN_1M, GROUP, *ack_ids)

        if changed:
            _publish_latest(r, snapshot)


if __name__ == "__main__":
    main()

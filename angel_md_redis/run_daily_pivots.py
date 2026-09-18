"""
run_daily_pivots.py
───────────────────
Consumes daily candles from md:candles:1d and writes classic pivot levels based on
previous day's H/L/C into:
  Key    : md:pivots:prevday:{SYMBOL}   (live snapshot for signal engines)
  Stream : md:pivots:prevday             (history for data_lake parquet archive)

Downstream signal engines read these levels for intraday pivot-break triggers.
"""

from __future__ import annotations

import os
from typing import Dict, Optional, Tuple

import redis

from app.candle_types import Candle
from app.candles_store import CandlesStore
from app.config import load_symbols
from app.history_bootstrap import seed_pivots_from_daily
from app.pivots import classic_pivots


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
IN_1D_STREAM = os.getenv("STREAM_CANDLES_1D", "md:candles:1d")
OUT_STREAM = os.getenv("STREAM_PIVOTS_PREVDAY", "md:pivots:prevday")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_PIVOTS_PREVDAY", "200000"))

GROUP = os.getenv("PIVOTS_GROUP", "pivots")
CONSUMER = os.getenv("PIVOTS_CONSUMER", "pivots-1")


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _safe_float(v) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _safe_int(v) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(float(v))
    except Exception:
        return None


def _parse_1d_fields(fields: dict) -> Optional[Tuple[str, str, Candle]]:
    sym = str(fields.get("symbol") or "").strip().upper()
    date = str(fields.get("date") or "").strip()
    # date field might be absent; fall back to empty and let pivots writer still store levels keyed by stream arrival.
    ts_ms = _safe_int(fields.get("ts_ms"))
    o = _safe_float(fields.get("o"))
    h = _safe_float(fields.get("h"))
    l = _safe_float(fields.get("l"))
    c = _safe_float(fields.get("c"))
    v = _safe_float(fields.get("v")) or 0.0
    if not sym or ts_ms is None or o is None or h is None or l is None or c is None:
        return None
    return sym, date, Candle(ts_ms=ts_ms, o=o, h=h, l=l, c=c, v=v)


def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_1D_STREAM, GROUP)
    store = CandlesStore()
    symbols = load_symbols()

    # Latest written daily ts so we do not re-emit identical bars.
    last_ts_by_symbol: Dict[str, int] = {}

    print(
        f"[PIVOTS] reading {IN_1D_STREAM} -> writing md:pivots:prevday:{{SYMBOL}} "
        f"and stream {OUT_STREAM}"
    )
    n = seed_pivots_from_daily(r, symbols, store=store)
    print(f"[PIVOTS] seeded {n} symbols from existing daily candles")

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={IN_1D_STREAM: ">"},
            count=5000,
            block=5000,
        )
        if not resp:
            continue

        for _stream, msgs in resp:
            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)
                parsed = _parse_1d_fields(fields)
                if not parsed:
                    continue

                sym, date_str, day_candle = parsed
                if last_ts_by_symbol.get(sym) == day_candle.ts_ms:
                    continue

                # Closed daily bar is the source for the NEXT session's pivots.
                p = classic_pivots(day_candle, date=date_str or "")
                store.write_pivots_prevday(f"md:pivots:prevday:{sym}", p)
                r.xadd(
                    OUT_STREAM,
                    {
                        "ts_ms": str(int(day_candle.ts_ms)),
                        "symbol": sym,
                        "date": p.date,
                        "P": f"{p.P:.2f}",
                        "R1": f"{p.R1:.2f}",
                        "S1": f"{p.S1:.2f}",
                        "R2": f"{p.R2:.2f}",
                        "S2": f"{p.S2:.2f}",
                    },
                    maxlen=OUT_MAXLEN,
                    approximate=True,
                )
                last_ts_by_symbol[sym] = day_candle.ts_ms

            if ack_ids:
                r.xack(IN_1D_STREAM, GROUP, *ack_ids)


if __name__ == "__main__":
    main()


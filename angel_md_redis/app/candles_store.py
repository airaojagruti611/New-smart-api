from __future__ import annotations

import json
from typing import Dict

from .redis_store import RedisStore
from .candle_types import Candle, PivotLevels


def candle_to_payload(symbol: str, tf: str, c: Candle) -> Dict[str, str]:
    return {
        "ts_ms": str(int(c.ts_ms)),
        "symbol": str(symbol).upper(),
        "tf": tf,
        "o": f"{c.o:.2f}",
        "h": f"{c.h:.2f}",
        "l": f"{c.l:.2f}",
        "c": f"{c.c:.2f}",
        "v": f"{c.v:.0f}",
    }


def pivots_to_json(p: PivotLevels) -> str:
    payload = {
        "date": p.date,
        "P": f"{p.P:.2f}",
        "R1": f"{p.R1:.2f}",
        "S1": f"{p.S1:.2f}",
        "R2": f"{p.R2:.2f}",
        "S2": f"{p.S2:.2f}",
    }
    return json.dumps(payload, separators=(",", ":"))


class CandlesStore:
    def __init__(self):
        self.rs = RedisStore()

    def write_candle(self, stream: str, maxlen: int, symbol: str, tf: str, c: Candle) -> None:
        self.rs.xadd(stream, candle_to_payload(symbol, tf, c), maxlen=maxlen)

    def write_pivots_prevday(self, key: str, p: PivotLevels, ex_sec: int = 7 * 24 * 3600) -> None:
        self.rs.set_latest(key, pivots_to_json(p), ex_sec=ex_sec)


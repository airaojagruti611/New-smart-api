import argparse
import json
import os

import redis

from app.candle_types import Candle
from app.trend_filter import mtf_supertrend_bias


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

STREAM_1M = os.getenv("STREAM_CANDLES_1M", "md:candles:1m")
STREAM_5M = os.getenv("STREAM_CANDLES_5M", "md:candles:5m")
STREAM_10M = os.getenv("STREAM_CANDLES_10M", "md:candles:10m")
STREAM_30M = os.getenv("STREAM_CANDLES_30M", "md:candles:30m")


def _parse_candle(fields) -> Candle:
    return Candle(
        ts_ms=int(fields["ts_ms"]),
        o=float(fields["o"]),
        h=float(fields["h"]),
        l=float(fields["l"]),
        c=float(fields["c"]),
        v=float(fields.get("v", "0") or 0),
    )


def read_last_candles(r: redis.Redis, symbol: str, tf: str, limit: int) -> list[Candle]:
    """
    Reads last N candles for (symbol, tf) from the appropriate tf stream.
    """
    stream_by_tf = {
        "1m": STREAM_1M,
        "5m": STREAM_5M,
        "10m": STREAM_10M,
        "30m": STREAM_30M,
    }
    stream = stream_by_tf.get(tf)
    if not stream:
        return []

    resp = r.xrevrange(stream, max="+", min="-", count=max(2000, limit * 5))
    out: list[Candle] = []
    for _msg_id, fields in resp:
        if fields.get("symbol", "").upper() != symbol.upper():
            continue
        out.append(_parse_candle(fields))
        if len(out) >= limit:
            break
    return list(reversed(out))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True, help="e.g. RELIANCE")
    ap.add_argument("--majority", type=int, default=int(os.getenv("ST_MAJORITY", "3")))
    ap.add_argument("--atr", type=int, default=int(os.getenv("ST_ATR", "7")))
    ap.add_argument("--mult", type=float, default=float(os.getenv("ST_MULT", "1.0")))
    ap.add_argument("--bars", type=int, default=int(os.getenv("ST_BARS", "200")))
    ap.add_argument("--tfs", default=os.getenv("ST_TFS", "30m,10m,5m,1m"))
    args = ap.parse_args()

    r = redis.from_url(REDIS_URL, decode_responses=True)

    candles_by_tf = {}
    for tf in [x.strip() for x in args.tfs.split(",") if x.strip()]:
        candles_by_tf[tf] = read_last_candles(r, args.symbol, tf, limit=args.bars)

    res = mtf_supertrend_bias(
        candles_by_tf=candles_by_tf,
        atr_period=args.atr,
        multiplier=args.mult,
        majority=args.majority,
    )

    print(
        json.dumps(
            {
                "symbol": args.symbol.upper(),
                "bias": res.bias,
                "bullish": res.bullish,
                "bearish": res.bearish,
                "per_tf": res.per_tf,
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()


"""
run_volume_analyzer.py
──────────────────────
Reads raw equity ticks from md:ticks:eq and operates in two modes:

  Live (event-driven):
    On every batch of incoming ticks, computes a partial-candle volume signal
    and writes it to md:volume:latest (throttled to VOLUME_LIVE_THROTTLE_SEC,
    default 1 s).  The snapshot includes "candle_open": "1" to signal the
    candle is still in progress.

  Candle close (timer-driven, every VOLUME_INTERVAL_SEC = 60 s):
    Closes the current candle, updates the rolling avg_volume history, writes
    a confirmed entry to md:volume:signal stream and overwrites md:volume:latest
    with "candle_open": "0".

Output:
  Stream : md:volume:signal  (one entry per symbol per closed candle)
  Key    : md:volume:latest  (live JSON snapshot, updated ~every 1 s, TTL 3600 s)

Consumer group is independent of run_market_regime.py — both read the same
stream without interfering with each other.
"""

import json
import os
import time
from typing import Dict, Optional

import redis

from app.volume_analyzer import VolumeAnalyzer, VolumeResult

REDIS_URL        = os.getenv("REDIS_URL",         "redis://localhost:6379/0")
EQ_STREAM        = os.getenv("STREAM_EQ",         "md:ticks:eq")
OUT_STREAM       = os.getenv("STREAM_VOLUME_SIGNAL", "md:volume:signal")
OUT_MAXLEN       = int(os.getenv("STREAM_MAXLEN_VOLUME",  "20000"))
GROUP            = os.getenv("VOLUME_GROUP",       "volume")
CONSUMER         = os.getenv("VOLUME_CONSUMER",    "volume-1")
INTERVAL_SEC     = int(os.getenv("VOLUME_INTERVAL_SEC",   "60"))
LATEST_KEY       = os.getenv("VOLUME_LATEST_KEY",  "md:volume:latest")
AVG_WINDOW       = int(os.getenv("VOLUME_AVG_WINDOW",     "20"))

# Symbols to track for volume analysis (comma-separated env var, or all seen).
# Example: VOLUME_SYMBOLS=NIFTY,BANKNIFTY
_VOLUME_SYMBOLS_ENV = os.getenv("VOLUME_SYMBOLS", "").strip()
VOLUME_SYMBOLS = (
    {s.strip().upper() for s in _VOLUME_SYMBOLS_ENV.split(",") if s.strip()}
    if _VOLUME_SYMBOLS_ENV
    else None   # None = track every symbol seen on the stream
)


def _safe_float(v) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


class CandleBuilder:
    """Accumulates raw ticks for one symbol into a running OHLCV candle."""

    __slots__ = ("open", "high", "low", "close", "volume", "_first")

    def __init__(self):
        self.open: Optional[float]   = None
        self.high: Optional[float]   = None
        self.low:  Optional[float]   = None
        self.close: Optional[float]  = None
        self.volume: float           = 0.0
        self._first: bool            = True

    def update(self, ltp: float, tick_vol: float) -> None:
        if self._first:
            self.open  = ltp
            self.high  = ltp
            self.low   = ltp
            self._first = False
        else:
            if ltp > self.high:
                self.high = ltp
            if ltp < self.low:
                self.low = ltp
        self.close   = ltp
        self.volume += tick_vol

    def is_ready(self) -> bool:
        return self.open is not None

    def reset(self) -> None:
        self.open    = None
        self.high    = None
        self.low     = None
        self.close   = None
        self.volume  = 0.0
        self._first  = True


def to_stream_payload(ts_ms: int, symbol: str, result: VolumeResult) -> Dict[str, str]:
    return {
        "ts_ms":        str(ts_ms),
        "symbol":       symbol,
        "high":         f"{result.high:.2f}",
        "low":          f"{result.low:.2f}",
        "close":        f"{result.close:.2f}",
        "volume":       f"{result.volume:.0f}",
        "buy_pct":      f"{result.buy_pct:.2f}",
        "sell_pct":     f"{result.sell_pct:.2f}",
        "volume_surge": f"{result.volume_surge:.2f}" if result.volume_surge is not None else "",
        "signal":       result.signal,
    }


def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, EQ_STREAM, GROUP)

    # Per-symbol state
    candles:   Dict[str, CandleBuilder]  = {}
    analyzers: Dict[str, VolumeAnalyzer] = {}

    # Track previous cumulative volume per symbol to compute per-tick delta
    prev_vol:  Dict[str, float] = {}

    next_eval = time.time() + INTERVAL_SEC

    # Live publish throttle: update Redis key at most once per second on tick-driven updates
    LIVE_THROTTLE_SEC = float(os.getenv("VOLUME_LIVE_THROTTLE_SEC", "1.0"))
    last_live_publish = 0.0

    print(
        f"[VOLUME] reading {EQ_STREAM}, writing {OUT_STREAM}, "
        f"candle_interval={INTERVAL_SEC}s, avg_window={AVG_WINDOW}, "
        f"live_throttle={LIVE_THROTTLE_SEC}s"
    )
    if VOLUME_SYMBOLS:
        print(f"[VOLUME] tracking symbols: {sorted(VOLUME_SYMBOLS)}")
    else:
        print("[VOLUME] tracking ALL symbols seen on stream")

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={EQ_STREAM: ">"},
            count=1000,
            block=1000,
        )

        changed = False
        if resp:
            for _stream, msgs in resp:
                ack_ids = []
                for msg_id, fields in msgs:
                    symbol = str(fields.get("symbol") or "").strip().upper()
                    if not symbol:
                        ack_ids.append(msg_id)
                        continue

                    if VOLUME_SYMBOLS and symbol not in VOLUME_SYMBOLS:
                        ack_ids.append(msg_id)
                        continue

                    ltp = _safe_float(fields.get("ltp"))
                    # ws_producer emits cumulative day volume as "vol"
                    # (keep fallback to "v" for compatibility with older payloads)
                    cum_vol = _safe_float(fields.get("vol"))
                    if cum_vol is None:
                        cum_vol = _safe_float(fields.get("v"))

                    if ltp is None:
                        ack_ids.append(msg_id)
                        continue

                    # Compute incremental tick volume from cumulative
                    if cum_vol is not None and cum_vol >= 0:
                        tick_vol = max(0.0, cum_vol - prev_vol.get(symbol, cum_vol))
                        prev_vol[symbol] = cum_vol
                    else:
                        tick_vol = 0.0

                    if symbol not in candles:
                        candles[symbol]   = CandleBuilder()
                        analyzers[symbol] = VolumeAnalyzer(avg_window=AVG_WINDOW)

                    candles[symbol].update(ltp, tick_vol)
                    ack_ids.append(msg_id)
                    changed = True

                if ack_ids:
                    r.xack(EQ_STREAM, GROUP, *ack_ids)

        # ── Live update: publish partial-candle signal whenever new ticks arrived ──
        now = time.time()
        if changed and (now - last_live_publish) >= LIVE_THROTTLE_SEC:
            ts_ms         = int(now * 1000)
            live_snapshot = {}

            for symbol, candle in candles.items():
                if not candle.is_ready():
                    continue

                analyzer = analyzers[symbol]
                # Analyze current partial candle — do NOT record or reset
                result = analyzer.analyze(
                    high=candle.high,
                    low=candle.low,
                    close=candle.close,
                    volume=candle.volume,
                )
                live_snapshot[symbol] = {
                    "ts_ms":        str(ts_ms),
                    "signal":       result.signal,
                    "buy_pct":      f"{result.buy_pct:.2f}",
                    "sell_pct":     f"{result.sell_pct:.2f}",
                    "volume":       f"{result.volume:.0f}",
                    "volume_surge": f"{result.volume_surge:.2f}" if result.volume_surge is not None else "",
                    "high":         f"{result.high:.2f}",
                    "low":          f"{result.low:.2f}",
                    "close":        f"{result.close:.2f}",
                    "candle_open":  "1",   # partial / live candle
                }

            if live_snapshot:
                live_snapshot["ts_ms"] = str(ts_ms)
                r.set(LATEST_KEY, json.dumps(live_snapshot), ex=3600)

            last_live_publish = now

        # ── Candle close: every INTERVAL_SEC flush completed candles ──
        if now >= next_eval:
            ts_ms    = int(now * 1000)
            snapshot = {}

            for symbol, candle in candles.items():
                if not candle.is_ready():
                    continue

                analyzer = analyzers[symbol]
                result   = analyzer.analyze(
                    high=candle.high,
                    low=candle.low,
                    close=candle.close,
                    volume=candle.volume,
                )
                # Only update rolling avg_volume on candle close
                analyzer.record_candle_volume(candle.volume)

                payload = to_stream_payload(ts_ms, symbol, result)
                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)

                snapshot[symbol] = {k: v for k, v in payload.items() if k != "symbol"}
                snapshot[symbol]["candle_open"] = "0"   # closed candle

                print(
                    "[VOLUME CLOSE]",
                    symbol,
                    f"O:{candle.open:.2f}",
                    f"H:{candle.high:.2f}",
                    f"L:{candle.low:.2f}",
                    f"C:{candle.close:.2f}",
                    f"V:{candle.volume:.0f}",
                    f"Buy:{result.buy_pct:.1f}%",
                    f"Sell:{result.sell_pct:.1f}%",
                    f"Surge:{result.volume_surge}",
                    f"→ {result.signal}",
                )

                candle.reset()

            if snapshot:
                snapshot["ts_ms"] = str(ts_ms)
                r.set(LATEST_KEY, json.dumps(snapshot), ex=3600)
                last_live_publish = now   # reset throttle after candle close write

            while next_eval <= now:
                next_eval += INTERVAL_SEC


if __name__ == "__main__":
    main()

"""
run_bidask_analyzer.py
───────────────────────
Bid-Ask Intelligence Module — reads md:ticks:eq (per symbol) and md:ticks:opt
(per contract), requires bid/ask/top-5-depth fields published by
ws_producer.py, and emits liquidity signals:

  Stream : md:bidask:signal
  Key    : md:bidask:latest:{SYMBOL}          (equities)
  Key    : md:bidask:latest:{TRADINGSYMBOL}   (options)

Stock path : fixed spread% thresholds (HIGH_LIQUIDITY / MODERATE_LIQUIDITY / THIN_AVOID)
Option path: spread% normalized against the CONTRACT'S OWN rolling average
             spread% (NORMAL / CAUTION / EXIT_TERRITORY)

Both paths also carry a 0-100 liquidity_score: top-5 size-weighted depth
normalized against its own rolling average depth.

Runs as an independent consumer group — does not interfere with
run_market_regime.py / run_volume_analyzer.py / run_candles_publisher.py,
which all read the same eq/opt streams independently.
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, List, Optional

import redis

from app.bidask_analyzer import BidAskAnalyzer, BidAskResult
from app.config import load_symbols
from app.logging_setup import setup_logger

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")
OPT_STREAM = os.getenv("STREAM_OPT", "md:ticks:opt")

OUT_STREAM = os.getenv("STREAM_BIDASK_SIGNAL", "md:bidask:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_BIDASK", "200000"))
LATEST_KEY_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")

GROUP = os.getenv("BIDASK_GROUP", "bidask")
CONSUMER = os.getenv("BIDASK_CONSUMER", "bidask-1")

DEPTH_AVG_WINDOW = int(os.getenv("BIDASK_DEPTH_AVG_WINDOW", "20"))
# NOTE: brief specifies "10-day average spread" for options. This is a
# rolling SAMPLE window (in-memory, resets on restart), not a calendar-day
# store — consistent with VOLUME_AVG_WINDOW / ST_WINDOW_BARS elsewhere in
# this pipeline. For a true multi-day EOD version, mirror run_daily_pivots.py
# (daily snapshot -> Redis list, capped at 10).
SPREAD_AVG_WINDOW = int(os.getenv("BIDASK_SPREAD_AVG_WINDOW", "20"))

LIVE_THROTTLE_SEC = float(os.getenv("BIDASK_LIVE_THROTTLE_SEC", "1.0"))
LATEST_TTL_SEC = int(os.getenv("BIDASK_LATEST_TTL_SEC", "3600"))

log = setup_logger("bidask_analyzer")


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


def _parse_depth5(raw: str) -> List[float]:
    if not raw:
        return []
    out: List[float] = []
    for part in raw.split(","):
        v = _safe_float(part)
        if v is not None:
            out.append(v)
    return out


def _to_payload(key: str, kind: str, res: BidAskResult, now_ms: int) -> Dict[str, str]:
    return {
        "ts_ms": str(now_ms),
        "key": key,
        "kind": kind,  # "eq" / "opt"
        "bid": f"{res.bid:.4f}",
        "ask": f"{res.ask:.4f}",
        "raw_spread": f"{res.raw_spread:.4f}",
        "spread_pct": f"{res.spread_pct:.4f}",
        "mid": f"{res.mid:.4f}",
        "depth": f"{res.depth:.0f}",
        "liquidity_score": f"{res.liquidity_score:.2f}",
        "signal": res.signal,
        "spread_ratio": "" if res.spread_ratio is None else f"{res.spread_ratio:.2f}",
    }


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)

    ensure_group(r, EQ_STREAM, GROUP)
    ensure_group(r, OPT_STREAM, GROUP)

    eq_analyzers: Dict[str, BidAskAnalyzer] = {}
    opt_analyzers: Dict[str, BidAskAnalyzer] = {}

    last_publish: Dict[str, float] = {}

    log.info(
        "START reading %s + %s -> %s + %s{{KEY}} "
        "(depth_avg_window=%s spread_avg_window=%s throttle=%ss symbols=%d)",
        EQ_STREAM,
        OPT_STREAM,
        OUT_STREAM,
        LATEST_KEY_PREFIX,
        DEPTH_AVG_WINDOW,
        SPREAD_AVG_WINDOW,
        LIVE_THROTTLE_SEC,
        len(symbols),
    )

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={EQ_STREAM: ">", OPT_STREAM: ">"},
            count=2000,
            block=2000,
        )
        if not resp:
            continue

        now = time.time()
        now_ms = int(now * 1000)

        for stream, msgs in resp:
            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)

                if stream == EQ_STREAM:
                    sym = str(fields.get("symbol") or "").strip().upper()
                    if not sym or sym not in symbols:
                        log.debug("SKIP unknown_symbol stream=eq symbol=%r", sym)
                        continue
                    key, kind = sym, "eq"
                    analyzers = eq_analyzers
                else:
                    tsym = str(fields.get("tradingsymbol") or "").strip().upper()
                    if not tsym:
                        log.debug("SKIP missing_tradingsymbol stream=opt")
                        continue
                    key, kind = tsym, "opt"
                    analyzers = opt_analyzers

                bid = _safe_float(fields.get("bid"))
                ask = _safe_float(fields.get("ask"))
                if bid is None or ask is None or bid <= 0 or ask <= 0:
                    log.debug("SKIP no_quote key=%s kind=%s bid=%s ask=%s", key, kind, bid, ask)
                    continue

                bid_sizes = _parse_depth5(fields.get("bid_depth5") or "")
                ask_sizes = _parse_depth5(fields.get("ask_depth5") or "")

                if key not in analyzers:
                    analyzers[key] = BidAskAnalyzer(
                        depth_avg_window=DEPTH_AVG_WINDOW,
                        spread_avg_window=SPREAD_AVG_WINDOW,
                        is_option=(kind == "opt"),
                    )

                res = analyzers[key].analyze(bid, ask, bid_sizes, ask_sizes)

                log.debug(
                    "LOGIC key=%s kind=%s bid=%.2f ask=%.2f spread_pct=%.4f "
                    "depth=%.0f liq_score=%.2f signal=%s ratio=%s",
                    key,
                    kind,
                    res.bid,
                    res.ask,
                    res.spread_pct,
                    res.depth,
                    res.liquidity_score,
                    res.signal,
                    res.spread_ratio,
                )

                prev_t = last_publish.get(key, 0.0)
                if (now - prev_t) < LIVE_THROTTLE_SEC:
                    continue
                last_publish[key] = now

                payload = _to_payload(key, kind, res, now_ms)
                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(
                    f"{LATEST_KEY_PREFIX}{key}",
                    json.dumps(payload, separators=(",", ":")),
                    ex=LATEST_TTL_SEC,
                )

                if res.signal in ("THIN_AVOID", "CAUTION", "EXIT_TERRITORY"):
                    log.info("EMIT key=%s kind=%s payload=%s", key, kind, payload)
                else:
                    log.debug("EMIT key=%s kind=%s payload=%s", key, kind, payload)

            if ack_ids:
                r.xack(stream, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

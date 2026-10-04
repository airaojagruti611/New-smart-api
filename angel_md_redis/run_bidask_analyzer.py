"""
run_bidask_analyzer.py
───────────────────────
Bid-Ask Intelligence Module — reads md:ticks:eq (per symbol) and md:ticks:opt
(per contract), requires bid/ask/top-5-depth fields published by
ws_producer.py, and emits liquidity signals:

  Stream : md:bidask:signal
  Key    : md:bidask:latest:{SYMBOL}          (equities)
  Key    : md:bidask:latest:{TRADINGSYMBOL}   (options)
  Hist   : md:bidask:spread_hist:{TRADINGSYMBOL}  (up to 10 daily avg spread%)

Stock path : fixed spread% thresholds (HIGH_LIQUIDITY / MODERATE_LIQUIDITY / THIN_AVOID)
Option path: spread% normalized against the CONTRACT'S OWN 10-day average
             spread% (NORMAL / CAUTION / EXIT_TERRITORY). Until daily history
             exists, uses the session mean — not a 20-tick window — so a
             single tick-size change cannot flip EXIT_TERRITORY.

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
from typing import Any, Dict, List, Optional

import redis

from app.bidask_analyzer import (
    OPTION_SPREAD_DAYS,
    SESSION_MIN_SAMPLES,
    BidAskAnalyzer,
    BidAskResult,
    ist_today,
)
from app.config import load_symbols
from app.logging_setup import setup_logger

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")
OPT_STREAM = os.getenv("STREAM_OPT", "md:ticks:opt")

OUT_STREAM = os.getenv("STREAM_BIDASK_SIGNAL", "md:bidask:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_BIDASK", "200000"))
LATEST_KEY_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")
SPREAD_HIST_PREFIX = os.getenv("BIDASK_SPREAD_HIST_PREFIX", "md:bidask:spread_hist:")

GROUP = os.getenv("BIDASK_GROUP", "bidask")
CONSUMER = os.getenv("BIDASK_CONSUMER", "bidask-1")

DEPTH_AVG_WINDOW = int(os.getenv("BIDASK_DEPTH_AVG_WINDOW", "20"))
SPREAD_HIST_DAYS = int(os.getenv("BIDASK_SPREAD_HIST_DAYS", str(OPTION_SPREAD_DAYS)))
SESSION_MIN = int(os.getenv("BIDASK_SESSION_MIN_SAMPLES", str(SESSION_MIN_SAMPLES)))
SPREAD_HIST_TTL_SEC = int(os.getenv("BIDASK_SPREAD_HIST_TTL_SEC", str(14 * 24 * 3600)))
HIST_FLUSH_SEC = float(os.getenv("BIDASK_HIST_FLUSH_SEC", "60"))

LIVE_THROTTLE_SEC = float(os.getenv("BIDASK_LIVE_THROTTLE_SEC", "1.0"))
# Keep latest keys short-lived so illiquid option CAUTION/EXIT cannot sit
# unread for tens of minutes. Equity ticks every second so this is plenty.
LATEST_TTL_SEC = int(os.getenv("BIDASK_LATEST_TTL_SEC", "120"))

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


def load_spread_hist(r: redis.Redis, key: str) -> List[Dict[str, Any]]:
    raw = r.get(f"{SPREAD_HIST_PREFIX}{key}")
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    out: List[Dict[str, Any]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        date = str(row.get("date") or "").strip()
        avg = _safe_float(row.get("avg"))
        n = int(row.get("n") or 0)
        if date and avg and avg > 0:
            out.append({"date": date, "avg": avg, "n": n})
    return out[-SPREAD_HIST_DAYS:]


def upsert_spread_day(r: redis.Redis, key: str, date: str, avg: float, n: int) -> None:
    hist = [h for h in load_spread_hist(r, key) if h.get("date") != date]
    hist.append({"date": date, "avg": round(float(avg), 6), "n": int(n)})
    hist = hist[-SPREAD_HIST_DAYS:]
    r.set(
        f"{SPREAD_HIST_PREFIX}{key}",
        json.dumps(hist, separators=(",", ":")),
        ex=SPREAD_HIST_TTL_SEC,
    )


def prior_daily_avgs(hist: List[Dict[str, Any]], today: str) -> List[float]:
    return [float(h["avg"]) for h in hist if h.get("date") and h["date"] != today]


def flush_option_hist(
    r: redis.Redis,
    analyzers: Dict[str, BidAskAnalyzer],
) -> int:
    """Persist closed days + today's running session mean. Returns rows written."""
    written = 0
    for key, az in analyzers.items():
        rolled = az.roll_if_new_day()
        if rolled:
            date, avg, n = rolled
            upsert_spread_day(r, key, date, avg, n)
            written += 1
        snap = az.session_snapshot()
        if snap:
            date, avg, n = snap
            upsert_spread_day(r, key, date, avg, n)
            written += 1
    return written


def _to_payload(key: str, kind: str, res: BidAskResult, now_ms: int, tick: Optional[dict] = None) -> Dict[str, str]:
    tick = tick or {}
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
        "spread_avg": "" if res.spread_avg is None else f"{res.spread_avg:.4f}",
        "spread_avg_source": res.spread_avg_source or "",
        "spread_days": str(int(res.spread_days or 0)),
        # raw book for the Order Executor (DECISION.md §7 E5): top-of-book sizes + 5 levels
        "ltp": str(tick.get("ltp") or ""),
        "bid_qty": str(tick.get("bid_sz") or ""),
        "ask_qty": str(tick.get("ask_sz") or ""),
        "bid_depth5": str(tick.get("bid_depth5") or ""),
        "ask_depth5": str(tick.get("ask_depth5") or ""),
        "bid_depth5_px": str(tick.get("bid_depth5_px") or ""),
        "ask_depth5_px": str(tick.get("ask_depth5_px") or ""),
    }


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)

    ensure_group(r, EQ_STREAM, GROUP)
    ensure_group(r, OPT_STREAM, GROUP)

    eq_analyzers: Dict[str, BidAskAnalyzer] = {}
    opt_analyzers: Dict[str, BidAskAnalyzer] = {}

    last_publish: Dict[str, float] = {}
    last_hist_flush = 0.0

    log.info(
        "START reading %s + %s -> %s + %s{{KEY}} "
        "(depth_avg_window=%s spread_hist_days=%s session_min=%s "
        "hist_flush=%ss throttle=%ss latest_ttl=%ss symbols=%d)",
        EQ_STREAM,
        OPT_STREAM,
        OUT_STREAM,
        LATEST_KEY_PREFIX,
        DEPTH_AVG_WINDOW,
        SPREAD_HIST_DAYS,
        SESSION_MIN,
        HIST_FLUSH_SEC,
        LIVE_THROTTLE_SEC,
        LATEST_TTL_SEC,
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
        now = time.time()
        if now - last_hist_flush >= HIST_FLUSH_SEC:
            n = flush_option_hist(r, opt_analyzers)
            last_hist_flush = now
            if n:
                log.debug("HIST_FLUSH rows=%d analyzers=%d", n, len(opt_analyzers))

        if not resp:
            continue

        now_ms = int(now * 1000)
        today = ist_today()

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
                    daily = []
                    if kind == "opt":
                        daily = prior_daily_avgs(load_spread_hist(r, key), today)
                    analyzers[key] = BidAskAnalyzer(
                        depth_avg_window=DEPTH_AVG_WINDOW,
                        is_option=(kind == "opt"),
                        daily_spread_avgs=daily,
                        session_min_samples=SESSION_MIN,
                        max_spread_days=SPREAD_HIST_DAYS,
                    )
                    if kind == "opt":
                        log.debug(
                            "INIT key=%s kind=opt prior_days=%d avgs=%s",
                            key,
                            len(daily),
                            [round(x, 4) for x in daily],
                        )

                res = analyzers[key].analyze(bid, ask, bid_sizes, ask_sizes)

                log.debug(
                    "LOGIC key=%s kind=%s bid=%.2f ask=%.2f spread_pct=%.4f "
                    "depth=%.0f liq_score=%.2f signal=%s ratio=%s "
                    "avg=%s src=%s days=%s",
                    key,
                    kind,
                    res.bid,
                    res.ask,
                    res.spread_pct,
                    res.depth,
                    res.liquidity_score,
                    res.signal,
                    res.spread_ratio,
                    res.spread_avg,
                    res.spread_avg_source,
                    res.spread_days,
                )

                prev_t = last_publish.get(key, 0.0)
                if (now - prev_t) < LIVE_THROTTLE_SEC:
                    continue
                last_publish[key] = now

                payload = _to_payload(key, kind, res, now_ms, fields)
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

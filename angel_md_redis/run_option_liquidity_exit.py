"""
run_option_liquidity_exit.py
───────────────────────
Option Exit When Spread Widens & Liquidity Disappears — reads md:ticks:opt
(bid/ask/sizes, already published by ws_producer.py) and md:ticks:eq (spot,
for the "stock hasn't moved" check in Stage 4), and emits:

  Stream : md:optexit:signal
  Key    : md:optexit:latest:{TRADINGSYMBOL}

Not throttled — this is a protection mechanism; every tick matters.
Own consumer group, independent of every other reader on md:ticks:opt /
md:ticks:eq.
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, Optional

import redis

from app.config import load_symbols
from app.logging_setup import setup_logger
from app.option_liquidity_exit import LiquidityExitResult, OptionLiquidityExitDetector

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")
OPT_STREAM = os.getenv("STREAM_OPT", "md:ticks:opt")

OUT_STREAM = os.getenv("STREAM_OPTEXIT_SIGNAL", "md:optexit:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_OPTEXIT", "200000"))
LATEST_KEY_PREFIX = os.getenv("OPTEXIT_LATEST_PREFIX", "md:optexit:latest:")

GROUP = os.getenv("OPTEXIT_GROUP", "optexit")
CONSUMER = os.getenv("OPTEXIT_CONSUMER", "optexit-1")

LATEST_TTL_SEC = int(os.getenv("OPTEXIT_LATEST_TTL_SEC", "3600"))

log = setup_logger("option_liquidity_exit")


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


def _to_payload(tsym: str, underlying: str, res: LiquidityExitResult, now_ms: int) -> Dict[str, str]:
    return {
        "ts_ms": str(now_ms),
        "tradingsymbol": tsym,
        "underlying": underlying,
        "spread_pct": f"{res.spread_pct:.4f}",
        "spread_session_avg": "" if res.spread_session_avg is None else f"{res.spread_session_avg:.4f}",
        "bid_size": f"{res.bid_size:.0f}",
        "bid_size_session_avg": "" if res.bid_size_session_avg is None else f"{res.bid_size_session_avg:.1f}",
        "refresh_rate": "" if res.refresh_rate is None else f"{res.refresh_rate:.3f}",
        "refresh_session_avg": "" if res.refresh_session_avg is None else f"{res.refresh_session_avg:.3f}",
        "bid_stale_sec": "" if res.bid_stale_sec is None else f"{res.bid_stale_sec:.2f}",
        "ask_stale_sec": "" if res.ask_stale_sec is None else f"{res.ask_stale_sec:.2f}",
        "bid_drop_pct": "" if res.bid_drop_pct is None else f"{res.bid_drop_pct:.3f}",
        "spot_move_pct": "" if res.spot_move_pct is None else f"{res.spot_move_pct:.4f}",
        "stage1_spread_drift": "1" if res.stage1_spread_drift else "0",
        "stage2_bid_shrink": "1" if res.stage2_bid_shrink else "0",
        "stage3_refresh_slowing": "1" if res.stage3_refresh_slowing else "0",
        "stage4_bid_pull": "1" if res.stage4_bid_pull else "0",
        "stage5_one_sided": "1" if res.stage5_one_sided else "0",
        "exit_status": res.exit_status,
    }


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)

    ensure_group(r, EQ_STREAM, GROUP)
    ensure_group(r, OPT_STREAM, GROUP)

    spot_by_sym: Dict[str, float] = {}
    detectors: Dict[str, OptionLiquidityExitDetector] = {}
    last_status: Dict[str, str] = {}

    log.info(
        "START reading %s + %s -> %s + %s{{TRADINGSYMBOL}} symbols=%d",
        EQ_STREAM, OPT_STREAM, OUT_STREAM, LATEST_KEY_PREFIX, len(symbols),
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

        now_ms = int(time.time() * 1000)

        for stream, msgs in resp:
            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)

                if stream == EQ_STREAM:
                    sym = str(fields.get("symbol") or "").strip().upper()
                    if not sym or sym not in symbols:
                        continue
                    ltp = _safe_float(fields.get("ltp"))
                    if ltp:
                        spot_by_sym[sym] = ltp
                    continue

                # opt tick
                und = str(fields.get("underlying") or "").strip().upper()
                tsym = str(fields.get("tradingsymbol") or "").strip().upper()
                if not tsym or (und and und not in symbols):
                    log.debug("SKIP unknown_or_missing symbol=%s tsym=%s", und, tsym)
                    continue

                bid = _safe_float(fields.get("bid"))
                ask = _safe_float(fields.get("ask"))
                bid_sz = _safe_float(fields.get("bid_sz"))
                ask_sz = _safe_float(fields.get("ask_sz"))
                if bid is None or ask is None or bid_sz is None or ask_sz is None or bid <= 0 or ask <= 0:
                    log.debug("SKIP no_quote tsym=%s", tsym)
                    continue

                spot = spot_by_sym.get(und)

                if tsym not in detectors:
                    detectors[tsym] = OptionLiquidityExitDetector()

                res = detectors[tsym].analyze(
                    ts_ms=now_ms, bid=bid, ask=ask, bid_sz=bid_sz, ask_sz=ask_sz, spot=spot,
                )

                log.debug(
                    "LOGIC tsym=%s spread=%.4f/%s bid_sz=%s/%s refresh=%s/%s "
                    "bid_stale=%s ask_stale=%s bid_drop=%s spot_move=%s "
                    "stages=[%s%s%s%s%s] status=%s",
                    tsym, res.spread_pct, res.spread_session_avg,
                    res.bid_size, res.bid_size_session_avg,
                    res.refresh_rate, res.refresh_session_avg,
                    res.bid_stale_sec, res.ask_stale_sec,
                    res.bid_drop_pct, res.spot_move_pct,
                    "1" if res.stage1_spread_drift else "0",
                    "1" if res.stage2_bid_shrink else "0",
                    "1" if res.stage3_refresh_slowing else "0",
                    "1" if res.stage4_bid_pull else "0",
                    "1" if res.stage5_one_sided else "0",
                    res.exit_status,
                )

                payload = _to_payload(tsym, und, res, now_ms)
                r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
                r.set(
                    f"{LATEST_KEY_PREFIX}{tsym}",
                    json.dumps(payload, separators=(",", ":")),
                    ex=LATEST_TTL_SEC,
                )

                prev = last_status.get(tsym)
                if res.exit_status != "NONE" or prev not in (None, "NONE"):
                    level = "info"
                    if res.exit_status == "EXIT_NOW":
                        log.info("EXIT_NOW tsym=%s payload=%s", tsym, payload)
                    elif res.exit_status == "ALREADY_TRAPPED":
                        log.info("ALREADY_TRAPPED tsym=%s payload=%s", tsym, payload)
                    else:
                        log.info("STATUS_CHANGE tsym=%s %s -> %s", tsym, prev, res.exit_status)
                last_status[tsym] = res.exit_status

            if ack_ids:
                r.xack(stream, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

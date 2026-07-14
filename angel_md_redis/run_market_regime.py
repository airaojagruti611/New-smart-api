import json
import os
import time

import redis

from app.market_regime_detector import MarketRegimeDetector


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")
OUT_STREAM = os.getenv("STREAM_REGIME", "md:regime")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_REGIME", "20000"))

GROUP = os.getenv("REGIME_GROUP", "regime")
CONSUMER = os.getenv("REGIME_CONSUMER", "regime-1")
INTERVAL_SEC = int(os.getenv("REGIME_INTERVAL_SEC", "60"))

LATEST_KEY = os.getenv("REGIME_LATEST_KEY", "md:regime:latest")


def ensure_group(r: redis.Redis, stream: str, group: str):
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def to_payload(ts_ms: int, result):
    breadth_ratio = "inf" if result.breadth_ratio == float("inf") else f"{result.breadth_ratio:.4f}"
    return {
        "ts_ms": str(ts_ms),
        "regime": result.regime,
        "total": str(result.total),
        "advance": str(result.advance),
        "decline": str(result.decline),
        "neutral": str(result.neutral),
        "breadth_ratio": breadth_ratio,
        "advance_pct": f"{result.advance_pct:.2f}",
        "decline_pct": f"{result.decline_pct:.2f}",
        "neutral_pct": f"{result.neutral_pct:.2f}",
        "call_alloc_pct": str(result.call_alloc_pct),
        "put_alloc_pct": str(result.put_alloc_pct),
    }


def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, EQ_STREAM, GROUP)

    detector = MarketRegimeDetector(neutral_eps=0.0)

    # Live publish throttle: emit at most once per second on tick-driven updates
    LIVE_THROTTLE_SEC = float(os.getenv("REGIME_LIVE_THROTTLE_SEC", "1.0"))
    last_publish = 0.0
    last_regime  = None

    print(
        f"[REGIME] reading {EQ_STREAM}, writing {OUT_STREAM}, "
        f"event-driven (throttle={LIVE_THROTTLE_SEC}s)"
    )

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={EQ_STREAM: ">"},
            count=1000,
            block=1000,
        )

        if not resp:
            continue

        changed = False
        for _stream, msgs in resp:
            ack_ids = []
            for msg_id, fields in msgs:
                detector.ingest_tick(fields)
                ack_ids.append(msg_id)
                changed = True
            if ack_ids:
                r.xack(EQ_STREAM, GROUP, *ack_ids)

        if not changed:
            continue

        now = time.time()
        result = detector.compute()

        # Publish immediately when regime flips, or when throttle window has elapsed
        if result.regime != last_regime or (now - last_publish) >= LIVE_THROTTLE_SEC:
            ts_ms   = int(now * 1000)
            payload = to_payload(ts_ms, result)

            r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
            r.set(LATEST_KEY, json.dumps(payload), ex=3600)

            print(
                "[REGIME]",
                payload["regime"],
                f"A:{payload['advance']}",
                f"D:{payload['decline']}",
                f"N:{payload['neutral']}",
                f"BR:{payload['breadth_ratio']}",
                f"CALL:{payload['call_alloc_pct']}%",
                f"PUT:{payload['put_alloc_pct']}%",
            )

            last_regime  = result.regime
            last_publish = now


if __name__ == "__main__":
    main()

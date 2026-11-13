"""Recompute bucket stats from md:journal:outcome every LEARN_INTERVAL_SEC -> md:learning:strike_stats."""

from __future__ import annotations

import json
import os
import time

import redis

from app.learning_keys import LEARNING_STATS_KEY, STREAM_JOURNAL_OUTCOME
from app.logging_setup import setup_logger
from app.strike_learning import compute_stats

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
INTERVAL = float(os.getenv("LEARN_INTERVAL_SEC", "300"))
MAX_ROWS = int(os.getenv("LEARN_MAX_ROWS", "50000"))  # newest N outcomes; bounds CPU/memory

log = setup_logger("strike_learning")


def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    log.info("START interval=%ss max_rows=%d", INTERVAL, MAX_ROWS)
    while True:
        try:
            entries = r.xrevrange(STREAM_JOURNAL_OUTCOME, count=MAX_ROWS)
            stats = compute_stats(f for _, f in entries)
            r.set(LEARNING_STATS_KEY,
                  json.dumps({k: v.to_dict() for k, v in stats.items()}, separators=(",", ":")))
            log.info("STATS rows=%d buckets=%d", len(entries), len(stats))
        except Exception as e:
            log.warning("recompute failed: %s", e)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()

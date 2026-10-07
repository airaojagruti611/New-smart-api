"""
run_cloud_mirror.py
───────────────────
Copies what the Streamlit dashboard reads from the local Redis to a small
cloud Redis (CLOUD_REDIS_URL), so the dashboard can run on Streamlit
Community Cloud while the pipeline and its full Redis stay on this PC.

Copied every MIRROR_INTERVAL_SEC (default 5):
  - latest-snapshot JSON keys     (app.dashboard_data.MIRROR_KEY_PATTERNS)
  - md:active_expiry hash          (replaced each cycle)
  - recent entries of tick/candle streams, trimmed to MIRROR_STREAM_LEN
  - md:mirror:health: the local stream sizes + mirror timestamp

Only changed keys are written. Nothing is ever read back from the cloud copy.

Usage:
  CLOUD_REDIS_URL=redis://default:<password>@<host>:<port>  (in .env)
  python run_cloud_mirror.py
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, Optional

import redis

from app.config import REDIS_URL, env_int, env_str
from app.dashboard_data import (
    MIRROR_HASHES,
    MIRROR_HEALTH_KEY,
    MIRROR_KEY_PATTERNS,
    MIRROR_STREAMS,
)

CLOUD_REDIS_URL = env_str("CLOUD_REDIS_URL")
INTERVAL_SEC = env_int("MIRROR_INTERVAL_SEC", 5)
STREAM_LEN = env_int("MIRROR_STREAM_LEN", 1000)
KEY_TTL_SEC = env_int("MIRROR_KEY_TTL_SEC", 7 * 24 * 3600)  # when the source key has no TTL


def _id_tuple(stream_id: str) -> tuple:
    ms, _, seq = stream_id.partition("-")
    return int(ms), int(seq or 0)


def _redact(url: str) -> str:
    if "@" not in url:
        return url
    scheme, rest = url.split("://", 1)
    return f"{scheme}://***@{rest.split('@', 1)[1]}"


class Mirror:
    def __init__(self, src: redis.Redis, dst: redis.Redis):
        self.src = src
        self.dst = dst
        self.sent: Dict[str, str] = {}           # key -> last value written
        self.last_id: Dict[str, Optional[str]] = {}  # stream -> last ID copied

    def _copy_keys(self) -> int:
        keys: list[str] = []
        for pattern in MIRROR_KEY_PATTERNS:
            if "*" in pattern:
                keys.extend(self.src.scan_iter(match=pattern, count=500))
            else:
                keys.append(pattern)
        if not keys:
            return 0
        pipe = self.src.pipeline()
        for k in keys:
            pipe.get(k)
            pipe.pttl(k)
        res = pipe.execute()
        out = self.dst.pipeline()
        n = 0
        for i, k in enumerate(keys):
            val, ttl = res[2 * i], res[2 * i + 1]
            if val is None or self.sent.get(k) == val:
                continue
            px = ttl if isinstance(ttl, int) and ttl > 0 else KEY_TTL_SEC * 1000
            out.set(k, val, px=px)
            self.sent[k] = val
            n += 1
        if n:
            out.execute()
        return n

    def _copy_hashes(self) -> None:
        pipe = self.dst.pipeline()
        for h in MIRROR_HASHES:
            data = self.src.hgetall(h)
            pipe.delete(h)
            if data:
                pipe.hset(h, mapping=data)
        pipe.execute()

    def _copy_streams(self) -> int:
        n = 0
        for s in MIRROR_STREAMS:
            if s not in self.last_id:
                # Resume after whatever the cloud copy already has.
                tail = self.dst.xrevrange(s, count=1)
                self.last_id[s] = tail[0][0] if tail else None
            last = self.last_id[s]
            if last is not None:
                head = self.src.xrevrange(s, count=1)
                if head and _id_tuple(head[0][0]) < _id_tuple(last):
                    # Local stream was reset (IDs went backwards): rebuild the copy.
                    print(f"[MIRROR] {s}: source IDs went backwards; rebuilding cloud copy")
                    self.dst.delete(s)
                    last = self.last_id[s] = None
            if last is None:
                rows = list(reversed(self.src.xrevrange(s, count=STREAM_LEN)))
            else:
                rows = self.src.xrange(s, min=f"({last}", count=STREAM_LEN * 5)
            if not rows:
                continue
            rows = rows[-STREAM_LEN:]
            pipe = self.dst.pipeline()
            for mid, fields in rows:
                pipe.xadd(s, fields, id=mid)
            pipe.xtrim(s, maxlen=STREAM_LEN, approximate=False)
            try:
                pipe.execute()
            except redis.exceptions.ResponseError as e:
                if "equal or smaller" not in str(e):
                    raise
                # Local stream was reset (IDs went backwards): rebuild the copy.
                print(f"[MIRROR] {s}: source IDs went backwards; rebuilding cloud copy")
                self.dst.delete(s)
                self.last_id[s] = None
                continue
            self.last_id[s] = rows[-1][0]
            n += len(rows)
        return n

    def _write_health(self) -> None:
        src = self.src
        health = {
            "ts_ms": int(time.time() * 1000),
            "eq": int(src.xlen("md:ticks:eq") or 0),
            "opt": int(src.xlen("md:ticks:opt") or 0),
            "c1m": int(src.xlen("md:candles:1m") or 0),
            "greeks": int(src.xlen("md:greeks:snap") or 0),
            "keys": int(src.dbsize() or 0),
        }
        self.dst.set(MIRROR_HEALTH_KEY, json.dumps(health, separators=(",", ":")), ex=KEY_TTL_SEC)

    def cycle(self) -> Dict[str, int]:
        keys = self._copy_keys()
        self._copy_hashes()
        rows = self._copy_streams()
        self._write_health()
        return {"keys": keys, "stream_rows": rows}


def main() -> None:
    if not CLOUD_REDIS_URL:
        print("[MIRROR] CLOUD_REDIS_URL is not set; nothing to do.")
        return
    src = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    print(f"[MIRROR] {_redact(REDIS_URL)} -> {_redact(CLOUD_REDIS_URL)} every {INTERVAL_SEC}s "
          f"(streams trimmed to {STREAM_LEN})")
    mirror = None
    last_log = 0.0
    while True:
        try:
            if mirror is None:
                dst = redis.Redis.from_url(CLOUD_REDIS_URL, decode_responses=True,
                                           socket_timeout=15, socket_connect_timeout=15,
                                           health_check_interval=30)
                dst.ping()
                mirror = Mirror(src, dst)
            stats = mirror.cycle()
            if time.time() - last_log >= 60:
                print(f"[MIRROR] ok keys_changed={stats['keys']} stream_rows={stats['stream_rows']}")
                last_log = time.time()
        except KeyboardInterrupt:
            return
        except Exception as e:
            # Cloud hiccup or local Redis restart: reconnect next cycle.
            print(f"[MIRROR] error: {e!r}; retrying")
            mirror = None
        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    main()

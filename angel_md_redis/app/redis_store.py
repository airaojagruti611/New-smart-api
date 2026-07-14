import json
import redis
from .config import REDIS_URL

class RedisStore:
    def __init__(self):
        self.r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

    def xadd(self, stream: str, payload: dict, maxlen: int):
        # approx trim for speed
        self.r.xadd(stream, payload, maxlen=maxlen, approximate=True)

    def set_latest(self, key: str, value: str, ex_sec: int = 3600):
        self.r.set(key, value, ex=ex_sec)

    def hset_meta(self, key: str, mapping: dict):
        self.r.hset(key, mapping=mapping)

    def hgetall(self, key: str) -> dict:
        return self.r.hgetall(key)

    def ensure_group(self, stream: str, group: str):
        try:
            self.r.xgroup_create(stream, group, id="0-0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def xreadgroup(self, group: str, name: str, streams: dict, count: int = 1000, block_ms: int = 2000):
        return self.r.xreadgroup(group, name, streams, count=count, block=block_ms)

    def xack(self, stream: str, group: str, msg_id: str):
        self.r.xack(stream, group, msg_id)

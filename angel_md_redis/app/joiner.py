import os
import json
import time
from typing import Dict, Any, Tuple

import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

TICKS_STREAM = os.getenv("TICKS_STREAM_OPT", "md:ticks:opt")
OUT_STREAM = os.getenv("FEATURES_STREAM_OPT", "md:features:opt")

OUT_MAXLEN = int(os.getenv("FEATURES_STREAM_MAXLEN", "500000"))
GROUP = os.getenv("JOINER_GROUP", "joiner")
CONSUMER = os.getenv("JOINER_CONSUMER", "joiner-1")

# refresh greeks cache at most every N seconds per (underlying, expiry)
GREEKS_REFRESH_SEC = float(os.getenv("GREEKS_REFRESH_SEC", "3.0"))


def _ensure_group(r: redis.Redis, stream: str, group: str):
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            return
        raise


def _lower_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k).lower(): v for k, v in d.items()}


class OptionsGreeksJoiner:
    def __init__(self):
        self.r = redis.from_url(REDIS_URL, decode_responses=True)
        _ensure_group(self.r, TICKS_STREAM, GROUP)

        self._cache: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}
        self._cache_t: Dict[Tuple[str, str], float] = {}

    def _load_greeks_map(self, underlying: str, expiry: str) -> Dict[str, Dict[str, Any]]:
        """
        Loads latest greeks list from:
          md:greeks:latest:{UNDERLYING}:{EXPIRY_ISO}
        Builds map by tradingsymbol -> greeks dict
        """
        key = f"md:greeks:latest:{underlying}:{expiry}"
        raw = self.r.get(key)
        if not raw:
            return {}

        try:
            items = json.loads(raw)
        except Exception:
            return {}

        m: Dict[str, Dict[str, Any]] = {}
        for it in items or []:
            norm = _lower_keys(it or {})
            tsym = norm.get("tradingsymbol") or norm.get("symbol")
            if not tsym:
                continue
            m[str(tsym)] = norm
        return m

    def _get_greeks_for(self, underlying: str, expiry: str, tradingsymbol: str) -> Dict[str, Any]:
        k = (underlying, expiry)
        now = time.time()
        if (k not in self._cache) or ((now - self._cache_t.get(k, 0)) > GREEKS_REFRESH_SEC):
            self._cache[k] = self._load_greeks_map(underlying, expiry)
            self._cache_t[k] = now
        return self._cache.get(k, {}).get(tradingsymbol, {})

    def run_forever(self):
        print(f"[JOINER] reading {TICKS_STREAM} -> writing {OUT_STREAM}")

        while True:
            resp = self.r.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={TICKS_STREAM: ">"},
                count=500,
                block=2000,
            )

            if not resp:
                continue

            for _stream, msgs in resp:
                ack_ids = []
                for msg_id, fields in msgs:
                    f = _lower_keys(fields)

                    underlying = f.get("underlying", "")
                    expiry = f.get("expiry", "")
                    tsym = f.get("tradingsymbol", "")

                    greeks = {}
                    if underlying and expiry and tsym:
                        greeks = self._get_greeks_for(str(underlying), str(expiry), str(tsym))

                    # pick common greeks keys (depends on API response)
                    out = dict(fields)  # keep original tick fields
                    out["iv"] = str(greeks.get("iv") or greeks.get("impliedvolatility") or "")
                    out["delta"] = str(greeks.get("delta") or "")
                    out["gamma"] = str(greeks.get("gamma") or "")
                    out["theta"] = str(greeks.get("theta") or "")
                    out["vega"] = str(greeks.get("vega") or "")

                    self.r.xadd(OUT_STREAM, out, maxlen=OUT_MAXLEN, approximate=True)
                    ack_ids.append(msg_id)

                if ack_ids:
                    self.r.xack(TICKS_STREAM, GROUP, *ack_ids)

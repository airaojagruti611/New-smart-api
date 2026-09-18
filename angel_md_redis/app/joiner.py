import os
import json
import time
from typing import Any, Dict, Optional, Tuple

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


def _norm_strike(v: Any) -> Optional[str]:
    """Normalize strike to a stable join key (handles '2120.0' vs '2120.000000')."""
    try:
        if v is None or v == "":
            return None
        return f"{float(v):.4f}"
    except (TypeError, ValueError):
        return None


def _norm_cp(v: Any) -> Optional[str]:
    s = str(v or "").strip().upper()
    if s in ("CE", "C", "CALL"):
        return "CE"
    if s in ("PE", "P", "PUT"):
        return "PE"
    return s or None


def strike_cp_key(strike: Any, cp: Any) -> Optional[str]:
    s = _norm_strike(strike)
    c = _norm_cp(cp)
    if not s or not c:
        return None
    return f"{s}:{c}"


class OptionsGreeksJoiner:
    def __init__(self):
        self.r = redis.from_url(REDIS_URL, decode_responses=True)
        _ensure_group(self.r, TICKS_STREAM, GROUP)

        # cache: (underlying, expiry) -> map of join-key -> greeks dict
        # join-key is preferably "strike:CE|PE" (Angel REST has no tradingsymbol);
        # tradingsymbol is kept as a secondary key when present.
        self._cache: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}
        self._cache_t: Dict[Tuple[str, str], float] = {}

    def _load_greeks_map(self, underlying: str, expiry: str) -> Dict[str, Dict[str, Any]]:
        """
        Loads latest greeks list from:
          md:greeks:latest:{UNDERLYING}:{EXPIRY_ISO}

        Angel Option Greeks API returns strikePrice + optionType + impliedVolatility
        (no tradingsymbol). Index primarily by "strike:CE|PE", with tradingsymbol
        as an optional secondary key when a provider includes it.
        """
        key = f"md:greeks:latest:{underlying}:{expiry}"
        raw = self.r.get(key)
        if not raw:
            return {}

        try:
            items = json.loads(raw)
        except Exception:
            return {}

        if not isinstance(items, list):
            return {}

        m: Dict[str, Dict[str, Any]] = {}
        for it in items or []:
            if not isinstance(it, dict):
                continue
            norm = _lower_keys(it)

            sk = strike_cp_key(
                norm.get("strikeprice") or norm.get("strike"),
                norm.get("optiontype") or norm.get("cp") or norm.get("type"),
            )
            if sk:
                m[sk] = norm

            tsym = norm.get("tradingsymbol") or norm.get("symbol")
            if tsym:
                m[str(tsym).strip().upper()] = norm
        return m

    def _get_greeks_for(
        self,
        underlying: str,
        expiry: str,
        tradingsymbol: str,
        strike: Any = None,
        cp: Any = None,
    ) -> Dict[str, Any]:
        k = (underlying, expiry)
        now = time.time()
        if (k not in self._cache) or ((now - self._cache_t.get(k, 0)) > GREEKS_REFRESH_SEC):
            self._cache[k] = self._load_greeks_map(underlying, expiry)
            self._cache_t[k] = now

        cache = self._cache.get(k, {})
        sk = strike_cp_key(strike, cp)
        if sk and sk in cache:
            return cache[sk]

        tsym = str(tradingsymbol or "").strip().upper()
        if tsym and tsym in cache:
            return cache[tsym]
        return {}

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
                    strike = f.get("strike", "")
                    cp = f.get("cp", "")

                    greeks = {}
                    if underlying and expiry and (tsym or (strike and cp)):
                        greeks = self._get_greeks_for(
                            str(underlying), str(expiry), str(tsym),
                            strike=strike, cp=cp,
                        )

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

import time
import json
import datetime as dt
from typing import Dict

from .redis_store import RedisStore
from .config import STREAM_GREEKS, STREAM_MAXLEN_GREEKS
from .angel_rest import fetch_option_greeks
from .utils import now_ms


def iso_to_expirydate(iso_date: str) -> str:
    # "2026-01-27" -> "27JAN2026"
    y, m, d = iso_date.split("-")
    d0 = dt.date(int(y), int(m), int(d))
    return d0.strftime("%d%b%Y").upper()


class GreeksPoller:
    def __init__(self, auth_token: str):
        self.auth_token = auth_token
        self.rs = RedisStore()

    def poll_once(self, active_expiry: Dict[str, str], per_request_sleep: float = 0.12):
        """
        active_expiry: {"IOC":"2026-01-27", ...}
        Writes:
          - STREAM_GREEKS (snapshots)
          - md:greeks:latest:{UNDERLYING}:{EXPIRY_ISO} (cached JSON list)
        """
        for underlying, expiry_iso in (active_expiry or {}).items():
            try:
                expirydate = iso_to_expirydate(expiry_iso)
                res = fetch_option_greeks(self.auth_token, underlying, expirydate)

                if not res or not res.get("status"):
                    time.sleep(per_request_sleep)
                    continue

                data_list = res.get("data", [])
                data_json = json.dumps(data_list, separators=(",", ":"))

                payload = {
                    "ts_recv": str(now_ms()),
                    "underlying": underlying,
                    "expiry": expiry_iso,  # keep ISO for joining
                    "data_json": data_json,
                }
                self.rs.xadd(STREAM_GREEKS, payload, maxlen=STREAM_MAXLEN_GREEKS)

                # cache latest for joiner
                self.rs.set_latest(f"md:greeks:latest:{underlying}:{expiry_iso}", data_json, ex_sec=3600)

                time.sleep(per_request_sleep)
            except Exception:
                time.sleep(0.5)

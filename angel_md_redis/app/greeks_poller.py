import time
import json
import datetime as dt
from typing import Dict, Optional

from .redis_store import RedisStore
from .config import STREAM_GREEKS, STREAM_MAXLEN_GREEKS
from .angel_rest import fetch_option_greeks
from .utils import now_ms
from .logging_setup import setup_logger

log = setup_logger("greeks_poller")

# English month abbreviations so expiry format stays DDMMMYYYY even on
# non-English Windows locales (strftime %b follows the process locale).
_MONTH_ABBR = (
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
)


def iso_to_expirydate(iso_date: str) -> str:
    # "2026-01-27" -> "27JAN2026" (locale-independent)
    y, m, d = iso_date.split("-")
    return f"{int(d):02d}{_MONTH_ABBR[int(m) - 1]}{int(y)}"


class GreeksPoller:
    def __init__(self, auth_token: str):
        self.auth_token = auth_token
        self.rs = RedisStore()

    def poll_once(self, active_expiry: Dict[str, str], per_request_sleep: float = 0.12) -> int:
        """
        active_expiry: {"IOC":"2026-01-27", ...}
        Writes:
          - STREAM_GREEKS (snapshots)
          - md:greeks:latest:{UNDERLYING}:{EXPIRY_ISO} (cached JSON list)

        Returns the number of underlyings successfully written this cycle.
        """
        ok_count = 0
        for underlying, expiry_iso in (active_expiry or {}).items():
            try:
                expirydate = iso_to_expirydate(expiry_iso)
                res = fetch_option_greeks(self.auth_token, underlying, expirydate)

                if not res or not res.get("status"):
                    log.warning(
                        "GREEKS_API_FAIL underlying=%s expiry=%s/%s status=%s message=%s errorcode=%s http=%s",
                        underlying,
                        expiry_iso,
                        expirydate,
                        (res or {}).get("status"),
                        (res or {}).get("message"),
                        (res or {}).get("errorcode") or (res or {}).get("errorCode"),
                        (res or {}).get("http_status"),
                    )
                    time.sleep(per_request_sleep)
                    continue

                data_list = res.get("data") or []
                if not isinstance(data_list, list) or not data_list:
                    log.warning(
                        "GREEKS_API_EMPTY underlying=%s expiry=%s/%s message=%s",
                        underlying, expiry_iso, expirydate, res.get("message"),
                    )
                    time.sleep(per_request_sleep)
                    continue

                data_json = json.dumps(data_list, separators=(",", ":"))

                payload = {
                    "ts_recv": str(now_ms()),
                    "underlying": underlying,
                    "expiry": expiry_iso,  # keep ISO for joining
                    "data_json": data_json,
                    "n_contracts": str(len(data_list)),
                }
                self.rs.xadd(STREAM_GREEKS, payload, maxlen=STREAM_MAXLEN_GREEKS)

                # cache latest for joiner
                self.rs.set_latest(f"md:greeks:latest:{underlying}:{expiry_iso}", data_json, ex_sec=3600)

                ok_count += 1
                sample = data_list[0] if isinstance(data_list[0], dict) else {}
                log.info(
                    "GREEKS_OK underlying=%s expiry=%s n=%d sample_keys=%s",
                    underlying, expiry_iso, len(data_list),
                    sorted(sample.keys()) if sample else [],
                )
                time.sleep(per_request_sleep)
            except Exception as e:
                log.exception(
                    "GREEKS_POLL_EXC underlying=%s expiry=%s err=%s",
                    underlying, expiry_iso, repr(e),
                )
                time.sleep(0.5)
        return ok_count

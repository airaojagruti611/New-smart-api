import json
import time
from typing import Any, Dict, List, Optional

from .redis_store import RedisStore
from .config import (
    STREAM_EQ,
    STREAM_GREEKS,
    STREAM_MAXLEN_GREEKS,
    STREAM_OPT,
    RISK_FREE_RATE,
    GREEKS_DIVIDEND_YIELD,
)
from .angel_rest import api_error_text, api_failed, fetch_option_greeks
from .option_pricing import greeks_from_market_premium, time_to_expiry_years
from .utils import now_ms, safe_float
from .logging_setup import setup_logger

log = setup_logger("greeks_poller")

# English month abbreviations so expiry format stays DDMMMYYYY even on
# non-English Windows locales (strftime %b follows the process locale).
_MONTH_ABBR = (
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
)

# Per-cycle snapshot windows. Ticks of every symbol are interleaved in one
# stream, so these scale with the universe instead of a fixed handful.
_OPT_SCAN = 50000
_EQ_SCAN_MAX = 200000
# After "Invalid API Key" (AG8004: key lacks market-data access) skip the
# broker call for this long and go straight to local Black-Scholes.
_BROKER_BACKOFF_SEC = 3600


def iso_to_expirydate(iso_date: str) -> str:
    # "2026-01-27" -> "27JAN2026" (locale-independent)
    y, m, d = iso_date.split("-")
    return f"{int(d):02d}{_MONTH_ABBR[int(m) - 1]}{int(y)}"


def stamp_greeks_items(data_list: list, ts_ms: int) -> list:
    """Add `ts_ms` (poll time, epoch ms) to every greeks item.

    md:greeks:latest:* stays a JSON list (backward compatible); readers judge
    freshness from the per-item ts_ms instead of the 1h key TTL.
    """
    out = []
    for it in data_list or []:
        if isinstance(it, dict):
            it = dict(it)
            it["ts_ms"] = int(ts_ms)
        out.append(it)
    return out


class GreeksPoller:
    def __init__(self, auth_token: str, smart_api=None):
        self.auth_token = auth_token
        self.smart_api = smart_api
        self.rs = RedisStore()
        self._spots: Dict[str, float] = {}
        self._quotes: Dict[tuple, List[Dict[str, Any]]] = {}
        self._broker_off_until = 0.0

    def _snapshot(self, underlyings: List[str]) -> None:
        """Latest spot per underlying and latest quote per option, once per cycle."""
        want = {u.upper() for u in underlyings}
        spots: Dict[str, float] = {}
        max_id, scanned = "+", 0
        while want - set(spots) and scanned < _EQ_SCAN_MAX:
            rows = self.rs.r.xrevrange(STREAM_EQ, max=max_id, count=5000)
            if not rows:
                break
            for _mid, fields in rows:
                sym = str(fields.get("symbol") or "").strip().upper()
                if sym in want and sym not in spots:
                    ltp = safe_float(fields.get("ltp"))
                    if ltp is not None:
                        spots[sym] = ltp
            scanned += len(rows)
            max_id = f"({rows[-1][0]}"
        self._spots = spots

        quotes: Dict[tuple, List[Dict[str, Any]]] = {}
        seen = set()
        for _mid, fields in self.rs.r.xrevrange(STREAM_OPT, count=_OPT_SCAN):
            tsym = str(fields.get("tradingsymbol") or "").strip().upper()
            if not tsym or tsym in seen:
                continue
            seen.add(tsym)
            key = (str(fields.get("underlying") or "").strip().upper(), str(fields.get("expiry") or "").strip())
            quotes.setdefault(key, []).append(fields)
        self._quotes = quotes

    def _publish(self, underlying: str, expiry_iso: str, data_list: list, source: str) -> None:
        ts = now_ms()
        data_list = stamp_greeks_items(data_list, ts)
        data_json = json.dumps(data_list, separators=(",", ":"))
        payload = {
            "ts_recv": str(ts),
            "ts_ms": str(ts),
            "underlying": underlying,
            "expiry": expiry_iso,
            "data_json": data_json,
            "n_contracts": str(len(data_list)),
            "source": source,
        }
        self.rs.xadd(STREAM_GREEKS, payload, maxlen=STREAM_MAXLEN_GREEKS)
        self.rs.set_latest(f"md:greeks:latest:{underlying}:{expiry_iso}", data_json, ex_sec=3600)
        sample = data_list[0] if isinstance(data_list[0], dict) else {}
        log.info(
            "GREEKS_OK source=%s underlying=%s expiry=%s n=%d sample_keys=%s",
            source, underlying, expiry_iso, len(data_list),
            sorted(sample.keys()) if sample else [],
        )

    def _latest_spot(self, underlying: str) -> Optional[float]:
        return self._spots.get(underlying.upper())

    def _latest_opt_quotes(self, underlying: str, expiry_iso: str) -> List[Dict[str, Any]]:
        return self._quotes.get((underlying.upper(), expiry_iso), [])

    def _compute_local_chain(self, underlying: str, expiry_iso: str) -> List[dict]:
        spot = self._latest_spot(underlying)
        tte = time_to_expiry_years(expiry_iso)
        if spot is None or not tte or tte <= 0:
            log.warning(
                "GREEKS_LOCAL_SKIP underlying=%s expiry=%s spot=%s tte=%s",
                underlying, expiry_iso, spot, tte,
            )
            return []

        out: List[dict] = []
        for q in self._latest_opt_quotes(underlying, expiry_iso):
            strike = safe_float(q.get("strike"))
            cp = str(q.get("cp") or "").strip().upper()
            ltp = safe_float(q.get("ltp"))
            tsym = str(q.get("tradingsymbol") or "").strip().upper()
            if strike is None or cp not in ("CE", "PE") or ltp is None or ltp <= 0:
                continue
            g = greeks_from_market_premium(
                spot, strike, cp, tte, ltp,
                risk_free_rate=RISK_FREE_RATE,
                dividend_or_carry=GREEKS_DIVIDEND_YIELD,
            )
            if not g:
                continue
            out.append({
                "name": underlying,
                "expiry": expiry_iso,
                "strikePrice": strike,
                "optionType": cp,
                "tradingsymbol": tsym,
                "delta": g["delta"],
                "gamma": g["gamma"],
                "theta": g["theta"],
                "vega": g["vega"],
                "impliedVolatility": g["impliedvolatility"],
                "tradeVolume": q.get("vol") or "",
                "source": g["source"],
            })
        return out

    def poll_once(self, active_expiry: Dict[str, str], per_request_sleep: float = 0.12) -> int:
        """
        active_expiry: {"IOC":"2026-01-27", ...}
        Writes:
          - STREAM_GREEKS (snapshots)
          - md:greeks:latest:{UNDERLYING}:{EXPIRY_ISO} (cached JSON list)

        Broker REST first; on AG8004 / empty, imply IV from live option LTPs.

        Returns the number of underlyings successfully written this cycle.
        """
        ok_count = 0
        self._snapshot(list((active_expiry or {}).keys()))
        for underlying, expiry_iso in (active_expiry or {}).items():
            try:
                expirydate = iso_to_expirydate(expiry_iso)
                if time.time() >= self._broker_off_until:
                    res = fetch_option_greeks(
                        self.auth_token, underlying, expirydate, smart_api=self.smart_api
                    )
                    data_list = res.get("data") if isinstance(res, dict) else None
                    if not api_failed(res) and isinstance(data_list, list) and data_list:
                        self._publish(underlying, expiry_iso, data_list, source="broker_api")
                        ok_count += 1
                        time.sleep(per_request_sleep)
                        continue

                    err = api_error_text(res)
                    log.warning(
                        "GREEKS_API_FAIL underlying=%s expiry=%s/%s err=%s — trying local BS",
                        underlying, expiry_iso, expirydate, err,
                    )
                    if "AG8004" in err:
                        self._broker_off_until = time.time() + _BROKER_BACKOFF_SEC
                        log.warning(
                            "GREEKS_API_OFF for %ds: API key has no option-Greeks access; local BS only",
                            _BROKER_BACKOFF_SEC,
                        )
                local = self._compute_local_chain(underlying, expiry_iso)
                if local:
                    self._publish(underlying, expiry_iso, local, source="theoretical_black_scholes")
                    ok_count += 1
                else:
                    log.warning(
                        "GREEKS_LOCAL_EMPTY underlying=%s expiry=%s",
                        underlying, expiry_iso,
                    )
                if time.time() < self._broker_off_until:
                    continue  # no REST call was made: no rate-limit pause needed
                time.sleep(per_request_sleep)
            except Exception as e:
                log.exception(
                    "GREEKS_POLL_EXC underlying=%s expiry=%s err=%s",
                    underlying, expiry_iso, repr(e),
                )
                time.sleep(0.5)
        return ok_count

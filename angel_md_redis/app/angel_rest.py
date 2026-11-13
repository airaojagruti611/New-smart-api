import json
import requests
from .config import (
    ANGEL_API_KEY, ANGEL_CLIENT_CODE,
    X_CLIENT_LOCAL_IP, X_CLIENT_PUBLIC_IP, X_MAC_ADDRESS
)
from .logging_setup import setup_logger
from .utils import (
    get_local_ip,
    get_mac,
    get_public_ip,
    is_private_ip,
    is_unusable_ip,
)

log = setup_logger("angel_rest")
_HEADERS_LOGGED = False

OPTION_GREEKS_URL = "https://apiconnect.angelone.in/rest/secure/angelbroking/marketData/v1/optionGreek"
CANDLE_DATA_URL = "https://apiconnect.angelone.in/rest/secure/angelbroking/historical/v1/getCandleData"

def api_failed(body: dict | None) -> bool:
    """Angel sometimes uses status, sometimes success, and AG8004 with HTTP 200."""
    if not body or not isinstance(body, dict):
        return True
    if body.get("status") is False or body.get("success") is False:
        return True
    code = str(body.get("errorcode") or body.get("errorCode") or "").upper()
    if code and code not in ("", "0", "NONE"):
        return True
    return False


def api_error_text(body: dict | None) -> str:
    if not body or not isinstance(body, dict):
        return "empty"
    msg = body.get("message") or body.get("msg") or ""
    code = body.get("errorcode") or body.get("errorCode") or ""
    http = body.get("http_status")
    return f"{msg} code={code} http={http}".strip()


def candle_rows(body: dict | None) -> list:
    if not body or not isinstance(body, dict):
        return []
    data = body.get("data")
    if isinstance(data, list):
        return data
    return []


def _pick_ip(configured: str, resolved: str, fallback: str = "") -> str:
    for candidate in (configured, resolved, fallback):
        s = (candidate or "").strip()
        if s and not is_unusable_ip(s):
            return s
    return ""


def build_headers(auth_token: str, force_public: bool = False) -> dict:
    """
    Angel market-data REST (optionGreek / getCandleData) returns AG8004
    "Invalid API Key" when X-ClientLocalIP is loopback, unspecified, or a
    private RFC1918 address. Use the egress public IP for BOTH IP headers.
    """
    global _HEADERS_LOGGED
    lan_ip = _pick_ip(X_CLIENT_LOCAL_IP, get_local_ip(""))
    pub_ip = _pick_ip(X_CLIENT_PUBLIC_IP, get_public_ip(""))

    if force_public or is_unusable_ip(lan_ip) or is_private_ip(lan_ip):
        local_ip = pub_ip or lan_ip
    else:
        local_ip = lan_ip
    if not pub_ip:
        pub_ip = local_ip
    if not local_ip:
        local_ip = pub_ip

    mac = X_MAC_ADDRESS or get_mac("")

    token = (auth_token or "").strip()
    if token and not token.lower().startswith("bearer "):
        token = f"Bearer {token}"

    h = {
        "Content-type": "application/json",
        "Accept": "application/json",
        "Authorization": token,
        "X-PrivateKey": ANGEL_API_KEY,
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": local_ip or "0.0.0.0",
        "X-ClientPublicIP": pub_ip or local_ip or "0.0.0.0",
        "X-MACAddress": mac or "00:00:00:00:00:00",
    }
    if not _HEADERS_LOGGED:
        log.info(
            "ANGEL_REST_HEADERS local_ip=%s public_ip=%s force_public=%s lan_detected=%s",
            h["X-ClientLocalIP"], h["X-ClientPublicIP"], force_public, lan_ip or "",
        )
        _HEADERS_LOGGED = True
    return h


def _request_json(url: str, auth_token: str, payload: dict, timeout: int) -> dict:
    def _post(force_public: bool) -> dict:
        headers = build_headers(auth_token, force_public=force_public)
        try:
            r = requests.post(
                url,
                headers=headers,
                data=json.dumps(payload),
                timeout=timeout,
            )
        except requests.RequestException as e:
            return {
                "status": False,
                "message": f"Request failed: {e}",
                "errorcode": "REQUEST_ERROR",
                "http_status": None,
                "data": None,
            }

        try:
            body = r.json()
        except Exception:
            return {
                "status": False,
                "message": f"Non-JSON response: {r.text[:200]}",
                "errorcode": "BAD_JSON",
                "http_status": r.status_code,
                "data": None,
            }

        if not isinstance(body, dict):
            return {
                "status": False,
                "message": f"Unexpected response type: {type(body).__name__}",
                "errorcode": "BAD_SHAPE",
                "http_status": r.status_code,
                "data": None,
            }

        body.setdefault("http_status", r.status_code)
        if r.status_code >= 400 and body.get("status") is not False:
            body["status"] = False
            body.setdefault("message", f"HTTP {r.status_code}")
        return body

    body = _post(force_public=False)
    code = str(body.get("errorcode") or body.get("errorCode") or "").upper()
    # Retry only if forcing the public IP actually changes the headers; an
    # identical retry just doubles calls against the rate limit.
    if code == "AG8004" and build_headers(auth_token, force_public=True) != build_headers(auth_token):
        log.warning(
            "AG8004 retrying with public-IP headers url=%s err=%s",
            url, api_error_text(body),
        )
        body = _post(force_public=True)
    return body


def fetch_option_greeks(
    auth_token: str,
    name: str,
    expirydate: str,
    timeout=20,
    smart_api=None,
) -> dict:
    """
    name: underlying like "TCS"
    expirydate: as per docs e.g. "25JAN2024"
    BUT your ScripMaster gives ISO dates; we convert elsewhere if needed.

    Angel response fields (per contract): name, expiry, strikePrice, optionType,
    delta, gamma, theta, vega, impliedVolatility, tradeVolume — no tradingsymbol.

    smart_api is accepted for call-site compatibility; we do not use
    SmartConnect.optionGreek as a second hop — the SDK hardcodes
    X-ClientLocalIP=127.0.0.1 and logs the API key on AG8004.
    """
    return _request_json(
        OPTION_GREEKS_URL,
        auth_token,
        {"name": name, "expirydate": expirydate},
        timeout,
    )


def fetch_candle_data(
    auth_token: str,
    exchange: str,
    symboltoken: str,
    interval: str,
    fromdate: str,
    todate: str,
    timeout: int = 30,
) -> dict:
    """
    Angel One historical candles.

    interval: ONE_MINUTE / FIVE_MINUTE / TEN_MINUTE / THIRTY_MINUTE / ONE_DAY
    fromdate/todate: "YYYY-MM-DD HH:MM"
    data rows: [timestamp, open, high, low, close, volume]
    """
    return _request_json(
        CANDLE_DATA_URL,
        auth_token,
        {
            "exchange": exchange,
            "symboltoken": str(symboltoken),
            "interval": interval,
            "fromdate": fromdate,
            "todate": todate,
        },
        timeout,
    )

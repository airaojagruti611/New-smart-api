import json
import requests
from .config import (
    ANGEL_API_KEY, ANGEL_CLIENT_CODE,
    X_CLIENT_LOCAL_IP, X_CLIENT_PUBLIC_IP, X_MAC_ADDRESS
)
from .utils import get_local_ip, get_mac

OPTION_GREEKS_URL = "https://apiconnect.angelone.in/rest/secure/angelbroking/marketData/v1/optionGreek"
CANDLE_DATA_URL = "https://apiconnect.angelone.in/rest/secure/angelbroking/historical/v1/getCandleData"

def build_headers(auth_token: str) -> dict:
    local_ip = X_CLIENT_LOCAL_IP or get_local_ip("127.0.0.1")
    mac = X_MAC_ADDRESS or get_mac("")
    pub_ip = X_CLIENT_PUBLIC_IP  # if empty, API usually still works for many users

    h = {
        "Content-type": "application/json",
        "Accept": "application/json",
        "Authorization": auth_token,
        "X-PrivateKey": ANGEL_API_KEY,
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": local_ip,
        "X-ClientPublicIP": pub_ip or local_ip,
        "X-MACAddress": mac or "00:00:00:00:00:00",
    }
    return h

def fetch_option_greeks(auth_token: str, name: str, expirydate: str, timeout=20) -> dict:
    """
    name: underlying like "TCS"
    expirydate: as per docs e.g. "25JAN2024"
    BUT your ScripMaster gives ISO dates; we convert elsewhere if needed.

    Angel response fields (per contract): name, expiry, strikePrice, optionType,
    delta, gamma, theta, vega, impliedVolatility, tradeVolume — no tradingsymbol.
    """
    headers = build_headers(auth_token)
    payload = {"name": name, "expirydate": expirydate}
    try:
        r = requests.post(
            OPTION_GREEKS_URL,
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
    headers = build_headers(auth_token)
    payload = {
        "exchange": exchange,
        "symboltoken": str(symboltoken),
        "interval": interval,
        "fromdate": fromdate,
        "todate": todate,
    }
    try:
        r = requests.post(
            CANDLE_DATA_URL,
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

import json
import requests
from .config import (
    ANGEL_API_KEY, ANGEL_CLIENT_CODE,
    X_CLIENT_LOCAL_IP, X_CLIENT_PUBLIC_IP, X_MAC_ADDRESS
)
from .utils import get_local_ip, get_mac

OPTION_GREEKS_URL = "https://apiconnect.angelone.in/rest/secure/angelbroking/marketData/v1/optionGreek"

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
    """
    headers = build_headers(auth_token)
    payload = {"name": name, "expirydate": expirydate}
    r = requests.post(OPTION_GREEKS_URL, headers=headers, data=json.dumps(payload), timeout=timeout)
    try:
        return r.json()
    except:
        return {"status": False, "message": f"Non-JSON response: {r.text[:200]}"}

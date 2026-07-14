import pyotp
from SmartApi import SmartConnect
from .config import ANGEL_API_KEY, ANGEL_CLIENT_CODE, ANGEL_PIN, ANGEL_TOTP_SECRET

def login():
    if not (ANGEL_API_KEY and ANGEL_CLIENT_CODE and ANGEL_PIN and ANGEL_TOTP_SECRET):
        raise RuntimeError("Missing Angel env vars. Check .env.example")

    obj = SmartConnect(api_key=ANGEL_API_KEY)
    otp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()

    sess = obj.generateSession(ANGEL_CLIENT_CODE, ANGEL_PIN, otp)
    if not sess or not sess.get("status"):
        raise RuntimeError(f"generateSession failed: {sess}")

    jwt = sess["data"].get("jwtToken")
    feed = sess["data"].get("feedToken") or obj.getfeedToken()

    if not jwt or not feed:
        raise RuntimeError(f"Missing jwt/feed token: {sess}")

    auth_token = jwt if str(jwt).lower().startswith("bearer ") else f"Bearer {jwt}"
    return obj, auth_token, feed

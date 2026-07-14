import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
SYMBOLS_FILE = BASE_DIR / "symbols.txt"

def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except:
        return default

def env_str(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()

ANGEL_API_KEY = env_str("ANGEL_API_KEY")
ANGEL_CLIENT_CODE = env_str("ANGEL_CLIENT_CODE")
ANGEL_PIN = env_str("ANGEL_PIN")
ANGEL_TOTP_SECRET = env_str("ANGEL_TOTP_SECRET")

REDIS_URL = env_str("REDIS_URL", "redis://localhost:6379/0")

WS_WARMUP_SEC = env_int("WS_WARMUP_SEC", 8)
STRIKES_AROUND = env_int("STRIKES_AROUND", 0)
MAX_WS_SUBS = env_int("MAX_WS_SUBS", 950)
SUBSCRIBE_MODE = env_str("SUBSCRIBE_MODE", "SNAP_QUOTE").upper()

STREAM_EQ = env_str("STREAM_EQ", "md:ticks:eq")
STREAM_OPT = env_str("STREAM_OPT", "md:ticks:opt")
STREAM_GREEKS = env_str("STREAM_GREEKS", "md:greeks:snap")
STREAM_OPT_FEATURES = env_str("STREAM_OPT_FEATURES", "md:features:opt")

STREAM_MAXLEN_EQ = env_int("STREAM_MAXLEN_EQ", 3_000_000)
STREAM_MAXLEN_OPT = env_int("STREAM_MAXLEN_OPT", 8_000_000)
STREAM_MAXLEN_GREEKS = env_int("STREAM_MAXLEN_GREEKS", 100_000)
STREAM_MAXLEN_FEATURES = env_int("STREAM_MAXLEN_FEATURES", 8_000_000)

GREEKS_POLL_SEC = env_int("GREEKS_POLL_SEC", 30)

STREAM_VOLUME_SIGNAL    = env_str("STREAM_VOLUME_SIGNAL", "md:volume:signal")
STREAM_MAXLEN_VOLUME    = env_int("STREAM_MAXLEN_VOLUME", 20_000)
VOLUME_INTERVAL_SEC     = env_int("VOLUME_INTERVAL_SEC", 60)
VOLUME_LATEST_KEY       = env_str("VOLUME_LATEST_KEY", "md:volume:latest")
VOLUME_AVG_WINDOW       = env_int("VOLUME_AVG_WINDOW", 20)   # completed candles for rolling avg_volume

X_CLIENT_LOCAL_IP = env_str("X_CLIENT_LOCAL_IP", "127.0.0.1")
X_CLIENT_PUBLIC_IP = env_str("X_CLIENT_PUBLIC_IP", "")
X_MAC_ADDRESS = env_str("X_MAC_ADDRESS", "")

def load_symbols() -> list[str]:
    if not SYMBOLS_FILE.exists():
        raise RuntimeError(f"symbols.txt not found at {SYMBOLS_FILE}")
    syms = []
    for line in SYMBOLS_FILE.read_text(encoding="utf-8").splitlines():
        s = line.strip().upper()
        if s:
            syms.append(s)
    # dedupe keep order
    out = []
    seen = set()
    for s in syms:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

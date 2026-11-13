import time
import uuid
import socket
import uuid as uuidlib

_PUBLIC_IP = None

def now_ms() -> int:
    return int(time.time() * 1000)


def is_private_ip(ip: str) -> bool:
    """RFC1918 / link-local. Angel market-data REST treats these like loopback (AG8004)."""
    s = (ip or "").strip().lower()
    if not s:
        return False
    if s.startswith("10."):
        return True
    if s.startswith("192.168."):
        return True
    if s.startswith("169.254."):
        return True
    if s.startswith("172."):
        try:
            second = int(s.split(".")[1])
        except (IndexError, ValueError):
            return False
        return 16 <= second <= 31
    return False


def is_unusable_ip(ip: str) -> bool:
    """True for empty / loopback / unspecified — Angel REST treats these as AG8004."""
    s = (ip or "").strip().lower()
    if not s:
        return True
    if s in ("0.0.0.0", "::1", "localhost"):
        return True
    if s.startswith("127."):
        return True
    return False


def get_public_ip(default: str = "") -> str:
    """Best-effort egress IP. Angel REST market-data calls reject 127.0.0.1."""
    global _PUBLIC_IP
    if _PUBLIC_IP:
        return _PUBLIC_IP
    try:
        import requests

        r = requests.get("https://api.ipify.org", timeout=4)
        ip = (r.text or "").strip()
        if ip:
            _PUBLIC_IP = ip
            return ip
    except Exception:
        pass
    return default

def gen_id() -> str:
    return str(uuid.uuid4())

def get_local_ip(default="") -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        if is_unusable_ip(ip):
            return default
        return ip
    except:
        return default

def get_mac(default="") -> str:
    try:
        mac = uuidlib.getnode()
        return ":".join(f"{(mac >> ele) & 0xff:02x}" for ele in range(40, -8, -8))
    except:
        return default

def safe_float(x):
    try:
        return float(x)
    except:
        return None

def paise_to_rupees(x):
    try:
        return float(x) / 100.0
    except:
        return None

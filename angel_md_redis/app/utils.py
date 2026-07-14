import time
import uuid
import socket
import uuid as uuidlib

def now_ms() -> int:
    return int(time.time() * 1000)

def gen_id() -> str:
    return str(uuid.uuid4())

def get_local_ip(default="127.0.0.1") -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
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

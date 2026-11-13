"""
run_greeks.py
─────────────
All-in-one variant: runs the WS producer in a thread (to plan the active
option expiry per underlying) and polls greeks with the real GreeksPoller API
(poll_once), exactly like run_greeks_only.py does against md:active_expiry.

Prefer run_producer.py + run_greeks_only.py; use this only when you want a
single process.
"""

import threading
import time

from app.angel_auth import login
from app.config import ANGEL_API_KEY, ANGEL_CLIENT_CODE, load_symbols
from app.greeks_poller import GreeksPoller
from app.ws_producer import MarketDataProducer

try:
    from app.config import GREEKS_POLL_SEC
except Exception:
    GREEKS_POLL_SEC = 5

PER_REQUEST_SLEEP = 0.12


def poll_loop(poller: GreeksPoller, active_expiry_fn, poll_sec: float = GREEKS_POLL_SEC, max_cycles: int = 0) -> int:
    """Poll greeks for the producer's active expiries. Returns cycles run."""
    cycles = 0
    while not max_cycles or cycles < max_cycles:
        active = dict(active_expiry_fn() or {})
        if active:
            ok = poller.poll_once(active_expiry=active, per_request_sleep=PER_REQUEST_SLEEP)
            print(f"[GREEKS] poll cycle ok={ok}/{len(active)} underlyings")
        cycles += 1
        if max_cycles and cycles >= max_cycles:
            break
        time.sleep(poll_sec)
    return cycles


def main():
    symbols = load_symbols()
    smart_api, auth_token, feed_token = login()

    producer = MarketDataProducer(
        auth_token=auth_token,
        feed_token=feed_token,
        client_code=ANGEL_CLIENT_CODE,
        api_key=ANGEL_API_KEY,
        symbols=symbols,
    )
    t = threading.Thread(target=producer.start, daemon=True)
    t.start()

    # wait until options planned (active_expiry_by_underlying populated)
    while not producer.options_subscribed:
        time.sleep(1.0)

    poller = GreeksPoller(auth_token=auth_token, smart_api=smart_api)
    poll_loop(poller, lambda: producer.active_expiry_by_underlying)


if __name__ == "__main__":
    main()

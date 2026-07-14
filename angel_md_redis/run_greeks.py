import time
from app.config import ANGEL_CLIENT_CODE, ANGEL_API_KEY
from app.angel_auth import login
from app.ws_producer import MarketDataProducer
from app.greeks_poller import GreeksPoller
from app.config import load_symbols

def main():
    # We need the same planning logic to know which underlyings+expiry are active.
    # Easiest: start WS producer first, let it plan options, then poll.
    symbols = load_symbols()
    obj, auth_token, feed_token = login()

    producer = MarketDataProducer(
        auth_token=auth_token,
        feed_token=feed_token,
        client_code=ANGEL_CLIENT_CODE,
        api_key=ANGEL_API_KEY,
        symbols=symbols
    )

    # Start WS in this process OR run producer separately.
    # If you already run run_producer.py, then instead read "active underlyings" from Redis/meta.
    # For simplicity, we run WS here too:
    import threading
    t = threading.Thread(target=producer.start, daemon=True)
    t.start()

    # wait until options planned
    while not producer.options_subscribed:
        time.sleep(1.0)

    poller = GreeksPoller(auth_token=auth_token, active_expiry_by_underlying=producer.active_expiry_by_underlying)
    poller.run_forever()

if __name__ == "__main__":
    main()

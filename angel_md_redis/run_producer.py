import time

from app.config import load_symbols, ANGEL_API_KEY, ANGEL_CLIENT_CODE
from app.angel_auth import login
from app.ws_producer import MarketDataProducer

RECONNECT_DELAY_SEC = 15


def main():
    symbols = load_symbols()

    # start() returns when the socket is closed for good (library gave up
    # after max retries, or the stale-feed watchdog closed it). Log in again
    # and reconnect instead of exiting and leaving the pipeline without ticks.
    while True:
        try:
            obj, auth_token, feed_token = login()
            producer = MarketDataProducer(
                auth_token=auth_token,
                feed_token=feed_token,
                client_code=ANGEL_CLIENT_CODE,
                api_key=ANGEL_API_KEY,
                symbols=symbols
            )
            producer.start()
            print(f"[WS] connection ended; reconnecting in {RECONNECT_DELAY_SEC}s")
        except KeyboardInterrupt:
            return
        except Exception as e:
            print(f"[WS] producer error: {e!r}; reconnecting in {RECONNECT_DELAY_SEC}s")
        time.sleep(RECONNECT_DELAY_SEC)

if __name__ == "__main__":
    main()

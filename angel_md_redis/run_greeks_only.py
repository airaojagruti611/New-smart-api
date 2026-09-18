import time

from app.angel_auth import login
from app.redis_store import RedisStore
from app.greeks_poller import GreeksPoller

try:
    from app.config import GREEKS_POLL_SEC
except Exception:
    GREEKS_POLL_SEC = 5  # seconds between polling cycles

# Sleep between each underlying REST call to avoid rate limits
PER_REQUEST_SLEEP = 0.12


def main():
    print("[GREEKS] logging in...")
    _, auth_token, _ = login()

    rs = RedisStore()
    poller = GreeksPoller(auth_token=auth_token)

    print("[GREEKS] started. Waiting for md:active_expiry from producer...")
    waiting_logged = True

    while True:
        try:
            active = rs.hgetall("md:active_expiry")  # {"IOC": "2026-01-27", ...}

            if not active:
                if not waiting_logged:
                    print("[GREEKS] still waiting for md:active_expiry...")
                    waiting_logged = True
                time.sleep(2)
                continue

            if waiting_logged:
                print(f"[GREEKS] active_expiry ready: {active}")
                waiting_logged = False

            ok = poller.poll_once(active_expiry=active, per_request_sleep=PER_REQUEST_SLEEP)
            print(f"[GREEKS] poll cycle ok={ok}/{len(active)} underlyings")

            time.sleep(GREEKS_POLL_SEC)

        except KeyboardInterrupt:
            print("\n[GREEKS] stopped by user.")
            return
        except Exception as e:
            # If token expired or temporary network issue, relogin and continue
            print("[GREEKS] error:", repr(e))
            print("[GREEKS] re-login in 3s...")
            time.sleep(3)
            try:
                _, auth_token, _ = login()
                poller = GreeksPoller(auth_token=auth_token)
            except Exception as e2:
                print("[GREEKS] relogin failed:", repr(e2))
                time.sleep(5)


if __name__ == "__main__":
    main()

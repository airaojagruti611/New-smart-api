import os
from app.config import load_symbols, ANGEL_API_KEY, ANGEL_CLIENT_CODE
from app.angel_auth import login
from app.ws_producer import MarketDataProducer

def main():
    symbols = load_symbols()
    obj, auth_token, feed_token = login()

    producer = MarketDataProducer(
        auth_token=auth_token,
        feed_token=feed_token,
        client_code=ANGEL_CLIENT_CODE,
        api_key=ANGEL_API_KEY,
        symbols=symbols
    )
    producer.start()

if __name__ == "__main__":
    main()

"""Refuse pipeline startup if its configured Redis is not the Docker instance."""
import argparse
import json
import sys
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.config import REDIS_URL
import redis


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--expected-run-id', required=True)
    args = parser.parse_args()
    client = redis.Redis.from_url(REDIS_URL, socket_timeout=10)
    actual = client.info('server')['run_id']
    if actual != args.expected_run_id:
        raise SystemExit('Redis instance mismatch: configured REDIS_URL does not reach project Docker Redis')
    url = urlsplit(REDIS_URL)
    print(json.dumps({'verified_project_redis': True, 'host': url.hostname,
                      'port': url.port or 6379, 'db': url.path}))


if __name__ == '__main__':
    main()

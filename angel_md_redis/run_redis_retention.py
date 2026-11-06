"""Bounded safe retention pass; independent of Drive upload completion."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import redis
from app.config import REDIS_URL, BASE_DIR
from app.redis_retention import trim_archived_history
from run_archiver_layers import STREAMS


def main():
    client = redis.Redis.from_url(REDIS_URL, socket_timeout=15)
    rows = trim_archived_history(client, [value[0] for value in STREAMS.values()],
                                 keep_minutes=int(os.getenv('REDIS_RETAIN_MINUTES', '60')))
    report = {'completed_at': datetime.now(timezone.utc).isoformat(),
              'removed': sum(row['removed'] for row in rows), 'streams': rows}
    path = BASE_DIR / 'runtime' / 'redis-retention.json'
    path.parent.mkdir(exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(report, indent=2), encoding='utf-8')
    temporary.replace(path)
    print(json.dumps(report), flush=True)


if __name__ == '__main__':
    main()

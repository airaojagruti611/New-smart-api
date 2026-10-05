"""Shared freshness guards for stream consumers.

Every consumer group is created at id "0", so on a first start (or after any
downtime) a worker replays the stream history. Many workers also stamp
``ts_ms=now`` on what they publish, so replayed data would look fresh to the
next layer. Decision-path workers therefore judge age from the *source*:

* the Redis stream id — its ms part is the XADD time, which no worker can
  re-stamp, and
* explicit source timestamps carried in payloads (``bar_ts_ms``,
  ``signal_ts_ms``, quote ``ts_ms``).
"""

from __future__ import annotations

import os
from typing import Mapping, Optional, Union

Id = Union[str, bytes]


def stream_id_ms(msg_id: Id) -> Optional[int]:
    """Milliseconds part of a Redis stream id ("1712345678901-0"), or None."""
    if isinstance(msg_id, bytes):
        msg_id = msg_id.decode("utf-8", "replace")
    try:
        return int(str(msg_id).split("-", 1)[0])
    except (TypeError, ValueError):
        return None


def message_age_ms(msg_id: Id, now_ms: int) -> Optional[int]:
    ms = stream_id_ms(msg_id)
    return None if ms is None else now_ms - ms


def is_stale_message(msg_id: Id, now_ms: int, max_age_ms: int) -> bool:
    """True when the message was added more than ``max_age_ms`` ago.

    An unparseable id is treated as stale (fail closed). ``max_age_ms <= 0``
    disables the check.
    """
    if max_age_ms <= 0:
        return False
    age = message_age_ms(msg_id, now_ms)
    return age is None or age > max_age_ms


def ts_field_ms(payload: Optional[Mapping], field: str = "ts_ms") -> Optional[int]:
    """Integer timestamp field from a payload (str/int/float), or None."""
    if not payload:
        return None
    v = payload.get(field)
    if isinstance(v, bytes):
        v = v.decode("utf-8", "replace")
    try:
        ms = int(float(v))
    except (TypeError, ValueError):
        return None
    return ms if ms > 0 else None


def is_fresh_ts(ts_ms: Optional[int], now_ms: int, max_age_ms: int) -> bool:
    """True when ``ts_ms`` is present and no older than ``max_age_ms``.

    Missing timestamps are NOT fresh (fail closed).
    """
    if ts_ms is None:
        return False
    if max_age_ms <= 0:
        return True
    return now_ms - ts_ms <= max_age_ms


def is_fresh_payload(payload: Optional[Mapping], now_ms: int, max_age_ms: int, field: str = "ts_ms") -> bool:
    return is_fresh_ts(ts_field_ms(payload, field), now_ms, max_age_ms)


def env_ms(name: str, default_sec: float) -> int:
    """Read a max-age env var given in seconds, returned in ms."""
    raw = os.getenv(name)
    try:
        sec = float(raw) if raw not in (None, "") else float(default_sec)
    except ValueError:
        sec = float(default_sec)
    return int(sec * 1000)

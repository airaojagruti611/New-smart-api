"""
Broker funds for capital allocation.

Reads the account snapshot that run_account.py publishes to md:account:latest
(paper ledger, or Angel getRMS when ACCOUNT_MODE=live) and returns the
available margin. Returns None when the snapshot is missing, unparsable,
carries an error, or is older than ACCOUNT_MAX_AGE_SEC, so the caller falls
back to TOTAL_CAPITAL minus open usage.
"""

from __future__ import annotations

import json
import math
import os
import time
from typing import Optional

ACCOUNT_KEY = os.getenv("ACCOUNT_LATEST_KEY", "md:account:latest")
ACCOUNT_MAX_AGE_SEC = float(os.getenv("ACCOUNT_MAX_AGE_SEC", "120"))


def funds_from_snapshot(doc: Optional[dict], now_ms: int, max_age_ms: float) -> Optional[float]:
    """Available margin from an AccountSnapshot dict, or None if it can't be trusted."""
    if not isinstance(doc, dict) or doc.get("error"):
        return None
    try:
        ts = int(float(doc.get("ts_ms") or 0))
        avail = float(doc.get("available_margin"))
    except (TypeError, ValueError):
        return None
    if ts <= 0 or now_ms - ts > max_age_ms or not math.isfinite(avail):
        return None
    return max(avail, 0.0)


def get_available_funds(r=None, now_ms: Optional[int] = None) -> Optional[float]:
    """Live available funds from md:account:latest, or None when unknown/stale."""
    if r is None:
        return None
    try:
        raw = r.get(ACCOUNT_KEY)
        doc = json.loads(raw) if raw else None
    except Exception:
        return None
    now = int(time.time() * 1000) if now_ms is None else now_ms
    return funds_from_snapshot(doc, now, ACCOUNT_MAX_AGE_SEC * 1000)

"""
Paper-trade tracker: md:strike:select OK -> virtual trade -> md:journal:outcome.
Ticks come from md:ticks:opt (fields: tradingsymbol, ltp, bid, ask, ts_recv ms).
Underlying ticks from md:ticks:eq (symbol, ltp) record the actual spot move vs
the expected move on each outcome row (actual_move_pct, move_vs_em, ...).
Open trades persist at md:paper:open:{TRADINGSYMBOL} so a restart resumes them.
"""

from __future__ import annotations

import dataclasses
import json
import os
import time

import redis

from app.learning_keys import PAPER_OPEN_PREFIX, STREAM_JOURNAL_OUTCOME
from app.logging_setup import setup_logger
from app.trade_outcome import (
    PaperConfig, PaperTrade, close_trade, exit_reason, is_session_end, on_spot, on_tick, open_trade,
)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SEL_STREAM = os.getenv("STREAM_STRIKE_SELECT", "md:strike:select")
TICK_STREAM = os.getenv("TICKS_STREAM_OPT", "md:ticks:opt")
EQ_STREAM = os.getenv("TICKS_STREAM_EQ", "md:ticks:eq")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_JOURNAL_OUTCOME", "200000"))
GROUP = os.getenv("PAPER_TRACKER_GROUP", "paper-tracker")
CONSUMER = os.getenv("PAPER_TRACKER_CONSUMER", "paper-tracker-1")
PERSIST_SEC = float(os.getenv("PAPER_PERSIST_SEC", "15"))

log = setup_logger("paper_tracker")
CFG = PaperConfig()


def _f(v):
    try:
        return float(v) if v not in (None, "") else None
    except ValueError:
        return None


def ensure_group(r, stream, group):
    try:
        r.xgroup_create(stream, group, id="$", mkstream=True)  # new signals only; never replay history
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _persist(r, t: PaperTrade):
    r.set(PAPER_OPEN_PREFIX + t.tradingsymbol, json.dumps(dataclasses.asdict(t)), ex=86400)


def _load(r) -> dict:
    out = {}
    for k in r.scan_iter(PAPER_OPEN_PREFIX + "*", count=500):
        try:
            out[k[len(PAPER_OPEN_PREFIX):]] = PaperTrade(**json.loads(r.get(k)))
        except Exception:
            continue
    return out


def _close(r, open_t: dict, ts: str, reason: str, now_ms: int):
    t = open_t.pop(ts)
    r.xadd(STREAM_JOURNAL_OUTCOME, close_trade(t, reason, now_ms), maxlen=OUT_MAXLEN, approximate=True)
    r.delete(PAPER_OPEN_PREFIX + ts)
    log.info("CLOSE %s reason=%s", ts, reason)


def apply_spot_ticks(open_t: dict, msgs) -> None:
    """Fold md:ticks:eq rows into every open trade on that underlying."""
    by_sym: dict = {}
    for ts, t in open_t.items():
        by_sym.setdefault((t.symbol or "").strip().upper(), []).append(ts)
    for _mid, f in msgs:
        sym = str(f.get("symbol") or "").strip().upper()
        for ts in by_sym.get(sym, ()):
            open_t[ts] = on_spot(open_t[ts], _f(f.get("ltp")))


def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, SEL_STREAM, GROUP)
    open_t = _load(r)
    last_id = f"{int(time.time() * 1000)}-0"
    eq_last_id = last_id
    last_persist = 0.0
    log.info("START resumed=%d", len(open_t))
    while True:
        now_ms = int(time.time() * 1000)
        # 1) new virtual entries
        resp = r.xreadgroup(GROUP, CONSUMER, {SEL_STREAM: ">"}, count=500, block=200)
        for _, msgs in resp or []:
            ids = []
            for mid, f in msgs:
                ids.append(mid)
                if f.get("status") != "OK":
                    continue
                ts = f.get("tradingsymbol") or ""
                if not ts or ts in open_t:
                    continue  # one open trade per tradingsymbol
                if is_session_end(now_ms, CFG):
                    continue  # would close instantly as session_end with 0 P&L and skew stats
                t = open_trade(f, now_ms, CFG)
                if t:
                    open_t[ts] = t
                    _persist(r, t)
                    log.info("OPEN %s entry=%s", ts, t.entry_premium)
            r.xack(SEL_STREAM, GROUP, *ids)
        # 2) ticks (plain XREAD, no group; only tradingsymbols we track)
        if open_t:
            resp = r.xread({TICK_STREAM: last_id}, count=5000, block=200)
            for _, msgs in resp or []:
                for mid, f in msgs:
                    last_id = mid
                    ts = f.get("tradingsymbol")
                    t = open_t.get(ts)
                    if t is None:
                        continue
                    tms = int(_f(f.get("ts_recv")) or now_ms)
                    open_t[ts] = on_tick(t, _f(f.get("ltp")), tms, CFG, _f(f.get("bid")), _f(f.get("ask")))
            resp = r.xread({EQ_STREAM: eq_last_id}, count=5000, block=1)
            for _, msgs in resp or []:
                if msgs:
                    eq_last_id = msgs[-1][0]
                apply_spot_ticks(open_t, msgs)
        else:
            last_id = f"{now_ms}-0"
            eq_last_id = last_id
        # 3) exits
        for ts in list(open_t):
            reason = exit_reason(open_t[ts], now_ms, CFG)
            if reason:
                _close(r, open_t, ts, reason, now_ms)
        if time.time() - last_persist > PERSIST_SEC:
            for t in open_t.values():
                _persist(r, t)
            last_persist = time.time()


if __name__ == "__main__":
    main()

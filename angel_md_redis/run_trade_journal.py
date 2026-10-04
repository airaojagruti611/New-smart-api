"""
run_trade_journal.py
────────────────────
Module 22 — paper-trade journal (DECISION.md D11). NO broker orders.

  md:icare APPROVED  ->  paper entry at the contract's ask           (EXEC_MODE=shadow, default)
  md:exec:fill       ->  entry at the executor's average fill / lots (EXEC_MODE=paper|live)
                         md:position:open:{TSYM}   (JSON PaperPosition; also read by
                                                    run_liquidity_score / run_account / run_icare)
  every JOURNAL_MARK_SEC: mark each open position at bid (md:bidask:latest:{TSYM}),
                         track MFE / MAE, exit on SL / TARGET / TIME / EOD (at bid);
                         Module 18 (md:tsl:state:{trade_id}): TSL_MODE=active adds
                         TRAILING_STOP and drops TARGET; shadow stores tsl_* fields
  on exit            ->  md:journal              stream, one record per closed trade
                         md:journal:stats        bucket stats (ALL / SYM / SYM_PHASE)
                         md:journal:daily:{date} realized PnL, trades, wins
                         md:journal:closed:{trade_id} the record (read by Module 18)

PnL is NET of charges (charges.json; entry charges from the executor when present),
gross kept as gross_pnl (DECISION.md §7 X-Q8). In shadow mode the executor's
simulated execution (md:exec:latest:tsym:{TSYM}) is attached as exec_* fields.

Restart-safe: open positions live in Redis, not in process memory.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time
from typing import Optional

import redis

from app.logging_setup import setup_logger
from app.option_pricing import IST
from app.order_executor.charges import charges, load_rates
from app.order_executor.engine import exec_fields
from app.trade_journal import (
    PaperPosition,
    apply_charges,
    close_position,
    exit_reason,
    mark,
    open_position,
    tsl_exit_decision,
    tsl_journal_fields,
    update_stats,
)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

EXEC_MODE = os.getenv("EXEC_MODE", "shadow").strip().lower()
IN_STREAM = (os.getenv("STREAM_EXEC_FILL", "md:exec:fill") if EXEC_MODE in ("paper", "live")
             else os.getenv("STREAM_ICARE", "md:icare"))
EXEC_LATEST_PREFIX = os.getenv("EXEC_LATEST_PREFIX", "md:exec:latest:")
EXEC_ATTACH_MAX_MS = 60_000
RATES = load_rates()
OUT_STREAM = os.getenv("STREAM_JOURNAL", "md:journal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_JOURNAL", "200000"))
POSITION_OPEN_PREFIX = os.getenv("POSITION_OPEN_PREFIX", "md:position:open:")
BIDASK_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")
STATS_KEY = os.getenv("JOURNAL_STATS_KEY", "md:journal:stats")
DAILY_PREFIX = os.getenv("JOURNAL_DAILY_PREFIX", "md:journal:daily:")

GROUP = os.getenv("JOURNAL_GROUP", "journal")
CONSUMER = os.getenv("JOURNAL_CONSUMER", "journal-1")
MARK_SEC = float(os.getenv("JOURNAL_MARK_SEC", "2"))
EOD_HHMM = os.getenv("JOURNAL_EOD_HHMM", "15:20")
ENABLED = os.getenv("PAPER_TRADING", "1") == "1"
CLOSED_PREFIX = os.getenv("JOURNAL_CLOSED_PREFIX", "md:journal:closed:")
TSL_STATE_PREFIX = os.getenv("TSL_STATE_PREFIX", "md:tsl:state:")
TSL_MODE = os.getenv("TSL_MODE", "shadow").strip().lower()
RANKING_LATEST_PREFIX = os.getenv("RANKING_LATEST_PREFIX", "md:ranking:latest:")
RANK_FIELDS = ("trade_score", "rank", "rank_decision", "rank_confidence", "rank_mode", "rank_components")
TSL_MAX_AGE_MS = int(float(os.getenv("JOURNAL_TSL_MAX_AGE_SEC", "60")) * 1000)

log = setup_logger("trade_journal")


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _f(v) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _load_json(r: redis.Redis, key: str) -> dict:
    raw = r.get(key)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def is_eod(now: dt.datetime, hhmm: str = EOD_HHMM) -> bool:
    hh, mm = (int(x) for x in hhmm.split(":"))
    return (now.hour, now.minute) >= (hh, mm)


def save_position(r: redis.Redis, pos: PaperPosition) -> None:
    r.set(f"{POSITION_OPEN_PREFIX}{pos.tradingsymbol}", json.dumps(pos.to_dict(), separators=(",", ":")))


def record_close(r: redis.Redis, rec: dict, today: str) -> None:
    r.xadd(OUT_STREAM, {k: "" if v is None else str(v) for k, v in rec.items()}, maxlen=OUT_MAXLEN, approximate=True)
    r.set(f"{CLOSED_PREFIX}{rec['trade_id']}", json.dumps(rec, separators=(",", ":"), default=str), ex=3 * 86400)
    stats = update_stats(_load_json(r, STATS_KEY), rec)
    r.set(STATS_KEY, json.dumps(stats, separators=(",", ":")))
    daily = _load_json(r, f"{DAILY_PREFIX}{today}") or {"realized_pnl": 0.0, "trades": 0, "wins": 0}
    daily["realized_pnl"] = round(float(daily.get("realized_pnl") or 0.0) + float(rec["pnl"]), 2)
    daily["trades"] = int(daily.get("trades") or 0) + 1
    daily["wins"] = int(daily.get("wins") or 0) + int(rec["win"])
    r.set(f"{DAILY_PREFIX}{today}", json.dumps(daily, separators=(",", ":")), ex=7 * 86400)


def with_ranking(r: redis.Redis, fields: dict) -> dict:
    """
    Module 13 (DECISION.md §6 R16): in shadow mode ICARE never sees the ranking,
    so attach the Trade Ranking verdict that existed for this contract, letting
    the journal show whether trades the ranker would have rejected lost money.
    """
    if fields.get("rank_decision"):
        return fields
    key = f"{RANKING_LATEST_PREFIX}{str(fields.get('symbol') or '').upper()}:{str(fields.get('side') or '').upper()}"
    rk = _load_json(r, key)
    if not rk or rk.get("tradingsymbol") != fields.get("tradingsymbol"):
        return fields
    return dict(fields, **{k: str(rk.get(k) or "") for k in RANK_FIELDS})


def handle_approval(r: redis.Redis, fields: dict, now_ms: int) -> None:
    if str(fields.get("status") or "").upper() != "APPROVED":
        return
    if is_eod(dt.datetime.now(IST)):
        log.info("SKIP after_eod tsym=%s", fields.get("tradingsymbol"))
        return
    tsym = str(fields.get("tradingsymbol") or "")
    if not tsym or r.exists(f"{POSITION_OPEN_PREFIX}{tsym}"):
        log.info("SKIP already_open tsym=%s", tsym)
        return
    if fields.get("fill_price"):        # md:exec:fill: the executor's actual average price
        fill = _f(fields.get("fill_price"))
    else:
        ba = _load_json(r, f"{BIDASK_PREFIX}{tsym}")
        fill = _f(ba.get("ask")) or _f(fields.get("premium"))
    pos = open_position(with_ranking(r, fields), fill or 0.0, now_ms)
    if pos is None:
        log.info("SKIP cannot_open tsym=%s fill=%s lots=%s", tsym, fill, fields.get("recommended_lots"))
        return
    save_position(r, pos)
    log.info("PAPER_OPEN %s lots=%s qty=%s entry=%s sl=%s target=%s time_stop_ms=%s",
             tsym, pos.lots, pos.qty, pos.entry_premium, pos.sl_premium, pos.target_premium, pos.time_stop_ms)


def entry_charges(pos: PaperPosition) -> float:
    """Executor-reported charges when the entry came from md:exec:fill, else a one-order estimate."""
    reported = _f(pos.context.get("entry_charges"))
    if reported is not None:
        return reported
    return charges("BUY", pos.entry_premium * pos.qty, 1, RATES)["total"]


def shadow_exec_fields(r: redis.Redis, pos: PaperPosition, rec: dict) -> dict:
    """EXEC_MODE=shadow: attach the executor's simulated execution of the same approval."""
    if rec.get("exec_execution_status"):
        return {}
    rep = _load_json(r, f"{EXEC_LATEST_PREFIX}tsym:{pos.tradingsymbol}")
    if not rep or abs(int(_f(rep.get("signal_ts_ms")) or 0) - pos.entry_ts_ms) > EXEC_ATTACH_MAX_MS:
        return {}
    return exec_fields(rep)


def mark_all(r: redis.Redis, now_ms: int) -> None:
    now = dt.datetime.now(IST)
    eod = is_eod(now)
    for key in list(r.scan_iter(match=f"{POSITION_OPEN_PREFIX}*", count=500)):
        doc = _load_json(r, key)
        if not doc or "trade_id" not in doc:
            continue  # not a paper position (e.g. manual entry for liquidity module)
        pos = PaperPosition.from_dict(doc)
        bid = _f(_load_json(r, f"{BIDASK_PREFIX}{pos.tradingsymbol}").get("bid"))
        pos = mark(pos, bid)
        tsl = _load_json(r, f"{TSL_STATE_PREFIX}{pos.trade_id}")
        force, use_target = tsl_exit_decision(tsl, pos.trade_id, now_ms, TSL_MODE, TSL_MAX_AGE_MS)
        reason = exit_reason(pos, bid, now_ms, eod, use_target=use_target, force=force)
        if reason is None:
            save_position(r, pos)
            continue
        exit_px = bid if bid and bid > 0 else (pos.last_premium or pos.entry_premium)
        rec = close_position(pos, exit_px, now_ms, reason)
        rec.update(tsl_journal_fields(tsl, pos))
        rec.update(shadow_exec_fields(r, pos, rec))
        rec = apply_charges(rec, entry_charges(pos), charges("SELL", exit_px * pos.qty, 1, RATES)["total"])
        record_close(r, rec, now.date().isoformat())
        r.delete(key)
        log.info("PAPER_CLOSE %s reason=%s entry=%s exit=%s pnl=%s mfe=%s mae=%s hold_min=%s",
                 pos.tradingsymbol, reason, pos.entry_premium, exit_px, rec["pnl"], rec["mfe"], rec["mae"],
                 rec["holding_minutes"])


def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_STREAM, GROUP)
    log.info("START reading %s, writing %s (paper=%s mark_sec=%s eod=%s exec_mode=%s)",
             IN_STREAM, OUT_STREAM, ENABLED, MARK_SEC, EOD_HHMM, EXEC_MODE)
    last_mark = 0.0

    while True:
        resp = r.xreadgroup(groupname=GROUP, consumername=CONSUMER, streams={IN_STREAM: ">"},
                            count=200, block=int(MARK_SEC * 1000))
        now_ms = int(time.time() * 1000)
        ack_ids = []
        for _stream, msgs in resp or []:
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)
                if ENABLED:
                    handle_approval(r, fields, now_ms)
        if ack_ids:
            r.xack(IN_STREAM, GROUP, *ack_ids)

        if time.time() - last_mark >= MARK_SEC:
            mark_all(r, now_ms)
            last_mark = time.time()


if __name__ == "__main__":
    main()

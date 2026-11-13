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

Freshness (every group starts at "0", so a restart replays history):
  * an md:icare / md:exec:fill message older than JOURNAL_MAX_FILL_AGE_SEC (120) by stream
    id is ignored — old simulated / historical fills never become positions
  * md:exec:fill opens a position only when its exec_mode equals this journal's EXEC_MODE
    (the executor does not publish fills in shadow mode at all)
  * the shadow entry at the ask needs a quote <= JOURNAL_MAX_QUOTE_AGE_SEC (30) old; marks /
    SL checks ignore a bid older than that (TIME / EOD still apply)

Exits (E17):
  shadow  closes at bid (bookkeeping), no orders
  paper   publishes md:exec:exit_request (the executor simulates the SELL) and closes at bid
  live    publishes md:exec:exit_request and keeps the position OPEN (exit_pending) until the
          broker-confirmed SELL arrives on md:exec:exit_fill; re-requests every
          JOURNAL_EXIT_RETRY_SEC (the executor never sells more than the position held)

Restart-safe: open positions live in Redis, not in process memory.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time
from dataclasses import replace
from typing import Optional

import redis

from app.freshness import env_ms, is_fresh_ts, is_stale_message
from app.logging_setup import setup_logger
from app.option_pricing import IST
from app.order_executor.charges import charges, load_rates
from app.order_executor.engine import exec_fields
from app.order_executor.market import quote_age_basis_ms
from app.trade_journal import (
    PaperPosition,
    apply_charges,
    apply_exit_fill,
    close_position,
    exit_request,
    merge_fill,
    with_forced,
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
MAX_FILL_AGE_MS = env_ms("JOURNAL_MAX_FILL_AGE_SEC", 120)
MAX_QUOTE_AGE_MS = env_ms("JOURNAL_MAX_QUOTE_AGE_SEC", 30)
EXIT_REQ_STREAM = os.getenv("STREAM_EXEC_EXIT_REQUEST", "md:exec:exit_request")
EXIT_FILL_STREAM = os.getenv("STREAM_EXEC_EXIT_FILL", "md:exec:exit_fill")
EXIT_RETRY_MS = env_ms("JOURNAL_EXIT_RETRY_SEC", 60)

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


def fresh_quote(r: redis.Redis, tsym: str, now_ms: int) -> dict:
    """md:bidask:latest:{TSYM} when its ts_ms is <= JOURNAL_MAX_QUOTE_AGE_SEC old, else {}."""
    ba = _load_json(r, f"{BIDASK_PREFIX}{tsym}")
    return ba if is_fresh_ts(quote_age_basis_ms(ba), now_ms, MAX_QUOTE_AGE_MS) else {}


FILL_SEEN_PREFIX = "md:journal:fill_seen:"


def handle_fill(r: redis.Redis, fields: dict, now_ms: int, msg_id: Optional[str], mode: str) -> Optional[PaperPosition]:
    """
    paper / live: an md:exec:fill of THIS mode is a fact — never discarded. It is opened even
    when a rule says no (at/below SL, after the EOD cut-off, an old message), flagged
    OPENED_FORCED:<reasons>, and exited at once; a second fill on an open contract is merged.
    """
    tsym = str(fields.get("tradingsymbol") or "")
    fill_mode = str(fields.get("exec_mode") or "").lower()
    if not fields.get("fill_price") or fill_mode != mode:
        log.info("SKIP exec_mode_mismatch tsym=%s fill_mode=%s journal_mode=%s", tsym, fill_mode, mode)
        return None
    exec_tid = str(fields.get("exec_trade_id") or "")
    if exec_tid and not r.set(f"{FILL_SEEN_PREFIX}{exec_tid}", "1", nx=True, ex=7 * 86400):
        log.info("SKIP duplicate_fill exec_trade_id=%s", exec_tid)      # the same fill delivered twice
        return None
    violations = []
    if msg_id is not None and is_stale_message(msg_id, now_ms, MAX_FILL_AGE_MS):
        violations.append("STALE_FILL")
    if is_eod(dt.datetime.now(IST)):
        violations.append("AFTER_EOD")
    pos = open_position(with_ranking(r, fields), _f(fields.get("fill_price")) or 0.0, now_ms, exec_mode=mode, force=True)
    if pos is None:
        log.error("UNOPENABLE_FILL tsym=%s fill=%s lots=%s — check the fill message", tsym,
                  fields.get("fill_price"), fields.get("recommended_lots"))
        return None
    pos = with_forced(pos, violations)
    key = f"{POSITION_OPEN_PREFIX}{tsym}"
    existing = _load_json(r, key)
    if existing and "trade_id" in existing:
        pos = merge_fill(PaperPosition.from_dict(existing), pos)
        log.warning("FILL_MERGED %s into %s qty=%s avg=%s", exec_tid, pos.trade_id, pos.qty, pos.entry_premium)
    save_position(r, pos)
    log.info("%s_OPEN %s lots=%s qty=%s entry=%s sl=%s forced=%s", mode.upper(), tsym, pos.lots, pos.qty,
             pos.entry_premium, pos.sl_premium, pos.opened_forced)
    if pos.opened_forced and not pos.exit_pending:
        reason = "FORCED_" + pos.opened_forced.split(":", 1)[1].split(",")[0]
        bid = _f(fresh_quote(r, tsym, now_ms).get("bid"))
        execute_exit(r, key, pos, reason, bid, now_ms, mode, {})
    return pos


def handle_approval(r: redis.Redis, fields: dict, now_ms: int, msg_id: Optional[str] = None,
                    mode: Optional[str] = None) -> None:
    mode = (mode or EXEC_MODE).lower()
    if str(fields.get("status") or "").upper() != "APPROVED":
        return
    if mode in ("paper", "live"):
        handle_fill(r, fields, now_ms, msg_id, mode)
        return
    # shadow: md:icare approvals, freshness-filtered (no real money behind them)
    tsym = str(fields.get("tradingsymbol") or "")
    if msg_id is not None and is_stale_message(msg_id, now_ms, MAX_FILL_AGE_MS):
        log.info("SKIP stale_message source=%s tsym=%s", msg_id, tsym)
        return
    if is_eod(dt.datetime.now(IST)):
        log.info("SKIP after_eod tsym=%s", fields.get("tradingsymbol"))
        return
    if not tsym or r.exists(f"{POSITION_OPEN_PREFIX}{tsym}"):
        log.info("SKIP already_open tsym=%s", tsym)
        return
    fill = _f(fresh_quote(r, tsym, now_ms).get("ask"))
    if not fill:
        log.info("SKIP stale_or_missing_quote tsym=%s", tsym)
        return
    pos = open_position(with_ranking(r, fields), fill, now_ms, exec_mode=mode)
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


def request_exit(r: redis.Redis, pos: PaperPosition, reason: str, now_ms: int, mode: str,
                 bid: Optional[float]) -> PaperPosition:
    r.xadd(EXIT_REQ_STREAM, exit_request(pos, reason, now_ms, mode, bid=bid), maxlen=OUT_MAXLEN, approximate=True)
    log.info("EXIT_REQUEST %s trade=%s qty=%s reason=%s mode=%s bid=%s", pos.tradingsymbol, pos.trade_id,
             pos.qty, reason, mode, bid)
    return replace(pos, exit_pending=reason, exit_requested_ms=now_ms)


def _close(r: redis.Redis, key: str, pos: PaperPosition, exit_px: float, now_ms: int, reason: str, tsl: dict,
           exit_ch: float, entry_ch: float, mode: str, delete: bool = True) -> dict:
    rec = close_position(pos, exit_px, now_ms, reason, mode="live" if mode == "live" else "paper")
    rec.update(tsl_journal_fields(tsl, pos))
    rec.update(shadow_exec_fields(r, pos, rec))
    rec = apply_charges(rec, entry_ch, exit_ch)
    record_close(r, rec, dt.datetime.fromtimestamp(now_ms / 1000, IST).date().isoformat())
    if delete:
        r.delete(key)
    return rec


def mark_all(r: redis.Redis, now_ms: int, mode: Optional[str] = None) -> None:
    mode = (mode or EXEC_MODE).lower()
    now = dt.datetime.now(IST)
    eod = is_eod(now)
    for key in list(r.scan_iter(match=f"{POSITION_OPEN_PREFIX}*", count=500)):
        doc = _load_json(r, key)
        if not doc or "trade_id" not in doc:
            continue  # not a paper position (e.g. manual entry for liquidity module)
        pos = PaperPosition.from_dict(doc)
        bid = _f(fresh_quote(r, pos.tradingsymbol, now_ms).get("bid"))     # stale bid = no mark / no SL test
        pos = mark(pos, bid)
        if mode == "live" and pos.exit_pending:
            # SELL working at the broker: the position stays open until md:exec:exit_fill confirms it
            if now_ms - pos.exit_requested_ms >= EXIT_RETRY_MS:
                pos = request_exit(r, pos, pos.exit_pending, now_ms, mode, bid)
            save_position(r, pos)
            continue
        tsl = _load_json(r, f"{TSL_STATE_PREFIX}{pos.trade_id}")
        force, use_target = tsl_exit_decision(tsl, pos.trade_id, now_ms, TSL_MODE, TSL_MAX_AGE_MS)
        reason = exit_reason(pos, bid, now_ms, eod, use_target=use_target, force=force)
        if reason is None:
            save_position(r, pos)
            continue
        execute_exit(r, key, pos, reason, bid, now_ms, mode, tsl)


def execute_exit(r: redis.Redis, key: str, pos: PaperPosition, reason: str, bid: Optional[float], now_ms: int,
                 mode: str, tsl: dict) -> Optional[dict]:
    """live: SELL request, position stays open until confirmed. paper: request (simulated) + close at bid.
    shadow: close at bid."""
    if mode == "live":
        save_position(r, request_exit(r, pos, reason, now_ms, mode, bid))
        return None
    if mode == "paper":
        request_exit(r, pos, reason, now_ms, mode, bid)     # executor simulates the SELL (audit)
    exit_px = bid if bid and bid > 0 else (pos.last_premium or pos.entry_premium)
    rec = _close(r, key, pos, exit_px, now_ms, reason, tsl,
                 charges("SELL", exit_px * pos.qty, 1, RATES)["total"], entry_charges(pos), mode)
    log.info("PAPER_CLOSE %s reason=%s entry=%s exit=%s pnl=%s mfe=%s mae=%s hold_min=%s",
             pos.tradingsymbol, reason, pos.entry_premium, exit_px, rec["pnl"], rec["mfe"], rec["mae"],
             rec["holding_minutes"])
    return rec


def handle_exit_fill(r: redis.Redis, fields: dict, now_ms: int, mode: Optional[str] = None) -> Optional[dict]:
    """
    Live: a broker-confirmed SELL (md:exec:exit_fill) closes the position — the only way a live
    position leaves the journal. Partial: the filled part is journaled, the rest stays open
    and is re-requested at once. Paper already closed at bid, so its exit fills are ignored.
    """
    mode = (mode or EXEC_MODE).lower()
    if mode != "live" or str(fields.get("exec_mode") or "").lower() != mode:
        return None
    tsym = str(fields.get("tradingsymbol") or "")
    key = f"{POSITION_OPEN_PREFIX}{tsym}"
    doc = _load_json(r, key)
    if not doc or str(doc.get("trade_id")) != str(fields.get("trade_id")):
        log.warning("EXIT_FILL without open position trade=%s tsym=%s", fields.get("trade_id"), tsym)
        return None
    pos = PaperPosition.from_dict(doc)
    closed, remaining, px = apply_exit_fill(pos, fields)
    rec = None
    if closed is not None:
        share = closed.qty / (pos.orig_qty or pos.qty)
        sell_ch = _f(fields.get("charges"))
        if sell_ch is None:
            sell_ch = charges("SELL", px * closed.qty, 1, RATES)["total"]
        else:
            sell_ch *= closed.qty / max(_f(fields.get("filled_qty")) or closed.qty, closed.qty)
        tsl = _load_json(r, f"{TSL_STATE_PREFIX}{pos.trade_id}")
        rec = _close(r, key, closed, px, now_ms, pos.exit_pending or str(fields.get("reason") or "EXIT"), tsl,
                     sell_ch, entry_charges(pos) * share, mode, delete=remaining is None)
        log.info("LIVE_CLOSE %s reason=%s qty=%s exit=%s pnl=%s", tsym, rec["exit_reason"], closed.qty, px, rec["pnl"])
    if remaining is not None:
        save_position(r, remaining)
        log.warning("LIVE_EXIT_INCOMPLETE %s left=%s status=%s — re-requesting", tsym, remaining.qty,
                    fields.get("status"))
    return rec


def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_STREAM, GROUP)
    streams = {IN_STREAM: ">"}
    if EXEC_MODE == "live":
        ensure_group(r, EXIT_FILL_STREAM, GROUP)
        streams[EXIT_FILL_STREAM] = ">"
    log.info("START reading %s, writing %s (paper=%s mark_sec=%s eod=%s exec_mode=%s)",
             IN_STREAM, OUT_STREAM, ENABLED, MARK_SEC, EOD_HHMM, EXEC_MODE)
    last_mark = 0.0

    while True:
        resp = r.xreadgroup(groupname=GROUP, consumername=CONSUMER, streams=streams,
                            count=200, block=int(MARK_SEC * 1000))
        now_ms = int(time.time() * 1000)
        for stream, msgs in resp or []:
            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)
                if stream == EXIT_FILL_STREAM:
                    handle_exit_fill(r, fields, now_ms)       # never skipped: a real SELL happened
                elif ENABLED:
                    handle_approval(r, fields, now_ms, msg_id=msg_id)
            if ack_ids:
                r.xack(stream, GROUP, *ack_ids)

        if time.time() - last_mark >= MARK_SEC:
            mark_all(r, now_ms)
            last_mark = time.time()


if __name__ == "__main__":
    main()

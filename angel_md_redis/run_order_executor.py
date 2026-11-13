"""
run_order_executor.py
─────────────────────
Module 14 — Order Executor / Trade Entry Engine, Redis wiring (DECISION.md §7).

Consumes md:icare (APPROVED only; includes Module 18 re-entries) and executes each
approval with app/order_executor (validation, capped limit ladder, slicing, partial
fills, timeout, kill switch). Reads:
  md:bidask:latest:{TSYM}        bid / ask / sizes / depth5 (run_bidask_analyzer.py)
  md:account:latest              available margin (paper ledger fallback, like ICARE)
  md:position:open:*             open positions (exposure / same underlying)
  md:control:kill_switch         "1" = cancel working orders, no new entries
  OpenAPIScripMaster.json        tick size, lot size, freeze qty, expiry per contract

Also consumes md:exec:exit_request (E17) from the journal: {trade_id, tradingsymbol, qty,
reason, limit_floor, lot_size, orig_qty, exec_mode} -> SELL LIMIT ladder (bid -> floor, never
abandoned, never market). Requests for another exec_mode are ignored.

Freshness: the md:icare group starts at "0". A command's signal time is `signal_ts_ms` (ICARE
carries the upstream source time) or the md:icare stream-id ms — never ICARE's ts_ms (publish
time). Messages whose stream id is older than EXEC_COMMAND_TTL_SEC are finished as
REJECTED_BEFORE_EXECUTION / COMMAND_EXPIRED without any order; signal_ts_ms older than
EXEC_MAX_SIGNAL_AGE_SEC (60) -> SIGNAL_EXPIRED.

Writes:
  md:exec                        stream: every COMMAND / VALIDATED / ORDER_* / FILL / FINAL event,
                                 one REPORT per trade (the final report) and MISSED_MOVE checks
  md:exec:fill                   stream: final reports with a fill, tagged exec_mode + signal_ts_ms
                                 (journal opens from it in paper/live). NOT written in shadow mode.
  md:exec:exit_fill              stream: final SELL reports for md:exec:exit_request (E17), tagged exec_mode
  md:exec:exit_state:{exit_id}   exit state machine (restart-safe; broker order ids persisted)
  md:exec:latest:{trade_id}      final report JSON
  md:exec:latest:tsym:{TSYM}     last final report for the contract (journal shadow attach)
  md:exec:state:{trade_id}       state machine (restart-safe)
  md:exec:active                 hash trade_id -> symbol while executing (ICARE / ranking exposure)
  md:exec:missed                 hash: pending opportunity-cost checks (missed_move_pct at +5 / +15 min)

EXEC_MODE=shadow (default) | paper | live
  shadow  simulate with PaperBroker; the journal keeps opening at the ask (exec_* attached);
          no md:exec:fill (a simulated fill must never become a paper/live position later)
  paper   simulate with PaperBroker; the journal opens from md:exec:fill
  live    REAL orders — refused unless EXEC_LIVE_ENABLED=1, EXEC_STATIC_IP matches the
          outgoing IP, a broker session exists, EXEC_LIVE_MAX_ORDER_VALUE is set (X-Q1, X-Q11)
          and the exit path is enabled (EXEC_EXIT_ENABLED=1, E17)
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
from typing import Dict, List, Optional

import redis

from app.broker_account import paper_snapshot
from app.config import load_symbols
from app.freshness import env_ms, is_stale_message
from app.logging_setup import setup_logger
from app.option_pricing import IST
from app.order_executor import state as S
from app.order_executor.broker import AngelBroker, LiveGate, PaperBroker, RateBucket
from app.order_executor.charges import load_rates
from app.order_executor.command import Context, build_command
from app.order_executor.config import ExecConfig
from app.order_executor.engine import (
    action_failed, build_report, exec_fields, new_state, recover_after_restart, set_broker_id, step,
)
from app.order_executor import exit_engine as X
from app.order_executor.market import Quote, quote_from_bidask

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
EXEC_MODE = os.getenv("EXEC_MODE", "shadow").strip().lower()

IN_STREAM = os.getenv("STREAM_ICARE", "md:icare")
OUT_STREAM = os.getenv("STREAM_EXEC", "md:exec")
FILL_STREAM = os.getenv("STREAM_EXEC_FILL", "md:exec:fill")
EXIT_REQ_STREAM = os.getenv("STREAM_EXEC_EXIT_REQUEST", "md:exec:exit_request")
EXIT_FILL_STREAM = os.getenv("STREAM_EXEC_EXIT_FILL", "md:exec:exit_fill")
EXIT_STATE_PREFIX = os.getenv("EXEC_EXIT_STATE_PREFIX", "md:exec:exit_state:")
EXIT_ACTIVE_KEY = "md:exec:exit:active"          # hash position trade_id -> exit_id
EXIT_SOLD_PREFIX = "md:exec:exit:sold:"          # units already sold per position (re-request guard)
EXIT_SEQ_PREFIX = "md:exec:exit:seq:"
EXIT_ENABLED = os.getenv("EXEC_EXIT_ENABLED", "1") == "1"
EXIT_REQ_MAX_AGE_MS = env_ms("EXEC_EXIT_REQUEST_MAX_AGE_SEC", 120)
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_EXEC", "200000"))
LATEST_PREFIX = os.getenv("EXEC_LATEST_PREFIX", "md:exec:latest:")
STATE_PREFIX = os.getenv("EXEC_STATE_PREFIX", "md:exec:state:")
ACTIVE_KEY = os.getenv("EXEC_ACTIVE_KEY", "md:exec:active")
MISSED_KEY = os.getenv("EXEC_MISSED_KEY", "md:exec:missed")
LOCK_PREFIX = "md:exec:lock:"
ICARE_COPY_PREFIX = "md:exec:icare:"
SEQ_PREFIX = "md:exec:seq:"
BIDASK_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")
ACCOUNT_KEY = os.getenv("ACCOUNT_LATEST_KEY", "md:account:latest")
POSITION_OPEN_PREFIX = os.getenv("POSITION_OPEN_PREFIX", "md:position:open:")
KILL_SWITCH_KEY = os.getenv("KILL_SWITCH_KEY", "md:control:kill_switch")
ACCOUNT_MAX_AGE_MS = int(os.getenv("ICARE_ACCOUNT_MAX_AGE_MS", "120000"))
TOTAL_CAPITAL = float(os.getenv("TOTAL_CAPITAL", "100000"))

POLL_MS = int(os.getenv("EXEC_POLL_MS", "500"))
PAPER_LATENCY_MS = int(os.getenv("EXEC_PAPER_LATENCY_MS", "300"))
MISSED_CHECK_MIN = (5, 15)
STATE_TTL_SEC = 3 * 86400

GROUP = os.getenv("EXEC_GROUP", "order_executor")
CONSUMER = os.getenv("EXEC_CONSUMER", "order_executor-1")

CFG = ExecConfig.from_env()
RATES = load_rates()

log = setup_logger("order_executor")


def ensure_group(r, stream: str, group: str) -> None:
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


def _load_json(r, key: str) -> dict:
    raw = r.get(key)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _flat(d: dict) -> Dict[str, str]:
    return {k: (json.dumps(v, separators=(",", ":"), default=str) if isinstance(v, (list, dict))
                else "" if v is None else str(v)) for k, v in d.items()}


def load_specs(symbols: List[str]) -> Dict[str, dict]:
    try:
        from app.scripmaster import load_scripmaster, option_specs
        specs = option_specs(load_scripmaster(), symbols)
    except Exception as e:      # executor still runs; tick 0.05 / fallback lots per order
        log.warning("SPECS_UNAVAILABLE err=%s (tick 0.05, EXEC_MAX_LOTS_PER_ORDER fallback)", e)
        return {}
    for v in specs.values():
        v["expiry"] = v["expiry"].isoformat() if v.get("expiry") else ""
    return specs


class ExecutorRunner:
    """All Redis / broker I/O around the pure engine. `now_ms` is passed in so tests can drive the clock."""

    def __init__(self, r, broker, specs: Dict[str, dict], mode: str = EXEC_MODE,
                 cfg: ExecConfig = CFG, rates=RATES):
        self.r, self.broker, self.specs, self.mode, self.cfg, self.rates = r, broker, specs, mode, cfg, rates
        self.states: Dict[str, S.ExecState] = {}
        self.icare: Dict[str, dict] = {}
        self.pending: Dict[str, List[S.OrderUpdate]] = {}
        self.exits: Dict[str, X.ExitState] = {}
        self.exit_req: Dict[str, dict] = {}

    # ── intake ──
    def next_trade_id(self, now_ms: int) -> str:
        day = dt.datetime.fromtimestamp(now_ms / 1000, IST).strftime("%Y%m%d")
        seq = self.r.incr(f"{SEQ_PREFIX}{day}")
        self.r.expire(f"{SEQ_PREFIX}{day}", 3 * 86400)
        return f"TRD_{day}_{int(seq):03d}"

    def on_icare(self, msg_id: str, fields: dict, now_ms: int) -> Optional[str]:
        if str(fields.get("status") or "").upper() != "APPROVED":
            return None
        if not self.r.set(f"{LOCK_PREFIX}{IN_STREAM}:{msg_id}", "1", nx=True, ex=86400):   # per stream
            log.info("SKIP duplicate source=%s", msg_id)
            return None
        if is_stale_message(msg_id, now_ms, int(self.cfg.command_ttl_sec * 1000)):
            # backlog replay: finished below as REJECTED_BEFORE_EXECUTION / COMMAND_EXPIRED (no order)
            log.warning("STALE_MESSAGE source=%s tsym=%s", msg_id, fields.get("tradingsymbol"))
        tid = self.next_trade_id(now_ms)
        tsym = str(fields.get("tradingsymbol") or "").upper()
        spec = self.specs.get(tsym)
        cmd = build_command(fields, tid, msg_id, spec, self.cfg)
        st = new_state(cmd, now_ms)
        self.states[tid], self.icare[tid] = st, dict(fields)
        self.r.hset(ACTIVE_KEY, tid, cmd.symbol)
        self.r.set(f"{ICARE_COPY_PREFIX}{tid}", json.dumps(fields, separators=(",", ":")), ex=STATE_TTL_SEC)
        self._save(st)
        log.info("COMMAND %s tsym=%s lots=%s premium=%s spec=%s mode=%s", tid, tsym, cmd.requested_lots,
                 cmd.signal_premium, "ok" if spec else "MISSING", self.mode)
        self.advance(tid, now_ms)
        return tid

    # ── context ──
    def quote(self, tsym: str) -> Optional[Quote]:
        return quote_from_bidask(_load_json(self.r, f"{BIDASK_PREFIX}{tsym}"))

    def positions(self) -> List[dict]:
        out = []
        for key in self.r.scan_iter(match=f"{POSITION_OPEN_PREFIX}*", count=500):
            doc = _load_json(self.r, key)
            if doc and "trade_id" in doc:
                out.append(doc)
        return out

    def context(self, st: S.ExecState, now_ms: int) -> Context:
        cmd = st.command
        now = dt.datetime.fromtimestamp(now_ms / 1000, IST)
        positions = self.positions()
        if self.mode == "shadow":
            # the journal mirrors this approval immediately in shadow mode — not a competing position
            positions = [p for p in positions if not (
                str(p.get("tradingsymbol") or "").upper() == cmd["tradingsymbol"]
                and int(_f(p.get("entry_ts_ms")) or 0) >= cmd["signal_ts_ms"] - 1000)]
        others = {tid: sym for tid, sym in (self.r.hgetall(ACTIVE_KEY) or {}).items() if tid != st.trade_id}
        account = _load_json(self.r, ACCOUNT_KEY)
        if not account or now_ms - int(_f(account.get("ts_ms")) or 0) > ACCOUNT_MAX_AGE_MS:
            account = paper_snapshot(TOTAL_CAPITAL, positions, 0.0, now_ms).to_dict()
        margin = _f(account.get("available_margin"))
        if margin is not None:      # margin promised to other trades still executing
            for tid in others:
                o = self.states.get(tid)
                if o and o.cap and o.status not in S.TERMINAL:
                    margin -= max(o.target_lots * o.command["lot_size"] - o.filled_units, 0) * o.cap
        return Context(
            hhmm=now.strftime("%H:%M"),
            kill_switch=str(self.r.get(KILL_SWITCH_KEY) or "").strip() == "1",
            expiry_today=cmd.get("expiry", "") == now.date().isoformat(),
            available_margin=margin,
            open_trades=len(positions) + len(others),
            underlying_busy=any(str(p.get("symbol") or "").upper() == cmd["symbol"] for p in positions)
            or cmd["symbol"] in others.values(),
        )

    # ── loop ──
    def advance(self, tid: str, now_ms: int) -> None:
        st = self.states[tid]
        tsym = st.command["tradingsymbol"]
        ctx = self.context(st, now_ms)
        updates = self.pending.pop(tid, [])
        st, actions, events = step(st, self.quote(tsym), ctx, updates, now_ms, self.cfg, self.rates)
        for a in actions:
            ok, why, broker_id = self._send(tid, a, now_ms)
            if ok and a["type"] == "PLACE":
                set_broker_id(st, a["order_id"], broker_id)
            elif not ok:
                st, ev = action_failed(st, a, why, now_ms)
                events.extend(ev)
        self.states[tid] = st
        for e in events:
            self.r.xadd(OUT_STREAM, _flat(dict(e, trade_id=tid, tradingsymbol=tsym, mode=self.mode)),
                        maxlen=OUT_MAXLEN, approximate=True)
            log.debug("LOGIC_OUT %s %s", tid, e)
        self._save(st)
        if st.status in S.TERMINAL:
            self._finish(st, now_ms)

    def _send(self, tid: str, a: dict, now_ms: int):
        """-> (ok, reason, broker_order_id). Failures go back to the engine (action_failed)."""
        broker_id = ""
        if a["type"] == "PLACE":
            ok, broker_id, why = self.broker.place(a, now_ms)
        elif a["type"] == "MODIFY":
            ok, why = self.broker.modify(a["order_id"], a["price"], a.get("qty"), now_ms,
                                         tradingsymbol=a.get("tradingsymbol"))
        else:
            ok, why = self.broker.cancel(a["order_id"], now_ms)
        log.info("ORDER %s %s %s %s price=%s qty=%s ok=%s %s", tid, a["type"], a.get("side", ""), a["order_id"],
                 a.get("price"), a.get("qty"), ok, why)
        return ok, why, broker_id

    def tick(self, now_ms: int) -> None:
        live = [tid for tid, st in self.states.items() if st.status not in S.TERMINAL]
        exits = [xid for xid, xs in self.exits.items() if xs.status not in X.EXIT_TERMINAL]
        if not live and not exits:
            self.check_missed(now_ms)
            return
        tsyms = {self.states[t].command["tradingsymbol"] for t in live} | {self.exits[x].tradingsymbol for x in exits}
        quotes = {ts: self.quote(ts) for ts in tsyms}
        by_order = {o["order_id"]: t for t in live for o in self.states[t].orders}
        by_order.update({o["order_id"]: x for x in exits for o in self.exits[x].orders})
        for u in self.broker.poll(quotes, now_ms):
            tid = by_order.get(u.order_id)
            if tid:
                self.pending.setdefault(tid, []).append(u)
        for orphan in getattr(self.broker, "orphans", []) or []:
            log.error("ORPHAN_ORDER %s (appeared after reconcile gave up; cancel sent)", orphan)
            self.r.xadd(OUT_STREAM, _flat(dict(orphan, event="ORPHAN_ORDER", ts_ms=now_ms, mode=self.mode)),
                        maxlen=OUT_MAXLEN, approximate=True)
        if getattr(self.broker, "orphans", None):
            self.broker.orphans.clear()
        for tid in live:
            self.advance(tid, now_ms)
        for xid in exits:
            self.advance_exit(xid, now_ms)
        self.check_missed(now_ms)

    # ── exits (E17) ──
    def on_exit_request(self, msg_id: str, fields: dict, now_ms: int) -> Optional[str]:
        if not EXIT_ENABLED:
            return None
        mode = str(fields.get("exec_mode") or "").lower()
        pos_tid = str(fields.get("trade_id") or "")
        if mode != self.mode or not pos_tid:
            log.info("SKIP exit_request source=%s exec_mode=%s (runner mode %s)", msg_id, mode, self.mode)
            return None
        if not self.r.set(f"{LOCK_PREFIX}{EXIT_REQ_STREAM}:{msg_id}", "1", nx=True, ex=86400):
            return None
        if is_stale_message(msg_id, now_ms, EXIT_REQ_MAX_AGE_MS):
            # the journal re-requests while the position is still open, so a backlog is never needed
            log.warning("SKIP stale exit_request source=%s trade=%s", msg_id, pos_tid)
            self.r.xadd(OUT_STREAM, _flat({"event": "EXIT_REQUEST_STALE", "trade_id": pos_tid, "source": msg_id,
                                           "ts_ms": now_ms, "mode": self.mode}), maxlen=OUT_MAXLEN, approximate=True)
            return None
        active = self.r.hgetall(EXIT_ACTIVE_KEY) or {}
        if pos_tid in active:
            log.info("SKIP exit_request trade=%s already exiting as %s", pos_tid, active[pos_tid])
            return None
        sold = float(self.r.get(f"{EXIT_SOLD_PREFIX}{pos_tid}") or 0)
        qty = _f(fields.get("qty")) or 0.0
        orig = _f(fields.get("orig_qty")) or qty
        qty = max(min(qty, orig - sold), 0.0)      # a re-request never sells more than the position held
        if qty <= 0:
            log.warning("SKIP exit_request trade=%s nothing left to sell (sold=%s orig=%s)", pos_tid, sold, orig)
            return None
        day = dt.datetime.fromtimestamp(now_ms / 1000, IST).strftime("%Y%m%d")
        seq = self.r.incr(f"{EXIT_SEQ_PREFIX}{day}")
        self.r.expire(f"{EXIT_SEQ_PREFIX}{day}", 3 * 86400)
        xid = f"EXT_{day}_{int(seq):03d}"
        req = dict(fields, qty=str(qty))
        xs = X.new_exit_state(xid, req, now_ms, self.specs.get(str(fields.get("tradingsymbol") or "").upper()))
        self.exits[xid], self.exit_req[xid] = xs, dict(fields)
        self.r.hset(EXIT_ACTIVE_KEY, pos_tid, xid)
        self._save_exit(xs)
        log.info("EXIT_COMMAND %s trade=%s tsym=%s qty=%s reason=%s mode=%s", xid, pos_tid, xs.tradingsymbol,
                 qty, xs.reason, self.mode)
        self.advance_exit(xid, now_ms)
        return xid

    def advance_exit(self, xid: str, now_ms: int) -> None:
        xs = self.exits[xid]
        updates = self.pending.pop(xid, [])
        xs, actions, events = X.step_exit(xs, self.quote(xs.tradingsymbol), updates, now_ms, self.cfg)
        for a in actions:
            ok, why, broker_id = self._send(xid, a, now_ms)
            if ok and a["type"] == "PLACE":
                set_broker_id(xs, a["order_id"], broker_id)
            elif not ok:
                xs, ev = X.exit_action_failed(xs, a, why, now_ms)
                events.extend(ev)
        self.exits[xid] = xs
        for e in events:
            self.r.xadd(OUT_STREAM, _flat(dict(e, trade_id=xs.position_trade_id, exit_id=xid,
                                               tradingsymbol=xs.tradingsymbol, mode=self.mode)),
                        maxlen=OUT_MAXLEN, approximate=True)
        self._save_exit(xs)
        if xs.status in X.EXIT_TERMINAL:
            self._finish_exit(xs, now_ms)

    def _save_exit(self, xs: X.ExitState) -> None:
        self.r.set(f"{EXIT_STATE_PREFIX}{xs.exit_id}", json.dumps(xs.to_dict(), separators=(",", ":"), default=str),
                   ex=STATE_TTL_SEC)

    def _finish_exit(self, xs: X.ExitState, now_ms: int) -> None:
        rep = X.exit_report(xs, self.rates, self.mode)
        xs.report = rep
        self._save_exit(xs)
        if xs.filled_units > 0:
            key = f"{EXIT_SOLD_PREFIX}{xs.position_trade_id}"
            self.r.set(key, str(float(self.r.get(key) or 0) + xs.filled_units), ex=STATE_TTL_SEC)
        self.r.hdel(EXIT_ACTIVE_KEY, xs.position_trade_id)
        self.r.xadd(OUT_STREAM, _flat(dict(rep, event="EXIT_REPORT", ts_ms=now_ms, mode=self.mode)),
                    maxlen=OUT_MAXLEN, approximate=True)
        if self.mode != "shadow":
            self.r.xadd(EXIT_FILL_STREAM, _flat(rep), maxlen=OUT_MAXLEN, approximate=True)
        self.exits.pop(xs.exit_id, None)
        self.exit_req.pop(xs.exit_id, None)
        (log.info if xs.status == X.EXIT_FILLED else log.error)(
            "EXIT_FINAL %s trade=%s tsym=%s status=%s filled=%s/%s avg=%s reasons=%s", xs.exit_id,
            xs.position_trade_id, xs.tradingsymbol, xs.status, xs.filled_units, xs.qty, rep["avg_price"],
            xs.reject_reasons)

    # ── output ──
    def _save(self, st: S.ExecState) -> None:
        self.r.set(f"{STATE_PREFIX}{st.trade_id}", json.dumps(st.to_dict(), separators=(",", ":"), default=str),
                   ex=STATE_TTL_SEC)

    def _finish(self, st: S.ExecState, now_ms: int) -> None:
        rep = build_report(st, self.rates, self.mode)
        st.report = rep
        self._save(st)
        doc = json.dumps(rep, separators=(",", ":"), default=str)
        self.r.set(f"{LATEST_PREFIX}{st.trade_id}", doc, ex=STATE_TTL_SEC)
        self.r.set(f"{LATEST_PREFIX}tsym:{rep['option_symbol']}", doc, ex=86400)
        self.r.hdel(ACTIVE_KEY, st.trade_id)
        report_event = {k: v for k, v in rep.items() if k != "orders"}
        report_event.update(event="REPORT", ts_ms=now_ms, first_spread_pct=(st.orders[0]["spread_pct"] if st.orders else ""))
        self.r.xadd(OUT_STREAM, _flat(report_event), maxlen=OUT_MAXLEN, approximate=True)
        if st.status in S.WITH_FILL and self.mode != "shadow":
            # shadow fills are simulations of the journal's own ask entry: never a position source
            self.r.xadd(FILL_STREAM, fill_message(self.icare.get(st.trade_id, {}), rep, self.mode),
                        maxlen=OUT_MAXLEN, approximate=True)
        if rep["remaining_lots"] > 0 or st.status not in S.WITH_FILL:
            if st.reference:     # opportunity cost of what was NOT bought (E11)
                self.r.hset(MISSED_KEY, st.trade_id, json.dumps({
                    "tsym": rep["option_symbol"], "reference": st.reference, "done_ms": now_ms, "checked": []}))
        self.states.pop(st.trade_id, None)
        self.icare.pop(st.trade_id, None)
        log.info("FINAL %s tsym=%s status=%s filled=%s/%s avg=%s slip=%s cost=%s reasons=%s", st.trade_id,
                 rep["option_symbol"], rep["execution_status"], rep["filled_lots"], rep["requested_lots"],
                 rep["average_fill_price"], rep["slippage"], rep["total_execution_cost"],
                 rep["reject_reasons"] or rep["cancel_reason"])

    def check_missed(self, now_ms: int) -> None:
        for tid, raw in (self.r.hgetall(MISSED_KEY) or {}).items():
            try:
                m = json.loads(raw)
            except Exception:
                self.r.hdel(MISSED_KEY, tid)
                continue
            due = [n for n in MISSED_CHECK_MIN if n not in m["checked"] and now_ms - m["done_ms"] >= n * 60_000]
            if not due:
                continue
            q = self.quote(m["tsym"])
            mid = q.mid if q else None
            for n in due:
                m["checked"].append(n)
                pct = round((mid / m["reference"] - 1.0) * 100.0, 4) if mid else None
                self.r.xadd(OUT_STREAM, _flat({"event": "MISSED_MOVE", "trade_id": tid, "tradingsymbol": m["tsym"],
                                               "minutes": n, "missed_move_pct": pct, "ts_ms": now_ms,
                                               "mode": self.mode}), maxlen=OUT_MAXLEN, approximate=True)
            if len(m["checked"]) == len(MISSED_CHECK_MIN):
                self.r.hdel(MISSED_KEY, tid)
            else:
                self.r.hset(MISSED_KEY, tid, json.dumps(m))

    def _restore_order(self, w: Optional[dict], now_ms: int, tradingsymbol: str = "") -> None:
        if w and self.mode == "live" and hasattr(self.broker, "restore"):
            self.broker.restore(w["order_id"], w.get("broker_id") or "", now_ms,
                                tradingsymbol=tradingsymbol, qty=w.get("qty"))

    def recover(self, now_ms: int) -> None:
        """
        Restart: unfinished entries are wound down. Paper / shadow: a working simulated order is
        treated as cancelled. Live: its persisted broker id is re-attached (or it is reconciled
        by ordertag), a real CANCEL is sent and the final fills are read from the order book.
        Unfinished exits continue (never abandoned).
        """
        live = self.mode == "live"
        for key in list(self.r.scan_iter(match=f"{EXIT_STATE_PREFIX}*", count=500)):
            doc = _load_json(self.r, key)
            if not doc or doc.get("status") in X.EXIT_TERMINAL:
                continue
            xs = X.ExitState.from_dict(doc)
            self._restore_order(xs.working, now_ms, xs.tradingsymbol)
            xs = X.recover_exit_after_restart(xs, live=live)
            self.exits[xs.exit_id] = xs
            log.warning("RECOVER_EXIT %s trade=%s filled=%s/%s", xs.exit_id, xs.position_trade_id,
                        xs.filled_units, xs.qty)
            self.advance_exit(xs.exit_id, now_ms)
        for key in list(self.r.scan_iter(match=f"{STATE_PREFIX}*", count=500)):
            doc = _load_json(self.r, key)
            if not doc or doc.get("status") in S.TERMINAL:
                continue
            st = S.ExecState.from_dict(doc)
            self._restore_order(st.working, now_ms, st.command.get("tradingsymbol", ""))
            st = recover_after_restart(st, live=live)
            self.states[st.trade_id] = st
            self.icare[st.trade_id] = _load_json(self.r, f"{ICARE_COPY_PREFIX}{st.trade_id}")
            log.warning("RECOVER %s status=%s filled=%s", st.trade_id, doc.get("status"), st.filled_units)
            self.advance(st.trade_id, now_ms)


def fill_message(icare: dict, rep: dict, mode: str = "") -> Dict[str, str]:
    """
    The ICARE payload the journal already understands, re-sized to the ACTUAL fill (E16).
    Tagged exec_mode (the journal opens only fills of its own mode) and signal_ts_ms (source
    time of the upstream signal, carried through).
    """
    msg = {k: "" if v is None else str(v) for k, v in icare.items()}
    msg["icare_recommended_lots"] = msg.get("recommended_lots", "")
    msg["recommended_lots"] = str(rep["filled_lots"])
    msg["fill_price"] = str(rep["average_fill_price"])
    msg["entry_charges"] = str(rep["charges"]["total"])
    msg.update(exec_fields(rep))
    msg["exec_mode"] = mode or str(rep.get("mode") or "")
    msg["signal_ts_ms"] = str(rep.get("signal_ts_ms") or msg.get("signal_ts_ms") or "")
    return msg


def build_broker(mode: str):
    if mode != "live":
        return PaperBroker(latency_ms=PAPER_LATENCY_MS)
    from app.angel_auth import login
    import requests
    try:
        public_ip = requests.get("https://api.ipify.org", timeout=10).text.strip()
    except Exception:
        public_ip = ""
    session_ok, smart = False, None
    try:
        smart, _auth, _feed = login()
        session_ok = True
    except Exception as e:
        log.error("LIVE login failed: %s", e)
    gate = LiveGate(mode, os.getenv("EXEC_LIVE_ENABLED", "0") == "1", os.getenv("EXEC_STATIC_IP", "").strip(),
                    public_ip, session_ok, float(os.getenv("EXEC_LIVE_MAX_ORDER_VALUE", "0") or 0),
                    exit_path=EXIT_ENABLED)
    problems = gate.problems()
    if problems:
        log.error("LIVE REFUSED %s (public_ip=%s) — see DECISION.md §7.5 X-Q11", problems, public_ip)
        sys.exit(2)
    return AngelBroker(api=smart, tokens={}, bucket=RateBucket(float(os.getenv("EXEC_MAX_OPS", "5")),
                                                                 lambda: int(time.time() * 1000)),
                       max_order_value=gate.max_order_value,
                       reconcile_ms=int(CFG.place_reconcile_sec * 1000))


def main():
    symbols = load_symbols()
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_STREAM, GROUP)
    ensure_group(r, EXIT_REQ_STREAM, GROUP)
    specs = load_specs(symbols)
    broker = build_broker(EXEC_MODE)
    if isinstance(broker, AngelBroker):
        broker.tokens = {k: v.get("token", "") for k, v in specs.items()}
    runner = ExecutorRunner(r, broker, specs)
    runner.recover(int(time.time() * 1000))
    log.info("START mode=%s reading %s writing %s / %s specs=%d cfg=%s rates=%s",
             EXEC_MODE, IN_STREAM, OUT_STREAM, FILL_STREAM, len(specs), CFG, RATES)
    spec_day = dt.datetime.now(IST).date()

    while True:
        resp = r.xreadgroup(groupname=GROUP, consumername=CONSUMER,
                            streams={IN_STREAM: ">", EXIT_REQ_STREAM: ">"}, count=50, block=POLL_MS)
        for stream, msgs in resp or []:
            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)
                try:
                    if stream == EXIT_REQ_STREAM:
                        runner.on_exit_request(msg_id, fields, int(time.time() * 1000))
                    else:
                        runner.on_icare(msg_id, fields, int(time.time() * 1000))
                except Exception:
                    log.exception("COMMAND_FAILED stream=%s source=%s", stream, msg_id)
            if ack_ids:
                r.xack(stream, GROUP, *ack_ids)
        runner.tick(int(time.time() * 1000))
        today = dt.datetime.now(IST).date()
        if today != spec_day:
            runner.specs, spec_day = load_specs(symbols), today


if __name__ == "__main__":
    main()

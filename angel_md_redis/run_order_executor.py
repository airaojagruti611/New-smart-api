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

Writes:
  md:exec                        stream: every COMMAND / VALIDATED / ORDER_* / FILL / FINAL event,
                                 one REPORT per trade (the final report) and MISSED_MOVE checks
  md:exec:fill                   stream: final reports with a fill (journal opens from it in paper/live)
  md:exec:latest:{trade_id}      final report JSON
  md:exec:latest:tsym:{TSYM}     last final report for the contract (journal shadow attach)
  md:exec:state:{trade_id}       state machine (restart-safe)
  md:exec:active                 hash trade_id -> symbol while executing (ICARE / ranking exposure)
  md:exec:missed                 hash: pending opportunity-cost checks (missed_move_pct at +5 / +15 min)

EXEC_MODE=shadow (default) | paper | live
  shadow  simulate with PaperBroker; the journal keeps opening at the ask (exec_* attached)
  paper   simulate with PaperBroker; the journal opens from md:exec:fill
  live    REAL orders — refused unless EXEC_LIVE_ENABLED=1, EXEC_STATIC_IP matches the
          outgoing IP, a broker session exists and EXEC_LIVE_MAX_ORDER_VALUE is set (X-Q1, X-Q11)
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
from app.logging_setup import setup_logger
from app.option_pricing import IST
from app.order_executor import state as S
from app.order_executor.broker import AngelBroker, LiveGate, PaperBroker, RateBucket
from app.order_executor.charges import load_rates
from app.order_executor.command import Context, build_command
from app.order_executor.config import ExecConfig
from app.order_executor.engine import build_report, exec_fields, new_state, recover_after_restart, step
from app.order_executor.market import Quote, quote_from_bidask

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
EXEC_MODE = os.getenv("EXEC_MODE", "shadow").strip().lower()

IN_STREAM = os.getenv("STREAM_ICARE", "md:icare")
OUT_STREAM = os.getenv("STREAM_EXEC", "md:exec")
FILL_STREAM = os.getenv("STREAM_EXEC_FILL", "md:exec:fill")
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

    # ── intake ──
    def next_trade_id(self, now_ms: int) -> str:
        day = dt.datetime.fromtimestamp(now_ms / 1000, IST).strftime("%Y%m%d")
        seq = self.r.incr(f"{SEQ_PREFIX}{day}")
        self.r.expire(f"{SEQ_PREFIX}{day}", 3 * 86400)
        return f"TRD_{day}_{int(seq):03d}"

    def on_icare(self, msg_id: str, fields: dict, now_ms: int) -> Optional[str]:
        if str(fields.get("status") or "").upper() != "APPROVED":
            return None
        if not self.r.set(f"{LOCK_PREFIX}{msg_id}", "1", nx=True, ex=86400):
            log.info("SKIP duplicate source=%s", msg_id)
            return None
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
            self._send(tid, a, now_ms)
        self.states[tid] = st
        for e in events:
            self.r.xadd(OUT_STREAM, _flat(dict(e, trade_id=tid, tradingsymbol=tsym, mode=self.mode)),
                        maxlen=OUT_MAXLEN, approximate=True)
            log.debug("LOGIC_OUT %s %s", tid, e)
        self._save(st)
        if st.status in S.TERMINAL:
            self._finish(st, now_ms)

    def _send(self, tid: str, a: dict, now_ms: int) -> None:
        if a["type"] == "PLACE":
            ok, _bid, why = self.broker.place(a, now_ms)
        elif a["type"] == "MODIFY":
            ok, why = self.broker.modify(a["order_id"], a["price"], a.get("qty"), now_ms)
        else:
            ok, why = self.broker.cancel(a["order_id"], now_ms)
        log.info("ORDER %s %s %s price=%s qty=%s ok=%s %s", tid, a["type"], a["order_id"], a.get("price"),
                 a.get("qty"), ok, why)
        if not ok and a["type"] == "PLACE":
            self.pending.setdefault(tid, []).append(S.OrderUpdate(a["order_id"], "REJECTED", 0, None, why))

    def tick(self, now_ms: int) -> None:
        live = [tid for tid, st in self.states.items() if st.status not in S.TERMINAL]
        if not live:
            self.check_missed(now_ms)
            return
        quotes = {self.states[t].command["tradingsymbol"]: self.quote(self.states[t].command["tradingsymbol"])
                  for t in live}
        by_order = {o["order_id"]: t for t in live for o in self.states[t].orders}
        for u in self.broker.poll(quotes, now_ms):
            tid = by_order.get(u.order_id)
            if tid:
                self.pending.setdefault(tid, []).append(u)
        for tid in live:
            self.advance(tid, now_ms)
        self.check_missed(now_ms)

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
        if st.status in S.WITH_FILL:
            self.r.xadd(FILL_STREAM, fill_message(self.icare.get(st.trade_id, {}), rep),
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

    def recover(self, now_ms: int) -> None:
        """Restart: unfinished states are closed (paper: working orders treated as cancelled)."""
        for key in list(self.r.scan_iter(match=f"{STATE_PREFIX}*", count=500)):
            doc = _load_json(self.r, key)
            if not doc or doc.get("status") in S.TERMINAL:
                continue
            st = recover_after_restart(S.ExecState.from_dict(doc))
            self.states[st.trade_id] = st
            self.icare[st.trade_id] = _load_json(self.r, f"{ICARE_COPY_PREFIX}{st.trade_id}")
            log.warning("RECOVER %s status=%s filled=%s", st.trade_id, doc.get("status"), st.filled_units)
            self.advance(st.trade_id, now_ms)


def fill_message(icare: dict, rep: dict) -> Dict[str, str]:
    """The ICARE payload the journal already understands, re-sized to the ACTUAL fill (E16)."""
    msg = {k: "" if v is None else str(v) for k, v in icare.items()}
    msg["icare_recommended_lots"] = msg.get("recommended_lots", "")
    msg["recommended_lots"] = str(rep["filled_lots"])
    msg["fill_price"] = str(rep["average_fill_price"])
    msg["entry_charges"] = str(rep["charges"]["total"])
    msg.update(exec_fields(rep))
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
                    public_ip, session_ok, float(os.getenv("EXEC_LIVE_MAX_ORDER_VALUE", "0") or 0))
    problems = gate.problems()
    if problems:
        log.error("LIVE REFUSED %s (public_ip=%s) — see DECISION.md §7.5 X-Q11", problems, public_ip)
        sys.exit(2)
    return AngelBroker(api=smart, tokens={}, bucket=RateBucket(float(os.getenv("EXEC_MAX_OPS", "5")),
                                                                 lambda: int(time.time() * 1000)),
                       max_order_value=gate.max_order_value)


def main():
    symbols = load_symbols()
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_STREAM, GROUP)
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
        resp = r.xreadgroup(groupname=GROUP, consumername=CONSUMER, streams={IN_STREAM: ">"}, count=50, block=POLL_MS)
        ack_ids = []
        for _stream, msgs in resp or []:
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)
                try:
                    runner.on_icare(msg_id, fields, int(time.time() * 1000))
                except Exception:
                    log.exception("COMMAND_FAILED source=%s", msg_id)
        if ack_ids:
            r.xack(IN_STREAM, GROUP, *ack_ids)
        runner.tick(int(time.time() * 1000))
        today = dt.datetime.now(IST).date()
        if today != spec_day:
            runner.specs, spec_day = load_specs(symbols), today


if __name__ == "__main__":
    main()

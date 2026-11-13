"""
run_circuit_breaker.py
──────────────────────
Module 17 — Circuit Breaker / Kill Switch, Redis wiring (logic: app/circuit_breaker.py).

Read-only observer of the pipeline (no consumer groups, nothing is acked):
  md:position:open:*             open journal positions
  md:journal:daily:{IST date}    realized PnL today
  md:journal:closed:{trade_id}   closed-trade record (entry filled + no open position is fine if closed)
  md:exec:state:* / md:exec:exit_state:*   executor entry / exit state machines
  md:exec (window)               COMMAND / ORDER_PLACED / ORDER_REJECTED events
  md:icare (window)              APPROVED commands (watchdog)
  CB_HEARTBEAT_KEYS (JSON docs with ts_ms, default md:account:latest) and
  CB_HEARTBEAT_STREAMS (latest stream id; default none)  -> stale worker detection

Writes:
  md:circuit:state               JSON state (ARMED/WARNING/TRIPPED) — persisted, a restart keeps TRIPPED
  md:circuit:events              stream of TRIPPED / WARNING / CLEARED / RESET / ESCALATED / EMERGENCY_* events
  md:circuit:latest              last evaluation (state + findings + pnl) with ts_ms (health key)
  md:control:kill_switch         "1" when TRIPPED (same key/format ICARE, ranking and executor already read);
                                 md:control:kill_switch:reason carries the reason
  md:exec:exit_request           ONLY for EMERGENCY severity AND CB_EMERGENCY_EXIT=1 (default 0 = shadow:
                                 log + EMERGENCY_EXIT_SHADOW event only)

TRIPPED never clears by itself. Manual reset:  python run_circuit_breaker.py --reset
(clears the state and, if the breaker set it, the kill switch). Status: --status
"""

from __future__ import annotations

import datetime as dt
from dataclasses import replace
import json
import os
import sys
import time
from typing import Dict, List, Optional

import redis

from app import circuit_breaker as C
from app.logging_setup import setup_logger
from app.option_pricing import IST

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STATE_KEY = os.getenv("CB_STATE_KEY", "md:circuit:state")
EVENTS_STREAM = os.getenv("STREAM_CIRCUIT_EVENTS", "md:circuit:events")
LATEST_KEY = os.getenv("CB_LATEST_KEY", "md:circuit:latest")
KILL_SWITCH_KEY = os.getenv("KILL_SWITCH_KEY", "md:control:kill_switch")
KILL_REASON_KEY = KILL_SWITCH_KEY + ":reason"
KILL_BY_BREAKER_KEY = "md:circuit:kill_by_breaker"
POSITION_OPEN_PREFIX = os.getenv("POSITION_OPEN_PREFIX", "md:position:open:")
JOURNAL_DAILY_PREFIX = os.getenv("JOURNAL_DAILY_PREFIX", "md:journal:daily:")
JOURNAL_CLOSED_PREFIX = os.getenv("JOURNAL_CLOSED_PREFIX", "md:journal:closed:")
EXEC_STATE_PREFIX = os.getenv("EXEC_STATE_PREFIX", "md:exec:state:")
EXIT_STATE_PREFIX = os.getenv("EXEC_EXIT_STATE_PREFIX", "md:exec:exit_state:")
EXEC_STREAM = os.getenv("STREAM_EXEC", "md:exec")
ICARE_STREAM = os.getenv("STREAM_ICARE", "md:icare")
EXIT_REQ_STREAM = os.getenv("STREAM_EXEC_EXIT_REQUEST", "md:exec:exit_request")
EXEC_MODE = os.getenv("EXEC_MODE", "shadow").lower()
EMERGENCY_EXIT = os.getenv("CB_EMERGENCY_EXIT", "0") == "1"
POLL_SEC = float(os.getenv("CB_POLL_SEC", "5"))
STREAM_WINDOW_SEC = float(os.getenv("CB_STREAM_WINDOW_SEC", "600"))
HEARTBEAT_KEYS = [k for k in os.getenv("CB_HEARTBEAT_KEYS", "md:account:latest").split(",") if k.strip()]
HEARTBEAT_STREAMS = [k for k in os.getenv("CB_HEARTBEAT_STREAMS", "").split(",") if k.strip()]
MARKET_HOURS_ONLY = os.getenv("CB_MARKET_HOURS_ONLY", "1") == "1"   # heartbeat checks only 09:15-15:30 IST, Mon-Fri
MAXLEN = int(os.getenv("STREAM_MAXLEN_CIRCUIT", "20000"))

log = setup_logger("circuit_breaker")


def _load(r, key: str) -> Optional[dict]:
    raw = r.get(key)
    if not raw:
        return None
    try:
        d = json.loads(raw)
    except Exception:
        return None
    return d if isinstance(d, dict) else None


def _load_prefix(r, prefix: str) -> List[dict]:
    out = []
    for k in r.scan_iter(match=f"{prefix}*"):
        d = _load(r, k)
        if d:
            out.append(d)
    return out


def _id_ms(msg_id: str) -> int:
    try:
        return int(str(msg_id).split("-")[0])
    except ValueError:
        return 0


def ist_date(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000.0, IST).date().isoformat()


def market_open(now_ms: int) -> bool:
    t = dt.datetime.fromtimestamp(now_ms / 1000.0, IST)
    return t.weekday() < 5 and dt.time(9, 15) <= t.time() <= dt.time(15, 30)


def _flat(d: dict) -> Dict[str, str]:
    return {k: (json.dumps(v, separators=(",", ":"), default=str) if isinstance(v, (list, dict))
                else "" if v is None else str(v)) for k, v in d.items()}


class CircuitRunner:
    def __init__(self, r, cfg: Optional[C.CircuitConfig] = None, exec_mode: str = EXEC_MODE,
                 emergency_exit: bool = EMERGENCY_EXIT):
        self.r, self.cfg, self.mode, self.emergency_exit = r, cfg or C.CircuitConfig.from_env(), exec_mode, emergency_exit
        self.prev_pnl: Optional[float] = None

    # ── state persistence ──
    def load_state(self) -> C.BreakerState:
        return C.BreakerState.from_dict(_load(self.r, STATE_KEY))

    def save_state(self, st: C.BreakerState) -> None:
        self.r.set(STATE_KEY, json.dumps(st.to_dict(), separators=(",", ":"), default=str))

    def publish(self, ev: dict, now_ms: int) -> None:
        self.r.xadd(EVENTS_STREAM, _flat(dict(ev, ts_ms=now_ms, mode=self.mode)), maxlen=MAXLEN, approximate=True)

    def trip_kill_switch(self, st: C.BreakerState) -> None:
        if str(self.r.get(KILL_SWITCH_KEY) or "").strip() != "1":
            self.r.set(KILL_BY_BREAKER_KEY, "1")
        self.r.set(KILL_SWITCH_KEY, "1")
        self.r.set(KILL_REASON_KEY, f"circuit_breaker:{st.severity}:{st.reason}")

    def recover(self, now_ms: int) -> C.BreakerState:
        """Restart protection: a persisted TRIPPED state re-asserts the kill switch and stays tripped."""
        st = self.load_state()
        if st.state == C.TRIPPED:
            self.trip_kill_switch(st)
            self.publish({"event": "RESTART_STILL_TRIPPED", "severity": st.severity, "reason": st.reason}, now_ms)
            log.warning("RESTART breaker is TRIPPED (%s) — kill switch re-asserted; manual --reset required", st.reason)
        return st

    def manual_reset(self, now_ms: int) -> C.BreakerState:
        st, ev = C.reset(self.load_state(), now_ms)
        self.save_state(st)
        if self.r.get(KILL_BY_BREAKER_KEY) == "1":            # only undo a kill switch the breaker itself set
            self.r.delete(KILL_SWITCH_KEY)
            self.r.delete(KILL_REASON_KEY)
        self.r.delete(KILL_BY_BREAKER_KEY)
        self.prev_pnl = None
        self.publish(ev, now_ms)
        return st

    # ── inputs ──
    def _window(self, stream: str, now_ms: int) -> List[tuple]:
        try:
            return self.r.xrange(stream, min=f"{int(now_ms - STREAM_WINDOW_SEC * 1000)}-0", max="+", count=5000) or []
        except Exception:
            return []

    def _stream_events(self, now_ms: int):
        commands, orders, rejects = [], [], []
        for mid, f in self._window(EXEC_STREAM, now_ms):
            ev, ts = f.get("event"), _id_ms(mid)
            if ev == "COMMAND":
                commands.append({"tsym": f.get("tradingsymbol", ""), "ts_ms": ts})
            elif ev in ("ORDER_PLACED", "EXIT_ORDER_PLACED"):
                orders.append(ts)
            elif ev in ("ORDER_REJECTED", "EXIT_ORDER_REJECTED"):
                rejects.append(ts)
        approvals = [{"tsym": f.get("tradingsymbol", ""), "ts_ms": _id_ms(mid)}
                     for mid, f in self._window(ICARE_STREAM, now_ms) if f.get("status") == "APPROVED"]
        return commands, orders, rejects, approvals

    def _heartbeats(self, now_ms: int) -> Dict[str, Optional[int]]:
        if MARKET_HOURS_ONLY and not market_open(now_ms):
            return {}
        out: Dict[str, Optional[int]] = {}
        for k in HEARTBEAT_KEYS:
            d = _load(self.r, k.strip())
            out[k.strip()] = int(float(d.get("ts_ms"))) if d and d.get("ts_ms") not in (None, "") else None
        for s in HEARTBEAT_STREAMS:
            try:
                last = self.r.xrevrange(s.strip(), count=1)
            except Exception:
                last = []
            out[s.strip()] = _id_ms(last[0][0]) if last else None
        return out

    # ── one cycle ──
    def tick(self, now_ms: int) -> C.BreakerState:
        r, cfg = self.r, self.cfg
        positions = _load_prefix(r, POSITION_OPEN_PREFIX)
        daily = _load(r, f"{JOURNAL_DAILY_PREFIX}{ist_date(now_ms)}") or {}
        realized = float(daily.get("realized_pnl") or 0.0)
        exec_states = _load_prefix(r, EXEC_STATE_PREFIX)
        exit_states = _load_prefix(r, EXIT_STATE_PREFIX)
        closed = {str(s.get("trade_id")) for s in exec_states
                  if s.get("trade_id") and r.exists(f"{JOURNAL_CLOSED_PREFIX}{s.get('trade_id')}")}
        commands, orders, rejects, approvals = self._stream_events(now_ms)
        pnl = C.total_pnl(realized, positions)

        findings = (C.check_loss_caps(cfg, realized, positions)
                    + C.check_mismatch(cfg, now_ms, positions, exec_states, exit_states, closed)
                    + C.check_watchdog(cfg, now_ms, approvals, commands, exec_states, exit_states, positions)
                    + C.check_abnormal(cfg, now_ms, orders, rejects, self._heartbeats(now_ms), self.prev_pnl, pnl))
        self.prev_pnl = pnl

        st = self.load_state()
        new, events = C.advance(st, findings, now_ms, cfg)
        for ev in events:
            self.publish(ev, now_ms)
            log.warning("CIRCUIT %s %s", ev.get("event"), ev.get("reason") or ev.get("severity") or "")
        if new.state == C.TRIPPED:
            self.trip_kill_switch(new)                         # idempotent; also restores a cleared key
        if C.should_emergency_exit(new):
            new = self._emergency(new, positions, now_ms)
        if new != st:
            self.save_state(new)
        r.set(LATEST_KEY, json.dumps({"ts_ms": now_ms, "state": new.state, "severity": new.severity,
                                      "reason": new.reason, "pnl": round(pnl, 2), "open_positions": len(positions),
                                      "findings": [f.to_dict() for f in findings]}, separators=(",", ":")))
        return new

    def _emergency(self, st: C.BreakerState, positions: List[dict], now_ms: int) -> C.BreakerState:
        reqs = C.emergency_exit_requests(positions, now_ms, self.mode)
        tids = [q["trade_id"] for q in reqs]
        if not self.emergency_exit:
            log.error("EMERGENCY (shadow, CB_EMERGENCY_EXIT=0): would exit %s", tids)
            self.publish({"event": "EMERGENCY_EXIT_SHADOW", "positions": tids, "severity": st.severity,
                          "reason": st.reason}, now_ms)
        else:
            for q in reqs:
                self.r.xadd(EXIT_REQ_STREAM, q, maxlen=200000, approximate=True)
            log.error("EMERGENCY exit requests published for %s", tids)
            self.publish({"event": "EMERGENCY_EXIT_PUBLISHED", "positions": tids, "severity": st.severity,
                          "reason": st.reason}, now_ms)
        return replace(st, emergency_handled=True)


def main(argv: List[str]) -> int:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    now = lambda: int(time.time() * 1000)  # noqa: E731
    runner = CircuitRunner(r)
    if "--reset" in argv:
        st = runner.manual_reset(now())
        print(f"circuit breaker RESET -> {st.state}; kill switch = {r.get(KILL_SWITCH_KEY)!r}")
        return 0
    if "--status" in argv:
        print(json.dumps(runner.load_state().to_dict(), indent=2), "kill_switch =", repr(r.get(KILL_SWITCH_KEY)))
        return 0
    runner.recover(now())
    log.info("START mode=%s emergency_exit=%s cfg=%s", EXEC_MODE, EMERGENCY_EXIT, runner.cfg)
    while True:
        try:
            runner.tick(now())
        except Exception:
            log.exception("TICK_FAILED")
        time.sleep(POLL_SEC)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

"""
Module 17 — Circuit Breaker / Kill Switch (pure logic, no I/O).

Watches loss caps, executor-vs-journal mismatches, stuck commands and abnormal behaviour,
and drives a state machine  ARMED -> WARNING -> TRIPPED -> (manual) RESET -> ARMED.
TRIPPED is sticky: it never clears by itself (also across restarts); only reset() clears it.
The runner (run_circuit_breaker.py) turns a trip into md:control:kill_switch = "1" and, for
EMERGENCY severity and only when enabled, into exit requests for every open position.

Inputs are plain dicts (JSON documents from Redis):
  position   md:position:open:* (PaperPosition.to_dict)
  exec_state md:exec:state:*    (ExecState.to_dict)
  exit_state md:exec:exit_state:* (ExitState.to_dict)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from typing import Dict, Iterable, List, Optional, Tuple

ARMED, WARNING, TRIPPED = "ARMED", "WARNING", "TRIPPED"
SEV_WARNING, SEV_CRITICAL, SEV_EMERGENCY = "WARNING", "CRITICAL", "EMERGENCY"
_SEV_RANK = {"": 0, SEV_WARNING: 1, SEV_CRITICAL: 2, SEV_EMERGENCY: 3}

# executor terminal states that leave a position (mirrors app.order_executor.state.WITH_FILL)
ENTRY_FILLED = frozenset({"FILLED", "PARTIAL_FILL_TIMEOUT", "PARTIAL_FILL_STOPPED", "PARTIAL_KILLED"})
ENTRY_TERMINAL = ENTRY_FILLED | frozenset({
    "CANCELLED_TIMEOUT", "ABORTED_SLIPPAGE_LIMIT", "ABORTED_PRICE_BAND", "REJECTED_BY_BROKER", "KILLED",
    "REJECTED_BEFORE_EXECUTION"})
EXIT_TERMINAL = frozenset({"EXIT_FILLED", "EXIT_FAILED"})


def _f(v, d: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def _env_f(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class CircuitConfig:
    daily_loss_cap: float = 25000.0          # Rs, realized + unrealized
    max_loss_per_trade: float = 10000.0      # Rs, unrealized loss of one open position
    max_total_exposure: float = 500000.0     # Rs, sum(qty * premium) of open positions
    warn_fraction: float = 0.8               # WARNING at this fraction of a cap
    emergency_multiple: float = 1.5          # EMERGENCY when a cap is exceeded by this multiple
    mismatch_grace_sec: float = 120.0
    qty_tolerance: float = 1.0               # units
    exit_unfilled_sec: float = 180.0
    cmd_timeout_sec: float = 120.0           # ICARE approval -> executor COMMAND
    exec_fill_timeout_sec: float = 300.0     # executor entry not terminal
    exit_fill_timeout_sec: float = 300.0     # exit state not terminal (CRITICAL at 3x)
    max_orders_per_min: int = 30
    max_rejects_window: int = 5              # ORDER_REJECTED in window (CRITICAL at 2x)
    reject_window_sec: float = 300.0
    heartbeat_max_age_sec: float = 120.0
    pnl_jump: float = 15000.0                # Rs change of total PnL between two checks
    warn_clear_sec: float = 300.0            # WARNING -> ARMED after this long without findings
    check_mismatch_entries: bool = True      # off in shadow mode (no md:exec:fill -> no journal position)

    @staticmethod
    def from_env() -> "CircuitConfig":
        d = CircuitConfig()
        g = _env_f
        return CircuitConfig(
            daily_loss_cap=g("CB_DAILY_LOSS_CAP", d.daily_loss_cap),
            max_loss_per_trade=g("CB_MAX_LOSS_PER_TRADE", d.max_loss_per_trade),
            max_total_exposure=g("CB_MAX_TOTAL_EXPOSURE", d.max_total_exposure),
            warn_fraction=g("CB_WARN_FRACTION", d.warn_fraction),
            emergency_multiple=g("CB_EMERGENCY_MULTIPLE", d.emergency_multiple),
            mismatch_grace_sec=g("CB_MISMATCH_GRACE_SEC", d.mismatch_grace_sec),
            qty_tolerance=g("CB_QTY_TOLERANCE", d.qty_tolerance),
            exit_unfilled_sec=g("CB_EXIT_UNFILLED_SEC", d.exit_unfilled_sec),
            cmd_timeout_sec=g("CB_CMD_TIMEOUT_SEC", d.cmd_timeout_sec),
            exec_fill_timeout_sec=g("CB_EXEC_FILL_TIMEOUT_SEC", d.exec_fill_timeout_sec),
            exit_fill_timeout_sec=g("CB_EXIT_FILL_TIMEOUT_SEC", d.exit_fill_timeout_sec),
            max_orders_per_min=int(g("CB_MAX_ORDERS_PER_MIN", d.max_orders_per_min)),
            max_rejects_window=int(g("CB_MAX_REJECTS", d.max_rejects_window)),
            reject_window_sec=g("CB_REJECT_WINDOW_SEC", d.reject_window_sec),
            heartbeat_max_age_sec=g("CB_HEARTBEAT_MAX_AGE_SEC", d.heartbeat_max_age_sec),
            pnl_jump=g("CB_PNL_JUMP", d.pnl_jump),
            warn_clear_sec=g("CB_WARN_CLEAR_SEC", d.warn_clear_sec),
            check_mismatch_entries=os.getenv("EXEC_MODE", "shadow").lower() != "shadow",
        )


@dataclass(frozen=True)
class Finding:
    code: str
    severity: str
    detail: str = ""
    key: str = ""          # trade_id / tsym / stream the finding is about

    def to_dict(self) -> dict:
        return {"code": self.code, "severity": self.severity, "detail": self.detail, "key": self.key}


def worst(findings: Iterable[Finding]) -> str:
    return max((f.severity for f in findings), key=lambda s: _SEV_RANK.get(s, 0), default="")


def _cap_severity(value: float, cap: float, cfg: CircuitConfig) -> str:
    """Severity of a non-negative measure against its cap ('' when comfortably below)."""
    if cap <= 0:
        return ""
    if value >= cap * cfg.emergency_multiple:
        return SEV_EMERGENCY
    if value >= cap:
        return SEV_CRITICAL
    if value >= cap * cfg.warn_fraction:
        return SEV_WARNING
    return ""


# ── 1. loss caps ─────────────────────────────────────────────────────────────
def position_pnl(pos: dict) -> float:
    px = pos.get("last_premium")
    px = _f(px) if px not in (None, "") else _f(pos.get("entry_premium"))
    return (px - _f(pos.get("entry_premium"))) * _f(pos.get("qty"))


def total_pnl(realized: float, positions: List[dict]) -> float:
    return realized + sum(position_pnl(p) for p in positions)


def check_loss_caps(cfg: CircuitConfig, realized_pnl: float, positions: List[dict]) -> List[Finding]:
    out: List[Finding] = []
    pnl = total_pnl(realized_pnl, positions)
    sev = _cap_severity(-pnl, cfg.daily_loss_cap, cfg)
    if sev:
        out.append(Finding("DAILY_LOSS_CAP", sev, f"pnl={pnl:.0f} cap={cfg.daily_loss_cap:.0f}"))
    for p in positions:
        loss = -position_pnl(p)
        sev = _cap_severity(loss, cfg.max_loss_per_trade, cfg)
        if sev:
            out.append(Finding("TRADE_LOSS_CAP", sev, f"loss={loss:.0f} cap={cfg.max_loss_per_trade:.0f}",
                               str(p.get("trade_id") or "")))
    exposure = sum(_f(p.get("qty")) * (_f(p.get("last_premium")) or _f(p.get("entry_premium"))) for p in positions)
    sev = _cap_severity(exposure, cfg.max_total_exposure, cfg)
    if sev:
        out.append(Finding("EXPOSURE_CAP", sev, f"exposure={exposure:.0f} cap={cfg.max_total_exposure:.0f}"))
    return out


# ── 2. entry / exit mismatch ─────────────────────────────────────────────────
def filled_units(state: dict) -> float:
    return sum(_f(f[1]) for f in (state.get("fills") or []) if isinstance(f, (list, tuple)) and len(f) > 1)


def check_mismatch(cfg: CircuitConfig, now_ms: int, positions: List[dict], exec_states: List[dict],
                   exit_states: List[dict], closed_trade_ids: Iterable[str] = ()) -> List[Finding]:
    out: List[Finding] = []
    closed = set(closed_trade_ids)
    pos_by_tid = {str(p.get("trade_id")): p for p in positions}
    grace = cfg.mismatch_grace_sec * 1000
    if cfg.check_mismatch_entries:
        for st in exec_states:
            tid = str(st.get("trade_id") or "")
            if st.get("status") not in ENTRY_FILLED:
                continue
            fin = int(_f(st.get("finished_ms")))
            if fin and now_ms - fin < grace:
                continue
            units = filled_units(st)
            pos = pos_by_tid.get(tid)
            if pos is None:
                if tid not in closed and units > 0:
                    out.append(Finding("ENTRY_FILLED_NO_POSITION", SEV_CRITICAL,
                                       f"filled={units:g} but no open position and no closed record", tid))
                continue
            orig = _f(pos.get("orig_qty")) or _f(pos.get("qty"))
            if units > 0 and abs(orig - units) > cfg.qty_tolerance and not pos.get("exit_pending"):
                out.append(Finding("ENTRY_QTY_MISMATCH", SEV_CRITICAL,
                                   f"executor filled={units:g} journal qty={orig:g}", tid))
    exit_by_tid: Dict[str, List[dict]] = {}
    for xs in exit_states:
        exit_by_tid.setdefault(str(xs.get("position_trade_id")), []).append(xs)
    for tid, pos in pos_by_tid.items():
        for xs in exit_by_tid.get(tid, []):
            if xs.get("status") == "EXIT_FILLED" and filled_units(xs) >= _f(pos.get("qty")) - cfg.qty_tolerance > 0:
                fin = int(_f(xs.get("finished_ms")))
                if not fin or now_ms - fin >= grace:
                    out.append(Finding("EXIT_FILLED_POSITION_OPEN", SEV_CRITICAL,
                                       f"exit filled={filled_units(xs):g} but position qty={_f(pos.get('qty')):g}", tid))
        req = int(_f(pos.get("exit_requested_ms")))
        if pos.get("exit_pending") and req and now_ms - req >= cfg.exit_unfilled_sec * 1000:
            out.append(Finding("EXIT_COMMANDED_NOT_FILLED", SEV_WARNING,
                               f"reason={pos.get('exit_pending')} age={(now_ms - req) / 1000:.0f}s", tid))
    return out


# ── 3. command watchdog ──────────────────────────────────────────────────────
def check_watchdog(cfg: CircuitConfig, now_ms: int, approvals: List[dict], exec_commands: List[dict],
                   exec_states: List[dict], exit_states: List[dict], positions: List[dict]) -> List[Finding]:
    """approvals: [{tsym, ts_ms}] ICARE APPROVED; exec_commands: [{tsym, ts_ms}] executor COMMAND events."""
    out: List[Finding] = []
    cmds: Dict[str, List[int]] = {}
    for c in exec_commands:
        cmds.setdefault(str(c.get("tsym")), []).append(int(_f(c.get("ts_ms"))))
    t = cfg.cmd_timeout_sec * 1000
    for a in approvals:
        ts, tsym = int(_f(a.get("ts_ms"))), str(a.get("tsym"))
        if not (t <= now_ms - ts <= 3 * t):
            continue
        if not any(c >= ts - 1000 for c in cmds.get(tsym, [])):
            out.append(Finding("APPROVAL_NOT_ACTED", SEV_WARNING, f"no executor COMMAND {(now_ms - ts) / 1000:.0f}s", tsym))
    for st in exec_states:
        if st.get("status") in ENTRY_TERMINAL:
            continue
        age = now_ms - int(_f(st.get("received_ms")))
        if st.get("received_ms") and age > cfg.exec_fill_timeout_sec * 1000:
            out.append(Finding("ENTRY_NOT_FILLED", SEV_WARNING, f"status={st.get('status')} age={age / 1000:.0f}s",
                               str(st.get("trade_id") or "")))
    active_exit = set()
    for xs in exit_states:
        if xs.get("status") in EXIT_TERMINAL:
            continue
        active_exit.add(str(xs.get("position_trade_id")))
        age = now_ms - int(_f(xs.get("received_ms")))
        if xs.get("received_ms") and age > cfg.exit_fill_timeout_sec * 1000:
            sev = SEV_CRITICAL if age > 3 * cfg.exit_fill_timeout_sec * 1000 else SEV_WARNING
            out.append(Finding("EXIT_NOT_FILLED", sev, f"status={xs.get('status')} age={age / 1000:.0f}s",
                               str(xs.get("position_trade_id") or "")))
    for p in positions:
        req = int(_f(p.get("exit_requested_ms")))
        tid = str(p.get("trade_id"))
        if p.get("exit_pending") and req and now_ms - req > cfg.cmd_timeout_sec * 1000 and tid not in active_exit:
            out.append(Finding("EXIT_REQUEST_NOT_ACTED", SEV_WARNING, f"no exit state {(now_ms - req) / 1000:.0f}s", tid))
    return out


# ── 4. abnormal behaviour ────────────────────────────────────────────────────
def check_abnormal(cfg: CircuitConfig, now_ms: int, order_ts: List[int], reject_ts: List[int],
                   heartbeats: Dict[str, Optional[int]], prev_pnl: Optional[float],
                   cur_pnl: Optional[float]) -> List[Finding]:
    out: List[Finding] = []
    n = sum(1 for t in order_ts if now_ms - t <= 60_000)
    if n > cfg.max_orders_per_min:
        out.append(Finding("ORDER_RATE", SEV_CRITICAL, f"{n} orders in 60s (max {cfg.max_orders_per_min})"))
    rj = sum(1 for t in reject_ts if now_ms - t <= cfg.reject_window_sec * 1000)
    if rj >= cfg.max_rejects_window:
        sev = SEV_CRITICAL if rj >= 2 * cfg.max_rejects_window else SEV_WARNING
        out.append(Finding("REPEATED_REJECTIONS", sev, f"{rj} rejections in {cfg.reject_window_sec:.0f}s"))
    for name, ts in sorted(heartbeats.items()):
        if ts is None or now_ms - ts > cfg.heartbeat_max_age_sec * 1000:
            age = "never" if ts is None else f"{(now_ms - ts) / 1000:.0f}s"
            out.append(Finding("STALE_HEARTBEAT", SEV_WARNING, f"age={age}", name))
    if prev_pnl is not None and cur_pnl is not None and cfg.pnl_jump > 0 and abs(cur_pnl - prev_pnl) >= cfg.pnl_jump:
        out.append(Finding("PNL_JUMP", SEV_CRITICAL, f"{prev_pnl:.0f} -> {cur_pnl:.0f}"))
    return out


# ── 6. state machine ─────────────────────────────────────────────────────────
@dataclass(frozen=True)
class BreakerState:
    state: str = ARMED
    since_ms: int = 0
    severity: str = ""
    reason: str = ""
    findings: Tuple[dict, ...] = ()
    last_finding_ms: int = 0          # last time a WARNING finding was present
    tripped_ms: int = 0
    emergency_handled: bool = False   # emergency exit pass already done for this trip

    def to_dict(self) -> dict:
        d = dict(self.__dict__)
        d["findings"] = list(self.findings)
        return d

    @staticmethod
    def from_dict(d: Optional[dict]) -> "BreakerState":
        if not d:
            return BreakerState()
        kw = {k: v for k, v in d.items() if k in BreakerState.__dataclass_fields__}
        kw["findings"] = tuple(kw.get("findings") or ())
        if kw.get("state") not in (ARMED, WARNING, TRIPPED):
            kw["state"] = ARMED
        return BreakerState(**kw)


def advance(st: BreakerState, findings: List[Finding], now_ms: int,
            cfg: CircuitConfig) -> Tuple[BreakerState, List[dict]]:
    """One evaluation cycle -> (new state, events). TRIPPED never changes here (manual reset only)."""
    events: List[dict] = []
    sev = worst(findings)
    fdicts = tuple(f.to_dict() for f in findings)
    if st.state == TRIPPED:
        if _SEV_RANK[sev] > _SEV_RANK.get(st.severity, 0):       # late escalation while already tripped
            events.append({"event": "ESCALATED", "severity": sev, "findings": list(fdicts)})
            return replace(st, severity=sev, findings=fdicts,
                           emergency_handled=st.emergency_handled and sev != SEV_EMERGENCY), events
        return st, events
    if _SEV_RANK[sev] >= _SEV_RANK[SEV_CRITICAL]:
        reason = ";".join(f"{f.code}{(':' + f.key) if f.key else ''}" for f in findings if f.severity == sev)
        events.append({"event": "TRIPPED", "severity": sev, "reason": reason, "findings": list(fdicts)})
        return BreakerState(TRIPPED, now_ms, sev, reason, fdicts, now_ms, now_ms, False), events
    if sev == SEV_WARNING:
        if st.state != WARNING:
            events.append({"event": "WARNING", "severity": sev, "findings": list(fdicts)})
        return replace(st, state=WARNING, since_ms=st.since_ms if st.state == WARNING else now_ms, severity=sev,
                       reason=";".join(f.code for f in findings), findings=fdicts, last_finding_ms=now_ms), events
    if st.state == WARNING and now_ms - st.last_finding_ms >= cfg.warn_clear_sec * 1000:
        events.append({"event": "CLEARED"})
        return BreakerState(ARMED, now_ms), events
    return st, events


def reset(st: BreakerState, now_ms: int) -> Tuple[BreakerState, dict]:
    """Manual reset (CLI): any state -> ARMED."""
    return BreakerState(ARMED, now_ms), {"event": "RESET", "from": st.state}


# ── 5. emergency protection ──────────────────────────────────────────────────
def should_emergency_exit(st: BreakerState) -> bool:
    return st.state == TRIPPED and st.severity == SEV_EMERGENCY and not st.emergency_handled


def emergency_exit_requests(positions: List[dict], now_ms: int, default_mode: str,
                            reason: str = "CIRCUIT_BREAKER") -> List[Dict[str, str]]:
    """md:exec:exit_request payloads (shape of app.trade_journal.exit_request) for positions not already exiting."""
    out = []
    for p in positions:
        if p.get("exit_pending") or _f(p.get("qty")) <= 0:
            continue
        out.append({
            "trade_id": str(p.get("trade_id") or ""), "tradingsymbol": str(p.get("tradingsymbol") or ""),
            "symbol": str(p.get("symbol") or ""), "qty": str(p.get("qty")),
            "orig_qty": str(p.get("orig_qty") or p.get("qty")), "lot_size": str(p.get("lot_size") or ""),
            "reason": reason, "limit_floor": "", "bid": "",
            "exec_mode": str(p.get("exec_mode") or default_mode), "ts_ms": str(now_ms),
        })
    return out

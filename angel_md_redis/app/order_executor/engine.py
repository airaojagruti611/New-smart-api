"""
OrderExecutor.step() — one pure transition of the execution state machine
(DECISION.md §7 E1, E4–E13). The runner owns the clock, Redis and the broker:

    state, actions, events = step(state, quote, ctx, updates, now_ms, cfg, rates)

  updates  broker OrderUpdates since the last step (cumulative per order)
  actions  what the runner must send: PLACE / MODIFY / CANCEL (limit orders only)
  events   audit records for md:exec

Rules the step enforces:
  * the price cap is anchored to the FIRST ask and never moves (no chasing, E6)
  * the quantity never exceeds ICARE's lots (E7); one working order at a time (E8)
  * a partial fill continues only if the benefit beats the extra cost (E9)
  * timeout / kill switch cancel what is left; fills already made are kept (E9, E13)
"""

from __future__ import annotations

import copy
from typing import List, Optional, Sequence, Tuple

from . import state as S
from .charges import ChargeRates, charges, per_order_cost
from .command import Context, ExecCommand, validate
from .config import ExecConfig
from .fills import avg_price, continue_decision, fill_delta
from .market import Quote
from .pricing import ladder_step, next_price, price_cap, start_price
from .quantity import executable_lots, slice_lots

CANCEL_RESEND_MS = 5000
PRICE_BAND_MAX_REJECTS = 2

_REJECT_FOR_LIMIT = {"margin": "INSUFFICIENT_MARGIN", "depth": "NO_DEPTH", "risk": "RISK_LIMIT",
                     "requested": "ZERO_LOTS"}


def _event(kind: str, now_ms: int, **kw) -> dict:
    return dict(event=kind, ts_ms=now_ms, **kw)


def _cmd(st: S.ExecState) -> ExecCommand:
    return ExecCommand.from_dict(st.command)


def new_state(cmd: ExecCommand, now_ms: int) -> S.ExecState:
    return S.ExecState(trade_id=cmd.trade_id, command=cmd.to_dict(), received_ms=now_ms)


# ── broker updates ─────────────────────────────────────────────────────

def _apply_updates(st: S.ExecState, updates: Sequence[S.OrderUpdate], now_ms: int, events: List[dict]) -> None:
    w = st.working
    for u in updates:
        if not w or u.order_id != w.get("order_id"):
            continue
        d = fill_delta(w["filled"], w.get("avg"), u.filled_qty, u.avg_price, w["price"])
        if d:
            px, units = d
            st.fills.append([px, units, now_ms, u.order_id])
            w["filled"], w["avg"] = u.filled_qty, u.avg_price if u.avg_price is not None else px
            events.append(_event("FILL", now_ms, order_id=u.order_id, price=px, qty=units,
                                 filled_total=st.filled_units))
        if u.status in ("COMPLETE", "CANCELLED", "REJECTED"):
            if u.status == "REJECTED":
                st.reject_reasons.append(u.reason or "REJECTED")
                events.append(_event("ORDER_REJECTED", now_ms, order_id=u.order_id, reason=u.reason))
                if "BAND" in (u.reason or "").upper() or "LPP" in (u.reason or "").upper():
                    st.lpp_rejects += 1
                    if st.lpp_rejects >= PRICE_BAND_MAX_REJECTS and not st.closing:
                        st.closing = "PRICE_OUT_OF_BAND"
                elif not st.closing:
                    st.closing = "BROKER_REJECTED"
            elif u.status == "CANCELLED":
                events.append(_event("CANCELLED", now_ms, order_id=u.order_id, filled=u.filled_qty))
            _close_order_record(st, w, u.status)
            st.working = w = None


def _close_order_record(st: S.ExecState, w: dict, status: str) -> None:
    for o in st.orders:
        if o["order_id"] == w["order_id"]:
            o.update(status=status, filled=w["filled"], avg=w.get("avg"))


# ── step ───────────────────────────────────────────────────────────────

def step(
    st: S.ExecState, q: Optional[Quote], ctx: Context, updates: Sequence[S.OrderUpdate],
    now_ms: int, cfg: ExecConfig, rates: ChargeRates,
) -> Tuple[S.ExecState, List[dict], List[dict]]:
    st = copy.deepcopy(st)
    actions: List[dict] = []
    events: List[dict] = []
    if st.status in S.TERMINAL:
        return st, actions, events
    _apply_updates(st, updates, now_ms, events)
    if st.status == S.RECEIVED:
        if not _start(st, q, ctx, now_ms, cfg, events):
            return st, actions, events
    _drive(st, q, ctx, now_ms, cfg, rates, actions, events)
    return st, actions, events


def _start(st: S.ExecState, q: Optional[Quote], ctx: Context, now_ms: int, cfg: ExecConfig,
           events: List[dict]) -> bool:
    """VALIDATING + SIZING. False = rejected before any order."""
    cmd = _cmd(st)
    events.append(_event("COMMAND", now_ms, requested_lots=cmd.requested_lots, signal_premium=cmd.signal_premium,
                         quote=q.to_dict() if q else None))
    reasons = validate(cmd, q, ctx, now_ms, cfg)
    if not reasons:
        reference = q.ask
        cap = price_cap(reference, cmd.tick, cfg)
        start = start_price(q, cmd.tick, cap, cfg)
        sl_pts = (cap - cmd.sl_premium) if cmd.sl_premium is not None else None
        lots, limiting, reductions, limits = executable_lots(
            cmd.requested_lots, cmd.lot_size, cap, ctx.available_margin, cmd.max_risk_amount, sl_pts,
            q.ask_units_within(cap), cmd.freeze_qty, cfg,
        )
        st.lot_limits, st.limiting_factor, st.reductions = limits, limiting, reductions
        if lots < cfg.min_lots:
            reasons = [_REJECT_FOR_LIMIT.get(limiting, "ZERO_LOTS") if cfg.allow_reduce or not reductions
                       else "REDUCE_NOT_ALLOWED"]
    if reasons:
        st.reject_reasons = reasons
        _finish(st, S.REJECTED_BEFORE_EXECUTION, now_ms, events)
        return False
    st.reference, st.cap, st.arrival_mid = reference, cap, round(q.mid, 4)
    st.initial_bid, st.initial_ask = q.bid, q.ask
    st.step = ladder_step(start, cap, cmd.tick, cfg)
    st.target_lots = lots
    st.started_ms = now_ms
    st.deadline_ms = now_ms + int(cmd.execution_timeout_seconds * 1000)
    st.status = S.WORKING
    events.append(_event("VALIDATED", now_ms, reference=reference, cap=cap, start=start, step=st.step,
                         executable_lots=lots, limiting=limiting, reductions=reductions, lot_limits=limits))
    return True


def _drive(st: S.ExecState, q: Optional[Quote], ctx: Context, now_ms: int, cfg: ExecConfig,
           rates: ChargeRates, actions: List[dict], events: List[dict]) -> None:
    cmd = _cmd(st)
    lot = cmd.lot_size
    remaining_lots = st.target_lots - int(round(st.filled_units / lot))

    if not st.closing:
        if ctx.kill_switch:
            st.closing = "KILL_SWITCH"
            events.append(_event("KILL_SWITCH", now_ms, filled=st.filled_units))
        elif remaining_lots <= 0 and not st.working:
            st.closing = "FILLED"
        elif now_ms >= st.deadline_ms:
            st.closing = "TIMEOUT"
            events.append(_event("TIMEOUT", now_ms, filled=st.filled_units))

    if st.closing:
        _wind_down(st, now_ms, actions, events)
        return

    st.status = S.PARTIAL if st.filled_units > 0 else S.WORKING

    # Fresh quote needed for any price decision; a working order just rests meanwhile.
    if q is None or q.age_ms(now_ms) > cfg.max_quote_age_sec * 1000:
        st.block_reason = "STALE_QUOTE"
        return
    if not q.tradable:
        st.block_reason = "NO_QUOTE"
        return
    if q.ask > st.cap + 1e-9:
        # brief §7: never follow the price up — cancel, re-evaluate, execute or abort
        st.block_reason = "ABOVE_CAP"
        if st.working and not st.working.get("cancel_sent"):
            _cancel(st, now_ms, actions, events, "ABOVE_CAP")
        return

    w = st.working
    if w:
        if w.get("cancel_sent"):
            _resend_cancel_if_stuck(st, now_ms, actions)
            return
        if (now_ms - w["priced_ms"] >= cfg.step_sec * 1000 and w["price"] < st.cap - 1e-9
                and w["filled"] < w["qty"]):
            new_px = next_price(w["price"], st.step, st.cap)
            actions.append({"type": "MODIFY", "order_id": w["order_id"], "price": new_px,
                            "qty": w["qty"] - w["filled"]})
            events.append(_event("ORDER_MODIFIED", now_ms, order_id=w["order_id"], old=w["price"], price=new_px))
            w["price"], w["priced_ms"] = new_px, now_ms
            for o in st.orders:
                if o["order_id"] == w["order_id"]:
                    o["last_price"] = new_px
                    o["modifies"] = o.get("modifies", 0) + 1
        return

    # No working order: re-check the book, then decide the next slice.
    if q.spread_pct > cfg.max_spread_pct:
        st.block_reason = "SPREAD_TOO_WIDE"
        return
    depth_units = q.ask_units_within(st.cap)
    size = slice_lots(remaining_lots, depth_units, lot, cmd.freeze_qty, cfg)
    if st.filled_units > 0:
        ok, why, detail = continue_decision(
            remaining_lots, lot, q.ask, avg_price(st.fills), cmd.signal_premium, cmd.ev_per_lot,
            max(size, 1), per_order_cost(rates), cfg,
        )
        st.decision = dict(detail, decision="CONTINUE" if ok else "STOP", why=why)
        events.append(_event("PARTIAL_DECISION", now_ms, **st.decision))
        if not ok:
            st.closing = "STOPPED"
            _wind_down(st, now_ms, actions, events)
            return
    if size < 1:
        st.block_reason = "NO_DEPTH"
        return
    price = start_price(q, cmd.tick, st.cap, cfg)
    order_id = f"{st.trade_id}-{len(st.orders) + 1}"
    qty = size * lot
    actions.append({"type": "PLACE", "order_id": order_id, "tradingsymbol": cmd.tradingsymbol,
                    "side": "BUY", "price": price, "qty": qty, "lots": size})
    st.working = {"order_id": order_id, "price": price, "qty": qty, "lots": size, "filled": 0.0, "avg": None,
                  "placed_ms": now_ms, "priced_ms": now_ms, "cancel_sent": False}
    st.orders.append({"order_id": order_id, "price": price, "last_price": price, "qty": qty, "lots": size,
                      "placed_ms": now_ms, "bid": q.bid, "ask": q.ask, "spread_pct": round(q.spread_pct, 4),
                      "status": "OPEN", "filled": 0.0, "avg": None, "modifies": 0})
    st.block_reason = ""
    events.append(_event("ORDER_PLACED", now_ms, order_id=order_id, price=price, lots=size, qty=qty,
                         bid=q.bid, ask=q.ask, spread_pct=round(q.spread_pct, 4), depth_units=depth_units))


def _cancel(st: S.ExecState, now_ms: int, actions: List[dict], events: List[dict], why: str) -> None:
    w = st.working
    actions.append({"type": "CANCEL", "order_id": w["order_id"]})
    w["cancel_sent"], w["cancel_ms"] = True, now_ms
    events.append(_event("CANCEL_SENT", now_ms, order_id=w["order_id"], reason=why))


def _resend_cancel_if_stuck(st: S.ExecState, now_ms: int, actions: List[dict]) -> None:
    w = st.working
    if now_ms - w.get("cancel_ms", now_ms) >= CANCEL_RESEND_MS:
        actions.append({"type": "CANCEL", "order_id": w["order_id"]})
        w["cancel_ms"] = now_ms


def _wind_down(st: S.ExecState, now_ms: int, actions: List[dict], events: List[dict]) -> None:
    """Cancel the working order (if any); finish once the broker has confirmed it is gone."""
    if st.working:
        st.status = S.CLOSING
        if not st.working.get("cancel_sent"):
            _cancel(st, now_ms, actions, events, st.closing)
        else:
            _resend_cancel_if_stuck(st, now_ms, actions)
        return
    filled = st.filled_units > 0
    status = {
        "FILLED": S.FILLED,
        "KILL_SWITCH": S.PARTIAL_KILLED if filled else S.KILLED,
        "STOPPED": S.PARTIAL_FILL_STOPPED,
        "PRICE_OUT_OF_BAND": S.PARTIAL_FILL_STOPPED if filled else S.ABORTED_PRICE_BAND,
        "BROKER_REJECTED": S.PARTIAL_FILL_STOPPED if filled else S.REJECTED_BY_BROKER,
        "RESTART": S.PARTIAL_FILL_STOPPED if filled else S.CANCELLED_TIMEOUT,
    }.get(st.closing)
    if status is None:   # TIMEOUT
        if filled:
            status = S.PARTIAL_FILL_TIMEOUT
        else:
            status = S.ABORTED_SLIPPAGE_LIMIT if st.block_reason == "ABOVE_CAP" else S.CANCELLED_TIMEOUT
    _finish(st, status, now_ms, events)


def _finish(st: S.ExecState, status: str, now_ms: int, events: List[dict]) -> None:
    st.status = status
    st.finished_ms = now_ms
    events.append(_event("FINAL", now_ms, status=status, closing=st.closing, reasons=st.reject_reasons))


# ── report (brief §22 + E11) ───────────────────────────────────────────

def build_report(st: S.ExecState, rates: ChargeRates, mode: str) -> dict:
    cmd = _cmd(st)
    units = st.filled_units
    avg = avg_price(st.fills)
    lots = int(round(units / cmd.lot_size)) if cmd.lot_size else 0
    executed_orders = sum(1 for o in st.orders if (o.get("filled") or 0) > 0)
    turnover = sum(f[0] * f[1] for f in st.fills)
    ch = charges("BUY", turnover, executed_orders, rates) if units > 0 else charges("BUY", 0.0, 0, rates)
    rep = {
        "trade_id": st.trade_id,
        "symbol": cmd.symbol,
        "option_symbol": cmd.tradingsymbol,
        "side": cmd.side,
        "direction": cmd.direction,
        "requested_lots": cmd.requested_lots,
        "executable_lots": st.target_lots,
        "filled_lots": lots,
        "remaining_lots": max(st.target_lots - lots, 0),
        "filled_qty": units,
        "lot_size": cmd.lot_size,
        "average_fill_price": round(avg, 4) if avg is not None else None,
        "initial_bid": st.initial_bid,
        "initial_ask": st.initial_ask,
        "reference_price": st.reference,
        "arrival_mid": st.arrival_mid,
        "price_cap": st.cap,
        "signal_premium": cmd.signal_premium,
        "execution_status": st.status,
        "cancel_reason": st.closing if st.status != S.FILLED else "",
        "reject_reasons": list(st.reject_reasons),
        "limiting_factor": st.limiting_factor,
        "reductions": list(st.reductions),
        "orders_used": len(st.orders),
        "orders_executed": executed_orders,
        "orders": st.orders,
        "charges": ch,
        "brokerage": ch["brokerage"],
        "signal_ts_ms": cmd.signal_ts_ms,
        "received_ms": st.received_ms,
        "first_order_ms": st.orders[0]["placed_ms"] if st.orders else None,
        "first_fill_ms": st.fills[0][2] if st.fills else None,
        "execution_duration_ms": (st.finished_ms - st.started_ms) if st.started_ms else 0,
        "timestamp": st.finished_ms,
        "partial_decision": st.decision,
        "mode": mode,
    }
    if avg is not None and st.reference:
        slip = avg - st.reference
        slip_cost = slip * units
        rep.update(
            slippage=round(slip, 4),
            slippage_pct=round(slip / st.reference * 100.0, 4),
            slippage_vs_mid=round(avg - st.arrival_mid, 4) if st.arrival_mid else None,
            slippage_cost=round(slip_cost, 2),
            total_execution_cost=round(ch["total"] + slip_cost, 2),
            implementation_shortfall=round((avg - st.arrival_mid) * units + ch["total"], 2) if st.arrival_mid else None,
        )
    else:
        rep.update(slippage=None, slippage_pct=None, slippage_vs_mid=None, slippage_cost=None,
                   total_execution_cost=None, implementation_shortfall=None)
    rep["signal_decay"] = (round(st.reference - cmd.signal_premium, 4)
                           if st.reference and cmd.signal_premium else None)
    return rep


EXEC_FIELDS = ("trade_id", "execution_status", "requested_lots", "filled_lots", "average_fill_price",
               "reference_price", "arrival_mid", "slippage", "slippage_pct", "slippage_vs_mid", "signal_decay",
               "implementation_shortfall", "total_execution_cost", "orders_used", "execution_duration_ms",
               "cancel_reason", "mode")


def exec_fields(rep: dict) -> dict:
    """Flat exec_* fields of a final report, carried on journal records (E15/E16)."""
    out = {f"exec_{k}": "" if rep.get(k) is None else str(rep.get(k)) for k in EXEC_FIELDS}
    out["exec_charges"] = str((rep.get("charges") or {}).get("total") or "")
    return out


def recover_after_restart(st: S.ExecState) -> S.ExecState:
    """Paper mode: an order that was working when the process died is treated as cancelled (fills kept)."""
    st = copy.deepcopy(st)
    if st.status in S.TERMINAL:
        return st
    if st.working:
        _close_order_record(st, st.working, "CANCELLED")
        st.working = None
    st.closing = st.closing or "RESTART"
    return st

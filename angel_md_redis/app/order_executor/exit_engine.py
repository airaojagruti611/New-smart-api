"""
Exit execution — the SELL path required before live (DECISION.md §7 E17).

The journal (Exit Engine) decides an exit (SL / TARGET / TIME / EOD / TRAILING_STOP) and
publishes md:exec:exit_request {trade_id, tradingsymbol, qty, reason, limit_floor, ...}. The
executor turns it into SELL LIMIT orders (never a market order, E14):

  * reference = bid at the first fresh quote; the ladder starts AT the bid and steps DOWN
    every `exit_step_sec` to a floor = max(limit_floor, bid x (1 - exit_max_slippage_pct))
  * an exit is never abandoned for price: at `exit_timeout_sec` it re-anchors on the
    CURRENT bid (new floor, limit_floor dropped) and keeps going
  * the kill switch does not stop exits (it blocks new risk, an exit removes risk)
  * quotes count as fresh up to EXEC_EXIT_QUOTE_MAX_AGE_SEC (30, receive-time based); when no
    fresh quote arrives for that long the exit is NOT blocked: it prices off the last known
    bid (quote or the journal's request bid) and ladders down to the floor
  * price-band (LPP) rejections re-quote inside the band (a tick above the refused price)
  * a definite broker refusal that repeats EXIT_MAX_REJECTS times -> EXIT_FAILED (alert;
    the journal keeps the position open and re-requests)

    st, actions, events = step_exit(st, quote, updates, now_ms, cfg)

Pure: no I/O. The runner (run_order_executor.py) owns Redis and the broker.
"""

from __future__ import annotations

import copy
from dataclasses import asdict, dataclass, field
from typing import List, Optional, Sequence, Tuple

from . import state as S
from .charges import ChargeRates, charges
from .config import ExecConfig
from .engine import _event, action_failed, band_requote_sell, is_band_reject
from .fills import avg_price, fill_delta
from .market import Quote
from .pricing import ceil_tick, floor_tick
from .quantity import max_lots_per_order

EXIT_RECEIVED = "EXIT_RECEIVED"
EXIT_WORKING = "EXIT_WORKING"
EXIT_FILLED = "EXIT_FILLED"
EXIT_FAILED = "EXIT_FAILED"
EXIT_TERMINAL = frozenset({EXIT_FILLED, EXIT_FAILED})
EXIT_MAX_REJECTS = 5
CANCEL_RESEND_MS = 5000


@dataclass
class ExitState:
    exit_id: str
    position_trade_id: str          # the journal's position trade_id
    tradingsymbol: str
    symbol: str
    reason: str                     # SL / TARGET / TIME / EOD / TRAILING_STOP
    qty: float                      # units to sell
    lot_size: float
    tick: float = 0.05
    freeze_qty: Optional[float] = None
    limit_floor: Optional[float] = None
    signal_ts_ms: int = 0           # when the journal decided the exit
    last_bid: Optional[float] = None    # last known bid (request bid, then every quote seen)
    status: str = EXIT_RECEIVED
    received_ms: int = 0
    started_ms: int = 0
    deadline_ms: int = 0
    reference: Optional[float] = None       # bid at the (latest) anchor
    first_reference: Optional[float] = None
    floor: Optional[float] = None
    step: Optional[float] = None
    reanchors: int = 0
    fills: List[list] = field(default_factory=list)
    orders: List[dict] = field(default_factory=list)
    working: Optional[dict] = None
    lpp_rejects: int = 0
    lpp_last_price: Optional[float] = None
    rejects: int = 0
    reject_reasons: List[str] = field(default_factory=list)
    block_reason: str = ""
    finished_ms: int = 0
    report: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "ExitState":
        return ExitState(**{k: d[k] for k in ExitState.__dataclass_fields__ if k in d})

    @property
    def filled_units(self) -> float:
        return sum(f[1] for f in self.fills)


def _fresh(q: Optional[Quote], now_ms: int, cfg: ExecConfig) -> bool:
    return q is not None and q.tradable and q.age_ms(now_ms) <= cfg.exit_quote_max_age_sec * 1000


def _usable_bid(st: "ExitState", q: Optional[Quote], now_ms: int, cfg: ExecConfig) -> Optional[float]:
    """Fresh bid; or, once we have waited exit_quote_max_age_sec, the last known bid (never stuck)."""
    if q is not None and q.bid and q.bid > 0:
        st.last_bid = q.bid
    if _fresh(q, now_ms, cfg):
        return q.bid
    if st.last_bid and now_ms - st.received_ms >= cfg.exit_quote_max_age_sec * 1000:
        return st.last_bid
    return None


def _floor_for(bid: float, tick: float, cfg: ExecConfig, limit_floor: Optional[float]) -> float:
    f = floor_tick(bid * (1.0 - cfg.exit_max_slippage_pct / 100.0), tick)
    if limit_floor:
        f = max(f, ceil_tick(limit_floor, tick))
    return round(max(min(f, bid), tick), 4)


def _apply_exit_updates(st: ExitState, updates: Sequence[S.OrderUpdate], now_ms: int, events: List[dict]) -> None:
    w = st.working
    for u in updates:
        if not w or u.order_id != w.get("order_id"):
            continue
        w.pop("pending_reconcile", None)
        d = fill_delta(w["filled"], w.get("avg"), u.filled_qty, u.avg_price, w["price"])
        if d:
            px, units = d
            st.fills.append([px, units, now_ms, u.order_id])
            w["filled"], w["avg"] = u.filled_qty, u.avg_price if u.avg_price is not None else px
            events.append(_event("EXIT_FILL", now_ms, order_id=u.order_id, price=px, qty=units,
                                 filled_total=st.filled_units))
        if u.status in ("COMPLETE", "CANCELLED", "REJECTED"):
            if u.status == "REJECTED":
                st.reject_reasons.append(u.reason or "REJECTED")
                events.append(_event("EXIT_ORDER_REJECTED", now_ms, order_id=u.order_id, reason=u.reason))
                if is_band_reject(u.reason):
                    st.lpp_rejects += 1
                    st.lpp_last_price = w["price"]
                else:
                    st.rejects += 1
            for o in st.orders:
                if o["order_id"] == w["order_id"]:
                    o.update(status=u.status, filled=w["filled"], avg=w.get("avg"))
            st.working = w = None


def new_exit_state(exit_id: str, req: dict, now_ms: int, spec: Optional[dict] = None) -> ExitState:
    spec = spec or {}

    def f(k, default=None):
        try:
            v = req.get(k)
            return default if v in (None, "") else float(v)
        except (TypeError, ValueError):
            return default

    return ExitState(
        exit_id=exit_id,
        position_trade_id=str(req.get("trade_id") or ""),
        tradingsymbol=str(req.get("tradingsymbol") or "").upper(),
        symbol=str(req.get("symbol") or "").upper(),
        reason=str(req.get("reason") or ""),
        qty=f("qty", 0.0) or 0.0,
        lot_size=f("lot_size") or float(spec.get("lot_size") or 0) or (f("qty", 0.0) or 0.0),
        tick=float(spec.get("tick") or 0.05),
        freeze_qty=spec.get("freeze_qty"),
        limit_floor=f("limit_floor"),
        signal_ts_ms=int(f("ts_ms", 0) or 0),
        last_bid=f("bid"),
        received_ms=now_ms,
    )


def step_exit(st: ExitState, q: Optional[Quote], updates: Sequence[S.OrderUpdate], now_ms: int,
              cfg: ExecConfig) -> Tuple[ExitState, List[dict], List[dict]]:
    st = copy.deepcopy(st)
    actions: List[dict] = []
    events: List[dict] = []
    if st.status in EXIT_TERMINAL:
        return st, actions, events
    _apply_exit_updates(st, updates, now_ms, events)
    remaining = st.qty - st.filled_units
    if remaining <= 1e-9 and not st.working:
        return _done(st, EXIT_FILLED, now_ms, events), actions, events
    if st.rejects >= EXIT_MAX_REJECTS and not st.working:
        return _done(st, EXIT_FAILED, now_ms, events), actions, events

    bid = _usable_bid(st, q, now_ms, cfg)
    stale_basis = bid is not None and not _fresh(q, now_ms, cfg)
    if st.status == EXIT_RECEIVED:
        if st.qty <= 0 or st.lot_size <= 0:
            st.reject_reasons.append("INVALID_EXIT")
            return _done(st, EXIT_FAILED, now_ms, events), actions, events
        if bid is None:
            st.block_reason = "STALE_QUOTE"
            return st, actions, events
        _anchor(st, bid, now_ms, cfg, st.limit_floor)
        st.started_ms = now_ms
        st.status = EXIT_WORKING
        events.append(_event("EXIT_VALIDATED", now_ms, reason=st.reason, qty=st.qty, reference=st.reference,
                             floor=st.floor, step=st.step, quote=q.to_dict() if q else None,
                             last_known_bid=stale_basis))

    w = st.working
    if w:
        if w.get("cancel_sent"):
            if now_ms - w.get("cancel_ms", now_ms) >= CANCEL_RESEND_MS:
                actions.append({"type": "CANCEL", "order_id": w["order_id"]})
                w["cancel_ms"] = now_ms
            return st, actions, events
        if w.get("pending_reconcile"):
            st.block_reason = "PENDING_RECONCILE"
            return st, actions, events
        new_px = None
        if now_ms >= st.deadline_ms:
            if bid is not None:  # never abandoned: re-anchor on the current bid and keep going
                _anchor(st, bid, now_ms, cfg, None)
                st.reanchors += 1
                events.append(_event("EXIT_REANCHOR", now_ms, reference=st.reference, floor=st.floor,
                                     reanchors=st.reanchors))
                new_px = st.reference
            else:
                st.block_reason = "STALE_QUOTE"
        elif now_ms - w["priced_ms"] >= cfg.exit_step_sec * 1000 and w["price"] > st.floor + 1e-9:
            new_px = round(max(w["price"] - st.step, st.floor), 4)
        if new_px is not None and abs(new_px - w["price"]) > 1e-9:
            # Angel modifyOrder takes the order's TOTAL quantity, not the remainder
            actions.append({"type": "MODIFY", "order_id": w["order_id"], "price": new_px, "qty": w["qty"],
                            "prev_price": w["price"], "tradingsymbol": st.tradingsymbol})
            events.append(_event("EXIT_ORDER_MODIFIED", now_ms, order_id=w["order_id"], old=w["price"], price=new_px))
            w["price"], w["priced_ms"] = new_px, now_ms
            for o in st.orders:
                if o["order_id"] == w["order_id"]:
                    o["last_price"] = new_px
                    o["modifies"] = o.get("modifies", 0) + 1
        elif new_px is not None:
            w["priced_ms"] = now_ms
        return st, actions, events

    # no working order: place the next SELL slice
    if bid is None:
        st.block_reason = "STALE_QUOTE"
        return st, actions, events
    if now_ms >= st.deadline_ms:
        _anchor(st, bid, now_ms, cfg, None)
        st.reanchors += 1
        events.append(_event("EXIT_REANCHOR", now_ms, reference=st.reference, floor=st.floor, reanchors=st.reanchors))
    price = max(bid, st.floor)
    if st.lpp_last_price is not None:
        if q is not None and q.tradable:
            price = band_requote_sell(q, st.tick, st.floor, st.lpp_last_price)
        else:
            price = max(price, round(ceil_tick(st.lpp_last_price + st.tick, st.tick), 4))
    lots_left = int(round(remaining / st.lot_size)) or 1
    size = min(lots_left, max_lots_per_order(st.lot_size, st.freeze_qty, cfg))
    qty = min(size * st.lot_size, remaining)
    order_id = f"{st.exit_id}-{len(st.orders) + 1}"
    actions.append({"type": "PLACE", "order_id": order_id, "tradingsymbol": st.tradingsymbol, "side": "SELL",
                    "price": price, "qty": qty, "lots": size})
    st.working = {"order_id": order_id, "price": price, "qty": qty, "lots": size, "filled": 0.0, "avg": None,
                  "placed_ms": now_ms, "priced_ms": now_ms, "cancel_sent": False}
    st.orders.append({"order_id": order_id, "side": "SELL", "price": price, "last_price": price, "qty": qty,
                      "lots": size, "placed_ms": now_ms, "bid": bid, "ask": q.ask if q else None, "status": "OPEN",
                      "filled": 0.0, "avg": None, "modifies": 0})
    st.block_reason = ""
    events.append(_event("EXIT_ORDER_PLACED", now_ms, order_id=order_id, price=price, qty=qty, lots=size,
                         bid=bid, ask=q.ask if q else None, floor=st.floor, last_known_bid=stale_basis))
    return st, actions, events


def _anchor(st: ExitState, bid: float, now_ms: int, cfg: ExecConfig, limit_floor: Optional[float]) -> None:
    st.reference = bid
    st.first_reference = st.first_reference or bid
    st.floor = _floor_for(bid, st.tick, cfg, limit_floor)
    st.step = max(st.tick, ceil_tick((bid - st.floor) / max(cfg.ladder_steps, 1), st.tick))
    st.deadline_ms = now_ms + int(cfg.exit_timeout_sec * 1000)


def _done(st: ExitState, status: str, now_ms: int, events: List[dict]) -> ExitState:
    st.status = status
    st.finished_ms = now_ms
    events.append(_event("EXIT_FINAL", now_ms, status=status, filled=st.filled_units, qty=st.qty,
                         reasons=st.reject_reasons))
    return st


def exit_action_failed(st: ExitState, a: dict, why: str, now_ms: int):
    return action_failed(st, a, why, now_ms, apply_fn=_apply_exit_updates)


def recover_exit_after_restart(st: ExitState, live: bool = False) -> ExitState:
    """Live: keep managing the resting SELL (broker id re-attached by the runner). Paper: it is gone."""
    st = copy.deepcopy(st)
    if st.status in EXIT_TERMINAL or not st.working:
        return st
    if not live:
        for o in st.orders:
            if o["order_id"] == st.working["order_id"]:
                o["status"] = "CANCELLED"
        st.working = None
    return st


def exit_report(st: ExitState, rates: ChargeRates, mode: str) -> dict:
    avg = avg_price(st.fills)
    units = st.filled_units
    executed = sum(1 for o in st.orders if (o.get("filled") or 0) > 0)
    turnover = sum(f[0] * f[1] for f in st.fills)
    ch = charges("SELL", turnover, executed, rates) if units > 0 else charges("SELL", 0.0, 0, rates)
    slip = round(st.first_reference - avg, 4) if avg is not None and st.first_reference else None
    return {
        "exit_id": st.exit_id,
        "trade_id": st.position_trade_id,
        "tradingsymbol": st.tradingsymbol,
        "symbol": st.symbol,
        "reason": st.reason,
        "status": st.status,
        "requested_qty": st.qty,
        "filled_qty": units,
        "remaining_qty": max(st.qty - units, 0.0),
        "avg_price": round(avg, 4) if avg is not None else None,
        "reference_bid": st.first_reference,
        "slippage": slip,                       # positive = sold below the first bid
        "reanchors": st.reanchors,
        "orders_used": len(st.orders),
        "charges": ch["total"],
        "reject_reasons": list(st.reject_reasons),
        "signal_ts_ms": st.signal_ts_ms,
        "received_ms": st.received_ms,
        "duration_ms": (st.finished_ms - st.started_ms) if st.started_ms else 0,
        "timestamp": st.finished_ms,
        "exec_mode": mode,
    }

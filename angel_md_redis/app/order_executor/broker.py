"""
Broker adapters (DECISION.md §7 E14).

  PaperBroker  simulated fills against the live book (shadow / paper modes)
  AngelBroker  SmartAPI LIMIT + DAY orders (live) — DISABLED by default (X-Q1),
               refused without a registered static IP (X-Q11)

Both expose the same calls the runner uses:
  place(action, now_ms) -> (ok, broker_order_id, reason)
  modify(order_id, price, qty, now_ms) -> (ok, reason)
  cancel(order_id, now_ms) -> (ok, reason)
  poll(quotes_by_tsym, now_ms) -> [OrderUpdate]          (cumulative per order)
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from .market import Quote
from .state import OrderUpdate


@dataclass
class _PaperOrder:
    order_id: str
    tradingsymbol: str
    price: float
    qty: float
    lot: float
    placed_ms: int
    filled: float = 0.0
    notional: float = 0.0
    status: str = "OPEN"
    last_quote_ts: int = -1
    reported: Tuple[float, str] = (0.0, "OPEN")


class PaperBroker:
    """
    A BUY limit fills when limit >= ask, at the resting ask prices <= limit, up to the
    visible depth (whole lots), once per new quote (a quote's depth is consumed only once).
    No queue-position model -> fills are OPTIMISTIC (§7.4). Orders become eligible after
    `latency_ms`.
    """

    def __init__(self, latency_ms: int = 300):
        self.latency_ms = latency_ms
        self.orders: Dict[str, _PaperOrder] = {}

    def place(self, a: dict, now_ms: int) -> Tuple[bool, str, str]:
        lot = a["qty"] / a["lots"] if a.get("lots") else a["qty"]
        self.orders[a["order_id"]] = _PaperOrder(a["order_id"], a["tradingsymbol"], a["price"], a["qty"], lot, now_ms)
        return True, a["order_id"], ""

    def modify(self, order_id: str, price: float, qty: Optional[float], now_ms: int) -> Tuple[bool, str]:
        o = self.orders.get(order_id)
        if not o or o.status != "OPEN":
            return False, "ORDER_NOT_OPEN"
        o.price = price
        return True, ""

    def cancel(self, order_id: str, now_ms: int) -> Tuple[bool, str]:
        o = self.orders.get(order_id)
        if not o or o.status != "OPEN":
            return False, "ORDER_NOT_OPEN"
        o.status = "CANCELLED"
        return True, ""

    def _try_fill(self, o: _PaperOrder, q: Quote, now_ms: int) -> None:
        if now_ms - o.placed_ms < self.latency_ms or q.ts_ms <= o.last_quote_ts or not q.tradable:
            return
        o.last_quote_ts = q.ts_ms
        if o.price + 1e-9 < q.ask:
            return
        levels = [lv for lv in q.ask_levels if lv[0] <= o.price + 1e-9] or (
            [(q.ask, q.ask_qty)] if q.ask_qty else [(q.ask, math.inf)])
        want = o.qty - o.filled
        got, notional = 0.0, 0.0
        for px, units in levels:
            take = min(units, want - got)
            got += take
            notional += take * px
            if got >= want:
                break
        whole = math.floor(got / o.lot + 1e-9) * o.lot
        if whole <= 0:
            return
        notional *= whole / got
        o.filled += whole
        o.notional += notional
        if o.filled >= o.qty - 1e-9:
            o.status = "COMPLETE"

    def poll(self, quotes: Dict[str, Quote], now_ms: int) -> List[OrderUpdate]:
        out: List[OrderUpdate] = []
        for oid, o in list(self.orders.items()):
            if o.status == "OPEN" and o.tradingsymbol in quotes and quotes[o.tradingsymbol] is not None:
                self._try_fill(o, quotes[o.tradingsymbol], now_ms)
            state = (o.filled, o.status)
            if state != o.reported:
                avg = round(o.notional / o.filled, 4) if o.filled else None
                out.append(OrderUpdate(oid, o.status, o.filled, avg))
                o.reported = state
            if o.status != "OPEN":
                del self.orders[oid]
        return out


# ── Live (P29) — disabled unless EXEC_MODE=live + EXEC_LIVE_ENABLED=1 + static IP ──

@dataclass
class LiveGate:
    """Startup checks for live mode. Every one must pass or the runner refuses to trade."""
    mode: str
    live_enabled: bool
    static_ip: str
    public_ip: str
    session_ok: bool
    max_order_value: float

    def problems(self) -> List[str]:
        out = []
        if self.mode != "live":
            out.append("EXEC_MODE_NOT_LIVE")
        if not self.live_enabled:
            out.append("EXEC_LIVE_ENABLED_NOT_SET")
        if not self.static_ip:
            out.append("LIVE_BLOCKED_NO_STATIC_IP")
        elif self.public_ip != self.static_ip:
            out.append("PUBLIC_IP_NOT_REGISTERED_STATIC_IP")
        if not self.session_ok:
            out.append("NO_BROKER_SESSION")
        if self.max_order_value <= 0:
            out.append("EXEC_LIVE_MAX_ORDER_VALUE_NOT_SET")
        return out


class RateBucket:
    """Token bucket for place + modify + cancel (SEBI < 10 orders/s; we budget EXEC_MAX_OPS)."""

    def __init__(self, per_sec: float, clock_ms: Callable[[], int]):
        self.per_sec = per_sec
        self.clock_ms = clock_ms
        self.tokens = per_sec
        self.last = clock_ms()

    def take(self) -> bool:
        now = self.clock_ms()
        self.tokens = min(self.per_sec, self.tokens + (now - self.last) / 1000.0 * self.per_sec)
        self.last = now
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


_ANGEL_STATUS = {
    "complete": "COMPLETE", "cancelled": "CANCELLED", "rejected": "REJECTED",
    "open": "OPEN", "open pending": "OPEN", "modified": "OPEN", "modify pending": "OPEN",
    "validation pending": "OPEN", "put order req received": "OPEN", "trigger pending": "OPEN",
    "cancel pending": "OPEN",
}


@dataclass
class AngelBroker:
    """
    SmartConnect wrapper. LIMIT orders only, duration DAY (market / IOC are not allowed
    for API orders since 1 Apr 2026). `ordertag` = our order id so a restart can reconcile.
    Fills come from order details (cumulative filledshares / averageprice).
    """
    api: object                                  # SmartConnect (or a fake in tests)
    tokens: Dict[str, str]                       # tradingsymbol -> symboltoken
    bucket: RateBucket
    max_order_value: float
    product: str = "INTRADAY"
    exchange: str = "NFO"
    ids: Dict[str, str] = field(default_factory=dict)        # our id -> broker id
    reported: Dict[str, Tuple[float, str]] = field(default_factory=dict)

    def _rate(self) -> Optional[str]:
        return None if self.bucket.take() else "RATE_LIMIT_LOCAL"

    def place(self, a: dict, now_ms: int) -> Tuple[bool, str, str]:
        if a["price"] * a["qty"] > self.max_order_value:
            return False, "", "MAX_ORDER_VALUE"
        err = self._rate()
        if err:
            return False, "", err
        params = {
            "variety": "NORMAL", "tradingsymbol": a["tradingsymbol"], "symboltoken": self.tokens.get(a["tradingsymbol"], ""),
            "transactiontype": a["side"], "exchange": self.exchange, "ordertype": "LIMIT",
            "producttype": self.product, "duration": "DAY", "price": f"{a['price']:.2f}",
            "quantity": str(int(a["qty"])), "ordertag": a["order_id"][-20:],
        }
        try:
            broker_id = self.api.placeOrder(params)
        except Exception as e:          # network / API error: report, the engine decides
            return False, "", f"PLACE_ERROR:{e}"
        if not broker_id:
            return False, "", "PLACE_FAILED"
        self.ids[a["order_id"]] = str(broker_id)
        return True, str(broker_id), ""

    def modify(self, order_id: str, price: float, qty: Optional[float], now_ms: int) -> Tuple[bool, str]:
        err = self._rate()
        if err:
            return False, err
        bid = self.ids.get(order_id)
        try:
            self.api.modifyOrder({"variety": "NORMAL", "orderid": bid, "ordertype": "LIMIT",
                                  "producttype": self.product, "duration": "DAY", "price": f"{price:.2f}",
                                  "quantity": str(int(qty or 0)), "exchange": self.exchange})
        except Exception as e:
            return False, f"MODIFY_ERROR:{e}"
        return True, ""

    def cancel(self, order_id: str, now_ms: int) -> Tuple[bool, str]:
        err = self._rate()
        if err:
            return False, err
        try:
            self.api.cancelOrder(self.ids.get(order_id), "NORMAL")
        except Exception as e:
            return False, f"CANCEL_ERROR:{e}"
        return True, ""

    def poll(self, quotes: Dict[str, Quote], now_ms: int) -> List[OrderUpdate]:
        """Order book read; the cancel-after-fill race is covered because fills are read, never assumed."""
        try:
            book = (self.api.orderBook() or {}).get("data") or []
        except Exception:
            return []
        by_broker = {str(o.get("orderid")): o for o in book}
        out: List[OrderUpdate] = []
        for ours, bid in list(self.ids.items()):
            o = by_broker.get(bid)
            if not o:
                continue
            status = _ANGEL_STATUS.get(str(o.get("status") or o.get("orderstatus") or "").lower(), "OPEN")
            filled = float(o.get("filledshares") or 0)
            avg = float(o.get("averageprice") or 0) or None
            if (filled, status) != self.reported.get(ours):
                out.append(OrderUpdate(ours, status, filled, avg, str(o.get("text") or "")))
                self.reported[ours] = (filled, status)
            if status != "OPEN":
                del self.ids[ours]
        return out

"""
Broker adapters (DECISION.md §7 E14).

  PaperBroker  simulated fills against the live book (shadow / paper modes)
  AngelBroker  SmartAPI LIMIT + DAY orders (live) — DISABLED by default (X-Q1),
               refused without a registered static IP (X-Q11)

Both expose the same calls the runner uses:
  place(action, now_ms) -> (ok, broker_order_id, reason)
  modify(order_id, price, qty, now_ms, tradingsymbol=None) -> (ok, reason)
                                                         qty = the order's TOTAL quantity (Angel)
  cancel(order_id, now_ms) -> (ok, reason)
  poll(quotes_by_tsym, now_ms) -> [OrderUpdate]          (cumulative per order)
  restore(order_id, broker_order_id, now_ms)             restart: re-attach a persisted order

A failed place() reason is one of three kinds (place_failure_kind):
  RETRY     local rate limit — nothing reached the broker, try again next step
  UNKNOWN   timeout / connection error — the order MAY be at the exchange: the broker
            looks for our unique ordertag in the order book before deciding (never re-placed blindly)
  REJECTED  the broker answered with an error — definite
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from .market import Quote
from .state import OrderUpdate

RATE_LIMIT_LOCAL = "RATE_LIMIT_LOCAL"
PLACE_UNKNOWN = "PLACE_UNKNOWN"
NOT_IN_BOOK = "PLACE_UNKNOWN_NOT_IN_BOOK"


def place_failure_kind(reason: str) -> str:
    r = str(reason or "")
    if r.startswith(RATE_LIMIT_LOCAL):
        return "RETRY"
    if r.startswith(PLACE_UNKNOWN):
        return "UNKNOWN"
    return "REJECTED"


# SmartAPI exception classes that mean "the broker answered and refused" (smartapi.smartExceptions)
_DEFINITE_EXC = ("InputException", "PermissionException", "TokenException", "OrderException")


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
    side: str = "BUY"


class PaperBroker:
    """
    A BUY limit fills when limit >= ask, at the resting ask prices <= limit, up to the
    visible depth (whole lots), once per new quote (a quote's depth is consumed only once).
    A SELL limit (exits, E17) mirrors it: fills when limit <= bid, at bid prices >= limit.
    No queue-position model -> fills are OPTIMISTIC (§7.4). Orders become eligible after
    `latency_ms`.
    """

    def __init__(self, latency_ms: int = 300):
        self.latency_ms = latency_ms
        self.orders: Dict[str, _PaperOrder] = {}

    def place(self, a: dict, now_ms: int) -> Tuple[bool, str, str]:
        lot = a["qty"] / a["lots"] if a.get("lots") else a["qty"]
        self.orders[a["order_id"]] = _PaperOrder(a["order_id"], a["tradingsymbol"], a["price"], a["qty"], lot, now_ms,
                                                 side=str(a.get("side") or "BUY").upper())
        return True, a["order_id"], ""

    def restore(self, order_id: str, broker_order_id: str, now_ms: int, **_kw) -> None:
        """Simulated orders do not survive a restart (the engine treats them as cancelled)."""

    def modify(self, order_id: str, price: float, qty: Optional[float], now_ms: int,
               tradingsymbol: Optional[str] = None) -> Tuple[bool, str]:
        o = self.orders.get(order_id)
        if not o or o.status != "OPEN":
            return False, "ORDER_NOT_OPEN"
        if qty is not None and abs(float(qty) - o.qty) > 1e-9:
            # same contract as Angel: modify carries the TOTAL order quantity, never the remainder
            return False, "MODIFY_QTY_NOT_TOTAL"
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
        if o.side == "SELL":
            if o.price - 1e-9 > q.bid:
                return
            levels = [lv for lv in q.bid_levels if lv[0] >= o.price - 1e-9] or (
                [(q.bid, q.bid_qty)] if q.bid_qty else [(q.bid, math.inf)])
        else:
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
    # E17: live may only run when positions can be CLOSED with real SELL orders
    # (journal -> md:exec:exit_request -> executor SELL ladder -> md:exec:exit_fill -> journal)
    exit_path: bool = False

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
        if not self.exit_path:
            out.append("LIVE_EXIT_PATH_MISSING")
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


def _tag(order_id: str) -> str:
    return order_id[-20:]


@dataclass
class AngelBroker:
    """
    SmartConnect wrapper. LIMIT orders only, duration DAY (market / IOC are not allowed
    for API orders since 1 Apr 2026). `ordertag` = our order id so a restart / an unknown
    place outcome can be reconciled. Fills come from the order book (cumulative
    filledshares / averageprice).
    """
    api: object                                  # SmartConnect (or a fake in tests)
    tokens: Dict[str, str]                       # tradingsymbol -> symboltoken
    bucket: RateBucket
    max_order_value: float
    product: str = "INTRADAY"
    exchange: str = "NFO"
    reconcile_ms: int = 15_000                   # unknown place: search our tag this long
    ids: Dict[str, str] = field(default_factory=dict)        # our id -> broker id
    meta: Dict[str, dict] = field(default_factory=dict)       # our id -> {tradingsymbol, qty} (modify params)
    reported: Dict[str, Tuple[float, str]] = field(default_factory=dict)
    unknown: Dict[str, int] = field(default_factory=dict)    # our id -> first ms (outcome unknown)
    abandoned: Dict[str, int] = field(default_factory=dict)  # gave up looking; cancelled if it ever shows
    orphans: List[dict] = field(default_factory=list)        # late-appearing orders we cancelled (audit)

    def _rate(self) -> Optional[str]:
        return None if self.bucket.take() else RATE_LIMIT_LOCAL

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
            "quantity": str(int(a["qty"])), "ordertag": _tag(a["order_id"]),
        }
        self.meta[a["order_id"]] = {"tradingsymbol": a["tradingsymbol"], "qty": a["qty"]}
        try:
            broker_id = self.api.placeOrder(params)
        except Exception as e:
            if type(e).__name__ in _DEFINITE_EXC:       # the broker answered: definite rejection
                return False, "", f"PLACE_REJECTED:{e}"
            # timeout / connection reset / unknown: it may have reached the exchange
            self.unknown[a["order_id"]] = now_ms
            return False, "", f"{PLACE_UNKNOWN}:{e}"
        if isinstance(broker_id, dict):                 # raw response variant
            if not broker_id.get("status", True):
                return False, "", f"PLACE_REJECTED:{broker_id.get('message') or broker_id.get('errorcode')}"
            broker_id = (broker_id.get("data") or {}).get("orderid")
        if not broker_id:
            return False, "", "PLACE_FAILED"
        self.ids[a["order_id"]] = str(broker_id)
        return True, str(broker_id), ""

    def restore(self, order_id: str, broker_order_id: str, now_ms: int, tradingsymbol: str = "",
                qty: Optional[float] = None) -> None:
        """Restart: re-attach a persisted order; without a broker id, reconcile it by tag."""
        if tradingsymbol:
            self.meta[order_id] = {"tradingsymbol": tradingsymbol, "qty": qty}
        if broker_order_id:
            self.ids[order_id] = str(broker_order_id)
        else:
            self.unknown.setdefault(order_id, now_ms)

    def modify(self, order_id: str, price: float, qty: Optional[float], now_ms: int,
               tradingsymbol: Optional[str] = None) -> Tuple[bool, str]:
        """SmartAPI modifyOrder: full order params; `quantity` = the order's TOTAL quantity."""
        bid = self.ids.get(order_id)
        if not bid:
            return False, "UNKNOWN_ORDER"
        m = self.meta.get(order_id) or {}
        tsym = tradingsymbol or m.get("tradingsymbol") or ""
        total = qty if qty is not None else m.get("qty")
        if not tsym or not total:
            return False, "MODIFY_PARAMS_UNKNOWN"
        err = self._rate()
        if err:
            return False, err
        try:
            self.api.modifyOrder({"variety": "NORMAL", "orderid": bid, "ordertype": "LIMIT",
                                  "producttype": self.product, "duration": "DAY", "price": f"{price:.2f}",
                                  "quantity": str(int(total)), "tradingsymbol": tsym,
                                  "symboltoken": self.tokens.get(tsym, ""), "exchange": self.exchange})
        except Exception as e:
            return False, f"MODIFY_ERROR:{e}"
        return True, ""

    def cancel(self, order_id: str, now_ms: int) -> Tuple[bool, str]:
        bid = self.ids.get(order_id)
        if not bid:
            return False, "UNKNOWN_ORDER"
        err = self._rate()
        if err:
            return False, err
        try:
            self.api.cancelOrder(bid, "NORMAL")
        except Exception as e:
            return False, f"CANCEL_ERROR:{e}"
        return True, ""

    def _reconcile_unknown(self, book: List[dict], now_ms: int, out: List[OrderUpdate]) -> None:
        by_tag = {str(o.get("ordertag") or ""): o for o in book if o.get("ordertag")}
        for ours, since in list(self.unknown.items()):
            o = by_tag.get(_tag(ours))
            if o and o.get("orderid"):
                self.ids[ours] = str(o["orderid"])       # it did reach the exchange: track it normally
                del self.unknown[ours]
            elif now_ms - since >= self.reconcile_ms:
                del self.unknown[ours]
                self.abandoned[ours] = now_ms
                out.append(OrderUpdate(ours, "REJECTED", 0.0, None, NOT_IN_BOOK))
        for ours in list(self.abandoned):
            o = by_tag.get(_tag(ours))
            if not o:
                continue
            del self.abandoned[ours]
            status = _ANGEL_STATUS.get(str(o.get("status") or o.get("orderstatus") or "").lower(), "OPEN")
            if status == "OPEN":
                try:
                    self.api.cancelOrder(str(o.get("orderid")), "NORMAL")
                except Exception:
                    pass
            self.orphans.append({"order_id": ours, "broker_id": str(o.get("orderid")), "status": status,
                                 "filled": float(o.get("filledshares") or 0)})

    def poll(self, quotes: Dict[str, Quote], now_ms: int) -> List[OrderUpdate]:
        """Order book read; the cancel-after-fill race is covered because fills are read, never assumed."""
        try:
            book = (self.api.orderBook() or {}).get("data") or []
        except Exception:
            return []           # no decision on unknown orders without a successful book read
        out: List[OrderUpdate] = []
        self._reconcile_unknown(book, now_ms, out)
        by_broker = {str(o.get("orderid")): o for o in book}
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

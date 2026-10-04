"""Bid/ask snapshot, spread and depth inside the price cap (DECISION.md §7 E5)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple


def _f(v) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _csv(raw) -> List[float]:
    out = []
    for x in str(raw or "").split(","):
        v = _f(x.strip())
        if v is not None:
            out.append(v)
    return out


@dataclass(frozen=True)
class Quote:
    ts_ms: int
    bid: Optional[float]
    ask: Optional[float]
    bid_qty: Optional[float] = None
    ask_qty: Optional[float] = None
    ltp: Optional[float] = None
    bid_levels: Tuple[Tuple[float, float], ...] = field(default_factory=tuple)   # (price, units) best first
    ask_levels: Tuple[Tuple[float, float], ...] = field(default_factory=tuple)

    @property
    def tradable(self) -> bool:
        return bool(self.bid and self.ask and self.bid > 0 and self.ask > 0 and self.ask > self.bid)

    @property
    def mid(self) -> Optional[float]:
        return (self.bid + self.ask) / 2.0 if self.tradable else None

    @property
    def spread(self) -> Optional[float]:
        return self.ask - self.bid if self.tradable else None

    @property
    def spread_pct(self) -> Optional[float]:
        """(ask - bid) / mid x 100 — the brief's 100 / 102 example = 1.98 %."""
        return (self.ask - self.bid) / self.mid * 100.0 if self.tradable else None

    def age_ms(self, now_ms: int) -> int:
        return now_ms - self.ts_ms

    def ask_units_within(self, cap: float) -> Optional[float]:
        """Visible offer quantity at prices <= cap; None when no book data at all."""
        if self.ask_levels:
            return sum(q for px, q in self.ask_levels if px <= cap + 1e-9)
        if self.ask_qty is not None and self.ask is not None:
            return self.ask_qty if self.ask <= cap + 1e-9 else 0.0
        return None

    def to_dict(self) -> dict:
        return {"ts_ms": self.ts_ms, "bid": self.bid, "ask": self.ask, "bid_qty": self.bid_qty,
                "ask_qty": self.ask_qty, "ltp": self.ltp, "spread_pct": self.spread_pct}


def _levels(px_raw, sz_raw) -> Tuple[Tuple[float, float], ...]:
    pxs, szs = _csv(px_raw), _csv(sz_raw)
    return tuple((p, s) for p, s in zip(pxs, szs) if p > 0 and s > 0)


def quote_from_bidask(doc: dict) -> Optional[Quote]:
    """md:bidask:latest:{TSYM} JSON -> Quote (None when the key is missing)."""
    if not doc:
        return None
    return Quote(
        ts_ms=int(_f(doc.get("ts_ms")) or 0),
        bid=_f(doc.get("bid")),
        ask=_f(doc.get("ask")),
        bid_qty=_f(doc.get("bid_qty")),
        ask_qty=_f(doc.get("ask_qty")),
        ltp=_f(doc.get("ltp")),
        bid_levels=_levels(doc.get("bid_depth5_px"), doc.get("bid_depth5")),
        ask_levels=_levels(doc.get("ask_depth5_px"), doc.get("ask_depth5")),
    )

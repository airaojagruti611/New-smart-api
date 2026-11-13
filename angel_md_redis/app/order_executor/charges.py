"""
Brokerage + statutory charges, kept separate from slippage (DECISION.md §7 E10).

Values come from charges.json (never hard-coded in the logic). Percentages
apply to premium turnover = price x quantity.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

DEFAULT_PATH = Path(__file__).resolve().parents[2] / "charges.json"


@dataclass(frozen=True)
class ChargeRates:
    brokerage_per_order: float = 20.0
    stt_sell_pct: float = 0.15
    exchange_txn_pct: float = 0.03503
    sebi_per_crore: float = 10.0
    stamp_buy_pct: float = 0.003
    gst_pct: float = 18.0


def load_rates(path: Path = DEFAULT_PATH) -> ChargeRates:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return ChargeRates()
    fields = ChargeRates.__dataclass_fields__
    return ChargeRates(**{k: float(v) for k, v in data.items() if k in fields})


def charges(side: str, turnover: float, orders: int, rates: ChargeRates) -> Dict[str, float]:
    """Charge breakdown for `orders` executed orders totalling `turnover` rupees on one side."""
    buy = side.upper() == "BUY"
    brokerage = rates.brokerage_per_order * max(orders, 0)
    exchange = turnover * rates.exchange_txn_pct / 100.0
    sebi = turnover * rates.sebi_per_crore / 1e7
    stt = 0.0 if buy else turnover * rates.stt_sell_pct / 100.0
    stamp = turnover * rates.stamp_buy_pct / 100.0 if buy else 0.0
    gst = (brokerage + exchange + sebi) * rates.gst_pct / 100.0
    out = {"brokerage": brokerage, "exchange": exchange, "sebi": sebi, "stt": stt, "stamp": stamp, "gst": gst}
    out = {k: round(v, 2) for k, v in out.items()}
    out["total"] = round(sum(out.values()), 2)
    return out


def per_order_cost(rates: ChargeRates) -> float:
    """Extra cost of one more order for the same quantity: flat brokerage + its GST (E9)."""
    return round(rates.brokerage_per_order * (1.0 + rates.gst_pct / 100.0), 2)

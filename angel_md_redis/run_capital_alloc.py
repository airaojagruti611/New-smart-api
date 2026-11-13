"""
run_capital_alloc.py
────────────────────
Consumes md:strike:select OK signals, buffers them for CAPITAL_BATCH_WINDOW_MS,
then allocates capital jointly (score-weighted, whole lots) and emits:
  md:capital:alloc
  md:capital:alloc:latest:{SYMBOL}
"""

from __future__ import annotations

import json
import os
import time

import redis

from app.broker_funds import get_available_funds
from app.capital_alloc import (
    CapitalConfig,
    ChargeRates,
    allocate,
    candidate_from_fields,
    capital_alloc,
)
from app.config import load_symbols
from app.logging_setup import setup_logger


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

IN_STREAM = os.getenv("STREAM_STRIKE_SELECT", "md:strike:select")
OUT_STREAM = os.getenv("STREAM_CAPITAL_ALLOC", "md:capital:alloc")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_CAPITAL_ALLOC", "200000"))
LATEST_KEY_PREFIX = os.getenv("CAPITAL_ALLOC_LATEST_PREFIX", "md:capital:alloc:latest:")
REGIME_LATEST_KEY = os.getenv("REGIME_LATEST_KEY", "md:regime:latest")

GROUP = os.getenv("CAPITAL_ALLOC_GROUP", "capital-alloc")
CONSUMER = os.getenv("CAPITAL_ALLOC_CONSUMER", "capital-alloc-1")

TOTAL_CAPITAL = float(os.getenv("TOTAL_CAPITAL", "100000"))
MAX_RISK_PER_TRADE_PCT = float(os.getenv("MAX_RISK_PER_TRADE_PCT", "5"))
BATCH_WINDOW_MS = int(os.getenv("CAPITAL_BATCH_WINDOW_MS", "60000"))
POSITION_OPEN_PREFIX = os.getenv("POSITION_OPEN_PREFIX", "md:position:open:")
PAPER_OPEN_PREFIX = os.getenv("PAPER_OPEN_PREFIX", "md:paper:open:")


def _ef(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


CFG = CapitalConfig(
    reserve_pct=_ef("CAPITAL_RESERVE_PCT", 20.0),
    u_max_pct=_ef("CAPITAL_U_MAX_PCT", 80.0),
    max_loss_pct=MAX_RISK_PER_TRADE_PCT,
    sl_pct=_ef("CAPITAL_SL_PCT", 30.0),
    liq_depth_fraction=_ef("CAPITAL_LIQ_DEPTH_FRACTION", 0.25),
    max_position_pct=_ef("CAPITAL_MAX_POSITION_PCT", 25.0),
    high_vol_factor=_ef("CAPITAL_HIGH_VOL_FACTOR", 0.7),
    poor_liq_factor=_ef("CAPITAL_POOR_LIQ_FACTOR", 0.7),
    poor_liq_below=_ef("CAPITAL_POOR_LIQ_BELOW", 40.0),
    rates=ChargeRates(
        brokerage_per_order=_ef("CHG_BROKERAGE_PER_ORDER", 20.0),
        stt_sell_pct=_ef("CHG_STT_SELL_PCT", 0.15),
        exch_txn_pct=_ef("CHG_EXCH_TXN_PCT", 0.03503),
        sebi_per_crore=_ef("CHG_SEBI_PER_CRORE", 10.0),
        stamp_buy_pct=_ef("CHG_STAMP_BUY_PCT", 0.003),
        gst_pct=_ef("CHG_GST_PCT", 18.0),
    ),
)

log = setup_logger("capital_alloc")


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _load_regime(r: redis.Redis) -> dict:
    raw = r.get(REGIME_LATEST_KEY)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _open_usage(r: redis.Redis) -> float:
    """Capital tied up in open md:position:open:* and md:paper:open:* (qty * entry price)."""
    used = 0.0
    for prefix in (POSITION_OPEN_PREFIX, PAPER_OPEN_PREFIX):
        for key in r.scan_iter(match=f"{prefix}*", count=200):
            try:
                doc = json.loads(r.get(key) or "{}")
                cost = doc.get("cost") or doc.get("used_capital") or doc.get("allocated")
                if cost is None:
                    qty = float(doc.get("qty") or doc.get("quantity") or 0)
                    px = float(
                        doc.get("entry_price")
                        or doc.get("entry_premium")
                        or doc.get("entry")
                        or doc.get("avg_price")
                        or doc.get("price")
                        or 0
                    )
                    cost = qty * px
                used += max(float(cost), 0.0)
            except Exception:
                log.warning("open position unparsable key=%s", key)
    return used


def _c_available(r: redis.Redis) -> float:
    funds = get_available_funds()  # stub returns None until Broker Bridge
    if funds is not None:
        return max(funds, 0.0)
    return max(TOTAL_CAPITAL - _open_usage(r), 0.0)


def _payload(now_ms: int, fields: dict, sym: str, legacy, a, c_avail: float) -> dict:
    p = {
        "ts_ms": str(now_ms),
        "symbol": sym,
        "signal": legacy.signal,
        "status": "OK" if a is not None and a.status == "OK" else "SKIP",
        "side": legacy.side,
        "regime": legacy.regime,
        "call_alloc_pct": str(legacy.call_alloc_pct),
        "put_alloc_pct": str(legacy.put_alloc_pct),
        "side_alloc_pct": str(legacy.side_alloc_pct),
        "total_capital": str(legacy.total_capital),
        "side_budget": str(legacy.side_budget),
        "trade_notional": str(legacy.trade_notional),
        "strike": str(fields.get("strike") or ""),
        "token": str(fields.get("token") or ""),
        "tradingsymbol": str(fields.get("tradingsymbol") or ""),
        "exchange": str(fields.get("exchange") or ""),
        "mode": str(fields.get("mode") or ""),
        "strength": str(fields.get("strength") or ""),
        "spot": str(fields.get("spot") or ""),
        "expiry": str(fields.get("expiry") or ""),
        "bar_ts_ms": str(fields.get("bar_ts_ms") or now_ms),
        "available_capital": str(round(c_avail, 2)),
    }
    if a is None:
        p["reason"] = legacy.reason
        return p
    p.update(
        {
            "lots": str(a.lots),
            "qty": str(a.qty),
            "cost_per_lot": str(a.cost_per_lot),
            "est_charges": str(a.est_charges),
            "allocated": str(a.allocated),
            "deployable_budget": str(a.deployable_budget),
            "reserve": str(a.reserve),
            "binding_limit": a.binding_limit,
            "weight": str(a.weight),
            "reason": a.reason,
        }
    )
    if a.status == "OK":  # chosen contract may be a fallback from top3
        p["strike"] = str(a.strike)
        p["token"] = a.token
        p["tradingsymbol"] = a.tradingsymbol
        p["premium"] = str(a.premium)
        p["lot_size"] = str(a.lot_size)
    return p


def _flush(r: redis.Redis, batch: dict, regime_snap: dict, now_ms: int) -> None:
    """Allocate one batch (latest OK row per symbol) jointly and emit."""
    c_avail = _c_available(r)
    items = []
    for sym, fields in batch.items():
        legacy = capital_alloc(
            signal=str(fields.get("signal") or ""),
            regime_snapshot=regime_snap,
            total_capital=TOTAL_CAPITAL,
            max_risk_per_trade_pct=MAX_RISK_PER_TRADE_PCT,
        )
        cand = candidate_from_fields(fields, regime_snap) if legacy.status == "OK" else None
        items.append((sym, fields, legacy, cand))

    allocs = iter(allocate([it[3] for it in items if it[3] is not None], c_avail, CFG))
    for sym, fields, legacy, cand in items:
        a = next(allocs) if cand is not None else None
        payload = _payload(now_ms, fields, sym, legacy, a, c_avail)
        r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
        r.set(f"{LATEST_KEY_PREFIX}{sym}", json.dumps(payload, separators=(",", ":")), ex=3600)
        log.info("%s symbol=%s payload=%s", "EMIT" if payload["status"] == "OK" else "SKIP_RESULT", sym, payload)


def main():
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_STREAM, GROUP)

    log.info(
        "START reading %s, writing %s (total_capital=%s window_ms=%s symbols=%d)",
        IN_STREAM, OUT_STREAM, TOTAL_CAPITAL, BATCH_WINDOW_MS, len(symbols),
    )

    batch: dict = {}
    batch_start_ms = 0
    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={IN_STREAM: ">"},
            count=2000,
            block=2000,
        )
        now_ms = int(time.time() * 1000)
        ack_ids = []
        for _stream, msgs in resp or []:
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)
                sym = str(fields.get("symbol") or "").strip().upper()
                if not sym or sym not in symbols:
                    continue
                if str(fields.get("status") or "").strip().upper() != "OK":
                    continue
                if not batch:
                    batch_start_ms = now_ms
                batch[sym] = dict(fields, symbol=sym)  # latest row per symbol wins

        if batch and now_ms - batch_start_ms >= BATCH_WINDOW_MS:
            _flush(r, batch, _load_regime(r), now_ms)
            batch = {}
        if ack_ids:
            r.xack(IN_STREAM, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

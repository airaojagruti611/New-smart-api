"""
run_composite.py
───────────────────────
Integration Architecture — synthesis layer over all prior signal modules.
Reads live spot from md:ticks:eq, tracks underlying -> tradingsymbols from
md:ticks:opt (to look up the option-liquidity-exit override), and
cross-references cached signals from:

  md:imbalance:latest:{SYMBOL}    (run_bidask_imbalance.py)
  md:orderflow:latest:{SYMBOL}    (run_order_flow.py)          -> bias, supports, resistances
  md:smartmoney:latest:{SYMBOL}   (run_smart_money.py)         -> composite
  md:bidask:latest:{SYMBOL}       (run_bidask_analyzer.py)     -> signal
  md:strikeflow:latest:{SYMBOL}   (run_strike_flow.py)         -> status, bias
  md:optexit:latest:{TRADINGSYMBOL} (run_option_liquidity_exit.py) -> exit_status (OVERRIDE)

...and emits:

  Stream : md:composite:signal
  Key    : md:composite:latest:{SYMBOL}

Evaluated on a periodic cycle (like run_strike_flow.py) rather than per-tick,
since it's synthesizing several independently-updating cached signals rather
than reacting to a single stream.
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, List, Optional

import redis

from app.composite_score import (
    CompositeResult,
    WEIGHTS,
    classify_composite,
    compute_composite,
    imbalance_component,
    net_delta_component,
    option_flow_component,
    smart_money_component,
    spread_health_component,
    sr_proximity_component,
)
from app.config import load_symbols
from app.logging_setup import setup_logger

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")
OPT_STREAM = os.getenv("STREAM_OPT", "md:ticks:opt")

IMBALANCE_LATEST_PREFIX = os.getenv("IMBALANCE_LATEST_PREFIX", "md:imbalance:latest:")
ORDERFLOW_LATEST_PREFIX = os.getenv("ORDERFLOW_LATEST_PREFIX", "md:orderflow:latest:")
SMARTMONEY_LATEST_PREFIX = os.getenv("SMARTMONEY_LATEST_PREFIX", "md:smartmoney:latest:")
BIDASK_LATEST_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")
STRIKEFLOW_LATEST_PREFIX = os.getenv("STRIKEFLOW_LATEST_PREFIX", "md:strikeflow:latest:")
OPTEXIT_LATEST_PREFIX = os.getenv("OPTEXIT_LATEST_PREFIX", "md:optexit:latest:")

OUT_STREAM = os.getenv("STREAM_COMPOSITE_SIGNAL", "md:composite:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_COMPOSITE", "50000"))
LATEST_KEY_PREFIX = os.getenv("COMPOSITE_LATEST_PREFIX", "md:composite:latest:")

GROUP = os.getenv("COMPOSITE_GROUP", "composite")
CONSUMER = os.getenv("COMPOSITE_CONSUMER", "composite-1")

EVAL_INTERVAL_SEC = float(os.getenv("COMPOSITE_EVAL_INTERVAL_SEC", "2.0"))
LATEST_TTL_SEC = int(os.getenv("COMPOSITE_LATEST_TTL_SEC", "3600"))

log = setup_logger("composite_score")


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _safe_float(v) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _load_json(r: redis.Redis, key: str) -> Optional[dict]:
    raw = r.get(key)
    if not raw:
        return None
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)

    ensure_group(r, EQ_STREAM, GROUP)
    ensure_group(r, OPT_STREAM, GROUP)

    spot_by_sym: Dict[str, float] = {}
    contracts_by_underlying: Dict[str, set] = {}

    next_eval = time.time() + EVAL_INTERVAL_SEC

    log.info(
        "START reading %s + %s -> %s + %s{{SYMBOL}} (eval_interval=%ss symbols=%d) weights=%s",
        EQ_STREAM, OPT_STREAM, OUT_STREAM, LATEST_KEY_PREFIX, EVAL_INTERVAL_SEC, len(symbols), WEIGHTS,
    )

    while True:
        resp = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={EQ_STREAM: ">", OPT_STREAM: ">"},
            count=2000,
            block=2000,
        )

        if resp:
            for stream, msgs in resp:
                ack_ids = []
                for msg_id, fields in msgs:
                    ack_ids.append(msg_id)

                    if stream == EQ_STREAM:
                        sym = str(fields.get("symbol") or "").strip().upper()
                        if not sym or sym not in symbols:
                            continue
                        ltp = _safe_float(fields.get("ltp"))
                        if ltp:
                            spot_by_sym[sym] = ltp
                        continue

                    und = str(fields.get("underlying") or "").strip().upper()
                    tsym = str(fields.get("tradingsymbol") or "").strip().upper()
                    if not und or not tsym or und not in symbols:
                        continue
                    contracts_by_underlying.setdefault(und, set()).add(tsym)

                if ack_ids:
                    r.xack(stream, GROUP, *ack_ids)

        now = time.time()
        if now < next_eval:
            continue
        next_eval = now + EVAL_INTERVAL_SEC
        now_ms = int(now * 1000)

        for sym in symbols:
            spot = spot_by_sym.get(sym)

            imb_doc = _load_json(r, f"{IMBALANCE_LATEST_PREFIX}{sym}") or {}
            of_doc = _load_json(r, f"{ORDERFLOW_LATEST_PREFIX}{sym}") or {}
            sm_doc = _load_json(r, f"{SMARTMONEY_LATEST_PREFIX}{sym}") or {}
            ba_doc = _load_json(r, f"{BIDASK_LATEST_PREFIX}{sym}") or {}
            sf_doc = _load_json(r, f"{STRIKEFLOW_LATEST_PREFIX}{sym}") or {}

            if not any((imb_doc, of_doc, sm_doc, ba_doc, sf_doc)):
                continue  # nothing upstream has produced data for this symbol yet

            try:
                supports = json.loads(of_doc.get("supports") or "[]")
                resistances = json.loads(of_doc.get("resistances") or "[]")
            except Exception:
                supports, resistances = [], []

            components = {
                "imbalance": imbalance_component(_safe_float(imb_doc.get("final_score"))),
                "net_delta": net_delta_component(of_doc.get("bias")),
                "smart_money": smart_money_component(sm_doc.get("composite")),
                "spread_health": spread_health_component(ba_doc.get("signal")),
                "sr_proximity": sr_proximity_component(spot, supports, resistances),
                "option_flow": option_flow_component(sf_doc.get("status"), sf_doc.get("bias")),
            }
            score = compute_composite(components)

            # Option-liquidity-exit override: liquidity loss always wins,
            # regardless of composite score.
            override_exit = False
            override_reason = ""
            for tsym in contracts_by_underlying.get(sym, ()):
                oe_doc = _load_json(r, f"{OPTEXIT_LATEST_PREFIX}{tsym}")
                if not oe_doc:
                    continue
                status = str(oe_doc.get("exit_status") or "")
                if status in ("EXIT_NOW", "ALREADY_TRAPPED"):
                    override_exit = True
                    override_reason = f"{tsym}:{status}"
                    break

            status, reason = classify_composite(score, override_exit, override_reason)

            log.debug(
                "LOGIC symbol=%s components=%s score=%.4f override=%s status=%s reason=%s",
                sym, components, score, override_exit, status, reason,
            )

            payload = {
                "ts_ms": str(now_ms),
                "symbol": sym,
                "score": f"{score:.4f}",
                "status": status,
                "reason": reason,
                "component_imbalance": f"{components['imbalance']:.4f}",
                "component_net_delta": f"{components['net_delta']:.4f}",
                "component_smart_money": f"{components['smart_money']:.4f}",
                "component_spread_health": f"{components['spread_health']:.4f}",
                "component_sr_proximity": f"{components['sr_proximity']:.4f}",
                "component_option_flow": f"{components['option_flow']:.4f}",
                "override_exit": "1" if override_exit else "0",
            }
            r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
            r.set(
                f"{LATEST_KEY_PREFIX}{sym}",
                json.dumps(payload, separators=(",", ":")),
                ex=LATEST_TTL_SEC,
            )

            if status != "NEUTRAL":
                log.info("EMIT symbol=%s status=%s score=%.4f payload=%s", sym, status, score, payload)


if __name__ == "__main__":
    main()

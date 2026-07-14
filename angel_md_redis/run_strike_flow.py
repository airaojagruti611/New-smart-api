"""
run_strike_flow.py
───────────────────────
Order Flow for Strike Price Selection — synthesis layer over three other
signal engines. Maintains live per-underlying option-chain state from
md:ticks:opt + spot from md:ticks:eq, cross-references cached signals from:

  md:orderflow:latest:{SYMBOL}         (run_order_flow.py)   -> stock bias
  md:orderflow:latest:{TRADINGSYMBOL}  (run_order_flow.py)   -> refresh_events
  md:smartmoney:latest:{TRADINGSYMBOL} (run_smart_money.py)  -> sweep signal
  md:bidask:latest:{TRADINGSYMBOL}     (run_bidask_analyzer.py) -> spread_ratio

...and emits:

  Stream : md:strikeflow:signal
  Key    : md:strikeflow:latest:{SYMBOL}

DEPENDENCY: STRIKES_AROUND (app/config.py) must be >= 1 for OTM candidates
to exist at all — with the default of 0, ws_producer.py only subscribes the
ATM strike per underlying, so the OTM1-on-strong-Vol/OI rule never has data
to act on.

ASSUMPTION: "only one side refreshed" is judged from accumulated refresh
history across evaluation cycles (bid vs ask ever seen refreshing in
md:orderflow:latest:{TRADINGSYMBOL}.refresh_events), not a single snapshot —
refresh_events itself is a per-tick snapshot with no persistent memory, so
that memory is kept here.
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, List, Optional

import redis

from app.config import load_symbols
from app.logging_setup import setup_logger
from app.strike_flow import (
    PutCallRatioTracker,
    StrikeCandidate,
    classify_vol_oi,
    moneyness,
    select_strike,
    vol_oi_ratio,
)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

EQ_STREAM = os.getenv("STREAM_EQ", "md:ticks:eq")
OPT_STREAM = os.getenv("STREAM_OPT", "md:ticks:opt")

ORDERFLOW_LATEST_PREFIX = os.getenv("ORDERFLOW_LATEST_PREFIX", "md:orderflow:latest:")
SMARTMONEY_LATEST_PREFIX = os.getenv("SMARTMONEY_LATEST_PREFIX", "md:smartmoney:latest:")
BIDASK_LATEST_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")

OUT_STREAM = os.getenv("STREAM_STRIKEFLOW_SIGNAL", "md:strikeflow:signal")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_STRIKEFLOW", "50000"))
LATEST_KEY_PREFIX = os.getenv("STRIKEFLOW_LATEST_PREFIX", "md:strikeflow:latest:")

GROUP = os.getenv("STRIKEFLOW_GROUP", "strikeflow")
CONSUMER = os.getenv("STRIKEFLOW_CONSUMER", "strikeflow-1")

EVAL_INTERVAL_SEC = float(os.getenv("STRIKEFLOW_EVAL_INTERVAL_SEC", "3.0"))
REFRESH_MIN_OBSERVATIONS = int(os.getenv("STRIKEFLOW_REFRESH_MIN_OBS", "3"))
LATEST_TTL_SEC = int(os.getenv("STRIKEFLOW_LATEST_TTL_SEC", "3600"))

log = setup_logger("strike_flow")


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


class ContractState:
    __slots__ = ("strike", "cp", "vol", "oi", "bid", "ask")

    def __init__(self):
        self.strike: float = 0.0
        self.cp: str = ""
        self.vol: float = 0.0
        self.oi: float = 0.0
        self.bid: float = 0.0
        self.ask: float = 0.0


class RefreshMemory:
    """Persistent (across evaluations) record of which side(s) of the book
    have ever been observed refreshing for a given contract."""

    def __init__(self):
        self.bid_seen = False
        self.ask_seen = False
        self.observations = 0

    def observe(self, refresh_events: List[dict]) -> None:
        self.observations += 1
        for ev in refresh_events or []:
            side = str(ev.get("side") or "").upper()
            if side == "BID":
                self.bid_seen = True
            elif side == "ASK":
                self.ask_seen = True

    @property
    def one_sided(self) -> bool:
        if self.observations < REFRESH_MIN_OBSERVATIONS:
            return False  # not enough history to judge yet
        return self.bid_seen != self.ask_seen  # exactly one side ever seen


def main() -> None:
    symbols = set(load_symbols())
    r = redis.from_url(REDIS_URL, decode_responses=True)

    ensure_group(r, EQ_STREAM, GROUP)
    ensure_group(r, OPT_STREAM, GROUP)

    spot_by_sym: Dict[str, float] = {}
    contracts_by_underlying: Dict[str, Dict[str, ContractState]] = {}
    pcr_tracker: Dict[str, PutCallRatioTracker] = {}
    refresh_memory: Dict[str, RefreshMemory] = {}

    next_eval = time.time() + EVAL_INTERVAL_SEC

    log.info(
        "START reading %s + %s -> %s + %s{{SYMBOL}} (eval_interval=%ss symbols=%d)",
        EQ_STREAM, OPT_STREAM, OUT_STREAM, LATEST_KEY_PREFIX, EVAL_INTERVAL_SEC, len(symbols),
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

                    # opt tick
                    und = str(fields.get("underlying") or "").strip().upper()
                    tsym = str(fields.get("tradingsymbol") or "").strip().upper()
                    if not und or not tsym or und not in symbols:
                        continue

                    strike = _safe_float(fields.get("strike")) or 0.0
                    cp = str(fields.get("cp") or "").strip().upper()
                    vol = _safe_float(fields.get("vol")) or 0.0
                    oi = _safe_float(fields.get("oi")) or 0.0
                    bid = _safe_float(fields.get("bid")) or 0.0
                    ask = _safe_float(fields.get("ask")) or 0.0

                    book = contracts_by_underlying.setdefault(und, {})
                    st = book.setdefault(tsym, ContractState())
                    st.strike, st.cp, st.vol, st.oi = strike, cp, vol, oi
                    st.bid, st.ask = bid, ask

                if ack_ids:
                    r.xack(stream, GROUP, *ack_ids)

        now = time.time()
        if now < next_eval:
            continue
        next_eval = now + EVAL_INTERVAL_SEC
        now_ms = int(now * 1000)

        for und, book in contracts_by_underlying.items():
            spot = spot_by_sym.get(und)
            if not spot or not book:
                continue

            strikes = sorted({st.strike for st in book.values() if st.strike > 0})
            if not strikes:
                continue
            atm = min(strikes, key=lambda s: abs(s - spot))
            step = 0.0
            if len(strikes) >= 2:
                diffs = [round(strikes[i + 1] - strikes[i], 4) for i in range(len(strikes) - 1)]
                diffs = [d for d in diffs if d > 0]
                step = min(diffs) if diffs else 0.0

            call_vol = sum(st.vol for st in book.values() if st.cp == "CE")
            put_vol = sum(st.vol for st in book.values() if st.cp == "PE")
            pcr = pcr_tracker.setdefault(und, PutCallRatioTracker())
            pcr.update(call_vol, put_vol)

            bias_doc = _load_json(r, f"{ORDERFLOW_LATEST_PREFIX}{und}")
            bias = str((bias_doc or {}).get("bias") or "NEUTRAL")

            candidates: List[StrikeCandidate] = []
            for tsym, st in book.items():
                if st.strike <= 0 or not st.cp:
                    continue

                voi = vol_oi_ratio(st.vol, st.oi)
                voi_class = classify_vol_oi(voi)

                sm_doc = _load_json(r, f"{SMARTMONEY_LATEST_PREFIX}{tsym}") or {}
                sweep_signal = str(sm_doc.get("sweep_signal") or "NONE")
                sweep_confirmed = str(sm_doc.get("sweep_confirmed") or "0") == "1"

                ba_doc = _load_json(r, f"{BIDASK_LATEST_PREFIX}{tsym}") or {}
                spread_pct = _safe_float(ba_doc.get("spread_pct")) or 0.0
                spread_ratio = _safe_float(ba_doc.get("spread_ratio"))

                of_doc = _load_json(r, f"{ORDERFLOW_LATEST_PREFIX}{tsym}") or {}
                refresh_events = []
                try:
                    refresh_events = json.loads(of_doc.get("refresh_events") or "[]")
                except Exception:
                    pass
                mem = refresh_memory.setdefault(tsym, RefreshMemory())
                mem.observe(refresh_events)

                candidates.append(StrikeCandidate(
                    tradingsymbol=tsym,
                    strike=st.strike,
                    cp=st.cp,
                    moneyness=moneyness(st.strike, atm, step, st.cp),
                    vol=st.vol,
                    oi=st.oi,
                    vol_oi_ratio=voi,
                    vol_oi_class=voi_class,
                    sweep_signal=sweep_signal,
                    sweep_confirmed=sweep_confirmed,
                    spread_pct=spread_pct,
                    spread_ratio=spread_ratio,
                    one_sided_refresh=mem.one_sided,
                    bid=st.bid,
                    ask=st.ask,
                ))

            selection = select_strike(candidates, bias, underlying=und)

            log.debug(
                "LOGIC underlying=%s spot=%s atm=%s step=%s bias=%s pcr=%s pcr_dropping=%s "
                "status=%s reason=%s chosen=%s",
                und, spot, atm, step, bias, round(put_vol / call_vol, 3) if call_vol else None,
                pcr.dropping_sharply, selection.status, selection.reason,
                selection.chosen.tradingsymbol if selection.chosen else None,
            )

            payload = {
                "ts_ms": str(now_ms),
                "underlying": und,
                "spot": f"{spot:.2f}",
                "atm": f"{atm:.2f}",
                "step": f"{step:.2f}",
                "bias": bias,
                "pcr_dropping_sharply": "1" if pcr.dropping_sharply else "0",
                "status": selection.status,
                "reason": selection.reason,
                "chosen_tradingsymbol": selection.chosen.tradingsymbol if selection.chosen else "",
                "chosen_strike": str(selection.chosen.strike) if selection.chosen else "",
                "chosen_cp": selection.chosen.cp if selection.chosen else "",
                "chosen_moneyness": selection.chosen.moneyness if selection.chosen else "",
                "chosen_vol_oi_ratio": str(selection.chosen.vol_oi_ratio) if selection.chosen else "",
                "chosen_spread_pct": str(selection.chosen.spread_pct) if selection.chosen else "",
                "chosen_bid": str(selection.chosen.bid) if selection.chosen else "",
                "chosen_ask": str(selection.chosen.ask) if selection.chosen else "",
                "rejected": json.dumps(selection.rejected, separators=(",", ":")),
            }
            r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
            r.set(
                f"{LATEST_KEY_PREFIX}{und}",
                json.dumps(payload, separators=(",", ":")),
                ex=LATEST_TTL_SEC,
            )

            if selection.status == "OK":
                log.info("EMIT underlying=%s payload=%s", und, payload)


if __name__ == "__main__":
    main()

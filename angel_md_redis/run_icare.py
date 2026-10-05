"""
run_icare.py
────────────
Modules 11 + 20 — ICARE Redis wiring (DECISION.md D9).

Consumes md:probability (which carries the SIE strike context) — or, with
RANK_MODE=active, md:ranking TAKE_TRADE emissions (rank_emit=1; the ranking
payload is the probability payload + rank_* fields, DECISION.md §6 R16) — and
md:tsl:reentry (Module 18 re-entry signals, same message shape), and reads:
  md:account:latest                  AccountSnapshot (run_account.py);
                                     missing / stale -> paper snapshot, flagged
                                     ACCOUNT_FALLBACK_PAPER, whose realized PnL is
                                     the WORSE of md:journal:daily:{IST date}.realized_pnl
                                     and a same-day stale snapshot's realized_pnl.
                                     Neither known: EXEC_MODE paper/shadow -> 0 (the
                                     journal is the ledger: no key = no close today);
                                     EXEC_MODE=live -> DAILY_PNL_UNKNOWN, every new
                                     trade rejected (daily_loss_unknown, fail closed)
  md:control:kill_switch             "1" -> every trade rejected (kill_switch_active)
  md:position:open:*                 open positions (sector exposure, symbols)
  md:liquidity:score:latest:{TSYM}   final_entry_size (lots) -> liquidity limit
  sectors.json                       optional underlying -> sector map
  md:tsl:block:{SYM}:{SIDE}          Module 18 block after two failed re-entries
  md:fo:ban                          NSE F&O ban-period stocks (run_fo_universe.py) -> fo_ban_period
  md:exec:active                     trades the Order Executor is still entering
                                     (EXEC_MODE=paper|live): count as open exposure

Freshness: a consumed message older than ICARE_MAX_SIGNAL_AGE_SEC (default 60;
0 disables) by its stream id or its `signal_ts_ms` (whichever is EARLIER) is
ACKed and skipped (SKIP stale_signal). Every md:icare message carries
`signal_ts_ms` = that origin time; `ts_ms` stays the ICARE publish time.

Emits the final execution report (no orders are placed anywhere):
  Stream : md:icare
  Key    : md:icare:latest:{SYMBOL}
  Key    : md:icare:origin:{TSYM}   (APPROVED only) the probability input, so
                                    Module 18 can re-score the contract for a re-entry
"""

from __future__ import annotations

import datetime as dt
import json
import math
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

import redis

from app.broker_account import paper_snapshot
from app.config import load_symbols
from app.freshness import env_ms
from app.icare import ICAREConfig, ICAREInputs, ICAREResult, PortfolioState, evaluate
from app.logging_setup import setup_logger
from app.option_pricing import IST
from app.probability_engine import signal_is_stale, signal_origin_ms

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

RANK_MODE = os.getenv("RANK_MODE", "shadow").strip().lower()
IN_STREAM = (
    os.getenv("STREAM_RANKING", "md:ranking") if RANK_MODE == "active"
    else os.getenv("STREAM_PROBABILITY", "md:probability")
)
REENTRY_STREAM = os.getenv("STREAM_TSL_REENTRY", "md:tsl:reentry")
OUT_RANKING_STREAM = os.getenv("STREAM_RANKING", "md:ranking")
ORIGIN_PREFIX = os.getenv("ICARE_ORIGIN_PREFIX", "md:icare:origin:")
ORIGIN_TTL_SEC = int(os.getenv("ICARE_ORIGIN_TTL_SEC", str(3 * 86400)))
BLOCK_PREFIX = os.getenv("TSL_BLOCK_PREFIX", "md:tsl:block:")
FO_BAN_KEY = os.getenv("FO_BAN_KEY", "md:fo:ban")
TSL_MODE = os.getenv("TSL_MODE", "shadow").strip().lower()
OUT_STREAM = os.getenv("STREAM_ICARE", "md:icare")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_ICARE", "200000"))
LATEST_KEY_PREFIX = os.getenv("ICARE_LATEST_PREFIX", "md:icare:latest:")

ACCOUNT_KEY = os.getenv("ACCOUNT_LATEST_KEY", "md:account:latest")
POSITION_OPEN_PREFIX = os.getenv("POSITION_OPEN_PREFIX", "md:position:open:")
LIQUIDITY_PREFIX = os.getenv("LIQUIDITY_SCORE_LATEST_PREFIX", "md:liquidity:score:latest:")
SECTORS_FILE = Path(os.getenv("SECTORS_FILE", str(Path(__file__).resolve().parent / "sectors.json")))
ACCOUNT_MAX_AGE_MS = int(os.getenv("ICARE_ACCOUNT_MAX_AGE_MS", "120000"))
TOTAL_CAPITAL = float(os.getenv("TOTAL_CAPITAL", "100000"))
EXEC_MODE = os.getenv("EXEC_MODE", "shadow").strip().lower()
EXEC_ACTIVE_KEY = os.getenv("EXEC_ACTIVE_KEY", "md:exec:active")
JOURNAL_DAILY_PREFIX = os.getenv("JOURNAL_DAILY_PREFIX", "md:journal:daily:")
KILL_SWITCH_KEY = os.getenv("KILL_SWITCH_KEY", "md:control:kill_switch")
MAX_SIGNAL_AGE_MS = env_ms("ICARE_MAX_SIGNAL_AGE_SEC", 60)

GROUP = os.getenv("ICARE_GROUP", "icare")
CONSUMER = os.getenv("ICARE_CONSUMER", "icare-1")

CFG = ICAREConfig(
    max_risk_per_trade=float(os.getenv("ICARE_MAX_RISK_PER_TRADE", "7500")),
    max_risk_pct=float(os.getenv("ICARE_MAX_RISK_PCT", "2.0")),
    max_lots_per_trade=int(os.getenv("ICARE_MAX_LOTS_PER_TRADE", "10")),
    max_open_trades=int(os.getenv("ICARE_MAX_OPEN_TRADES", "5")),
    max_margin_util_pct=float(os.getenv("ICARE_MAX_MARGIN_UTIL_PCT", "80")),
    daily_loss_limit_pct=float(os.getenv("ICARE_DAILY_LOSS_LIMIT_PCT", "2.0")),
    max_portfolio_risk_pct=float(os.getenv("ICARE_MAX_PORTFOLIO_RISK_PCT", "5.0")),
    max_sector_exposure_pct=float(os.getenv("ICARE_MAX_SECTOR_EXPOSURE_PCT", "30")),
    min_sl_pct=float(os.getenv("ICARE_MIN_SL_PCT", "10")),
    max_sl_pct=float(os.getenv("ICARE_MAX_SL_PCT", "30")),
    margin_buffer_pct=float(os.getenv("ICARE_MARGIN_BUFFER_PCT", "1.0")),
    min_ev=float(os.getenv("ICARE_MIN_EV", "0")),
    min_history_samples=int(os.getenv("PROB_MIN_HISTORICAL_SAMPLES", "30")),
)

log = setup_logger("icare")


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _f(v) -> Optional[float]:
    """float, or None when missing / unparseable / NaN / inf."""
    try:
        if v is None or v == "":
            return None
        x = float(v)
    except Exception:
        return None
    return x if math.isfinite(x) else None


def _load_json(r: redis.Redis, key: str) -> dict:
    raw = r.get(key)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def load_sectors(path: Path = SECTORS_FILE) -> Dict[str, str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return {str(k).upper(): str(v) for k, v in data.items() if not str(k).startswith("_")}


def load_positions(r: redis.Redis, exec_mode: str = EXEC_MODE) -> List[dict]:
    out = []
    for key in r.scan_iter(match=f"{POSITION_OPEN_PREFIX}*", count=500):
        doc = _load_json(r, key)
        if doc:
            out.append(doc)
    if exec_mode in ("paper", "live"):
        # DECISION.md §7 E4: an approval still being executed is not a position yet,
        # but it must already block the same underlying and use an open-trade slot.
        for tid, sym in (r.hgetall(EXEC_ACTIVE_KEY) or {}).items():
            out.append({"symbol": sym, "trade_id": tid, "executing": 1, "entry_premium": 0, "qty": 0})
    return out


def ist_date(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000.0, IST).date().isoformat()


def load_journal_daily(r: redis.Redis, now_ms: int) -> Optional[dict]:
    """md:journal:daily:{IST date} (run_trade_journal / run_account), or None when absent."""
    raw = r.get(f"{JOURNAL_DAILY_PREFIX}{ist_date(now_ms)}")
    if not raw:
        return None
    try:
        doc = json.loads(raw)
    except Exception:
        return None
    return doc if isinstance(doc, dict) else None


def fallback_realized_pnl(
    account: dict, journal_daily: Optional[dict], now_ms: int, exec_mode: str = EXEC_MODE,
) -> tuple[Optional[float], str]:
    """
    Today's realized PnL when md:account:latest is missing / stale:
    the WORSE (lower) of the journal daily record and a same-IST-day stale
    snapshot's realized_pnl. Neither known: 0 in paper / shadow mode (the
    journal is the ledger and writes the key on the first close of the day),
    None in live mode (broker PnL unknown -> the caller fails closed).
    """
    found = []
    if journal_daily is not None:
        v = _f(journal_daily.get("realized_pnl"))
        if v is not None:
            found.append((v, "journal"))
    ts = int(_f((account or {}).get("ts_ms")) or 0)
    if account and ts > 0 and ist_date(ts) == ist_date(now_ms):
        v = _f(account.get("realized_pnl"))
        if v is not None:
            found.append((v, "stale_account"))
    if found:
        return min(found)
    if exec_mode == "live":
        return None, "unknown"
    return 0.0, "paper_no_close_today"


def portfolio_state(
    account: dict, positions: List[dict], sectors: Dict[str, str], now_ms: int,
    journal_daily: Optional[dict] = None, exec_mode: str = EXEC_MODE, kill_switch: bool = False,
) -> tuple[PortfolioState, List[str]]:
    flags: List[str] = []
    known = True
    ts = int(_f(account.get("ts_ms")) or 0) if account else 0
    if not account or now_ms - ts > ACCOUNT_MAX_AGE_MS:
        flags.append("ACCOUNT_FALLBACK_PAPER")
        realized, source = fallback_realized_pnl(account, journal_daily, now_ms, exec_mode)
        if realized is None:
            known = False
            flags.append("DAILY_PNL_UNKNOWN")
        else:
            flags.append(f"REALIZED_PNL_FROM_{source.upper()}")
        account = paper_snapshot(TOTAL_CAPITAL, positions, realized or 0.0, now_ms).to_dict()
    if kill_switch:
        flags.append("KILL_SWITCH")
    exposure: Dict[str, float] = {}
    for p in positions:
        sec = sectors.get(str(p.get("symbol") or "").upper())
        if sec:
            exposure[sec] = exposure.get(sec, 0.0) + (_f(p.get("entry_premium")) or 0.0) * (_f(p.get("qty")) or 0.0)
    return PortfolioState(
        total_capital=_f(account.get("total_capital")) or 0.0,
        available_margin=_f(account.get("available_margin")) or 0.0,
        margin_utilization_pct=_f(account.get("margin_utilization_pct")) or 0.0,
        day_pnl=_f(account.get("day_pnl")) or 0.0,
        open_positions=int(_f(account.get("open_positions")) or 0) + sum(1 for p in positions if p.get("executing")),
        open_risk=_f(account.get("open_risk")) or 0.0,
        sector_exposure=exposure,
        open_symbols=frozenset(str(p.get("symbol") or "").upper() for p in positions),
        day_pnl_known=known,
        kill_switch=kill_switch,
    ), flags


def kill_switch_on(r: redis.Redis) -> bool:
    """md:control:kill_switch == "1" (same format as run_order_executor / run_trade_ranking)."""
    return str(r.get(KILL_SWITCH_KEY) or "").strip() == "1"


def load_portfolio(r: redis.Redis, sectors: Dict[str, str], now_ms: int) -> tuple[PortfolioState, List[str], List[dict]]:
    positions = load_positions(r)
    pf, flags = portfolio_state(_load_json(r, ACCOUNT_KEY), positions, sectors, now_ms,
                                journal_daily=load_journal_daily(r, now_ms), kill_switch=kill_switch_on(r))
    return pf, flags, positions


def liquidity_max_lots(liq: dict) -> Optional[float]:
    if str(liq.get("size_unit") or "").lower() != "lots":
        return None
    return _f(liq.get("final_entry_size"))


def is_reentry(prob: dict) -> bool:
    return str(prob.get("reentry") or "") == "1"


def reentry_shadow(prob: dict, mode: str = TSL_MODE) -> bool:
    """A re-entry trades only when both the engine and ICARE run TSL_MODE=active."""
    return is_reentry(prob) and not (mode == "active" and str(prob.get("tsl_mode") or "") == "active")


def accept_message(stream: str, fields: dict) -> bool:
    """md:ranking carries every decision change; ICARE sizes only the TAKE_TRADE emissions."""
    if stream == OUT_RANKING_STREAM:
        return str(fields.get("rank_emit") or "") == "1" and str(fields.get("rank_decision") or "") == "TAKE_TRADE"
    return True


def build_inputs(
    prob: dict, liq: dict, sectors: Dict[str, str], blocked: bool = False, fo_banned: bool = False,
) -> ICAREInputs:
    sym = str(prob.get("symbol") or "").upper()
    em_conflict = str(prob.get("em_conflict") or "") == "1"
    em_conf = _f(prob.get("em_confidence"))
    try:
        prob_rejects = json.loads(prob.get("reject_reasons") or "[]")
    except Exception:
        prob_rejects = []
    return ICAREInputs(
        symbol=sym,
        side=str(prob.get("side") or ""),
        tradingsymbol=str(prob.get("tradingsymbol") or ""),
        strike=_f(prob.get("strike")) or 0.0,
        premium=_f(prob.get("premium")),
        lot_size=_f(prob.get("lot_size")),
        probability=_f(prob.get("probability")),
        probability_decision=str(prob.get("decision") or ""),
        probability_reject_reasons=prob_rejects,
        trend=_f(prob.get("p_confluence")),
        expected_move_score=0.0 if em_conflict else em_conf,
        strike_score=_f(prob.get("strike_score")),
        liquidity=_f(prob.get("liquidity_score")),
        greeks=_f(prob.get("greeks_score")),
        bidask=_f(prob.get("execution_quality")),
        projected_gain=_f(prob.get("projected_premium_gain")),
        adverse_change=_f(prob.get("projected_premium_change_adverse")),
        history_samples=int(_f(prob.get("history_samples")) or 0),
        history_win_rate=_f(prob.get("history_win_rate")),
        history_avg_win_pct=_f(prob.get("history_avg_win_pct")),
        history_avg_loss_pct=_f(prob.get("history_avg_loss_pct")),
        liquidity_max_lots=liquidity_max_lots(liq),
        sector=sectors.get(sym, ""),
        reentry_max_lots=_f(prob.get("max_lots")) if is_reentry(prob) else None,   # "0" caps at 0 lots
        reentry_shadow=reentry_shadow(prob),
        blocked=blocked,
        fo_banned=fo_banned,
        rank_conditional=str(prob.get("rank_confidence") or "") == "CONDITIONAL",
    )


def build_payload(
    res: ICAREResult, prob: dict, pf: PortfolioState, flags: List[str], now_ms: int,
    signal_ts_ms: Optional[int] = None,
) -> Dict[str, str]:
    """`ts_ms` = ICARE publish time; `signal_ts_ms` = origin of the consumed signal (signal_origin_ms)."""
    d = res.to_dict()
    payload: Dict[str, str] = {"ts_ms": str(now_ms)}
    if signal_ts_ms is None:
        signal_ts_ms = signal_origin_ms(None, prob)
    payload["signal_ts_ms"] = "" if signal_ts_ms is None else str(int(signal_ts_ms))
    for k, v in d.items():
        if isinstance(v, (list, dict)):
            payload[k] = json.dumps(v, separators=(",", ":"))
        else:
            payload[k] = "" if v is None else str(v)
    payload["flags"] = json.dumps(res.flags + flags)
    payload["execution_status"] = res.status
    payload["available_margin"] = str(pf.available_margin)
    payload["total_capital"] = str(pf.total_capital)
    for k in ("probability", "grade", "decision", "spot", "expiry", "token", "exchange", "market_phase",
              "expected_move", "em_direction", "delta", "theta_per_day", "gamma", "iv", "liquidity_score",
              "spread_pct", "strike_score", "hold_minutes", "entry_signal", "entry_bar_ts_ms",
              "reentry", "chain_id", "parent_trade_id", "reentry_no",
              "trade_score", "rank", "rank_decision", "rank_confidence", "rank_mode", "rank_components"):
        payload.setdefault(k, str(prob.get(k) or ""))
    payload["risk_taken"] = payload["expected_max_loss"]
    return payload


def handle_message(
    r: redis.Redis, stream: str, msg_id, fields: dict, symbols: set, sectors: Dict[str, str], now_ms: int,
    max_signal_age_ms: int = MAX_SIGNAL_AGE_MS,
) -> Optional[Dict[str, str]]:
    """Size one md:probability / md:ranking / md:tsl:reentry message; None when skipped (caller ACKs)."""
    sym = str(fields.get("symbol") or "").strip().upper()
    if not sym or sym not in symbols or not accept_message(stream, fields):
        return None
    origin = signal_origin_ms(msg_id, fields)
    if signal_is_stale(origin, now_ms, max_signal_age_ms):
        log.info("SKIP stale_signal stream=%s id=%s symbol=%s tsym=%s age_ms=%s max_ms=%s", stream, msg_id, sym,
                 fields.get("tradingsymbol"), None if origin is None else now_ms - origin, max_signal_age_ms)
        return None

    pf, flags, _positions = load_portfolio(r, sectors, now_ms)
    tsym = str(fields.get("tradingsymbol") or "")
    blocked = bool(r.exists(f"{BLOCK_PREFIX}{sym}:{str(fields.get('side') or '').upper()}"))
    fo_banned = bool(r.sismember(FO_BAN_KEY, sym))
    inp = build_inputs(fields, _load_json(r, f"{LIQUIDITY_PREFIX}{tsym}"), sectors, blocked, fo_banned)
    log.debug("LOGIC_IN symbol=%s inputs=%s portfolio=%s", sym, inp, pf)

    res = evaluate(inp, pf, CFG)
    payload = build_payload(res, fields, pf, flags, now_ms, signal_ts_ms=origin)
    log.info(
        "LOGIC reentry=%s symbol=%s tsym=%s status=%s quality=%s class=%s ev=%s(%s gross=%s charges=%s) lots=%s "
        "[margin=%s risk=%s capital=%s portfolio=%s liq=%s sector=%s daily=%s] limit=%s reasons=%s flags=%s "
        "signal_age_ms=%s",
        fields.get("reentry_no") if is_reentry(fields) else "0",
        sym, tsym, res.status, res.trade_quality, res.risk_class, res.expected_value, res.ev_source,
        res.gross_ev, res.charges, res.recommended_lots, res.lots_by_margin, res.lots_by_risk, res.lots_by_capital,
        res.lots_by_portfolio, res.lots_by_liquidity, res.lots_by_sector, res.lots_by_daily_loss,
        res.limiting_factor, res.reasons, res.flags + flags, None if origin is None else now_ms - origin,
    )
    if res.status == "APPROVED":
        # Written before the md:icare message, so the journal never opens a
        # position whose origin Module 18 cannot find. signal_ts_ms is dropped:
        # a later re-entry is a NEW signal and must not inherit this origin time.
        origin_doc = {k: v for k, v in fields.items() if k != "signal_ts_ms"}
        r.set(f"{ORIGIN_PREFIX}{tsym}", json.dumps(origin_doc, separators=(",", ":")), ex=ORIGIN_TTL_SEC)
    r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
    r.set(f"{LATEST_KEY_PREFIX}{sym}", json.dumps(payload, separators=(",", ":")), ex=3600)
    return payload


def main():
    symbols = set(load_symbols())
    sectors = load_sectors()
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_STREAM, GROUP)
    ensure_group(r, REENTRY_STREAM, GROUP)
    log.info("START reading %s + %s, writing %s (cfg=%s sectors=%d symbols=%d tsl_mode=%s rank_mode=%s "
             "max_signal_age_ms=%s)", IN_STREAM, REENTRY_STREAM, OUT_STREAM, CFG, len(sectors), len(symbols),
             TSL_MODE, RANK_MODE, MAX_SIGNAL_AGE_MS)

    while True:
        resp = r.xreadgroup(groupname=GROUP, consumername=CONSUMER,
                            streams={IN_STREAM: ">", REENTRY_STREAM: ">"}, count=500, block=2000)
        if not resp:
            continue

        for stream, msgs in resp:
            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)
                handle_message(r, stream, msg_id, fields, symbols, sectors, int(time.time() * 1000))
            if ack_ids:
                r.xack(stream, GROUP, *ack_ids)


if __name__ == "__main__":
    main()

"""
run_trade_ranking.py
────────────────────
Module 13 — Trade Ranking Engine, Redis wiring (DECISION.md §6). Pure logic
lives in app/trade_ranking/.

Consumes md:probability. A message older than RANK_MAX_SIGNAL_AGE_SEC (default
60; 0 disables) by its stream id or its `signal_ts_ms` (the EARLIER) is ACKed
and skipped (SKIP stale_signal). Every other candidate without a Probability
hard-filter rejection enters the candidate book md:ranking:book (field SYM:SIDE,
latest wins, expires RANK_CANDIDATE_TTL_SEC after its SIGNAL time — not its
arrival — so a replayed backlog cannot linger). The forwarded payload carries
`signal_ts_ms` = that origin time. A rank cycle runs on new candidates and
every RANK_CYCLE_SEC:

  1. re-read live inputs per candidate (None when missing or stale):
       md:indicator:score:latest:{SYM}  md:volume:latest  md:regime:latest
       md:imbalance:latest:{TSYM}  md:bidask:latest:{TSYM}  md:liquidity:score:latest:{TSYM}
       md:oi:underlying:latest:{SYM}  md:expected_move:latest:{SYM}  md:greeks:phase:latest:{TSYM}
       md:htf:trend:latest:{SYM}  md:supertrend:bias:latest:{SYM}
     + Module 18's TSL proposal on a snapshot of the contract (run_adaptive_tsl.build_snapshot)
  2. account / portfolio (md:account:latest, md:position:open:*, sectors.json — same as ICARE),
     Module 18 blocks md:tsl:block:*, kill switch md:control:kill_switch
  3. TradeRankingEngine.rank() -> decisions, ranks, correlation / slot filter, NO_TRADE
  4. publish; TAKE_TRADE candidates are emitted ONCE, in rank order, after the
     RANK_BATCH_WINDOW_SEC of the newest TAKE candidate has passed

Emits:
  Stream : md:ranking                  first evaluation, decision changes, TAKE_TRADE emissions
                                       (rank_emit=1). Probability payload forwarded + rank_* fields
                                       + ranking_json (the brief's output object)
  Key    : md:ranking:latest:{SYM}:{SIDE}
  ZSet   : md:ranking:rank             SYM:SIDE -> trade_score (Module 13 view)
  Stream : md:ranking:cycle            cycle summary (scanned / rejected / ... / NO_TRADE),
                                       on change + every RANK_CYCLE_HEARTBEAT_SEC
  Key    : md:ranking:cycle:latest

RANK_MODE=shadow (default): ICARE keeps consuming md:probability. RANK_MODE=active:
ICARE consumes md:ranking (rank_emit=1 only).
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, List, Optional, Tuple

import redis

from app.config import load_symbols
from app.freshness import env_ms, ts_field_ms
from app.logging_setup import setup_logger
from app.probability_engine import signal_is_stale, signal_origin_ms
from app.trade_ranking import Candidate, Context, RankConfig, RankResult, TradeRankingEngine, get_profile
from app.trade_ranking.config import WEIGHTS
from app.trade_ranking.portfolio import CycleSummary
from app.trade_ranking.sl_tsl import propose_tsl
from run_adaptive_tsl import CFG as TSL_CFG
from run_adaptive_tsl import Inputs, build_snapshot
from run_icare import CFG as ICARE_CFG
from run_icare import liquidity_max_lots, load_journal_daily, load_positions, load_sectors, portfolio_state

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MODE = os.getenv("RANK_MODE", "shadow").strip().lower()

IN_STREAM = os.getenv("STREAM_PROBABILITY", "md:probability")
OUT_STREAM = os.getenv("STREAM_RANKING", "md:ranking")
OUT_MAXLEN = int(os.getenv("STREAM_MAXLEN_RANKING", "200000"))
CYCLE_STREAM = os.getenv("STREAM_RANKING_CYCLE", "md:ranking:cycle")
CYCLE_MAXLEN = int(os.getenv("STREAM_MAXLEN_RANKING_CYCLE", "50000"))
LATEST_PREFIX = os.getenv("RANKING_LATEST_PREFIX", "md:ranking:latest:")
RANK_KEY = os.getenv("RANKING_RANK_KEY", "md:ranking:rank")
BOOK_KEY = os.getenv("RANKING_BOOK_KEY", "md:ranking:book")
CYCLE_LATEST_KEY = os.getenv("RANKING_CYCLE_LATEST_KEY", "md:ranking:cycle:latest")
KILL_SWITCH_KEY = os.getenv("KILL_SWITCH_KEY", "md:control:kill_switch")

ACCOUNT_KEY = os.getenv("ACCOUNT_LATEST_KEY", "md:account:latest")
BLOCK_PREFIX = os.getenv("TSL_BLOCK_PREFIX", "md:tsl:block:")
INDICATOR_PREFIX = os.getenv("INDICATOR_SCORE_LATEST_PREFIX", "md:indicator:score:latest:")
IMBALANCE_PREFIX = os.getenv("IMBALANCE_LATEST_PREFIX", "md:imbalance:latest:")
BIDASK_PREFIX = os.getenv("BIDASK_LATEST_PREFIX", "md:bidask:latest:")
LIQUIDITY_PREFIX = os.getenv("LIQUIDITY_SCORE_LATEST_PREFIX", "md:liquidity:score:latest:")
OI_UND_PREFIX = os.getenv("OI_UNDERLYING_LATEST_PREFIX", "md:oi:underlying:latest:")
EM_PREFIX = os.getenv("EXPECTED_MOVE_LATEST_PREFIX", "md:expected_move:latest:")
GREEKS_PREFIX = os.getenv("GREEKS_PHASE_LATEST_PREFIX", "md:greeks:phase:latest:")
HTF_PREFIX = os.getenv("HTF_TREND_LATEST_PREFIX", "md:htf:trend:latest:")
ST_PREFIX = os.getenv("SUPERTREND_BIAS_LATEST_PREFIX", "md:supertrend:bias:latest:")

# Default = ICARE_MAX_SIGNAL_AGE_SEC: older emissions would be dropped by ICARE anyway.
CANDIDATE_TTL_MS = int(float(os.getenv("RANK_CANDIDATE_TTL_SEC", "60")) * 1000)
CYCLE_SEC = float(os.getenv("RANK_CYCLE_SEC", "5"))
BATCH_WINDOW_MS = int(float(os.getenv("RANK_BATCH_WINDOW_SEC", "3")) * 1000)
MAX_AGE_MS = int(float(os.getenv("RANK_MAX_AGE_SEC", "120")) * 1000)
MAX_QUOTE_AGE_MS = int(float(os.getenv("RANK_MAX_QUOTE_AGE_SEC", "30")) * 1000)
CYCLE_HEARTBEAT_SEC = float(os.getenv("RANK_CYCLE_HEARTBEAT_SEC", "60"))
MAX_SIGNAL_AGE_MS = env_ms("RANK_MAX_SIGNAL_AGE_SEC", 60)

GROUP = os.getenv("RANKING_GROUP", "trade-ranking")
CONSUMER = os.getenv("RANKING_CONSUMER", "trade-ranking-1")


def _weights_from_env() -> Dict[str, float]:
    w = {k: float(os.getenv(f"RANK_W_{k.upper()}", str(v))) for k, v in WEIGHTS.items()}
    if abs(sum(w.values()) - 100.0) > 1e-6:
        raise SystemExit(f"RANK_W_* weights must sum to 100, got {sum(w.values())}: {w}")
    return w


_D = RankConfig()
CFG = RankConfig(
    profile=get_profile(os.getenv("TRADING_PROFILE", "normal_intraday")),
    weights=_weights_from_env(),
    dq_missing_penalty=float(os.getenv("RANK_DQ_MISSING_PENALTY", str(_D.dq_missing_penalty))),
    agreement_full=float(os.getenv("RANK_AGREEMENT_FULL", str(_D.agreement_full))),
    min_agreement=float(os.getenv("RANK_MIN_AGREEMENT", str(_D.min_agreement))),
    min_decisive_votes=int(os.getenv("RANK_MIN_DECISIVE_VOTES", str(_D.min_decisive_votes))),
    target_lots=int(os.getenv("RANK_TARGET_LOTS", str(_D.target_lots))),
    min_liquidity=float(os.getenv("SIE_MIN_LIQUIDITY", str(_D.min_liquidity))),
    max_spread_pct=float(os.getenv("PROB_MAX_SPREAD_PCT", str(_D.max_spread_pct))),
    circuit_band_pct=float(os.getenv("RANK_CIRCUIT_BAND_PCT", str(_D.circuit_band_pct))),
    max_per_sector=int(os.getenv("RANK_MAX_PER_SECTOR", str(_D.max_per_sector))),
)

log = setup_logger("trade_ranking")


# ── helpers ─────────────────────────────────────────────────────────────

def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def _f(v) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _s(v) -> Optional[str]:
    v = "" if v is None else str(v).strip()
    return v or None


def _json(raw, default):
    try:
        out = json.loads(raw) if raw else default
        return out if isinstance(out, type(default)) else default
    except Exception:
        return default


def fresh(doc: dict, now_ms: int, max_age_ms: int) -> dict:
    ts = _f(doc.get("ts_ms")) if doc else None
    return doc if ts is not None and now_ms - ts <= max_age_ms else {}


def book_field(prob: dict) -> str:
    return f"{str(prob.get('symbol') or '').upper()}:{str(prob.get('side') or '').upper()}"


def candidate_id(prob: dict) -> str:
    return f"{prob.get('tradingsymbol') or ''}@{prob.get('entry_bar_ts_ms') or prob.get('ts_ms') or ''}"


def is_hard_rejected(prob: dict) -> bool:
    return bool(_json(prob.get("reject_reasons"), []))


def em_fit_from_top(prob: dict) -> Optional[float]:
    """SIE's expected-move-fit sub-score of the chosen strike (from the `top` list)."""
    tsym = prob.get("tradingsymbol")
    for row in _json(prob.get("top"), []):
        if isinstance(row, dict) and row.get("tradingsymbol") == tsym:
            return _f((row.get("components") or {}).get("em_fit"))
    return None


# ── candidate assembly ─────────────────────────────────────────────────

def build_candidate(prob: dict, inp: Inputs, sectors: Dict[str, str]) -> Candidate:
    """Probability message (+ SIE context) + live latest keys -> Candidate. Stale -> None."""
    now = inp.now_ms
    sym = str(prob.get("symbol") or "").upper()
    side = str(prob.get("side") or "").upper()
    tsym = str(prob.get("tradingsymbol") or "")
    stale = set()

    def doc(key: str, max_age: int = MAX_AGE_MS) -> dict:
        return fresh(inp.doc(key, check_age=False), now, max_age)

    ind = doc(f"{INDICATOR_PREFIX}{sym}")
    vol = fresh(inp.volume_entry(sym), now, MAX_AGE_MS)
    regime = fresh(inp.regime, now, MAX_AGE_MS)
    imb = doc(f"{IMBALANCE_PREFIX}{tsym}", MAX_QUOTE_AGE_MS)
    ba = doc(f"{BIDASK_PREFIX}{tsym}", MAX_QUOTE_AGE_MS)
    liq = doc(f"{LIQUIDITY_PREFIX}{tsym}", MAX_QUOTE_AGE_MS)
    oi = doc(f"{OI_UND_PREFIX}{sym}")
    em = doc(f"{EM_PREFIX}{sym}")
    greeks = doc(f"{GREEKS_PREFIX}{tsym}")
    htf = inp.doc(f"{HTF_PREFIX}{sym}", check_age=False)
    st = doc(f"{ST_PREFIX}{sym}")

    # Gates use LIVE quote values; a stale quote is missing (critical -> DATA_INSUFFICIENT).
    if not ba:
        stale.add("bidask")
    if not liq:
        stale.add("liquidity")

    vol_sig = _s(vol.get("signal"))
    snap = build_snapshot(inp, sym, tsym, str(prob.get("expiry") or ""))
    tsl_pct, tsl_rule = propose_tsl(snap, side, TSL_CFG)
    try:
        hist_samples = int(_f(prob.get("history_samples")) or 0)
    except Exception:
        hist_samples = 0

    return Candidate(
        symbol=sym,
        side=side,
        tradingsymbol=tsym,
        candidate_id=candidate_id(prob),
        ts_ms=int(_f(prob.get("ts_ms")) or 0),
        strike=_f(prob.get("strike")),
        expiry=str(prob.get("expiry") or ""),
        probability=_f(prob.get("probability")),
        probability_decision=str(prob.get("decision") or ""),
        p_oi=_f(prob.get("p_oi")),
        indicator_score=_f(ind.get("score")),
        volume_signal=vol_sig or _s(prob.get("entry_volume_signal")),
        volume_surge=bool(vol_sig and vol_sig.lower().startswith("strong")),
        regime=_s(regime.get("regime")),
        market_phase=_s(prob.get("market_phase")),
        execution_quality=_f(prob.get("execution_quality")),
        imbalance=_s(imb.get("signal")),
        oi_positioning=_s(oi.get("positioning")) or _s(prob.get("entry_oi_positioning")),
        greeks_score=_f(prob.get("greeks_score")),
        liquidity_score=_f(liq.get("liquidity_score")),
        liquidity_band=str(liq.get("liquidity_band") or ""),
        spread_pct=_f(ba.get("spread_pct")),
        strike_score=_f(prob.get("strike_score")),
        em_fit=em_fit_from_top(prob),
        em_direction=_s(em.get("direction")) or _s(prob.get("em_direction")),
        em_direction_score=_f(em.get("direction_score")),
        em_confidence=_f(em.get("confidence")) if em else _f(prob.get("em_confidence")),
        htf_bias=_s(htf.get("bias")),
        st_bias=_s(st.get("bias")),
        amd_phase=_s(greeks.get("phase")),
        premium=_f(ba.get("mid")) or _f(prob.get("premium")),
        lot_size=_f(prob.get("lot_size")),
        projected_gain=_f(prob.get("projected_premium_gain")),
        projected_gain_iv_down=_f(prob.get("projected_premium_gain_iv_down")),
        adverse_change=_f(prob.get("projected_premium_change_adverse")),
        history_samples=hist_samples,
        history_win_rate=_f(prob.get("history_win_rate")),
        history_avg_win_pct=_f(prob.get("history_avg_win_pct")),
        history_avg_loss_pct=_f(prob.get("history_avg_loss_pct")),
        liquidity_max_lots=liquidity_max_lots(liq),
        tsl_pct=tsl_pct,
        tsl_rule=tsl_rule,
        stale=frozenset(stale),
        sector=sectors.get(sym, ""),
    )


def build_context(r: redis.Redis, sectors: Dict[str, str], now_ms: int) -> Tuple[Context, List[str]]:
    positions = load_positions(r)
    raw = r.get(ACCOUNT_KEY)
    pf, flags = portfolio_state(_json(raw, {}), positions, sectors, now_ms,
                                journal_daily=load_journal_daily(r, now_ms))
    open_sectors: Dict[str, int] = {}
    for p in positions:
        sec = sectors.get(str(p.get("symbol") or "").upper())
        if sec:
            open_sectors[sec] = open_sectors.get(sec, 0) + 1
    blocked = frozenset(k[len(BLOCK_PREFIX):].upper() for k in r.scan_iter(match=f"{BLOCK_PREFIX}*", count=500))
    return Context(
        total_capital=pf.total_capital,
        available_margin=pf.available_margin,
        day_pnl=pf.day_pnl,
        open_positions=pf.open_positions,
        open_risk=pf.open_risk,
        open_symbols=pf.open_symbols,
        open_sectors=open_sectors,
        blocked=blocked,
        kill_switch=str(r.get(KILL_SWITCH_KEY) or "").strip() == "1",
        day_pnl_known=pf.day_pnl_known,
        max_risk_per_trade=ICARE_CFG.max_risk_per_trade,
        max_risk_pct=ICARE_CFG.max_risk_pct,
        daily_loss_limit_pct=ICARE_CFG.daily_loss_limit_pct,
        max_portfolio_risk_pct=ICARE_CFG.max_portfolio_risk_pct,
        max_open_trades=ICARE_CFG.max_open_trades,
    ), flags


# ── output ─────────────────────────────────────────────────────────────

def brief_object(res: RankResult, now_ms: int) -> dict:
    """The brief's §22 output object."""
    out = {
        "symbol": res.symbol,
        "option": res.option,
        "direction": res.direction,
        "trade_score": res.trade_score,
        "rank": res.rank,
        "probability": res.probability,
    }
    for k, v in res.components.items():
        out[f"{k}_score"] = v
    out.update({
        "expected_gain_pct": res.expected_gain_pct,
        "expected_value": res.expected_value,
        "initial_stop_loss_pct": res.initial_stop_loss_pct,
        "trailing_stop_pct": res.trailing_stop_pct,
        "trailing_activation_pct": res.trailing_activation_pct,
        "decision": res.decision,
        "confidence": res.confidence,
        "capital_required": res.capital_required,
        "risk_amount": res.risk_amount,
        "reasons": res.reasons,
        "warnings": res.warnings,
        "timestamp": now_ms,
    })
    return out


def build_payload(res: RankResult, prob: dict, now_ms: int, emit: bool, flags: List[str]) -> Dict[str, str]:
    payload: Dict[str, str] = {k: "" if v is None else str(v) for k, v in prob.items()}
    d = res.to_dict()
    for k, v in d.items():
        key = k if k.startswith(("rank", "trade_score")) else f"rank_{k}"
        if isinstance(v, (list, dict)):
            payload[key] = json.dumps(v, separators=(",", ":"))
        else:
            payload[key] = "" if v is None else str(v)
    payload["rank_flags"] = json.dumps(res.flags + flags)
    payload["rank_emit"] = "1" if emit else "0"
    payload["rank_mode"] = MODE
    payload["rank_profile"] = CFG.profile.name
    payload["rank_ts_ms"] = str(now_ms)
    payload["ranking_json"] = json.dumps(brief_object(res, now_ms), separators=(",", ":"))
    return payload


# ── cycle ──────────────────────────────────────────────────────────────

def entry_signal_ms(entry: dict) -> Optional[int]:
    """Signal (origin) time of a book entry; None (-> expired) when unknown."""
    v = _f(entry.get("signal_ms"))
    if v is not None and v > 0:
        return int(v)
    return ts_field_ms(entry.get("prob") if isinstance(entry.get("prob"), dict) else None, "signal_ts_ms")


def load_book(r: redis.Redis, now_ms: int) -> Dict[str, dict]:
    book: Dict[str, dict] = {}
    expired = []
    for field, raw in (r.hgetall(BOOK_KEY) or {}).items():
        entry = _json(raw, {})
        sig = entry_signal_ms(entry) if entry else None
        if not entry or sig is None or now_ms - sig > CANDIDATE_TTL_MS:
            expired.append(field)
            continue
        book[field] = entry
    if expired:
        r.hdel(BOOK_KEY, *expired)
        r.zrem(RANK_KEY, *expired)
    return book


def add_to_book(r: redis.Redis, prob: dict, now_ms: int, signal_ms: Optional[int] = None) -> None:
    """
    `signal_ms` = origin time of the probability message (signal_origin_ms);
    defaults to the payload's signal_ts_ms, else now_ms. The book TTL counts
    from it; `arrived_ms` only drives the R3 batch window.
    """
    field = book_field(prob)
    prev = _json(r.hget(BOOK_KEY, field), {})
    cid = candidate_id(prob)
    same = prev.get("candidate_id") == cid
    if signal_ms is None:
        signal_ms = ts_field_ms(prob, "signal_ts_ms") or now_ms
    entry = {
        "prob": prob,
        "candidate_id": cid,
        "signal_ms": int(signal_ms),
        "arrived_ms": prev.get("arrived_ms", now_ms) if same else now_ms,
        "emitted": bool(prev.get("emitted")) if same else False,
        "last_decision": prev.get("last_decision", "") if same else "",
    }
    r.hset(BOOK_KEY, field, json.dumps(entry, separators=(",", ":")))


def emission_ready(results: List[RankResult], book: Dict[str, dict], now_ms: int) -> bool:
    """Wait until the newest TAKE candidate's batch window has passed (R3)."""
    by_cid = {e["candidate_id"]: e for e in book.values()}
    for res in results:
        e = by_cid.get(res.candidate_id)
        if res.decision == "TAKE_TRADE" and e and not e.get("emitted") and now_ms - int(e["arrived_ms"]) < BATCH_WINDOW_MS:
            return False
    return True


def ingest(r: redis.Redis, msg_id, fields: dict, symbols: set, now_ms: int,
           max_signal_age_ms: int = MAX_SIGNAL_AGE_MS) -> bool:
    """One md:probability message -> candidate book. True when it entered the book (caller ACKs either way)."""
    sym = str(fields.get("symbol") or "").strip().upper()
    if not sym or sym not in symbols:
        return False
    origin = signal_origin_ms(msg_id, fields)
    if signal_is_stale(origin, now_ms, max_signal_age_ms):
        log.info("SKIP stale_signal id=%s symbol=%s tsym=%s age_ms=%s max_ms=%s", msg_id, sym,
                 fields.get("tradingsymbol"), None if origin is None else now_ms - origin, max_signal_age_ms)
        return False
    if is_hard_rejected(fields):
        log.debug("SKIP probability_hard_reject symbol=%s reasons=%s", sym, fields.get("reject_reasons"))
        return False
    prob = dict(fields)
    if origin is not None:
        prob["signal_ts_ms"] = str(origin)
    add_to_book(r, prob, now_ms, signal_ms=origin)
    return True


_last_cycle: Dict[str, object] = {"sig": None, "ts": 0.0}


def run_cycle(r: redis.Redis, engine: TradeRankingEngine, sectors: Dict[str, str]) -> None:
    now_ms = int(time.time() * 1000)
    book = load_book(r, now_ms)
    if not book:
        # Nothing to rank: still publish the heartbeat cycle so the dashboard /
        # pipeline health can tell "running, idle" from "worker dead".
        _publish_cycle(r, CycleSummary(0, 0, 0, 0, 0, 0, "NO_TRADE", []), [], now_ms)
        return
    inp = Inputs(r, now_ms)
    ctx, ctx_flags = build_context(r, sectors, now_ms)
    cands = []
    for entry in book.values():
        c = build_candidate(entry["prob"], inp, sectors)
        log.debug("LOGIC_IN candidate=%s", c)
        cands.append(c)
    results, summary = engine.rank(cands, ctx)
    ready = emission_ready(results, book, now_ms)
    by_cid = {e["candidate_id"]: (f, e) for f, e in book.items()}

    for res in results:   # rank order: rank 1 is emitted (and reaches ICARE) first
        field, entry = by_cid[res.candidate_id]
        emit = res.decision == "TAKE_TRADE" and ready and not entry.get("emitted")
        changed = res.decision != entry.get("last_decision")
        payload = build_payload(res, entry["prob"], now_ms, emit, ctx_flags)
        r.set(f"{LATEST_PREFIX}{field}", json.dumps(payload, separators=(",", ":")), ex=3600)
        if res.trade_score is not None:
            r.zadd(RANK_KEY, {field: res.trade_score})
        else:
            r.zrem(RANK_KEY, field)
        if emit or changed:
            r.xadd(OUT_STREAM, payload, maxlen=OUT_MAXLEN, approximate=True)
            log.info(
                "LOGIC %s %s rank=%s score=%s decision=%s conf=%s prob=%s agree=%s ev=%s(%s) lots=%s "
                "tsl=%s(%s) reasons=%s flags=%s mode=%s",
                "EMIT" if emit else "CHANGE", res.option, res.rank, res.trade_score, res.decision, res.confidence,
                res.probability, res.agreement, res.expected_value, res.ev_source, res.feasible_lots,
                res.trailing_stop_pct, res.tsl_rule, res.reject_reasons, res.flags + ctx_flags, MODE,
            )
        if emit or changed:
            entry = dict(entry, emitted=entry.get("emitted") or emit, last_decision=res.decision)
            r.hset(BOOK_KEY, field, json.dumps(entry, separators=(",", ":")))

    _publish_cycle(r, summary, results, now_ms)


def _publish_cycle(r: redis.Redis, summary: CycleSummary, results: List[RankResult], now_ms: int) -> None:
    sig = (summary.scanned, summary.data_insufficient, summary.rejected, summary.watch,
           summary.eligible, summary.taken, tuple(summary.taken_ids))
    if sig != _last_cycle["sig"] or time.time() - float(_last_cycle["ts"]) >= CYCLE_HEARTBEAT_SEC:
        doc = dict(summary.to_dict(), ts_ms=now_ms, profile=CFG.profile.name, mode=MODE,
                   ranked=[{"rank": x.rank, "option": x.option, "score": x.trade_score, "decision": x.decision}
                           for x in results])
        flat = {k: json.dumps(v, separators=(",", ":")) if isinstance(v, (list, dict)) else str(v) for k, v in doc.items()}
        r.xadd(CYCLE_STREAM, flat, maxlen=CYCLE_MAXLEN, approximate=True)
        r.set(CYCLE_LATEST_KEY, json.dumps(doc, separators=(",", ":")), ex=3600)
        log.info("CYCLE outcome=%s scanned=%s insufficient=%s rejected=%s watch=%s eligible=%s taken=%s",
                 summary.outcome, summary.scanned, summary.data_insufficient, summary.rejected, summary.watch,
                 summary.eligible, summary.taken)
        _last_cycle.update(sig=sig, ts=time.time())


def main():
    symbols = set(load_symbols())
    sectors = load_sectors()
    engine = TradeRankingEngine(CFG, ICARE_CFG)
    r = redis.from_url(REDIS_URL, decode_responses=True)
    ensure_group(r, IN_STREAM, GROUP)
    log.info("START reading %s, writing %s (profile=%s mode=%s ttl=%ss cycle=%ss window=%ss symbols=%d)",
             IN_STREAM, OUT_STREAM, CFG.profile, MODE, CANDIDATE_TTL_MS / 1000, CYCLE_SEC,
             BATCH_WINDOW_MS / 1000, len(symbols))
    last_cycle = 0.0
    while True:
        block_ms = max(int((CYCLE_SEC - (time.time() - last_cycle)) * 1000), 1)
        resp = r.xreadgroup(groupname=GROUP, consumername=CONSUMER, streams={IN_STREAM: ">"},
                            count=500, block=min(block_ms, int(CYCLE_SEC * 1000)))
        new = False
        for _stream, msgs in resp or []:
            ack_ids = []
            for msg_id, fields in msgs:
                ack_ids.append(msg_id)
                if ingest(r, msg_id, fields, symbols, int(time.time() * 1000)):
                    new = True
            if ack_ids:
                r.xack(IN_STREAM, GROUP, *ack_ids)
        if new or time.time() - last_cycle >= CYCLE_SEC:
            try:
                run_cycle(r, engine, sectors)
            except Exception:
                log.exception("CYCLE_FAILED")
            last_cycle = time.time()


if __name__ == "__main__":
    main()

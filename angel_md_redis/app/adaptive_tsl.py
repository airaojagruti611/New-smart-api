"""
app/adaptive_tsl.py
───────────────────────
Module 18 — Adaptive Trailing Stop Loss & Re-entry Engine (DECISION.md §5).

Manages an ALREADY APPROVED trade; never opens one by itself. A re-entry is
only a signal that goes back through ICARE (T10).

  Step 1  volatility_score()   ATR %, expected move %, IV change, gamma, DTE -> 0..100
  Step 2  trend_strength()     Supertrend, EMA distance, direction, intensity, AMD
                               (side-aligned) -> 0..100 + Weak/Moderate/Strong/Explosive
  Step 3  select_tsl_pct()     dynamic range table (T6) + profit tiers (R multiple);
                               ratchet: the % and the stop only ever tighten
  Step 4  manage()             trail on the option premium at BID; activates once the
                               trail would reach breakeven (T7); ICARE SL until then.
                               The stop distance is tsl_pct of the HIGHEST bid, never of
                               the entry: entry 10, high 100, 10 % -> stop 90 (T14)
  Step 5  validate_exit()      bid <= stop AND bid-ask bearish AND direction weak AND
                               EMA weak, OR bid <= stop AND distribution; safety valves:
                               hard breach (3 % below stop) and breach timeout (60 s) (T8)
  Step 6  exit context         stored on the exit (basis for re-entry)
  Step 7  Chain                re-entry watch state machine
  Step 8  evaluate_reentry()   all 7 mandatory checks; missing data = fail (T9)
          cooldown / max re-entries / BLOCKED (T11)

Missing inputs are None. For exit confirmation a missing input CONFIRMS the
exit (fail-safe to flat); for re-entry it FAILS the check (fail-safe to flat).

"Side-aligned": for a BUY PUT a bearish underlying is "bullish for the trade",
a swing-high break means the underlying breaking below its last swing low.

No I/O here — Redis wiring lives in run_adaptive_tsl.py.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field, replace
from typing import Dict, List, Optional, Tuple

from .market_structure import bars_to_candles, completed_bars, last_swing_high, update_bars
from .probability_engine import amd_score, intensity_score, normalize_side, signed_to_score

# ── Constants / config ─────────────────────────────────────────────────

VOL_WEIGHTS = {"atr": 0.30, "expected_move": 0.25, "iv": 0.15, "gamma": 0.15, "dte": 0.15}
TREND_WEIGHTS = {"supertrend": 0.25, "ema": 0.20, "direction": 0.25, "intensity": 0.15, "amd": 0.15}

# Original Probability decision -> minimum recomputed probability for a re-entry (T-Q5).
BAND_FLOORS = {"SMALL_POSITION": 65.0, "TRADE": 75.0, "HIGH_CONVICTION": 85.0}

EXIT_TRAILING_STOP = "TRAILING_STOP"

# Chain states
IN_TRADE = "IN_TRADE"
WAITING = "WAITING_FOR_SWING_BREAK"
PENDING = "REENTRY_PENDING"
EXPIRED = "EXPIRED"
DONE = "DONE"
BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class TSLConfig:
    # Step 1 normalisation (value at score 0, value at score 100)
    atr_pct_lo: float = 0.05
    atr_pct_hi: float = 0.40
    em_pct_lo: float = 0.3
    em_pct_hi: float = 2.0
    iv_change_span_pct: float = 20.0      # +/- this much IV change -> 100 / 0
    gamma_shift_hi: float = 0.25          # delta change per 1 % spot move at score 100
    dte_hi: float = 7.0                   # >= this many days -> 0
    vol_low_max: float = 40.0             # Low < 40 <= Medium < 70 <= High
    vol_high_min: float = 70.0
    # Step 2
    ema_atr_full: float = 1.0             # EMA gap of 1 ATR -> 100
    # Step 3 ranges (percent)
    gamma_explosion_min: float = 85.0
    range_gamma: Tuple[float, float] = (18.0, 25.0)
    range_expiry: Tuple[float, float] = (15.0, 20.0)
    range_strong_low_vol: Tuple[float, float] = (4.0, 6.0)
    range_strong_high_vol: Tuple[float, float] = (8.0, 10.0)
    range_moderate: Tuple[float, float] = (10.0, 12.0)
    range_weak: Tuple[float, float] = (12.0, 15.0)
    # Profit tiers on the peak R multiple
    tier_breakeven_r: float = 1.0
    tier2_r: float = 2.0
    tier2_mult: float = 0.8
    tier3_r: float = 3.
    tier3_mult: float = 0.6
    # Step 5
    dir_weak: float = 0.0                 # side-aligned direction score below this = weak
    dir_drop: float = 0.3                 # or dropped this much since entry
    ema_weak_bars: int = 3                # gap shrinking over this many bars
    hard_breach_pct: float = 3.0
    confirm_max_sec: float = 60.0
    # Step 7/8
    tick: float = 0.05
    watch_buffer_pct: float = 0.15
    min_liquidity: float = 70.0
    max_spread_pct: float = 3.0
    max_reentries: int = 2
    cooldown_min: float = 10.0
    watch_min: float = 60.0
    pending_ack_sec: float = 30.0
    retry_sec: float = 60.0


def _clip(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def _lin(v: Optional[float], lo: float, hi: float) -> Optional[float]:
    """0 at `lo`, 100 at `hi`, clipped."""
    if v is None or hi == lo:
        return None
    return round(_clip((float(v) - lo) / (hi - lo) * 100.0), 4)


def _sign(side: str) -> float:
    return 1.0 if normalize_side(side) == "CE" else -1.0


def _weighted(components: Dict[str, Optional[float]], weights: Dict[str, float]) -> Tuple[Optional[float], List[str]]:
    """Weighted mean over known components (renormalised); returns (score, missing)."""
    missing = [k for k in weights if components.get(k) is None]
    wsum = sum(w for k, w in weights.items() if components.get(k) is not None)
    if wsum <= 0:
        return None, missing
    score = sum(float(components[k]) * w for k, w in weights.items() if components.get(k) is not None) / wsum
    return round(score, 4), missing


# ── Market snapshot ────────────────────────────────────────────────────

@dataclass(frozen=True)
class Snapshot:
    """Everything the engine reads at one moment. None = missing or stale."""
    now_ms: int
    bid: Optional[float] = None
    ask: Optional[float] = None
    spot: Optional[float] = None
    atr_pct: Optional[float] = None              # underlying 1m ATR as % of spot
    em_pct: Optional[float] = None               # Module 8 expected move, percent
    em_direction: Optional[str] = None
    em_confidence: Optional[float] = None
    direction_score: Optional[float] = None      # Module 8, -1..+1 (bullish +)
    iv_change_pct: Optional[float] = None        # contract IV change, percent
    gamma: Optional[float] = None
    delta: Optional[float] = None
    dte: Optional[int] = None                    # calendar days to expiry (0 = expiry day)
    st_bias: Optional[str] = None                # CALL / PUT / NEUTRAL
    st_bullish: Optional[int] = None             # MTF timeframes bullish
    st_bearish: Optional[int] = None
    st_total: Optional[int] = None               # timeframes with a value
    ema9: Optional[float] = None
    ema26: Optional[float] = None
    ema_gaps: Tuple[float, ...] = ()             # ema9 - ema26 per completed bar, oldest first
    volume_signal: Optional[str] = None
    volume_surge: bool = False
    amd_phase: Optional[str] = None              # greeks phase of the contract
    contract_imbalance: Optional[str] = None     # BULLISH / BEARISH / NEUTRAL (bid-ask on the contract)
    smart_money: Optional[str] = None            # composite label on the contract
    liquidity: Optional[float] = None
    spread_pct: Optional[float] = None
    oi_buildup: Optional[str] = None             # underlying dominant build-up
    sideways: bool = False
    und_close: Optional[float] = None            # last completed 1m underlying close
    und_swing_high: Optional[float] = None
    und_swing_low: Optional[float] = None
    pivots: Dict[str, float] = field(default_factory=dict)
    fib: Dict[str, float] = field(default_factory=dict)
    probability: Optional[float] = None          # recomputed now (re-entry only)
    eod: bool = False


# ── Step 1: volatility ─────────────────────────────────────────────────

def gamma_component(gamma: Optional[float], spot: Optional[float], cfg: TSLConfig) -> Optional[float]:
    """Delta shift for a 1 % spot move (gamma x spot x 1 %), 0..100."""
    if gamma is None or spot is None:
        return None
    return _lin(abs(gamma) * spot * 0.01, 0.0, cfg.gamma_shift_hi)


def volatility_score(snap: Snapshot, cfg: TSLConfig) -> Tuple[Optional[float], str, Dict[str, Optional[float]], List[str]]:
    iv = None
    if snap.iv_change_pct is not None:
        iv = round(_clip(50.0 + snap.iv_change_pct / cfg.iv_change_span_pct * 50.0), 4)
    dte = None
    if snap.dte is not None:
        dte = round(_clip((1.0 - max(snap.dte, 0) / cfg.dte_hi) * 100.0), 4)
    comps = {
        "atr": _lin(snap.atr_pct, cfg.atr_pct_lo, cfg.atr_pct_hi),
        "expected_move": _lin(snap.em_pct, cfg.em_pct_lo, cfg.em_pct_hi),
        "iv": iv,
        "gamma": gamma_component(snap.gamma, snap.spot, cfg),
        "dte": dte,
    }
    score, missing = _weighted(comps, VOL_WEIGHTS)
    if score is None:
        band = "UNKNOWN"
    elif score < cfg.vol_low_max:
        band = "LOW"
    elif score < cfg.vol_high_min:
        band = "MEDIUM"
    else:
        band = "HIGH"
    return score, band, comps, missing


# ── Step 2: trend strength ─────────────────────────────────────────────

def trend_label(score: Optional[float]) -> str:
    if score is None:
        return "UNKNOWN"
    if score >= 80:
        return "EXPLOSIVE"
    if score >= 60:
        return "STRONG"
    if score >= 40:
        return "MODERATE"
    return "WEAK"


def trend_strength(snap: Snapshot, side: str, cfg: TSLConfig) -> Tuple[Optional[float], str, Dict[str, Optional[float]], List[str]]:
    s = normalize_side(side)
    st = None
    if snap.st_total:
        aligned = snap.st_bullish if s == "CE" else snap.st_bearish
        if aligned is not None:
            st = round(_clip(aligned / snap.st_total * 100.0), 4)
    ema = None
    if snap.ema9 is not None and snap.ema26 is not None and snap.atr_pct and snap.spot:
        atr_abs = snap.atr_pct / 100.0 * snap.spot
        if atr_abs > 0:
            gap_atr = (snap.ema9 - snap.ema26) * _sign(s) / atr_abs
            ema = round(_clip(gap_atr / cfg.ema_atr_full * 100.0), 4)
    comps = {
        "supertrend": st,
        "ema": ema,
        "direction": signed_to_score(snap.direction_score, s),
        "intensity": intensity_score(snap.volume_signal, s, snap.volume_surge, snap.em_confidence),
        "amd": amd_score(snap.amd_phase),
    }
    score, missing = _weighted(comps, TREND_WEIGHTS)
    return score, trend_label(score), comps, missing


# ── Step 3: dynamic TSL % ──────────────────────────────────────────────

def _pick(rng: Tuple[float, float], t: float) -> float:
    lo, hi = rng
    return round(lo + (hi - lo) * _clip(t, 0.0, 1.0), 4)


def select_tsl_pct(
    trend: str, vol: Optional[float], vol_band: str, dte: Optional[int], gamma_comp: Optional[float],
    sideways: bool, cfg: TSLConfig,
) -> Tuple[float, str]:
    """(trailing %, rule name). First match wins (T6). Unknown volatility = mid of range."""
    v = 50.0 if vol is None else vol
    if dte == 0 and gamma_comp is not None and gamma_comp >= cfg.gamma_explosion_min:
        return _pick(cfg.range_gamma, v / 100.0), "GAMMA_EXPLOSION"
    if dte == 0:
        return _pick(cfg.range_expiry, v / 100.0), "EXPIRY_DAY"
    if not sideways and trend in ("STRONG", "EXPLOSIVE"):
        if vol_band == "LOW":
            return _pick(cfg.range_strong_low_vol, v / cfg.vol_low_max), "STRONG_LOW_VOL"
        return _pick(cfg.range_strong_high_vol, (v - cfg.vol_low_max) / (100.0 - cfg.vol_low_max)), "STRONG_HIGH_VOL"
    if not sideways and trend == "MODERATE":
        return _pick(cfg.range_moderate, v / 100.0), "MODERATE"
    return _pick(cfg.range_weak, v / 100.0), "SIDEWAYS" if sideways else "WEAK"


def apply_profit_tiers(pct: float, peak_r: Optional[float], cfg: TSLConfig) -> float:
    if peak_r is None:
        return pct
    if peak_r >= cfg.tier3_r:
        return round(pct * cfg.tier3_mult, 4)
    if peak_r >= cfg.tier2_r:
        return round(pct * cfg.tier2_mult, 4)
    return pct


# ── Trade state ────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TradeState:
    trade_id: str
    chain_id: str
    reentry_no: int
    symbol: str
    tradingsymbol: str
    side: str
    qty: float
    entry_premium: float
    initial_sl: float
    entry_ts_ms: int
    time_stop_ms: int = 0
    virtual: bool = False                       # shadow re-entry simulated by the engine
    entry_direction_score: Optional[float] = None
    highest: Optional[float] = None
    tsl_pct: Optional[float] = None
    stop: Optional[float] = None
    activated: bool = False
    tsl_rule: str = ""
    breach_since_ms: Optional[int] = None
    status: str = "ACTIVE"                      # ACTIVE / EXIT / CLOSED
    exit_reason: str = ""
    exit_trigger: str = ""
    exit_price: Optional[float] = None
    exit_ts_ms: Optional[int] = None
    premium_bars: List[dict] = field(default_factory=list)
    ema_gaps: List[list] = field(default_factory=list)   # [[bar_ts_ms, gap], ...]

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "TradeState":
        return TradeState(**{k: d[k] for k in TradeState.__dataclass_fields__ if k in d})

    @property
    def risk_pts(self) -> float:
        return max(self.entry_premium - self.initial_sl, 0.0)


def new_trade(
    trade_id: str, chain_id: str, reentry_no: int, symbol: str, tradingsymbol: str, side: str, qty: float,
    entry_premium: float, initial_sl: float, entry_ts_ms: int, time_stop_ms: int = 0, virtual: bool = False,
    entry_direction_score: Optional[float] = None,
) -> TradeState:
    return TradeState(
        trade_id=trade_id, chain_id=chain_id or trade_id, reentry_no=int(reentry_no or 0), symbol=symbol.upper(),
        tradingsymbol=tradingsymbol, side=normalize_side(side), qty=qty, entry_premium=entry_premium,
        initial_sl=initial_sl, entry_ts_ms=entry_ts_ms, time_stop_ms=time_stop_ms, virtual=virtual,
        entry_direction_score=entry_direction_score, highest=entry_premium, stop=initial_sl,
    )


def r_multiple(st: TradeState, price: Optional[float]) -> Optional[float]:
    if price is None or st.risk_pts <= 0:
        return None
    return round((price - st.entry_premium) / st.risk_pts, 4)


def record_ema_gap(gaps: List[list], bar_ts_ms: Optional[int], gap: Optional[float], keep: int = 10) -> List[list]:
    """Append (bar, gap) once per completed EMA bar."""
    if bar_ts_ms is None or gap is None:
        return list(gaps)
    out = [list(g) for g in gaps]
    if out and int(out[-1][0]) == int(bar_ts_ms):
        out[-1] = [int(bar_ts_ms), gap]
    elif not out or int(bar_ts_ms) > int(out[-1][0]):
        out.append([int(bar_ts_ms), gap])
    return out[-keep:]


# ── Step 5: exit validation ────────────────────────────────────────────

def exit_checks(st: TradeState, snap: Snapshot, cfg: TSLConfig) -> Dict[str, bool]:
    sgn = _sign(st.side)
    # bid-ask bearish on the contract we hold (selling pressure); missing -> confirms
    bidask_bearish = snap.contract_imbalance is None or snap.contract_imbalance.strip().upper() == "BEARISH"
    # direction weakening
    if snap.direction_score is None:
        direction_weak = True
    else:
        aligned = snap.direction_score * sgn
        dropped = (
            st.entry_direction_score is not None
            and st.entry_direction_score * sgn - aligned >= cfg.dir_drop
        )
        direction_weak = aligned < cfg.dir_weak or dropped
    # EMA momentum weakening
    gaps = [g * sgn for g in snap.ema_gaps]
    if snap.ema9 is not None and snap.ema26 is not None:
        live = (snap.ema9 - snap.ema26) * sgn
        if not gaps or gaps[-1] != live:
            gaps.append(live)
    if not gaps:
        ema_weak = True
    else:
        n = cfg.ema_weak_bars
        tail = gaps[-n:]
        shrinking = len(tail) >= n and all(tail[i] > tail[i + 1] for i in range(len(tail) - 1))
        ema_weak = gaps[-1] <= 0 or shrinking
    distribution = (snap.amd_phase or "").strip().upper() == "DISTRIBUTION" or (
        (snap.smart_money or "").strip().upper() == "WATCH_DISTRIBUTION"
    )
    return {
        "bidask_bearish": bidask_bearish,
        "direction_weak": direction_weak,
        "ema_weak": ema_weak,
        "distribution": distribution,
    }


def validate_exit(st: TradeState, snap: Snapshot, cfg: TSLConfig) -> Tuple[Optional[str], Dict[str, bool], Optional[int]]:
    """
    Called only when bid <= stop. Returns (trigger or None, checks, breach_since_ms).
    Triggers: HARD_BREACH, BREACH_TIMEOUT, CONFIRMED, DISTRIBUTION.
    """
    checks = exit_checks(st, snap, cfg)
    since = st.breach_since_ms if st.breach_since_ms is not None else snap.now_ms
    bid, stop = snap.bid, st.stop
    if bid is not None and stop is not None and bid <= stop * (1.0 - cfg.hard_breach_pct / 100.0):
        return "HARD_BREACH", checks, since
    if snap.now_ms - since >= cfg.confirm_max_sec * 1000.0:
        return "BREACH_TIMEOUT", checks, since
    if checks["bidask_bearish"] and checks["direction_weak"] and checks["ema_weak"]:
        return "CONFIRMED", checks, since
    if checks["distribution"]:
        return "DISTRIBUTION", checks, since
    return None, checks, since


# ── Step 4: manage one tick ────────────────────────────────────────────

@dataclass(frozen=True)
class TickResult:
    state: TradeState
    output: dict
    exit_context: Optional[dict] = None
    closed_reason: str = ""          # virtual trades only: SL / TIME / EOD / TRAILING_STOP


def premium_swing_high(st: TradeState, now_ms: int) -> Optional[float]:
    bars = completed_bars(st.premium_bars, now_ms)
    return last_swing_high(bars_to_candles(bars)) if bars else None


def build_exit_context(st: TradeState, snap: Snapshot, trend: str, vol: Optional[float]) -> dict:
    sgn = _sign(st.side)
    return {
        "trade_id": st.trade_id,
        "chain_id": st.chain_id,
        "entry_price": st.entry_premium,
        "exit_price": snap.bid,
        "highest_price": st.highest,
        "trailing_stop": st.stop,
        "trailing_percentage": st.tsl_pct,
        "last_swing_high": premium_swing_high(st, snap.now_ms),
        "underlying_swing_high": snap.und_swing_high,
        "underlying_swing_low": snap.und_swing_low,
        # the level the underlying must break on re-entry (side-aligned)
        "underlying_break_level": snap.und_swing_high if sgn > 0 else snap.und_swing_low,
        "pivots": dict(snap.pivots),
        "fib": dict(snap.fib),
        "trend": "Bullish" if sgn > 0 else "Bearish",
        "trend_strength": trend,
        "volatility_score": vol,
        "exit_reason": EXIT_TRAILING_STOP,
        "exit_ts_ms": snap.now_ms,
    }


def manage(st: TradeState, snap: Snapshot, cfg: TSLConfig) -> TickResult:
    """One evaluation of an ACTIVE trade. Non-ACTIVE states are returned unchanged."""
    if st.status != "ACTIVE":
        return TickResult(st, active_output(st, snap, {}, None, None, "", []))

    bid = snap.bid
    bars = update_bars(st.premium_bars, snap.now_ms, bid)
    highest = max(st.highest or st.entry_premium, bid) if bid and bid > 0 else (st.highest or st.entry_premium)

    vol, vol_band, _vc, vol_missing = volatility_score(snap, cfg)
    trend_score, trend, _tc, trend_missing = trend_strength(snap, st.side, cfg)
    pct, rule = select_tsl_pct(
        trend, vol, vol_band, snap.dte, gamma_component(snap.gamma, snap.spot, cfg), snap.sideways, cfg
    )
    peak_r = r_multiple(st, highest)
    pct = apply_profit_tiers(pct, peak_r, cfg)
    if st.tsl_pct is not None and pct > st.tsl_pct:
        pct, rule = st.tsl_pct, st.tsl_rule          # never widen
    activated = st.activated or highest >= st.entry_premium * (1.0 + pct / 100.0)

    stop = max(st.stop if st.stop is not None else st.initial_sl, st.initial_sl)
    if activated:
        candidate = highest * (1.0 - pct / 100.0)
        if peak_r is not None and peak_r >= cfg.tier_breakeven_r:
            candidate = max(candidate, st.entry_premium)
        stop = max(stop, candidate)
    stop = round(stop, 4)

    st = replace(st, premium_bars=bars, highest=round(highest, 4), tsl_pct=pct, tsl_rule=rule,
                 activated=activated, stop=stop)
    flags = [f"VOL_MISSING:{k}" for k in vol_missing] + [f"TREND_MISSING:{k}" for k in trend_missing]
    diag = {"volatility_score": vol, "volatility_band": vol_band, "trend_score": trend_score,
            "trend_strength": trend, "r_multiple": r_multiple(st, bid), "peak_r": peak_r}

    # Virtual (shadow re-entry) trades also need the journal's exits.
    if st.virtual and bid is not None and bid <= st.initial_sl and not activated:
        return _close_virtual(st, snap, "SL", diag, flags)

    checks: Dict[str, bool] = {}
    trigger = None
    if activated and bid is not None and bid <= stop:
        trigger, checks, since = validate_exit(st, snap, cfg)
        st = replace(st, breach_since_ms=since)
    else:
        st = replace(st, breach_since_ms=None)

    if trigger:
        ctx = build_exit_context(st, snap, trend, vol)
        st = replace(st, status="EXIT", exit_reason=EXIT_TRAILING_STOP, exit_trigger=trigger,
                     exit_price=bid, exit_ts_ms=snap.now_ms)
        out = active_output(st, snap, checks, diag, None, "", flags)
        out.update({"status": "EXIT", "exit_signal": True, "exit_reason": EXIT_TRAILING_STOP,
                    "exit_trigger": trigger})
        return TickResult(st, out, ctx, EXIT_TRAILING_STOP if st.virtual else "")

    if st.virtual:
        if st.time_stop_ms and snap.now_ms >= st.time_stop_ms:
            return _close_virtual(st, snap, "TIME", diag, flags)
        if snap.eod:
            return _close_virtual(st, snap, "EOD", diag, flags)
    return TickResult(st, active_output(st, snap, checks, diag, None, "", flags))


def _close_virtual(st: TradeState, snap: Snapshot, reason: str, diag: dict, flags: List[str]) -> TickResult:
    px = snap.bid if snap.bid else st.highest
    st = replace(st, status="CLOSED", exit_reason=reason, exit_price=px, exit_ts_ms=snap.now_ms)
    out = active_output(st, snap, {}, diag, None, "", flags)
    out.update({"status": "CLOSED", "exit_reason": reason})
    return TickResult(st, out, None, reason)


def active_output(
    st: TradeState, snap: Snapshot, checks: Dict[str, bool], diag: Optional[dict],
    watch_price: Optional[float], reentry_state: str, flags: List[str],
) -> dict:
    """The brief's ACTIVE shape plus diagnostics."""
    out = {
        "symbol": st.symbol,
        "current_trailing_stop": st.stop,
        "trailing_percentage": st.tsl_pct,
        "highest_price": st.highest,
        "status": st.status,
        "exit_signal": st.status == "EXIT",
        "reentry_state": reentry_state or "NONE",
        "trade_id": st.trade_id,
        "chain_id": st.chain_id,
        "reentry_no": st.reentry_no,
        "tradingsymbol": st.tradingsymbol,
        "side": st.side,
        "entry_price": st.entry_premium,
        "initial_stop": st.initial_sl,
        "current_price": snap.bid,
        "activated": st.activated,
        "tsl_rule": st.tsl_rule,
        "stop_breached": st.breach_since_ms is not None,
        "virtual": st.virtual,
        "checks": checks,
        "flags": flags,
    }
    if diag:
        out.update(diag)
    if watch_price is not None:
        out["watch_price"] = watch_price
    return out


# ── Steps 7–8: re-entry chain ──────────────────────────────────────────

@dataclass(frozen=True)
class Chain:
    chain_id: str
    symbol: str
    tradingsymbol: str
    side: str
    qty: float
    sl_pct: float                         # original stop distance as a fraction of entry
    hold_ms: int
    band_floor: float
    original_lots: int = 0
    state: str = IN_TRADE
    active_trade_id: str = ""
    reentries_used: int = 0
    failed_reentries: int = 0
    last_exit_ms: Optional[int] = None
    watch_price: Optional[float] = None
    watch_started_ms: Optional[int] = None
    exit_context: Dict = field(default_factory=dict)
    pending_since_ms: Optional[int] = None
    retry_after_ms: Optional[int] = None
    end_reason: str = ""
    bars: List[dict] = field(default_factory=list)   # 1-min premium bars while watching

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "Chain":
        return Chain(**{k: d[k] for k in Chain.__dataclass_fields__ if k in d})


def band_floor(decision: Optional[str]) -> float:
    return BAND_FLOORS.get((decision or "").strip().upper(), BAND_FLOORS["SMALL_POSITION"])


def watch_price_for(highest: Optional[float], swing_high: Optional[float], cfg: TSLConfig) -> Optional[float]:
    """max(trade high, premium swing high) + max(1 tick, 0.15 %), rounded UP to the tick."""
    ref = max([x for x in (highest, swing_high) if x is not None], default=None)
    if ref is None:
        return None
    level = ref + max(cfg.tick, ref * cfg.watch_buffer_pct / 100.0)
    return round(math.ceil(round(level / cfg.tick, 6)) * cfg.tick, 4)


def on_trade_closed(chain: Chain, reentry_no: int, reason: str, pnl: Optional[float],
                    exit_ctx: Optional[dict], now_ms: int, cfg: TSLConfig) -> Chain:
    """A trade of the chain ended (any reason). Decides what the chain does next."""
    failed = chain.failed_reentries
    if reentry_no >= 1 and (pnl is None or pnl <= 0):
        failed += 1
    chain = replace(chain, failed_reentries=failed, last_exit_ms=now_ms, active_trade_id="")
    if failed >= cfg.max_reentries:
        return replace(chain, state=BLOCKED, end_reason="REENTRIES_FAILED", watch_price=None)
    if reason == EXIT_TRAILING_STOP and chain.reentries_used < cfg.max_reentries and exit_ctx:
        wp = watch_price_for(exit_ctx.get("highest_price"), exit_ctx.get("last_swing_high"), cfg)
        return replace(chain, state=WAITING, watch_price=wp, watch_started_ms=now_ms, exit_context=dict(exit_ctx),
                       retry_after_ms=None)
    end = "MAX_REENTRIES" if reason == EXIT_TRAILING_STOP else f"EXIT_{reason or 'UNKNOWN'}"
    return replace(chain, state=DONE, end_reason=end, watch_price=None)


def reentry_checks(chain: Chain, snap: Snapshot, cfg: TSLConfig) -> Dict[str, bool]:
    """The 7 mandatory checks (T9) + spread. Missing data fails a check."""
    s = normalize_side(chain.side)
    sgn = _sign(s)
    done = completed_bars(chain.bars, snap.now_ms)
    after_exit = [b for b in done if chain.last_exit_ms is None or int(b["t"]) + 60_000 > chain.last_exit_ms]
    last_close = after_exit[-1]["c"] if after_exit else None
    level = chain.exit_context.get("underlying_break_level") if chain.exit_context else None
    und_break = (
        level is not None and snap.und_close is not None
        and (snap.und_close > level if sgn > 0 else snap.und_close < level)
    )
    ema_ok = snap.ema9 is not None and snap.ema26 is not None and (snap.ema9 - snap.ema26) * sgn > 0
    bias = (snap.st_bias or "").strip().upper()
    want_buildup = "LONG_BUILDUP" if s == "CE" else "SHORT_BUILDUP"
    return {
        "price_break": last_close is not None and chain.watch_price is not None and last_close >= chain.watch_price,
        "underlying_break": bool(und_break),
        "supertrend": bias == ("CALL" if s == "CE" else "PUT"),
        "ema": ema_ok,
        "bidask": (snap.contract_imbalance or "").strip().upper() == "BULLISH",
        "liquidity": snap.liquidity is not None and snap.liquidity >= cfg.min_liquidity,
        "probability": snap.probability is not None and snap.probability >= chain.band_floor,
        "oi": (snap.oi_buildup or "").strip().upper() == want_buildup,
        "spread": snap.spread_pct is not None and snap.spread_pct <= cfg.max_spread_pct,
    }


def trend_invalidated(chain: Chain, snap: Snapshot) -> bool:
    bias = (snap.st_bias or "").strip().upper()
    return bias == ("PUT" if normalize_side(chain.side) == "CE" else "CALL")


def evaluate_reentry(chain: Chain, snap: Snapshot, cfg: TSLConfig) -> Tuple[Chain, Optional[dict], Dict[str, bool], str]:
    """
    For a WAITING chain: fold the bid into its premium bars, then expire it,
    keep watching, or emit the REENTER signal (the brief's REENTER shape).
    Returns (chain, signal or None, checks, wait reason).
    """
    if chain.state != WAITING:
        return chain, None, {}, ""
    now = snap.now_ms
    chain = replace(chain, bars=update_bars(chain.bars, now, snap.bid))
    if snap.eod:
        return replace(chain, state=EXPIRED, end_reason="EOD", watch_price=None), None, {}, "EOD"
    if chain.watch_started_ms is not None and now - chain.watch_started_ms >= cfg.watch_min * 60_000:
        return replace(chain, state=EXPIRED, end_reason="WATCH_TIMEOUT", watch_price=None), None, {}, "WATCH_TIMEOUT"
    if trend_invalidated(chain, snap):
        return replace(chain, state=EXPIRED, end_reason="TREND_INVALIDATED", watch_price=None), None, {}, "TREND_INVALIDATED"
    checks = reentry_checks(chain, snap, cfg)
    if chain.last_exit_ms is not None and now - chain.last_exit_ms < cfg.cooldown_min * 60_000:
        return chain, None, checks, "COOLDOWN"
    if chain.retry_after_ms is not None and now < chain.retry_after_ms:
        return chain, None, checks, "RETRY_WAIT"
    if chain.reentries_used >= cfg.max_reentries:
        return replace(chain, state=DONE, end_reason="MAX_REENTRIES", watch_price=None), None, checks, "MAX_REENTRIES"
    failed = [k for k, ok in checks.items() if not ok]
    if failed:
        return chain, None, checks, "CHECKS:" + ",".join(failed)
    entry = snap.ask if snap.ask else snap.bid
    signal = {
        "symbol": chain.symbol,
        "status": "REENTER",
        "entry_price": entry,
        "confidence": snap.probability,
        "reason": "SWING_HIGH_BREAK_WITH_CONFLUENCE",
        "chain_id": chain.chain_id,
        "parent_trade_id": (chain.exit_context or {}).get("trade_id", ""),
        "reentry_no": chain.reentries_used + 1,
        "tradingsymbol": chain.tradingsymbol,
        "side": chain.side,
        "watch_price": chain.watch_price,
        "max_lots": chain.original_lots,
        "checks": checks,
    }
    chain = replace(chain, state=PENDING, reentries_used=chain.reentries_used + 1, pending_since_ms=now)
    return chain, signal, checks, ""


def on_reentry_opened(chain: Chain, trade_id: str) -> Chain:
    return replace(chain, state=IN_TRADE, active_trade_id=trade_id, pending_since_ms=None, watch_price=None)


def pending_timed_out(chain: Chain, now_ms: int, cfg: TSLConfig) -> bool:
    return (
        chain.state == PENDING and chain.pending_since_ms is not None
        and now_ms - chain.pending_since_ms >= cfg.pending_ack_sec * 1000.0
    )


def revert_pending(chain: Chain, now_ms: int, cfg: TSLConfig) -> Chain:
    """ICARE did not approve the re-entry: the attempt does not count; wait and retry."""
    return replace(chain, state=WAITING, reentries_used=max(chain.reentries_used - 1, 0),
                   pending_since_ms=None, retry_after_ms=now_ms + int(cfg.retry_sec * 1000))


def exit_output(chain: Chain) -> dict:
    """The brief's EXIT shape."""
    return {
        "symbol": chain.symbol,
        "status": "EXIT",
        "exit_reason": EXIT_TRAILING_STOP,
        "reentry_state": chain.state,
        "watch_price": chain.watch_price,
    }

from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from typing import Mapping, Optional, Sequence

import pandas as pd

from .greeks_change import reprice_option
from .learning_keys import bucket_key, delta_band, em_bucket, strike_bucket
from .logging_setup import setup_logger
from .scripmaster import build_atm_option_tokens

log = setup_logger("strike_select")


@dataclass(frozen=True)
class StrikeSelectResult:
    status: str  # "OK" / "SKIP"
    signal: str
    side: str  # "CE" / "PE" / ""
    mode: str  # "atm" / "otm1" / ... / "oi_target"
    spot: float
    atm: float
    strike: float
    step: float
    expiry: str
    token: str
    tradingsymbol: str
    exchange: str
    reason: str
    # --- Strike Intelligence Engine fields (defaults keep old callers working) ---
    strike_score: Optional[float] = None
    confidence: Optional[float] = None
    reasons: tuple = ()
    regime: str = ""
    regime_class: str = ""
    em_pct: Optional[float] = None
    em_bucket: str = ""
    delta_band: str = ""
    strike_bucket: str = ""
    hold_min: Optional[float] = None
    best: Optional["StrikeCandidate"] = None
    top3: tuple = ()  # tuple[StrikeCandidate], best first


def _skip(signal: str, reason: str, spot: float = 0.0, **extra) -> StrikeSelectResult:
    return StrikeSelectResult(
        status="SKIP", signal=signal, side="", mode="", spot=spot, atm=0.0,
        strike=0.0, step=0.0, expiry="", token="", tradingsymbol="", exchange="",
        reason=reason, **extra,
    )


# --------------------------------------------------------------------------
# Strike Intelligence Engine (DECISION.MD section A). Pure: no I/O.
# --------------------------------------------------------------------------

def _envf(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)


def _band(name: str, default: tuple) -> tuple:
    raw = os.getenv(name, "")
    try:
        lo, hi = (float(x) for x in raw.split(","))
        return (lo, hi)
    except Exception:
        return default


@dataclass(frozen=True)
class StrikeConfig:
    """All tunables; env-driven via from_env() so they can be tuned after backtests."""
    w_liq: float = 0.25
    w_em: float = 0.25
    w_delta: float = 0.20
    w_theta: float = 0.15
    w_gamma: float = 0.10
    w_vega: float = 0.05
    w_learn: float = 0.15            # applied only when a learning score is available
    min_liquidity: float = 70.0      # hard filter
    max_spread_pct: float = 5.0      # hard filter (percent of mid)
    max_age_sec: float = 120.0       # hard filter on live-data age
    min_confidence: float = 70.0
    hold_min: float = 30.0
    trading_min_per_day: float = 375.0
    theta_risk_max: float = 0.10     # theta cost / premium at which efficiency hits 0
    delta_decay: float = 0.30        # |delta| distance outside band at which score hits 0
    exec_no_live: float = 50.0       # reduced execution score when no live bid-ask
    exec_weight_in_liq: float = 0.15 # share of execution quality inside the liquidity term
    oi_target_bonus: float = 5.0
    vol_ref: float = 5000.0          # fallback liquidity: volume giving 100
    oi_ref: float = 50000.0          # fallback liquidity: OI giving 100
    iv_contract_pts: float = 2.0     # assumed IV drop (vol points) when view=CONTRACT
    high_vol_em_pct: float = 2.0
    risk_free: float = 0.065
    carry: float = 0.0
    band_strong_trend: tuple = (0.60, 0.75)
    band_normal: tuple = (0.45, 0.60)
    band_high_vol: tuple = (0.35, 0.50)
    band_expiry_day: tuple = (0.55, 0.70)
    missing_penalty: float = 5.0     # confidence points lost per missing optional input

    @staticmethod
    def from_env() -> "StrikeConfig":
        d = StrikeConfig()
        return StrikeConfig(
            w_liq=_envf("STRIKE_W_LIQUIDITY", d.w_liq),
            w_em=_envf("STRIKE_W_EM_FIT", d.w_em),
            w_delta=_envf("STRIKE_W_DELTA", d.w_delta),
            w_theta=_envf("STRIKE_W_THETA", d.w_theta),
            w_gamma=_envf("STRIKE_W_GAMMA", d.w_gamma),
            w_vega=_envf("STRIKE_W_VEGA", d.w_vega),
            w_learn=_envf("STRIKE_W_LEARNING", d.w_learn),
            min_liquidity=_envf("STRIKE_MIN_LIQUIDITY", d.min_liquidity),
            max_spread_pct=_envf("STRIKE_MAX_SPREAD_PCT", d.max_spread_pct),
            max_age_sec=_envf("STRIKE_MAX_AGE_SEC", d.max_age_sec),
            min_confidence=_envf("STRIKE_MIN_CONFIDENCE", d.min_confidence),
            hold_min=_envf("STRIKE_EXPECTED_HOLD_MIN", d.hold_min),
            trading_min_per_day=_envf("STRIKE_TRADING_MIN_PER_DAY", d.trading_min_per_day),
            theta_risk_max=_envf("STRIKE_THETA_RISK_MAX", d.theta_risk_max),
            delta_decay=_envf("STRIKE_DELTA_DECAY", d.delta_decay),
            exec_no_live=_envf("STRIKE_EXEC_NO_LIVE", d.exec_no_live),
            exec_weight_in_liq=_envf("STRIKE_EXEC_WEIGHT_IN_LIQ", d.exec_weight_in_liq),
            oi_target_bonus=_envf("STRIKE_OI_TARGET_BONUS", d.oi_target_bonus),
            vol_ref=_envf("STRIKE_LIQ_VOL_REF", d.vol_ref),
            oi_ref=_envf("STRIKE_LIQ_OI_REF", d.oi_ref),
            iv_contract_pts=_envf("STRIKE_IV_CONTRACT_PTS", d.iv_contract_pts),
            high_vol_em_pct=_envf("STRIKE_HIGH_VOL_EM_PCT", d.high_vol_em_pct),
            risk_free=_envf("GREEKS_CHANGE_RISK_FREE_RATE", d.risk_free),
            carry=_envf("GREEKS_CHANGE_DIVIDEND_OR_CARRY", d.carry),
            band_strong_trend=_band("STRIKE_BAND_STRONG_TREND", d.band_strong_trend),
            band_normal=_band("STRIKE_BAND_NORMAL", d.band_normal),
            band_high_vol=_band("STRIKE_BAND_HIGH_VOL", d.band_high_vol),
            band_expiry_day=_band("STRIKE_BAND_EXPIRY_DAY", d.band_expiry_day),
            missing_penalty=_envf("STRIKE_MISSING_PENALTY", d.missing_penalty),
        )

    def band_for(self, regime_class: str) -> tuple:
        return {
            "STRONG_TREND": self.band_strong_trend,
            "HIGH_VOL": self.band_high_vol,
            "EXPIRY_DAY": self.band_expiry_day,
        }.get(regime_class, self.band_normal)


@dataclass(frozen=True)
class StrikeContext:
    """Per-signal market context (all optional; missing => neutral scores)."""
    spot: float
    side: str                         # "CE"/"PE"
    atm: float = 0.0
    step: float = 0.0
    regime: str = ""                  # raw md:regime value (BULLISH/BEARISH/NEUTRAL/NO_DATA)
    regime_class: str = "NORMAL"      # STRONG_TREND / NORMAL / HIGH_VOL / EXPIRY_DAY
    em: Optional[float] = None        # expected move, rupees
    em_lower: Optional[float] = None
    em_upper: Optional[float] = None
    years_to_expiry: Optional[float] = None
    iv_view: str = ""                 # "CONTRACT" / "EXPAND" / ""
    oi_target_strike: Optional[float] = None
    hold_min: Optional[float] = None  # None -> cfg.hold_min
    learning_stats: Optional[Mapping[str, Mapping]] = None
    use_learning: bool = False
    learn_min_samples: int = 50

    @property
    def em_pct(self) -> Optional[float]:
        if self.em is None or self.spot <= 0:
            return None
        return abs(self.em) / self.spot * 100.0


@dataclass(frozen=True)
class StrikeCandidate:
    # identity
    strike: float
    token: str = ""
    tradingsymbol: str = ""
    side: str = ""
    lot_size: Optional[int] = None
    freeze_qty: Optional[int] = None
    # raw inputs (None = missing)
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None      # per day, premium points
    vega: Optional[float] = None       # per vol point
    iv: Optional[float] = None         # annualised decimal
    premium: Optional[float] = None    # live mid only; never modelled
    liquidity: Optional[float] = None  # 0-100 from md:liquidity:score
    spread_pct: Optional[float] = None
    oi: Optional[float] = None
    volume: Optional[float] = None
    live_bidask: bool = False
    data_age_sec: Optional[float] = None
    # projected at +1 EM (in-process Black-Scholes)
    proj_delta: Optional[float] = None
    proj_gamma: Optional[float] = None
    # outputs
    score: float = 0.0
    confidence: float = 0.0
    rejected: str = ""                 # "" when eligible
    reasons: tuple = ()
    liquidity_score: Optional[float] = None
    exec_quality: Optional[float] = None
    em_fit: Optional[float] = None
    delta_score: Optional[float] = None
    theta_score: Optional[float] = None
    gamma_score: Optional[float] = None
    vega_score: Optional[float] = None
    theta_risk: Optional[float] = None
    hist_score: Optional[float] = None
    bucket: str = ""
    strike_bucket: str = ""
    delta_band: str = ""

    def as_top3_dict(self) -> dict:
        def r(v, n=4):
            return None if v is None else round(float(v), n)
        return {
            "strike": self.strike, "token": self.token, "tradingsymbol": self.tradingsymbol,
            "score": r(self.score, 2), "confidence": r(self.confidence, 2),
            "delta": r(self.delta), "gamma": r(self.gamma, 6), "theta": r(self.theta),
            "vega": r(self.vega), "iv": r(self.iv), "premium": r(self.premium, 2),
            "lot_size": self.lot_size, "freeze_qty": self.freeze_qty,
            "theta_risk": r(self.theta_risk), "liquidity_score": r(self.liquidity_score, 2),
            "exec_quality": r(self.exec_quality, 2), "em_fit": r(self.em_fit, 2),
            "strike_bucket": self.strike_bucket,
        }


def _clip(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def classify_regime(
    market_regime: str,
    signal: str,
    em_pct: Optional[float],
    years_to_expiry: Optional[float],
    cfg: StrikeConfig,
) -> str:
    """Map md:regime (BULLISH/BEARISH/NEUTRAL/NO_DATA) + EM + DTE onto the delta-band regimes."""
    if years_to_expiry is not None and years_to_expiry * 365.0 < 1.0:
        return "EXPIRY_DAY"
    if em_pct is not None and em_pct >= cfg.high_vol_em_pct:
        return "HIGH_VOL"
    mr = (market_regime or "").upper()
    sig = (signal or "").upper()
    if (mr == "BULLISH" and sig == "BUY CALL") or (mr == "BEARISH" and sig == "BUY PUT"):
        return "STRONG_TREND"
    return "NORMAL"


def project_greeks(
    ctx: StrikeContext, strike: float, iv: Optional[float], cfg: StrikeConfig
) -> tuple[Optional[float], Optional[float]]:
    """Delta/gamma after a +1 EM move in the signal direction (pure Black-Scholes reprice)."""
    if ctx.em is None or iv is None or ctx.years_to_expiry is None or ctx.spot <= 0:
        return None, None
    hold = ctx.hold_min if ctx.hold_min is not None else cfg.hold_min
    t = ctx.years_to_expiry - hold / (365.0 * 1440.0)
    if t <= 0 or iv <= 0:
        return None, None
    moved = ctx.spot + abs(ctx.em) * (1 if ctx.side == "CE" else -1)
    try:
        p = reprice_option(moved, strike, ctx.side, t, iv, cfg.risk_free, cfg.carry)
        return float(p.delta), float(p.gamma)
    except Exception:
        return None, None


def _exec_quality(c: StrikeCandidate, cfg: StrikeConfig) -> float:
    if not c.live_bidask or c.spread_pct is None:
        return cfg.exec_no_live
    half = cfg.max_spread_pct / 2.0
    if c.spread_pct <= half:
        return 100.0
    return _clip(100.0 * (cfg.max_spread_pct - c.spread_pct) / half)


def _liquidity(c: StrikeCandidate, cfg: StrikeConfig) -> Optional[float]:
    if c.liquidity is not None:
        return _clip(float(c.liquidity))
    if c.oi is None and c.volume is None:
        return None
    parts = []
    if c.oi is not None:
        parts.append(min(1.0, max(0.0, c.oi) / cfg.oi_ref))
    if c.volume is not None:
        parts.append(min(1.0, max(0.0, c.volume) / cfg.vol_ref))
    return 100.0 * sum(parts) / len(parts)


def em_fit_score(strike: float, ctx: StrikeContext) -> Optional[float]:
    """100 inside expected range; linear to 0 at 2x EM beyond the range edge."""
    if ctx.em is None or ctx.em <= 0:
        return None
    lo = ctx.em_lower if ctx.em_lower is not None else ctx.spot - ctx.em
    hi = ctx.em_upper if ctx.em_upper is not None else ctx.spot + ctx.em
    if lo <= strike <= hi:
        return 100.0
    dist = (lo - strike) if strike < lo else (strike - hi)
    return _clip(100.0 * (1.0 - dist / (2.0 * ctx.em)))


def delta_suitability(delta: float, band: tuple, decay: float) -> float:
    d = abs(delta)
    lo, hi = band
    if lo <= d <= hi:
        return 100.0
    dist = (lo - d) if d < lo else (d - hi)
    return _clip(100.0 * (1.0 - dist / max(decay, 1e-9)))


def score_strike(
    cand: StrikeCandidate,
    ctx: StrikeContext,
    cfg: Optional[StrikeConfig] = None,
    gamma_ref: Optional[float] = None,
) -> StrikeCandidate:
    """
    Score one strike (Strike Score = 25% Liquidity + 25% EM fit + 20% Delta + 15% Theta
    + 10% Gamma + 5% Vega; doc section 10). Returns a copy with score/confidence/reasons;
    `rejected` is non-empty if a hard filter failed. Missing optional data => neutral
    component plus a confidence penalty (nothing is invented).
    """
    cfg = cfg or StrikeConfig()
    hold = ctx.hold_min if ctx.hold_min is not None else cfg.hold_min
    sb = strike_bucket(cand.strike, ctx.atm or ctx.spot, ctx.step, ctx.side)
    base = replace(cand, side=cand.side or ctx.side, strike_bucket=sb)

    # ---- hard filters ----
    if base.delta is None or base.gamma is None or base.theta is None or base.vega is None:
        return replace(base, rejected="no_greeks")
    if base.data_age_sec is not None and base.data_age_sec > cfg.max_age_sec:
        return replace(base, rejected="stale_data")
    liq = _liquidity(base, cfg)
    if liq is None:
        return replace(base, rejected="no_liquidity_data")
    if liq < cfg.min_liquidity:
        return replace(base, liquidity_score=liq, rejected="low_liquidity")
    if base.live_bidask and base.spread_pct is not None and base.spread_pct > cfg.max_spread_pct:
        return replace(base, liquidity_score=liq, rejected="wide_spread")

    reasons: list[str] = []
    missing = 0

    exq = _exec_quality(base, cfg)
    if not base.live_bidask:
        missing += 1
    liq_term = (1.0 - cfg.exec_weight_in_liq) * liq + cfg.exec_weight_in_liq * exq
    reasons.append(f"liquidity {liq:.0f}" + ("" if base.live_bidask else " (no live bid-ask)"))

    emf = em_fit_score(base.strike, ctx)
    if emf is None:
        emf_s = 50.0
        missing += 1
        reasons.append("expected move unavailable (neutral)")
    else:
        emf_s = emf
        reasons.append("inside expected move" if emf >= 100.0 else f"EM fit {emf:.0f}")

    band = cfg.band_for(ctx.regime_class)
    dsc = delta_suitability(base.delta, band, cfg.delta_decay)
    reasons.append(f"delta {abs(base.delta):.2f} vs {ctx.regime_class} band {band[0]:.2f}-{band[1]:.2f}")

    # theta efficiency: theta cost over expected hold, relative to premium
    theta_risk = None
    if base.premium is not None and base.premium > 0:
        theta_risk = abs(base.theta) * (hold / cfg.trading_min_per_day) / base.premium
        tsc = _clip(100.0 * (1.0 - theta_risk / cfg.theta_risk_max))
        reasons.append(f"theta risk {theta_risk * 100:.1f}% over {hold:.0f}m")
    else:
        tsc = 50.0
        missing += 1
        reasons.append("premium unavailable (theta neutral)")

    # gamma: rewarded in trend, penalised in chop; relative to best candidate
    g_now = base.proj_gamma if base.proj_gamma is not None else base.gamma
    norm = 0.5
    if gamma_ref and gamma_ref > 0 and g_now is not None:
        norm = _clip(g_now / gamma_ref, 0.0, 1.0)
    choppy = ctx.regime.upper() == "NEUTRAL" and ctx.regime_class == "NORMAL"
    if ctx.regime_class == "STRONG_TREND":
        gsc = 50.0 + 50.0 * norm
        reasons.append("gamma rewarded (trend)")
    elif choppy:
        gsc = 100.0 - 60.0 * norm
        reasons.append("gamma penalised (choppy)")
    else:
        gsc = 50.0 + 25.0 * norm

    # vega: penalise when IV expected to contract
    if ctx.iv_view.upper() == "CONTRACT" and base.premium and base.premium > 0:
        loss = abs(base.vega) * cfg.iv_contract_pts / base.premium
        vsc = _clip(100.0 * (1.0 - loss / 0.10))
        reasons.append("vega penalised (IV contraction)")
    else:
        vsc = 70.0

    num = (cfg.w_liq * liq_term + cfg.w_em * emf_s + cfg.w_delta * dsc
           + cfg.w_theta * tsc + cfg.w_gamma * gsc + cfg.w_vega * vsc)
    den = cfg.w_liq + cfg.w_em + cfg.w_delta + cfg.w_theta + cfg.w_gamma + cfg.w_vega

    # learning term: only with enough samples, else skipped (base weights untouched)
    hist = None
    bkey = bucket_key(ctx.regime, em_bucket(ctx.em_pct), sb, delta_band(base.delta))
    if ctx.use_learning and ctx.learning_stats:
        st = ctx.learning_stats.get(bkey)
        try:
            if st and int(st.get("n", 0)) >= ctx.learn_min_samples and st.get("hist_score") is not None:
                hist = _clip(float(st["hist_score"]))
        except Exception:
            hist = None
    if hist is not None:
        score = (1.0 - cfg.w_learn) * (num / den) + cfg.w_learn * hist  # others renormalised
        reasons.append(f"historical bucket score {hist:.0f}")
    else:
        score = num / den

    if ctx.oi_target_strike and abs(base.strike - float(ctx.oi_target_strike)) < 1e-6:
        score += cfg.oi_target_bonus
        reasons.append("OI target strike bonus")
    score = _clip(score)

    if ctx.regime in ("", "NA", "NO_DATA"):
        missing += 1
    conf = _clip(score - cfg.missing_penalty * missing)

    return replace(
        base,
        score=round(score, 2), confidence=round(conf, 2), reasons=tuple(reasons),
        liquidity_score=liq, exec_quality=exq, em_fit=emf, delta_score=dsc,
        theta_score=tsc, gamma_score=gsc, vega_score=vsc, theta_risk=theta_risk,
        hist_score=hist, bucket=bkey, delta_band=delta_band(base.delta),
    )


def rank_strikes(
    cands: Sequence[StrikeCandidate],
    ctx: StrikeContext,
    cfg: Optional[StrikeConfig] = None,
) -> list[StrikeCandidate]:
    """
    Score all candidates, best first; rejected ones trail. Projected (+1 EM)
    Greeks are computed in-process when a candidate lacks them.
    """
    cfg = cfg or StrikeConfig()
    prepared = []
    for c in cands:
        if c.proj_delta is None and c.proj_gamma is None:
            pd_, pg_ = project_greeks(ctx, c.strike, c.iv, cfg)
            c = replace(c, proj_delta=pd_, proj_gamma=pg_)
        prepared.append(c)
    gs = [(c.proj_gamma if c.proj_gamma is not None else c.gamma) for c in prepared]
    gs = [g for g in gs if g is not None and g > 0]
    gref = max(gs) if gs else None
    scored = [score_strike(c, ctx, cfg, gref) for c in prepared]
    ok = sorted((c for c in scored if not c.rejected), key=lambda c: (-c.score, -c.confidence, c.strike))
    bad = [c for c in scored if c.rejected]
    return ok + bad


def build_candidates(
    side_rows: Sequence[Mapping],
    quotes: Mapping[str, Mapping],
) -> list[StrikeCandidate]:
    """ScripMaster rows (token/tradingsymbol/strike/lotsize/freeze_qty) + quotes[token] -> candidates."""
    def f(d, k):
        try:
            v = d.get(k)
            return None if v is None or v == "" else float(v)
        except Exception:
            return None

    def i(d, k):
        v = f(d, k)
        return None if v is None else int(v)

    out = []
    for row in side_rows:
        q = quotes.get(str(row.get("token") or "")) or {}
        out.append(StrikeCandidate(
            strike=float(row["strike"]), token=str(row.get("token") or ""),
            tradingsymbol=str(row.get("tradingsymbol") or ""), side=str(row.get("cp") or ""),
            lot_size=i(row, "lotsize"), freeze_qty=i(row, "freeze_qty"),
            delta=f(q, "delta"), gamma=f(q, "gamma"), theta=f(q, "theta"), vega=f(q, "vega"),
            iv=f(q, "iv"), premium=f(q, "premium"), liquidity=f(q, "liquidity"),
            spread_pct=f(q, "spread_pct"), oi=f(q, "oi"), volume=f(q, "volume"),
            live_bidask=bool(q.get("live_bidask")), data_age_sec=f(q, "data_age_sec"),
        ))
    return out


def top3_json(res: StrikeSelectResult) -> str:
    return json.dumps([c.as_top3_dict() for c in res.top3], separators=(",", ":"))


def _strike_select_scored(
    sig: str,
    sym: str,
    cp: str,
    spot: float,
    df: pd.DataFrame,
    oi_target_strike: Optional[float],
    quotes: Mapping[str, Mapping],
    extra: dict,
    cfg: StrikeConfig,
    around: int,
) -> StrikeSelectResult:
    around = max(1, min(int(around), 10))
    contracts, expiry = build_atm_option_tokens(df, sym, spot, around)
    if not contracts or not expiry:
        return _skip(sig, "no_option_chain", spot)
    side_rows = [c for c in contracts if str(c.get("cp") or "").upper() == cp]
    if not side_rows:
        return _skip(sig, f"no_{cp}_contracts", spot)

    atm = float(min(side_rows, key=lambda c: abs(float(c["strike"]) - spot))["strike"])
    strikes = sorted({float(c["strike"]) for c in side_rows})
    diffs = [round(b - a, 6) for a, b in zip(strikes, strikes[1:]) if b > a]
    step = min(diffs) if diffs else 0.0

    ctx0 = StrikeContext(
        spot=spot, side=cp, atm=atm, step=step,
        regime=str(extra.get("regime") or "").upper(),
        em=extra.get("em"), em_lower=extra.get("em_lower"), em_upper=extra.get("em_upper"),
        years_to_expiry=extra.get("years_to_expiry"),
        iv_view=str(extra.get("iv_view") or ""),
        oi_target_strike=oi_target_strike if oi_target_strike and oi_target_strike > 0 else None,
        hold_min=extra.get("hold_min"),
        learning_stats=extra.get("learning_stats"),
        use_learning=bool(extra.get("use_learning")),
        learn_min_samples=int(extra.get("learn_min_samples", 50)),
    )
    rc = classify_regime(ctx0.regime, sig, ctx0.em_pct, ctx0.years_to_expiry, cfg)
    ctx = replace(ctx0, regime_class=rc)
    hold = ctx.hold_min if ctx.hold_min is not None else cfg.hold_min

    ranked = rank_strikes(build_candidates(side_rows, quotes), ctx, cfg)
    eligible = [c for c in ranked if not c.rejected]
    common = dict(
        regime=ctx.regime, regime_class=rc, em_pct=ctx.em_pct,
        em_bucket=em_bucket(ctx.em_pct), hold_min=float(hold),
    )
    if not eligible:
        why = ranked[0].rejected if ranked else "no_candidates"
        return _skip(sig, f"no_eligible_strike:{why}", spot, **common)

    best = eligible[0]
    n = int(round((best.strike - atm) / step)) if step > 0 else 0
    if ctx.oi_target_strike and abs(best.strike - ctx.oi_target_strike) < 1e-6:
        mode = "oi_target"
    else:
        k = n if cp == "CE" else -n
        mode = "atm" if k == 0 else (f"otm{k}" if k > 0 else f"itm{-k}")

    fields = dict(
        common, signal=sig, side=cp, mode=mode, spot=spot, atm=atm, strike=best.strike,
        step=float(step), expiry=str(expiry), token=best.token,
        tradingsymbol=best.tradingsymbol, exchange="NFO",
        strike_score=best.score, confidence=best.confidence, reasons=best.reasons,
        delta_band=best.delta_band, strike_bucket=best.strike_bucket, best=best,
        top3=tuple(eligible[:3]),
    )
    if best.confidence < cfg.min_confidence:
        return StrikeSelectResult(status="SKIP", reason="below_confidence", **fields)
    return StrikeSelectResult(status="OK", reason="ok", **fields)


def strike_select(
    signal: str,
    strength: str,
    underlying: str,
    spot: float,
    df: pd.DataFrame,
    offset_base: int = 0,
    offset_strong: int = 1,
    oi_target_strike: Optional[float] = None,
    quotes: Optional[Mapping[str, Mapping]] = None,
    ctx_extra: Optional[Mapping] = None,
    cfg: Optional[StrikeConfig] = None,
    candidates_around: int = 3,
) -> StrikeSelectResult:
    """
    If `quotes` (token -> inputs dict, see build_candidates) is given, ATM +/-
    `candidates_around` strikes on the signal side are scored and the best is
    returned (SKIP below_confidence if best confidence < cfg.min_confidence; the
    OI target is then a score bonus, not an override). `ctx_extra` may carry
    regime, em, em_lower, em_upper, years_to_expiry, iv_view, hold_min,
    learning_stats, use_learning, learn_min_samples.

    Without `quotes`, legacy behaviour: BUY CALL -> CE, BUY PUT -> PE; base -> ATM,
    strong -> +offset_strong steps OTM; oi_target_strike OVERRIDES the offset
    (nearest available contract is chosen if the window does not contain it).
    """
    sig = (signal or "").strip().upper()
    strength_l = (strength or "").strip().lower()
    sym = (underlying or "").strip().upper()

    log.debug(
        "LOGIC_IN signal=%s strength=%s underlying=%s spot=%s offset_base=%s offset_strong=%s oi_target=%s",
        sig, strength_l, sym, spot, offset_base, offset_strong, oi_target_strike,
    )

    if not sym:
        return _skip(sig or "NEUTRAL", "missing_symbol", spot)
    if spot is None or spot <= 0:
        return _skip(sig or "NEUTRAL", "invalid_spot", float(spot or 0.0))

    if sig == "BUY CALL":
        cp = "CE"
        direction = 1
    elif sig == "BUY PUT":
        cp = "PE"
        direction = -1
    else:
        return _skip(sig or "NEUTRAL", "not_buy_signal", float(spot))

    if quotes is not None:
        result = _strike_select_scored(
            sig, sym, cp, float(spot), df, oi_target_strike, quotes,
            dict(ctx_extra or {}), cfg or StrikeConfig(), candidates_around,
        )
        log.debug("LOGIC_OUT %s", result)
        return result

    offset = offset_strong if strength_l == "strong" else offset_base
    try:
        offset = int(offset)
    except Exception:
        offset = 0
    offset = abs(offset)

    # Widen the fetch window when an OI target implies more strikes-out.
    around = max(offset, 0)
    if oi_target_strike and spot > 0:
        approx_steps = int(abs(oi_target_strike - spot) / max(spot * 0.005, 0.01))
        around = max(around, min(approx_steps, 10))

    contracts, expiry = build_atm_option_tokens(df, sym, float(spot), around)
    if not contracts or not expiry:
        return _skip(sig, "no_option_chain", float(spot))

    side_rows = [c for c in contracts if str(c.get("cp") or "").upper() == cp]
    if not side_rows:
        return _skip(sig, f"no_{cp}_contracts", float(spot))

    atm_row = min(side_rows, key=lambda c: abs(float(c["strike"]) - float(spot)))
    atm = float(atm_row["strike"])

    strikes = sorted({float(c["strike"]) for c in side_rows})
    step = 0.0
    if len(strikes) >= 2:
        diffs = [round(strikes[i + 1] - strikes[i], 6) for i in range(len(strikes) - 1)]
        diffs = [d for d in diffs if d > 0]
        step = min(diffs) if diffs else 0.0

    use_oi_target = oi_target_strike is not None and oi_target_strike > 0
    if use_oi_target:
        target = float(oi_target_strike)
        mode = "oi_target"
    else:
        target = atm + (direction * offset * step) if step > 0 else atm
        mode = "atm" if offset == 0 else f"otm{offset}"

    chosen = min(side_rows, key=lambda c: abs(float(c["strike"]) - target))

    result = StrikeSelectResult(
        status="OK", signal=sig, side=cp, mode=mode, spot=float(spot), atm=atm,
        strike=float(chosen["strike"]), step=float(step), expiry=str(expiry),
        token=str(chosen.get("token") or ""),
        tradingsymbol=str(chosen.get("tradingsymbol") or ""),
        exchange=str(chosen.get("exchange") or "NFO"), reason="ok",
    )
    log.debug("LOGIC_OUT %s", result)
    return result

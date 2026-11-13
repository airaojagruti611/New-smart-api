import pandas as pd

from app.strike_select import (
    StrikeCandidate, StrikeConfig, StrikeContext, em_fit_score, rank_strikes,
    score_strike, strike_select,
)

CFG = StrikeConfig()


def _ctx(**kw):
    d = dict(spot=24480.0, side="CE", atm=24500.0, step=50.0, regime="BULLISH",
             regime_class="STRONG_TREND", em=100.0, years_to_expiry=3 / 365.0)
    d.update(kw)
    return StrikeContext(**d)


def _cand(strike, delta, gamma, theta, premium, liq=92.0, spread=0.6, **kw):
    d = dict(strike=strike, token=str(int(strike)), tradingsymbol=f"NIFTY{int(strike)}CE",
             side="CE", delta=delta, gamma=gamma, theta=theta, vega=6.0, iv=0.15,
             premium=premium, liquidity=liq, spread_pct=spread, live_bidask=True)
    d.update(kw)
    return StrikeCandidate(**d)


def _doc_chain():
    return [
        _cand(24500, 0.62, 0.0011, -9.0, 150.0, liq=96, spread=0.4),
        _cand(24600, 0.40, 0.0010, -8.5, 85.0, liq=86),
        _cand(24700, 0.25, 0.0007, -6.0, 40.0, liq=78),
        _cand(24800, 0.13, 0.0004, -3.5, 18.0, liq=72),
    ]


def test_doc_example_24500_wins_about_94():
    ranked = rank_strikes(_doc_chain(), _ctx(), CFG)
    assert ranked[0].strike == 24500
    assert 90 <= ranked[0].score <= 98
    assert [c.strike for c in ranked[1:]] == [24600, 24700, 24800]
    assert ranked[0].score > ranked[1].score + 10


def test_em_fit_decays_to_zero_at_2x_em_beyond_range():
    ctx = _ctx()  # range 24380..24580, em=100
    assert em_fit_score(24500, ctx) == 100.0
    assert em_fit_score(24680, ctx) == 50.0
    assert em_fit_score(24780, ctx) == 0.0
    assert em_fit_score(24500, _ctx(em=None)) is None


def test_hard_filters_reject_with_reason():
    c = score_strike(_cand(24500, .6, .001, -9, 150, liq=60), _ctx(), CFG)
    assert c.rejected == "low_liquidity"
    c = score_strike(_cand(24500, .6, .001, -9, 150, spread=9.0), _ctx(), CFG)
    assert c.rejected == "wide_spread"
    c = score_strike(_cand(24500, None, .001, -9, 150), _ctx(), CFG)
    assert c.rejected == "no_greeks"
    c = score_strike(_cand(24500, .6, .001, -9, 150, data_age_sec=999.0), _ctx(), CFG)
    assert c.rejected == "stale_data"
    c = score_strike(_cand(24500, .6, .001, -9, 150, liquidity=None), _ctx(), CFG)
    assert c.rejected == "no_liquidity_data"
    # OI/volume fallback when no liquidity score
    c = score_strike(_cand(24500, .6, .001, -9, 150, liquidity=None, oi=60000, volume=6000), _ctx(), CFG)
    assert not c.rejected and c.liquidity_score == 100.0


def test_missing_data_is_neutral_and_lowers_confidence():
    full = score_strike(_cand(24500, .62, .0011, -9, 150), _ctx(), CFG)
    sparse = score_strike(
        _cand(24500, .62, .0011, -9, None, live_bidask=False, spread_pct=None),
        _ctx(em=None, regime="NO_DATA"), CFG)
    assert not sparse.rejected
    assert sparse.confidence < sparse.score  # penalty applied
    assert sparse.confidence < full.confidence
    assert sparse.premium is None and sparse.theta_risk is None


def test_regime_changes_delta_band():
    c = _cand(24500, 0.68, .0011, -9, 150)
    trend = score_strike(c, _ctx(regime_class="STRONG_TREND"), CFG)
    hv = score_strike(c, _ctx(regime_class="HIGH_VOL"), CFG)
    assert trend.delta_score == 100.0 and hv.delta_score < 100.0


def test_learning_term_needs_min_samples():
    c = _cand(24500, .62, .0011, -9, 150)
    base = score_strike(c, _ctx(), CFG)
    key = base.bucket
    stats = {key: {"n": 10, "hist_score": 0.0}}
    low_n = score_strike(c, _ctx(learning_stats=stats, use_learning=True), CFG)
    assert low_n.score == base.score and low_n.hist_score is None
    stats = {key: {"n": 60, "hist_score": 0.0}}
    enough = score_strike(c, _ctx(learning_stats=stats, use_learning=True), CFG)
    assert enough.hist_score == 0.0 and enough.score < base.score
    assert abs(enough.score - base.score * 0.85) < 0.05  # 15% weight, others renormalised
    off = score_strike(c, _ctx(learning_stats=stats, use_learning=False), CFG)
    assert off.score == base.score


def test_oi_target_is_bonus_not_override():
    chain = _doc_chain()
    ranked = rank_strikes(chain, _ctx(oi_target_strike=24600.0), CFG)
    bonus = next(c for c in ranked if c.strike == 24600)
    plain = next(c for c in rank_strikes(chain, _ctx(), CFG) if c.strike == 24600)
    assert abs(bonus.score - plain.score - CFG.oi_target_bonus) < 0.05
    assert ranked[0].strike == 24500  # 24500 still wins


def _df():
    rows = []
    for k in range(24300, 24800, 50):
        for cp in ("CE", "PE"):
            rows.append({"token": f"{k}{cp}", "symbol": f"NIFTY27NOV26{k}{cp}", "name": "NIFTY",
                         "expiry": "27NOV2099", "strike": str(k * 100.0), "lotsize": "75",
                         "instrumenttype": "OPTIDX", "exch_seg": "NFO"})
    return rows


def _load(tmp_path):
    import json
    from app.scripmaster import load_scripmaster
    f = tmp_path / "sm.json"
    f.write_text(json.dumps(_df()), encoding="utf-8")
    return load_scripmaster(f)


def _quotes():
    q = {}
    for c in _doc_chain():
        q[f"{int(c.strike)}CE"] = dict(delta=c.delta, gamma=c.gamma, theta=c.theta, vega=c.vega,
                                       iv=c.iv, premium=c.premium, liquidity=c.liquidity,
                                       spread_pct=c.spread_pct, live_bidask=True)
    return q


def test_end_to_end_scored_and_legacy(tmp_path):
    df = _load(tmp_path)
    legacy = strike_select("BUY CALL", "base", "NIFTY", 24480.0, df)
    assert legacy.status == "OK" and legacy.strike_score is None and legacy.top3 == ()

    extra = dict(regime="BULLISH", em=100.0, years_to_expiry=3 / 365.0)
    res = strike_select("BUY CALL", "base", "NIFTY", 24480.0, df, quotes=_quotes(),
                        ctx_extra=extra, candidates_around=3)
    assert res.status == "OK" and res.strike == 24500.0
    assert res.best.lot_size == 75 and res.top3[0].strike == 24500.0
    assert res.strike_score >= 70 and res.regime_class == "STRONG_TREND"


def test_below_confidence_skips(tmp_path):
    df = _load(tmp_path)
    res = strike_select("BUY CALL", "base", "NIFTY", 24480.0, df, quotes=_quotes(),
                        ctx_extra=dict(regime="BULLISH", em=100.0, years_to_expiry=3 / 365.0),
                        cfg=StrikeConfig(min_confidence=99.5))
    assert res.status == "SKIP" and res.reason == "below_confidence"
    assert res.strike_score is not None

from app.capital_alloc import (
    Candidate, CapitalConfig, StrikeOption, allocate, candidate_from_fields,
    capital_alloc, deployable_budget, estimate_charges,
)

LOOSE = CapitalConfig(max_loss_pct=100.0, max_position_pct=100.0)


def opt(prem=100.0, lot=50, ts="X", **kw):
    return StrikeOption(strike=1.0, token=ts, tradingsymbol=ts, premium=prem, lot_size=lot, **kw)


def cand(sym, score=80.0, conf=100.0, side="CE", opts=None, side_pct=None):
    return Candidate(sym, side, score, conf, "TREND", side_pct, tuple(opts or [opt(ts=sym)]))


def test_budget_64000():
    assert deployable_budget(100000) == (20000, 80000, 64000)


def test_charges_positive_and_zero_guard():
    assert estimate_charges(100, 50) > 40  # 2 orders of brokerage + GST + STT etc.
    assert estimate_charges(0, 50) == 0


def test_scenario_a_four_trades():
    out = allocate([cand(f"S{i}") for i in range(4)], 100000, LOOSE)
    assert all(a.status == "OK" and a.lots == 3 for a in out)  # 16000 / ~5050 -> 3
    assert sum(a.allocated for a in out) <= 64000
    assert all(a.binding_limit == "L_budget" for a in out)


def test_scenario_b_twelve_trades():
    out = allocate([cand(f"S{i}") for i in range(12)], 100000, LOOSE)
    assert all(a.lots == 1 for a in out)  # 5333 each -> 1 lot
    assert sum(a.allocated for a in out) <= 64000


def test_zero_denominator():
    out = allocate([cand("A", score=0), cand("B", conf=0)], 100000, LOOSE)
    assert all(a.status == "SKIP" and a.reason == "zero_score_sum" for a in out)
    assert allocate([], 100000) == []


def test_floor_never_rounds_up():
    a = allocate([cand("A")], 100000, LOOSE)[0]  # 64000 / ~5050 = 12.67
    assert a.lots == 12 and a.allocated <= 64000


def test_fallback_to_affordable_strike():
    expensive = opt(prem=2000, lot=50, ts="BIG")  # ~100000 per lot
    cheap = opt(prem=100, lot=50, ts="CHEAP")
    a = allocate([cand("A", opts=[expensive, cheap])], 100000, LOOSE)[0]
    assert a.status == "OK" and a.tradingsymbol == "CHEAP"
    only = allocate([cand("A", opts=[expensive])], 100000, LOOSE)[0]
    assert only.status == "SKIP" and only.reason.startswith("zero_lots")


def test_risk_limit_binds_and_missing_price_rejected():
    a = allocate([cand("A")], 100000, CapitalConfig(max_position_pct=100.0))[0]
    assert a.binding_limit == "L_risk" and a.lots == 3  # 5000 / (100*.3*50) = 3.33
    bad = allocate([cand("A", opts=[opt(prem=0)])], 100000, LOOSE)[0]
    assert bad.status == "SKIP" and bad.reason == "no_priced_option"


def test_side_cap_and_legacy_compat():
    cs = [cand("A", side="CE", side_pct=25), cand("B", side="PE", side_pct=75)]
    out = allocate(cs, 100000, LOOSE)
    assert out[0].allocated <= 16000
    r = capital_alloc("BUY CALL", {"regime": "TREND", "call_alloc_pct": 60, "put_alloc_pct": 40}, 100000)
    assert r.status == "OK" and r.side_budget == 60000.0


def test_candidate_from_fields_tolerates_missing():
    f = {"symbol": "nifty", "signal": "BUY CALL", "strike_score": "80", "confidence": "90",
         "premium": "100", "lot_size": "50", "tradingsymbol": "N1",
         "top3": '[{"tradingsymbol":"N2","premium":50,"lot_size":50}]'}
    c = candidate_from_fields(f, {})
    assert c.symbol == "NIFTY" and len(c.options) == 2 and c.side_alloc_pct is None
    assert candidate_from_fields({"signal": "NEUTRAL"}) is None


def test_fallback_strike_never_borrows_primary_premium():
    import json
    f = {"signal": "BUY CALL", "symbol": "NIFTY", "strike": "24500", "tradingsymbol": "A",
         "premium": "100", "lot_size": "75", "strike_score": "90", "confidence": "90",
         "regime_class": "HIGH_VOL",
         "top3": json.dumps([{"strike": 24500, "tradingsymbol": "A", "premium": 100, "liquidity_score": 30},
                             {"strike": 24600, "tradingsymbol": "B", "premium": None}])}
    c = candidate_from_fields(f, {})
    assert [o.tradingsymbol for o in c.options] == ["A"]  # B has no own price -> dropped
    assert c.options[0].liquidity_score == 30             # primary enriched from its top3 row
    assert c.regime_class == "HIGH_VOL"

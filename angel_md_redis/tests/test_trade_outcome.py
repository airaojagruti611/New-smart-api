import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.trade_outcome import (IST, PaperConfig, close_trade, exit_reason, on_tick,
                               open_trade, target_price)

CFG = PaperConfig(target_pct=30, sl_pct=20, max_hold_min=60, no_tick_timeout_sec=120, session_end_hhmm=1520)
T0 = int(datetime(2026, 11, 11, 10, 0, tzinfo=IST).timestamp() * 1000)
SEL = {"tradingsymbol": "X24500CE", "symbol": "X", "side": "CE", "premium": "100", "delta": "0.5",
       "spot": "24500", "em_pct": "0.4", "lot_size": "50", "hold_min": "30", "strike": "24500",
       "atm": "24500", "step": "50", "regime": "TREND_UP"}


def mk(**kw):
    return open_trade({**SEL, **kw}, T0, CFG)


def test_target_uses_delta_em_points_and_fallback():
    t = mk()
    assert abs(t.target_px - (100 + 0.5 * 24500 * 0.004)) < 1e-9  # 149
    assert t.sl_px == 80.0
    assert target_price(100, None, None, None, 30) == 130.0


def test_mfe_mae_and_pnl_per_lot():
    t = mk()
    for i, px in enumerate([110, 90, 105, 95]):
        t = on_tick(t, px, T0 + (i + 1) * 1000, CFG)
    row = close_trade(t, "timeout", T0 + 10_000)
    assert float(row["mfe"]) == 10 and float(row["mae"]) == 10
    assert float(row["pnl_pts"]) == -5 and float(row["pnl_per_lot"]) == -250
    assert row["excluded_from_stats"] == "false" and row["bucket_key"].startswith("TREND_UP|")


def test_exit_stop_before_target_and_target():
    assert exit_reason(on_tick(mk(), 79, T0 + 1, CFG), T0 + 2, CFG) == "stop"
    assert exit_reason(on_tick(mk(), 150, T0 + 1, CFG), T0 + 2, CFG) == "target"
    assert exit_reason(on_tick(mk(), 100, T0 + 1, CFG), T0 + 2, CFG) is None


def test_exit_timeout_and_session_end():
    t = on_tick(mk(), 101, T0 + 1, CFG)
    assert exit_reason(t, T0 + 30 * 60_000 + 1, CFG) == "timeout"
    late = int(datetime(2026, 11, 11, 15, 20, tzinfo=IST).timestamp() * 1000)
    assert exit_reason(t, late, CFG) == "session_end"


def test_no_ticks_excluded():
    t = mk(premium="")
    assert t.entry_premium is None
    assert exit_reason(t, T0 + 119_000, CFG) is None
    assert exit_reason(t, T0 + 121_000, CFG) == "no_ticks"
    row = close_trade(t, "no_ticks", T0 + 121_000)
    assert row["exit_reason"] == "no_ticks" and row["excluded_from_stats"] == "true"
    assert row["pnl_pts"] == ""


def test_pending_trade_enters_on_first_tick():
    t = on_tick(mk(premium=""), 120, T0 + 5000, CFG, bid=119, ask=121)
    assert t.entry_premium == 120 and t.entry_spread == 2 and t.sl_px == 96.0


def test_actual_move_vs_expected_ce():
    from app.trade_outcome import on_spot
    t = mk()  # CE, spot 24500, em 0.4%
    for px in (24520, 24600, 24450, 24549):
        t = on_spot(t, px)
    row = close_trade(on_tick(t, 105, T0 + 60_000, CFG), "timeout", T0 + 120_000)
    assert row["spot_exit"] == "24549.00"
    assert abs(float(row["actual_move_pct"]) - 0.2) < 1e-6
    assert abs(float(row["dir_move_pct"]) - 0.2) < 1e-6
    assert abs(float(row["move_vs_em"]) - 0.5) < 1e-6
    assert abs(float(row["fav_move_pct"]) - 100 * 100 / 24500) < 1e-3
    assert abs(float(row["adv_move_pct"]) - 100 * 50 / 24500) < 1e-3


def test_actual_move_pe_signed_in_favour_and_missing_spot():
    from app.trade_outcome import on_spot
    t = on_spot(mk(side="PE", tradingsymbol="X24500PE"), 24255)  # -1%
    row = close_trade(t, "timeout", T0 + 1000)
    assert abs(float(row["dir_move_pct"]) - 1.0) < 1e-6
    assert abs(float(row["fav_move_pct"]) - 1.0) < 1e-6
    assert row["adv_move_pct"] == "0.0000"
    row = close_trade(mk(), "timeout", T0 + 1000)  # no spot ticks
    assert row["actual_move_pct"] == "" and row["spot_exit"] == ""


def test_paper_trade_restores_old_persisted_doc():
    import dataclasses
    from app.trade_outcome import PaperTrade
    d = dataclasses.asdict(mk())
    for k in ("spot_last", "spot_max", "spot_min"):
        d.pop(k)
    assert PaperTrade(**d).spot_last is None


def test_tracker_apply_spot_ticks():
    import run_paper_tracker as rpt
    open_t = {"X24500CE": mk(), "Y1CE": mk(symbol="Y", tradingsymbol="Y1CE")}
    rpt.apply_spot_ticks(open_t, [("1-0", {"symbol": "x", "ltp": "24600"}), ("2-0", {"symbol": "Z", "ltp": "1"})])
    assert open_t["X24500CE"].spot_last == 24600 and open_t["Y1CE"].spot_last is None

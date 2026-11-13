"""Tests for Module 18 — Adaptive Trailing Stop Loss & Re-entry (DECISION.md §5)."""

from __future__ import annotations

import unittest
from dataclasses import replace

from app.adaptive_tsl import (
    BLOCKED,
    DONE,
    EXPIRED,
    IN_TRADE,
    PENDING,
    WAITING,
    Chain,
    Snapshot,
    TradeState,
    TSLConfig,
    apply_profit_tiers,
    band_floor,
    evaluate_reentry,
    exit_output,
    manage,
    new_trade,
    on_reentry_opened,
    on_trade_closed,
    pending_timed_out,
    revert_pending,
    select_tsl_pct,
    trend_strength,
    volatility_score,
    watch_price_for,
)

CFG5 = TSLConfig(range_strong_low_vol=(5.0, 5.0))   # pin the brief's 5 % example

# Strong bullish trend, low volatility (spot 2000).
STRONG = dict(
    spot=2000.0, atr_pct=0.06, em_pct=0.4, iv_change_pct=0.0, gamma=0.001, dte=10,
    st_bias="CALL", st_bullish=4, st_bearish=0, st_total=4, ema9=2004.0, ema26=2000.0,
    direction_score=0.8, em_direction="STRONG_BULLISH", em_confidence=90.0,
    volume_signal="Strong Bullish Volume", volume_surge=True, amd_phase="MARKUP",
    contract_imbalance="BULLISH", liquidity=80.0, spread_pct=1.0, oi_buildup="LONG_BUILDUP",
    und_close=2010.0, und_swing_high=2008.0, und_swing_low=1990.0,
)
# Everything confirming an exit for a CE.
WEAK = dict(STRONG, contract_imbalance="BEARISH", direction_score=-0.2, ema9=1999.0, ema26=2000.0,
            st_bias="NEUTRAL", st_bullish=2, amd_phase="NEUTRAL", volume_signal="Bearish Volume",
            volume_surge=False, em_confidence=30.0)


def snap(now_ms: int, bid: float, **kw) -> Snapshot:
    base = dict(STRONG)
    base.update(kw)
    return Snapshot(now_ms=now_ms, bid=bid, ask=bid + 0.1, **base)


def trade(entry=100.0, sl=70.0, side="CE", **kw) -> TradeState:
    return new_trade("T1", "T1", 0, "SBIN", "SBIN26OCT800CE", side, 750, entry, sl, 0,
                     entry_direction_score=0.8, **kw)


def run(st, cfg, *ticks):
    res = None
    for t in ticks:
        res = manage(st, t, cfg)
        st = res.state
    return res


class ScoresTest(unittest.TestCase):
    def test_strong_trend_low_volatility(self):
        vol, band, _c, missing = volatility_score(snap(0, 100), TSLConfig())
        self.assertLess(vol, 40)
        self.assertEqual((band, missing), ("LOW", []))
        score, label, _c, _m = trend_strength(snap(0, 100), "CE", TSLConfig())
        self.assertGreaterEqual(score, 80)
        self.assertEqual(label, "EXPLOSIVE")

    def test_trend_is_side_aligned(self):
        _s, label, _c, _m = trend_strength(snap(0, 100), "PE", TSLConfig())
        self.assertEqual(label, "WEAK")

    def test_missing_volatility_renormalised(self):
        vol, _b, comps, missing = volatility_score(Snapshot(now_ms=0, atr_pct=0.40, dte=None), TSLConfig())
        self.assertEqual(vol, 100.0)
        self.assertIn("dte", missing)
        self.assertIsNone(comps["iv"])
        self.assertEqual(volatility_score(Snapshot(now_ms=0), TSLConfig())[1], "UNKNOWN")


class SelectorTest(unittest.TestCase):
    cfg = TSLConfig()

    def test_rules(self):
        c = self.cfg
        self.assertEqual(select_tsl_pct("STRONG", 20, "LOW", 5, 10, False, c), (5.0, "STRONG_LOW_VOL"))
        self.assertEqual(select_tsl_pct("EXPLOSIVE", 100, "HIGH", 5, 10, False, c), (10.0, "STRONG_HIGH_VOL"))
        self.assertEqual(select_tsl_pct("STRONG", 40, "MEDIUM", 5, 10, False, c), (8.0, "STRONG_HIGH_VOL"))
        self.assertEqual(select_tsl_pct("MODERATE", 50, "MEDIUM", 5, 10, False, c), (11.0, "MODERATE"))
        self.assertEqual(select_tsl_pct("WEAK", 0, "LOW", 5, 10, False, c), (12.0, "WEAK"))
        self.assertEqual(select_tsl_pct("STRONG", 0, "LOW", 5, 10, True, c), (12.0, "SIDEWAYS"))

    def test_expiry_and_gamma_explosion(self):
        c = self.cfg
        self.assertEqual(select_tsl_pct("STRONG", 60, "MEDIUM", 0, 50, False, c), (18.0, "EXPIRY_DAY"))
        self.assertEqual(select_tsl_pct("STRONG", 0, "LOW", 0, 90, False, c), (18.0, "GAMMA_EXPLOSION"))

    def test_profit_tiers(self):
        c = self.cfg
        self.assertEqual(apply_profit_tiers(10.0, 1.5, c), 10.0)
        self.assertEqual(apply_profit_tiers(10.0, 2.0, c), 8.0)
        self.assertEqual(apply_profit_tiers(10.0, 3.2, c), 6.0)


class TrailTest(unittest.TestCase):
    def test_brief_example_114_then_118_75(self):
        st = trade()
        r = run(st, CFG5, snap(1000, 100), snap(2000, 120))
        self.assertTrue(r.state.activated)
        self.assertEqual(r.output["current_trailing_stop"], 114.0)
        self.assertEqual(r.output["trailing_percentage"], 5.0)
        r = manage(r.state, snap(3000, 125), CFG5)
        self.assertEqual(r.output["current_trailing_stop"], 118.75)
        self.assertEqual(r.output["highest_price"], 125)
        self.assertEqual((r.output["status"], r.output["exit_signal"], r.output["reentry_state"]),
                         ("ACTIVE", False, "NONE"))

    def test_not_activated_keeps_icare_stop(self):
        r = run(trade(), CFG5, snap(1000, 103))
        self.assertFalse(r.state.activated)
        self.assertEqual(r.state.stop, 70.0)

    def test_never_widens(self):
        r = run(trade(), CFG5, snap(1000, 120), snap(2000, 125))
        r = manage(r.state, snap(3000, 124, **WEAK), CFG5)    # weak trend would pick 12-15 %
        self.assertEqual(r.state.tsl_pct, 5.0)
        self.assertEqual(r.state.stop, 118.75)

    def test_stop_never_falls_on_pullback(self):
        r = run(trade(), CFG5, snap(1000, 125), snap(2000, 121))
        self.assertEqual(r.state.stop, 118.75)

    def test_trail_is_percent_of_current_price_not_entry(self):
        # T14: bought at 10, premium now 100, 10 % trail -> stop 90 (10 below 100), not 9 / 1 from entry.
        cfg10 = TSLConfig(range_strong_low_vol=(10.0, 10.0), tier2_r=1e9, tier3_r=1e9)
        st = trade(entry=10.0, sl=8.0)
        r = run(st, cfg10, snap(1000, 50), snap(2000, 100))
        self.assertTrue(r.state.activated)
        self.assertEqual(r.state.tsl_pct, 10.0)
        self.assertEqual(r.state.stop, 90.0)
        # the price falls back to 95: the stop stays at 90 (ratchet), it does not drop to 85.5
        r = manage(r.state, snap(3000, 95), cfg10)
        self.assertEqual(r.state.stop, 90.0)
        self.assertEqual(r.state.highest, 100.0)

    def test_breakeven_floor_at_1r(self):
        # SL 90 -> 1R = 10. 12 % weak trail from 112 would be 98.56 < entry.
        st = trade(sl=90.0)
        r = run(st, TSLConfig(), snap(1000, 112.5, **WEAK))
        self.assertTrue(r.state.activated)
        self.assertEqual(r.state.stop, 100.0)


class ExitValidationTest(unittest.TestCase):
    def setUp(self):
        self.st = run(trade(), CFG5, snap(1000, 125)).state     # stop 118.75

    def test_touch_without_confirmation_holds(self):
        r = manage(self.st, snap(2000, 118.5), CFG5)
        self.assertEqual(r.output["status"], "ACTIVE")
        self.assertTrue(r.output["stop_breached"])
        self.assertIsNone(r.exit_context)

    def test_confirmed_exit(self):
        r = manage(self.st, snap(2000, 118.5, **WEAK), CFG5)
        self.assertEqual((r.output["status"], r.output["exit_trigger"]), ("EXIT", "CONFIRMED"))
        self.assertTrue(r.output["exit_signal"])
        ctx = r.exit_context
        self.assertEqual((ctx["entry_price"], ctx["exit_price"], ctx["highest_price"]), (100.0, 118.5, 125))
        self.assertEqual(ctx["trailing_stop"], 118.75)
        self.assertEqual((ctx["trend"], ctx["exit_reason"]), ("Bullish", "TRAILING_STOP"))
        self.assertEqual(ctx["underlying_break_level"], 2008.0)

    def test_distribution_alone_exits(self):
        r = manage(self.st, snap(2000, 118.5, amd_phase="DISTRIBUTION"), CFG5)
        self.assertEqual(r.output["exit_trigger"], "DISTRIBUTION")

    def test_hard_breach(self):
        r = manage(self.st, snap(2000, 115.0), CFG5)       # <= 118.75 x 0.97
        self.assertEqual(r.output["exit_trigger"], "HARD_BREACH")

    def test_breach_timeout(self):
        r = manage(self.st, snap(2000, 118.5), CFG5)
        r = manage(r.state, snap(30_000, 118.6), CFG5)
        self.assertEqual(r.output["status"], "ACTIVE")
        r = manage(r.state, snap(62_000, 118.6), CFG5)
        self.assertEqual(r.output["exit_trigger"], "BREACH_TIMEOUT")

    def test_breach_timer_resets_on_recovery(self):
        r = manage(self.st, snap(2000, 118.5), CFG5)
        r = manage(r.state, snap(30_000, 119.5), CFG5)
        self.assertIsNone(r.state.breach_since_ms)
        r = manage(r.state, snap(70_000, 118.6), CFG5)
        self.assertEqual(r.output["status"], "ACTIVE")

    def test_missing_inputs_confirm_exit(self):
        bare = Snapshot(now_ms=2000, bid=118.5)
        r = manage(self.st, bare, CFG5)
        self.assertEqual(r.output["exit_trigger"], "CONFIRMED")

    def test_exit_state_is_terminal(self):
        r = manage(self.st, snap(2000, 118.5, **WEAK), CFG5)
        r2 = manage(r.state, snap(3000, 130), CFG5)
        self.assertEqual(r2.state, r.state)


class PutSideTest(unittest.TestCase):
    def test_put_trails_premium_and_reads_bearish_structure(self):
        bear = dict(STRONG, st_bias="PUT", st_bullish=0, st_bearish=4, ema9=1996.0, ema26=2000.0,
                    direction_score=-0.8, volume_signal="Strong Bearish Volume", em_direction="STRONG_BEARISH")
        st = trade(side="PE")
        r = run(st, CFG5, snap(1000, 120, **bear), snap(2000, 125, **bear))
        self.assertEqual(r.state.stop, 118.75)
        r = manage(r.state, snap(3000, 118.5, **bear), CFG5)     # trend still bearish: hold
        self.assertEqual(r.output["status"], "ACTIVE")
        r = manage(r.state, snap(4000, 118.5, **dict(bear, contract_imbalance="BEARISH", direction_score=0.1,
                                                      ema9=2001.0)), CFG5)
        self.assertEqual(r.output["status"], "EXIT")
        self.assertEqual(r.exit_context["underlying_break_level"], 1990.0)


class VirtualTradeTest(unittest.TestCase):
    def test_virtual_sl_time_eod(self):
        st = trade(virtual=True, time_stop_ms=10_000)
        self.assertEqual(manage(st, snap(1000, 69), CFG5).closed_reason, "SL")
        self.assertEqual(manage(st, snap(11_000, 101), CFG5).closed_reason, "TIME")
        self.assertEqual(manage(st, snap(1000, 101, eod=True), CFG5).closed_reason, "EOD")
        real = trade(time_stop_ms=10_000)
        self.assertEqual(manage(real, snap(11_000, 101), CFG5).closed_reason, "")


def chain(**kw) -> Chain:
    base = dict(chain_id="T1", symbol="SBIN", tradingsymbol="SBIN26OCT800CE", side="CE", qty=750,
                sl_pct=0.3, hold_ms=3_600_000, band_floor=75.0, original_lots=2)
    base.update(kw)
    return Chain(**base)


EXIT_CTX = {"trade_id": "T1", "highest_price": 124.0, "last_swing_high": 123.0, "underlying_break_level": 2008.0}
MIN = 60_000


class ReentryTest(unittest.TestCase):
    def waiting(self, exit_ms=0):
        return on_trade_closed(chain(), 0, "TRAILING_STOP", 18 * 750, EXIT_CTX, exit_ms, TSLConfig())

    def test_watch_price_brief_example(self):
        self.assertEqual(watch_price_for(124.0, 123.0, TSLConfig()), 124.2)
        c = self.waiting()
        self.assertEqual((c.state, c.watch_price), (WAITING, 124.2))
        self.assertEqual(exit_output(c), {"symbol": "SBIN", "status": "EXIT", "exit_reason": "TRAILING_STOP",
                                          "reentry_state": WAITING, "watch_price": 124.2})

    def feed(self, c, path, start_min=11, **kw):
        """One sample per minute; returns (chain, signal) after the last."""
        sig = None
        for i, px in enumerate(path):
            s = Snapshot(now_ms=(start_min + i) * MIN, bid=px, ask=px + 0.1, probability=91.0,
                         **dict(STRONG, **kw))
            c, sig, _checks, _why = evaluate_reentry(c, s, TSLConfig())
            if sig:
                break
        return c, sig

    def test_brief_example_pullback_recovery_breakout(self):
        c = self.waiting()
        c, sig = self.feed(c, [118, 115, 123, 123.5])
        self.assertIsNone(sig)                       # 123 is not a break
        c, sig = self.feed(c, [124.2, 124.3], start_min=15)
        self.assertIsNotNone(sig)                    # 124.20 minute close -> next bar fires
        self.assertEqual((sig["status"], sig["reason"]), ("REENTER", "SWING_HIGH_BREAK_WITH_CONFLUENCE"))
        self.assertEqual((sig["confidence"], sig["reentry_no"], sig["max_lots"]), (91.0, 1, 2))
        self.assertEqual(c.state, PENDING)

    def test_cooldown(self):
        c = self.waiting(exit_ms=0)
        s = Snapshot(now_ms=5 * MIN, bid=125, probability=91.0, **STRONG)
        c = replace(c, bars=[{"t": 3 * MIN, "o": 125, "h": 125, "l": 125, "c": 125}])
        c, sig, _ch, why = evaluate_reentry(c, s, TSLConfig())
        self.assertIsNone(sig)
        self.assertEqual(why, "COOLDOWN")

    def test_each_check_is_mandatory(self):
        for kw in ({"st_bias": "NEUTRAL"}, {"ema9": 1999.0}, {"contract_imbalance": "NEUTRAL"},
                   {"liquidity": 60.0}, {"oi_buildup": "SHORT_COVERING"}, {"und_close": 2005.0},
                   {"spread_pct": 4.0}, {"liquidity": None}):
            c, sig = self.feed(self.waiting(), [125, 125.5, 126], **kw)
            self.assertIsNone(sig, kw)

    def test_probability_below_original_band(self):
        c = self.waiting()
        s = Snapshot(now_ms=12 * MIN, bid=125, probability=70.0, **STRONG)
        c = replace(c, bars=[{"t": 11 * MIN, "o": 125, "h": 125, "l": 125, "c": 125}])
        _c, sig, checks, _why = evaluate_reentry(c, s, TSLConfig())
        self.assertIsNone(sig)
        self.assertFalse(checks["probability"])
        self.assertEqual(band_floor("HIGH_CONVICTION"), 85.0)

    def test_expiry_paths(self):
        c = self.waiting()
        s = Snapshot(now_ms=12 * MIN, bid=125, **dict(STRONG, st_bias="PUT"))
        self.assertEqual(evaluate_reentry(c, s, TSLConfig())[0].end_reason, "TREND_INVALIDATED")
        s = Snapshot(now_ms=12 * MIN, bid=125, eod=True, **STRONG)
        self.assertEqual(evaluate_reentry(c, s, TSLConfig())[0].state, EXPIRED)
        s = Snapshot(now_ms=61 * MIN, bid=125, **STRONG)
        self.assertEqual(evaluate_reentry(c, s, TSLConfig())[0].end_reason, "WATCH_TIMEOUT")

    def test_pending_timeout_reverts_without_counting(self):
        c = replace(self.waiting(), state=PENDING, reentries_used=1, pending_since_ms=12 * MIN)
        self.assertFalse(pending_timed_out(c, 12 * MIN + 10_000, TSLConfig()))
        self.assertTrue(pending_timed_out(c, 12 * MIN + 30_000, TSLConfig()))
        c = revert_pending(c, 12 * MIN + 30_000, TSLConfig())
        self.assertEqual((c.state, c.reentries_used), (WAITING, 0))
        s = Snapshot(now_ms=12 * MIN + 40_000, bid=125, probability=91.0, **STRONG)
        self.assertEqual(evaluate_reentry(c, s, TSLConfig())[3], "RETRY_WAIT")

    def test_two_failed_reentries_block(self):
        cfg = TSLConfig()
        c = self.waiting()
        c = on_reentry_opened(replace(c, state=PENDING, reentries_used=1), "T1-R1")
        self.assertEqual((c.state, c.active_trade_id), (IN_TRADE, "T1-R1"))
        c = on_trade_closed(c, 1, "TRAILING_STOP", -500, EXIT_CTX, 30 * MIN, cfg)
        self.assertEqual((c.state, c.failed_reentries), (WAITING, 1))
        c = on_reentry_opened(replace(c, state=PENDING, reentries_used=2), "T1-R2")
        c = on_trade_closed(c, 2, "SL", -900, None, 50 * MIN, cfg)
        self.assertEqual((c.state, c.end_reason), (BLOCKED, "REENTRIES_FAILED"))

    def test_max_reentries_without_failure_is_done(self):
        c = replace(self.waiting(), reentries_used=2, state=IN_TRADE)
        c = on_trade_closed(c, 2, "TRAILING_STOP", 400, EXIT_CTX, 50 * MIN, TSLConfig())
        self.assertEqual((c.state, c.end_reason), (DONE, "MAX_REENTRIES"))

    def test_non_tsl_exit_ends_chain(self):
        c = on_trade_closed(chain(), 0, "SL", -900, None, 0, TSLConfig())
        self.assertEqual((c.state, c.end_reason), (DONE, "EXIT_SL"))

    def test_put_reentry_breaks_below_swing_low(self):
        bear = dict(STRONG, st_bias="PUT", ema9=1996.0, ema26=2000.0, contract_imbalance="BULLISH",
                    oi_buildup="SHORT_BUILDUP", und_close=1985.0)
        ctx = dict(EXIT_CTX, underlying_break_level=1990.0)
        c = on_trade_closed(chain(side="PE"), 0, "TRAILING_STOP", 100, ctx, 0, TSLConfig())
        c, sig = self.feed(c, [125, 125.5], **bear)
        self.assertIsNotNone(sig)
        c2 = on_trade_closed(chain(side="PE"), 0, "TRAILING_STOP", 100, ctx, 0, TSLConfig())
        _c, sig = self.feed(c2, [125, 125.5], **dict(bear, und_close=1995.0))
        self.assertIsNone(sig)


class SerializationTest(unittest.TestCase):
    def test_roundtrip(self):
        st = run(trade(), CFG5, snap(1000, 120)).state
        self.assertEqual(TradeState.from_dict(st.to_dict()), st)
        c = on_trade_closed(chain(), 0, "TRAILING_STOP", 1, EXIT_CTX, 0, TSLConfig())
        self.assertEqual(Chain.from_dict(c.to_dict()), c)


if __name__ == "__main__":
    unittest.main()

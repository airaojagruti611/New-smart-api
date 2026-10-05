"""A symbol whose first spot tick arrives after the first option pass still gets its option chain."""

from __future__ import annotations

import time
import unittest
from unittest import mock

try:
    import app.ws_producer as wp
except Exception as exc:  # pragma: no cover
    raise unittest.SkipTest(f"ws_producer import failed: {exc}")


def _producer(symbols):
    p = object.__new__(wp.MarketDataProducer)        # skip __init__: no Angel login / Redis
    p.symbols = symbols
    p.eq_map = {s: {"token": str(i)} for i, s in enumerate(symbols)}
    p.df = None
    p.rs = mock.Mock()
    p.sws = mock.Mock()
    p.opt_meta = {}
    p.active_expiry_by_underlying = {}
    p.spot_ltp = {}
    p.ws_open_t = time.time() - 60          # warm-up elapsed
    p.options_subscribed = False
    p.options_planned = set()
    p._opt_batch_no = 0
    p.EXCH_NFO = 2
    p.mode_opt = 3
    return p


def _chain(_df, sym, spot, around):
    return [{"token": f"{sym}-{k}", "underlying": sym, "tradingsymbol": f"{sym}{k}", "expiry": "2026-10-27",
             "strike": spot + k, "cp": "CE"} for k in range(2)], "2026-10-27"


class LateSymbolOptionsTest(unittest.TestCase):
    def test_late_symbol_subscribed_on_its_first_tick(self):
        p = _producer(["A", "B"])
        with mock.patch.object(wp, "build_atm_option_tokens", side_effect=_chain), \
             mock.patch.object(wp, "MAX_WS_SUBS", 950):
            p.spot_ltp["A"] = 100.0
            p._maybe_subscribe_options()
            self.assertTrue(p.options_subscribed)
            self.assertEqual(set(p.opt_meta), {"A-0", "A-1"})

            p._maybe_subscribe_options()                      # nothing new -> no extra subscribe
            self.assertEqual(p.sws.subscribe.call_count, 1)

            p.spot_ltp["B"] = 200.0                           # B's first tick arrives late
            p._maybe_subscribe_options()
            self.assertEqual(set(p.opt_meta), {"A-0", "A-1", "B-0", "B-1"})
            self.assertEqual(p.sws.subscribe.call_count, 2)
            ids = [c.kwargs["correlation_id"] for c in p.sws.subscribe.call_args_list]
            self.assertEqual(len(set(ids)), 2)
            self.assertEqual(p.active_expiry_by_underlying, {"A": "2026-10-27", "B": "2026-10-27"})

    def test_cap_counts_already_subscribed(self):
        p = _producer(["A", "B"])
        with mock.patch.object(wp, "build_atm_option_tokens", side_effect=_chain), \
             mock.patch.object(wp, "MAX_WS_SUBS", 5):         # 2 EQ + 2 A options -> room for 1
            p.spot_ltp["A"] = 100.0
            p._maybe_subscribe_options()
            p.spot_ltp["B"] = 200.0
            p._maybe_subscribe_options()
            self.assertEqual(len(p.opt_meta), 3)


if __name__ == "__main__":
    unittest.main()

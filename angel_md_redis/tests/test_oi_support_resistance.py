"""OI support/resistance must not collapse onto the same above-spot put wall."""

from __future__ import annotations

import unittest

from app.oi_analysis import OILevel, oi_concentration


class SupportResistanceSpotTest(unittest.TestCase):
    def test_max_put_above_spot_is_not_support(self):
        levels = [
            OILevel(strike=1200, call_oi=100, put_oi=400),
            OILevel(strike=1250, call_oi=200, put_oi=150),
            OILevel(strike=1300, call_oi=800, put_oi=5000),
        ]
        conc = oi_concentration(levels, spot=1260.0)
        self.assertEqual(conc.primary_support, 1200)
        self.assertEqual(conc.primary_resistance, 1300)
        self.assertNotEqual(conc.primary_support, conc.primary_resistance)

    def test_without_spot_still_picks_raw_maxima(self):
        levels = [
            OILevel(strike=1200, call_oi=100, put_oi=400),
            OILevel(strike=1300, call_oi=800, put_oi=5000),
        ]
        conc = oi_concentration(levels)
        self.assertEqual(conc.primary_support, 1300)
        self.assertEqual(conc.primary_resistance, 1300)


if __name__ == "__main__":
    unittest.main()

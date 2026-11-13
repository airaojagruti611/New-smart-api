import json
import unittest

from app.broker_funds import funds_from_snapshot, get_available_funds

NOW = 1_700_000_000_000


class FakeRedis:
    def __init__(self, kv=None):
        self.kv = kv or {}

    def get(self, k):
        return self.kv.get(k)


def snap(**kw):
    d = {"available_margin": 50000.0, "ts_ms": NOW - 1000, "error": ""}
    d.update(kw)
    return d


class FundsFromSnapshot(unittest.TestCase):
    def test_fresh(self):
        self.assertEqual(funds_from_snapshot(snap(), NOW, 120_000), 50000.0)

    def test_stale(self):
        self.assertIsNone(funds_from_snapshot(snap(ts_ms=NOW - 200_000), NOW, 120_000))

    def test_error(self):
        self.assertIsNone(funds_from_snapshot(snap(error="rms_error:x"), NOW, 120_000))

    def test_bad_values(self):
        self.assertIsNone(funds_from_snapshot(snap(available_margin=None), NOW, 120_000))
        self.assertIsNone(funds_from_snapshot(snap(available_margin="nan"), NOW, 120_000))
        self.assertIsNone(funds_from_snapshot(snap(ts_ms=0), NOW, 120_000))
        self.assertIsNone(funds_from_snapshot(None, NOW, 120_000))

    def test_negative_clamped(self):
        self.assertEqual(funds_from_snapshot(snap(available_margin=-5), NOW, 120_000), 0.0)


class GetAvailableFunds(unittest.TestCase):
    def test_reads_redis(self):
        r = FakeRedis({"md:account:latest": json.dumps(snap())})
        self.assertEqual(get_available_funds(r, NOW), 50000.0)

    def test_missing_or_garbage(self):
        self.assertIsNone(get_available_funds(FakeRedis(), NOW))
        self.assertIsNone(get_available_funds(FakeRedis({"md:account:latest": "{bad"}), NOW))
        self.assertIsNone(get_available_funds(None, NOW))


if __name__ == "__main__":
    unittest.main()

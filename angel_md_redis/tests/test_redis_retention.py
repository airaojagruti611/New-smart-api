import os
import unittest
import uuid

import redis
from app.redis_retention import trim_archived_history


@unittest.skipUnless(os.getenv('RUN_REDIS_INTEGRATION') == '1', 'explicit local Redis integration opt-in')
class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.client = redis.Redis.from_url(os.getenv('TEST_REDIS_URL', 'redis://127.0.0.1:6380/15'), decode_responses=True)
        self.stream = 'test:retention:' + uuid.uuid4().hex
        self.addCleanup(self.client.delete, self.stream)
        for value in (1, 2, 3, 4, 5, 6):
            for seq in range(100):
                self.client.xadd(self.stream, {'value': value}, id=f'{value * 60000}-{seq}')

    def trim(self):
        return trim_archived_history(self.client, [self.stream], keep_minutes=30,
                                     now_ms=34*60000, limit=10000)[0]

    def group(self, name, delivered):
        self.client.xgroup_create(self.stream, name, id=delivered)

    def test_missing_archive_group_refuses_trim(self):
        self.assertEqual(self.trim()['removed'], 0)
        self.assertEqual(self.client.xlen(self.stream), 600)

    def test_undelivered_group_protects_its_boundary(self):
        self.group('archive', '360000-0')
        self.group('slow', '120000-0')
        self.trim()
        self.assertTrue(self.client.xrange(self.stream, min='120000-0', max='120000-0'))
        self.assertGreaterEqual(self.client.xlen(self.stream), 500)

    def test_oldest_pending_is_retained_even_when_delivered_is_newer(self):
        self.group('archive', '0-0')
        messages=self.client.xreadgroup('archive','reader',{self.stream:'>'},count=600)[0][1]
        self.client.xack(self.stream,'archive',*[mid for mid,_ in messages if mid != '120000-0'])
        self.trim()
        self.assertEqual(self.client.xpending(self.stream,'archive')['pending'], 1)
        self.assertTrue(self.client.xrange(self.stream,min='120000-0',max='120000-0'))

    def test_age_and_acknowledgements_allow_old_history_removal(self):
        self.group('archive', '360000-0')
        self.group('analytics', '360000-0')
        self.trim()
        self.assertTrue(self.client.xrange(self.stream,min='240000-0',max='240000-0'))
        self.assertGreaterEqual(self.client.xlen(self.stream), 300)
        self.assertLess(self.client.xlen(self.stream), 600)

    def test_group_at_zero_protects_whole_stream(self):
        self.group('archive', '360000-0')
        self.group('new', '0-0')
        self.assertEqual(self.trim()['removed'], 0)

    def test_candle_and_pivot_histories_are_excluded(self):
        self.assertEqual(trim_archived_history(self.client, ['md:candles:1m','md:pivots:prevday']), [])

    def test_short_retention_rejected(self):
        with self.assertRaises(ValueError):
            trim_archived_history(self.client,[self.stream],keep_minutes=1)

import threading
import os
import tempfile
import uuid
import time
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

from app.archiver import ArchiveDataLossError, StreamParquetArchiver
import pandas as pd
import redis


class ArchiverShutdownTests(unittest.TestCase):
    def test_drains_pending_once_and_flushes_last_new_batch(self):
        archiver = StreamParquetArchiver.__new__(StreamParquetArchiver)
        archiver.stream, archiver.group, archiver.consumer = "test", "archive", "test"
        archiver.batch_size, archiver.flush_sec = 8000, 10
        archiver._buf_rows, archiver._buf_ids = [], []
        archiver._last_flush = 0
        archiver.delete_after_ack = False
        archiver.r = MagicMock()
        archiver._write_batch = MagicMock()
        stop = threading.Event()
        stop.set()
        page = [(b"test", [(b"1-0", {b"symbol": b"A"})])]
        new_page = [(b"test", [(b"2-0", {b"symbol": b"B"})])]
        archiver._xreadgroup = MagicMock(side_effect=[page, [], new_page, []])
        archiver.run_forever(stop_event=stop)
        self.assertEqual(archiver._write_batch.call_count, 2)
        self.assertEqual(archiver.r.xack.call_count, 2)
        self.assertEqual(archiver._buf_ids, [])

    def test_failed_write_does_not_acknowledge_messages(self):
        archiver = StreamParquetArchiver.__new__(StreamParquetArchiver)
        archiver._buf_rows = [{"symbol": "A"}]
        archiver._buf_ids = ["1-0"]
        archiver.r = MagicMock()
        archiver._write_batch = MagicMock(side_effect=OSError("disk full"))
        with self.assertRaises(OSError):
            archiver._flush()
        archiver.r.xack.assert_not_called()
        self.assertEqual(archiver._buf_ids, ["1-0"])


@unittest.skipUnless(os.getenv("RUN_REDIS_INTEGRATION") == "1", "explicit local Redis integration opt-in")
class RedisDrainTests(unittest.TestCase):
    def test_actual_redis_pending_and_new_messages_are_archived_once(self):
        url = "redis://localhost:6379/15"
        client = redis.Redis.from_url(url)
        stream = "test:archive:" + uuid.uuid4().hex
        self.addCleanup(client.delete, stream)
        client.xgroup_create(stream, "archive", id="0", mkstream=True)
        for i in range(4):
            client.xadd(stream, {"symbol": "TEST", "ts_recv": 1793150000000, "value": i})
        client.xreadgroup("archive", "test", {stream: ">"}, count=2)
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {"REDIS_URL": url}):
            archiver = StreamParquetArchiver(stream, "archive", "test", out_dir=folder, block_ms=50)
            stop = threading.Event()
            stop.set()
            archiver.run_forever(stop_event=stop)
            files = list(Path(folder).rglob("*.parquet"))
            rows = pd.concat([pd.read_parquet(file) for file in files])
            self.assertEqual(len(rows), 4)
            self.assertEqual(rows["_redis_id"].nunique(), 4)
            self.assertEqual(client.xpending(stream, "archive")["pending"], 0)

    def test_multiple_pending_pages_and_new_messages_are_batched_without_duplicates(self):
        url = 'redis://localhost:6379/15'
        client = redis.Redis.from_url(url)
        stream = 'test:archive:' + uuid.uuid4().hex
        self.addCleanup(client.delete, stream)
        client.xgroup_create(stream, 'archive', id='0', mkstream=True)
        pipe = client.pipeline()
        for i in range(6000):
            pipe.xadd(stream, {'symbol': f'S{i % 100}', 'value': str(i)})
        expected = set(pipe.execute())
        client.xreadgroup('archive', 'test', {stream: '>'}, count=2400)
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {'REDIS_URL': url}):
            archiver = StreamParquetArchiver(stream, 'archive', 'test', out_dir=folder, batch_size=10000,
                                            read_count=500, partition_by_symbol=False, block_ms=50)
            stop = threading.Event()
            stop.set()
            archiver.run_forever(stop)
            files = list(Path(folder).rglob('*.parquet'))
            rows = pd.concat([pd.read_parquet(file) for file in files])
            self.assertEqual(len(rows), 6000)
            self.assertEqual({value.encode() for value in rows['_redis_id']}, expected)
            self.assertEqual(len(files), 2)
            self.assertEqual(client.xpending(stream, 'archive')['pending'], 0)

    def test_stale_consumer_pending_is_recovered(self):
        url = 'redis://localhost:6379/15'
        client = redis.Redis.from_url(url)
        stream = 'test:archive:' + uuid.uuid4().hex
        self.addCleanup(client.delete, stream)
        client.xgroup_create(stream, 'archive', id='0', mkstream=True)
        for i in range(10):
            client.xadd(stream, {'value': i})
        client.xreadgroup('archive', 'abandoned', {stream: '>'}, count=10)
        time.sleep(0.01)
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {'REDIS_URL': url}):
            archiver = StreamParquetArchiver(stream, 'archive', 'replacement', out_dir=folder,
                                            partition_by_symbol=False, claim_idle_ms=1)
            stop = threading.Event()
            stop.set()
            archiver.run_forever(stop)
            rows = pd.concat([pd.read_parquet(file) for file in Path(folder).rglob('*.parquet')])
            self.assertEqual(rows['_redis_id'].nunique(), 10)
            self.assertEqual(client.xpending(stream, 'archive')['pending'], 0)

    def test_failed_ack_replay_reuses_the_complete_part(self):
        url = 'redis://localhost:6379/15'
        client = redis.Redis.from_url(url)
        stream = 'test:archive:' + uuid.uuid4().hex
        self.addCleanup(client.delete, stream)
        client.xgroup_create(stream, 'archive', id='0', mkstream=True)
        for i in range(3):
            client.xadd(stream, {'value': i})
        stop = threading.Event()
        stop.set()
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {'REDIS_URL': url}):
            archiver = StreamParquetArchiver(stream, 'archive', 'test', out_dir=folder, partition_by_symbol=False)
            with patch.object(archiver.r, 'xack', side_effect=redis.ConnectionError('simulated lost ack')):
                with self.assertRaises(redis.ConnectionError):
                    archiver.run_forever(stop)
            replacement = StreamParquetArchiver(stream, 'archive', 'test', out_dir=folder, partition_by_symbol=False)
            replacement.run_forever(stop)
            files = list(Path(folder).rglob('*.parquet'))
            self.assertEqual(len(files), 1)
            self.assertEqual(len(pd.read_parquet(files[0])), 3)
            self.assertEqual(client.xpending(stream, 'archive')['pending'], 0)


    def test_trimmed_abandoned_payload_is_reported_as_a_data_gap(self):
        url = 'redis://localhost:6379/15'
        client = redis.Redis.from_url(url)
        stream = 'test:archive:' + uuid.uuid4().hex
        self.addCleanup(client.delete, stream)
        client.xgroup_create(stream, 'archive', id='0', mkstream=True)
        message = client.xadd(stream, {'value': 'unarchived'})
        client.xreadgroup('archive', 'abandoned', {stream: '>'}, count=1)
        client.xdel(stream, message)
        time.sleep(0.01)
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {'REDIS_URL': url}):
            archiver = StreamParquetArchiver(stream, 'archive', 'replacement', out_dir=folder, claim_idle_ms=1)
            stop = threading.Event()
            stop.set()
            with self.assertRaises(ArchiveDataLossError):
                archiver.run_forever(stop)
            self.assertEqual(list(Path(folder).rglob('*.parquet')), [])


if __name__ == "__main__":
    unittest.main()

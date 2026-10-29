"""Roundtrip and recovery checks for batched Parquet archives."""
from datetime import datetime, timezone
import os
from pathlib import Path
import tempfile
import json
import threading
import unittest
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq

from app.archiver import StreamParquetArchiver
from app.archiver import ArchiveDataLossError
import run_archiver_layers
from run_archiver_layers import archiver_options


class ArchiveAccuracyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.client = MagicMock()
        self.client.xautoclaim.return_value = [b"0-0", [], []]
        self.client.xpending.return_value = {"pending": 0}
        with patch('redis.from_url', return_value=self.client), patch.dict(os.environ, {'ARCHIVE_TZ': 'Asia/Kolkata'}):
            self.archive = StreamParquetArchiver('test', 'archive', 'consumer', out_dir=self.temp.name,
                                                batch_size=8000, flush_sec=60, partition_by_symbol=False)

    def rows(self):
        result = []
        for path in Path(self.temp.name).rglob('*.parquet'):
            result.extend(pq.ParquetFile(path, page_checksum_verification=True).read().to_pylist())
        return result

    def page(self, first, count):
        return [(b'test', [(f'1793258100000-{i}'.encode(), {b'symbol': f'S{i % 100}'.encode(), b'value': str(i).encode()})
                           for i in range(first, first + count)])]

    def test_shutdown_keeps_normal_batch_size_and_preserves_every_id(self):
        stop = threading.Event()
        stop.set()
        self.archive._xreadgroup = MagicMock(side_effect=[[], self.page(0, 1000), self.page(1000, 1000),
                                                        self.page(2000, 1000), []])
        self.archive.run_forever(stop)
        rows = self.rows()
        self.assertEqual(len(rows), 3000)
        self.assertEqual(len({row['_redis_id'] for row in rows}), 3000)
        self.assertEqual(len(list(Path(self.temp.name).rglob('*.parquet'))), 1)
        self.client.xack.assert_called_once()

    def test_pending_cursor_advances_without_repeating_unacknowledged_pages(self):
        stop = threading.Event()
        stop.set()
        self.archive._xreadgroup = MagicMock(side_effect=[self.page(0, 2), self.page(2, 2), self.page(4, 2), [], []])
        self.archive.run_forever(stop)
        cursors = [call.args[0] for call in self.archive._xreadgroup.call_args_list]
        self.assertEqual(cursors, ['0', '1793258100000-1', '1793258100000-3', '1793258100000-5', '>'])
        self.assertEqual(len(self.rows()), 6)
        self.assertEqual(len(list(Path(self.temp.name).rglob('*.parquet'))), 1)

    def test_missing_partition_values_and_later_columns_are_retained(self):
        self.archive.partition_by_symbol = True
        self.archive._write_batch([
            {'_redis_id': '1793258100000-1', 'symbol': 'A', 'first': 'one'},
            {'_redis_id': '1793258100000-2', 'late_column': 'two'},
            {'_redis_id': '1793258100000-3', 'symbol': None, 'late_column': 'three'},
            {'_redis_id': '1793258100000-4', 'symbol': 'A/../../outside', 'late_column': 'four'},
        ])
        rows = {row['_redis_id']: row for row in self.rows()}
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows['1793258100000-2']['late_column'], 'two')
        self.assertEqual(rows['1793258100000-4']['symbol'], 'A/../../outside')
        self.assertTrue(any(p.parent.name == 'symbol=UNKNOWN' for p in Path(self.temp.name).rglob('*.parquet')))
        self.assertTrue(all(p.resolve().is_relative_to(Path(self.temp.name).resolve()) for p in Path(self.temp.name).rglob('*.parquet')))

    def test_receive_date_survives_replay_and_ist_midnight(self):
        before = int(datetime(2026, 10, 28, 18, 29, 59, 999000, tzinfo=timezone.utc).timestamp() * 1000)
        after = before + 1
        self.archive._write_batch([
            {'_redis_id': f'{before}-0', 'ts_recv': 'invalid', 'value': 'before'},
            {'_redis_id': f'{after}-0', 'value': 'after'},
        ])
        paths = list(Path(self.temp.name).rglob('*.parquet'))
        self.assertEqual({p.parent.name for p in paths}, {'dt=2026-10-28', 'dt=2026-10-29'})
        self.assertEqual({row['ts_recv'] for row in self.rows()}, {before, after})

    def test_replaying_identical_batch_does_not_create_extra_parts(self):
        self.archive._ingest_messages(self.page(0, 10))
        rows = list(self.archive._buf_rows)
        self.archive._write_batch(rows)
        self.archive._write_batch(rows)
        self.assertEqual(len(self.rows()), 10)
        self.assertEqual(len(list(Path(self.temp.name).rglob('*.parquet'))), 1)

    def test_failed_write_retains_ids_and_removes_partial_temp_file(self):
        self.archive._ingest_messages(self.page(0, 3))
        def fail(table, output, **kwargs):
            output.write(b'partial parquet')
            raise OSError('disk full')
        with patch('app.archiver.pq.write_table', side_effect=fail):
            with self.assertRaises(OSError):
                self.archive._flush()
        self.client.xack.assert_not_called()
        self.assertEqual(len(self.archive._buf_ids), 3)
        self.assertEqual(list(Path(self.temp.name).rglob('.tmp-*')), [])
        self.archive._flush()
        self.client.xack.assert_called_once()
        self.assertEqual(len(self.rows()), 3)

    def test_fsync_and_complete_parquet_precede_acknowledgement(self):
        self.archive._ingest_messages(self.page(0, 3))
        events = []
        fsync = os.fsync
        def sync(fd):
            fsync(fd)
            events.append('fsync')
        def ack(*args):
            self.assertEqual(len(self.rows()), 3)
            events.append('ack')
        self.client.xack.side_effect = ack
        with patch('app.archiver.os.fsync', side_effect=sync):
            self.archive._flush()
        self.assertEqual(events, ['fsync', 'ack'])

    def test_footer_row_count_failure_prevents_ack_and_final_publication(self):
        self.archive._ingest_messages(self.page(0, 3))
        wrong_metadata = MagicMock()
        wrong_metadata.metadata.num_rows = 1
        with patch('app.archiver.pq.ParquetFile', return_value=wrong_metadata):
            with self.assertRaisesRegex(RuntimeError, 'row count'):
                self.archive._flush()
        self.client.xack.assert_not_called()
        self.assertEqual(len(self.archive._buf_ids), 3)
        self.assertEqual(list(Path(self.temp.name).rglob('*.parquet')), [])

    def test_trimmed_payload_gap_is_persisted_instead_of_retried_as_success(self):
        error_file = Path(self.temp.name) / 'archiver-errors.json'
        with patch.object(run_archiver_layers, 'ERROR_FILE', error_file), \
             patch.object(run_archiver_layers, 'FATAL_ERRORS', {}), \
             patch.object(run_archiver_layers, 'StreamParquetArchiver', side_effect=ArchiveDataLossError('missing record')) as writer:
            run_archiver_layers._run_one('test', 'test', 'consumer', 8000)
        writer.assert_called_once()
        self.assertEqual(json.loads(error_file.read_text())['test'], 'missing record')

    def test_memory_limit_flushes_large_payloads_before_row_limit(self):
        self.archive.max_buffer_bytes = 2000
        self.archive._ingest_messages([(b'test', [(b'1793258100000-1', {b'payload': b'x' * 4000})])])
        self.assertEqual(self.archive._buffer_bytes, 0)
        self.assertEqual(len(self.rows()), 1)
        self.client.xack.assert_called_once()

    def test_deleted_pending_payload_is_reported_without_ack(self):
        with self.assertRaisesRegex(RuntimeError, 'removed from Redis'):
            self.archive._ingest_messages([(b'test', [(b'1-0', None)])])
        self.client.xack.assert_not_called()

    def test_invalid_utf8_is_not_silently_changed_and_acknowledged(self):
        with self.assertRaises(UnicodeDecodeError):
            self.archive._ingest_messages([(b'test', [(b'1-0', {b'value': b'\xff'})])])
        self.client.xack.assert_not_called()

    def test_shutdown_reads_are_nonblocking(self):
        self.archive._stop_event = threading.Event()
        self.archive._stop_event.set()
        self.archive._xreadgroup('>')
        self.assertIsNone(self.client.xreadgroup.call_args.kwargs['block'])

    def test_launcher_uses_combined_parts_with_bounded_buffers(self):
        with patch.dict(os.environ, {}, clear=True):
            options = archiver_options(8000)
        self.assertFalse(options['partition_by_symbol'])
        self.assertEqual(options['batch_size'], 32000)
        self.assertEqual(options['max_buffer_bytes'], 16 * 1024 * 1024)


if __name__ == '__main__':
    unittest.main()

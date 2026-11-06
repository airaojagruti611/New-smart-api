from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

from market_automation import BASE, DriveUploader, load_config, session, verify_drive_account, drain_uploads, final_upload_ready, upload_coverage


class CalendarTests(unittest.TestCase):
    def phase(self, value):
        return session(load_config(), datetime.fromisoformat(value).replace(tzinfo=ZoneInfo("Asia/Kolkata")))["phase"]

    def test_boundaries(self):
        for value, expected in [("09:09", "before"), ("09:10", "prepare"), ("09:14", "prepare"),
                                ("09:15", "open"), ("15:39", "open"), ("15:40", "after")]:
            with self.subTest(value=value):
                self.assertEqual(self.phase("2026-10-28T" + value), expected)

    def test_holiday_weekend_unknown_year(self):
        self.assertEqual(self.phase("2026-11-10T10:00"), "closed")
        self.assertEqual(self.phase("2026-10-31T10:00"), "closed")
        self.assertEqual(self.phase("2027-01-04T10:00"), "calendar_expired")

    def test_special_session_and_timezone(self):
        config = load_config()
        config["special_sessions"]["2026-11-08"] = {"prepare_time": "17:55", "open_time": "18:00", "close_time": "19:00"}
        self.assertEqual(session(config, datetime.fromisoformat("2026-11-08T12:30:00+00:00"))["phase"], "open")


class AccountVerificationTests(unittest.TestCase):
    def test_wrong_account_blocks_verification(self):
        from unittest.mock import MagicMock
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            config = load_config()
            config["expected_drive_email"] = "target@example.com"
            (base / "automation.json").write_text(json.dumps(config))
            (base / "runtime").mkdir()
            (base / "runtime" / "rclone.conf").write_text('[gdrive]\ntoken = {"access_token":"test-only"}\n')
            response = MagicMock(ok=True)
            response.json.return_value = {"user": {"emailAddress": "wrong@example.com"}}
            with patch("requests.get", return_value=response):
                with self.assertRaisesRegex(RuntimeError, "Wrong Google account"):
                    verify_drive_account(base, run=MagicMock(return_value=MagicMock(returncode=0)))

    def test_matching_account_is_verified(self):
        from unittest.mock import MagicMock
        import requests
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            config = load_config()
            config["expected_drive_email"] = "target@example.com"
            (base / "automation.json").write_text(json.dumps(config))
            (base / "runtime").mkdir()
            (base / "runtime" / "rclone.conf").write_text('[gdrive]\ntoken = {"access_token":"test-only"}\n')
            response = MagicMock(ok=True)
            response.json.return_value = {"user": {"emailAddress": "TARGET@example.com"}}
            with patch("requests.get", side_effect=[requests.exceptions.SSLError('temporary EOF'), response]) as get, patch('market_automation.time.sleep'):
                result = verify_drive_account(base, run=MagicMock(return_value=MagicMock(returncode=0)))
            self.assertEqual(get.call_count, 2)
            self.assertTrue(result["verified"])


class UploadDrainTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        (self.base / 'runtime').mkdir()

    def test_after_hours_continues_across_chunk_limits_until_complete(self):
        from unittest.mock import MagicMock
        uploader = MagicMock(config=load_config())
        uploader.upload.side_effect = [
            {'uploaded_checked':80,'removed_local':0,'pass_complete':False},
            {'uploaded_checked':30,'removed_local':0,'pass_complete':False},
            {'uploaded_checked':0,'removed_local':0,'pass_complete':True},
        ]
        with patch('market_automation.session', return_value={'phase':'after'}), patch('market_automation.final_upload_ready',return_value=True):
            result = drain_uploads(self.base, uploader_factory=lambda base:uploader)
        self.assertEqual(uploader.upload.call_count,3)
        self.assertEqual(result['uploaded_checked'],110)
        self.assertEqual(result['state'],'caught_up')

    def test_market_hours_yield_between_chunks_to_refresh_live_priority(self):
        from unittest.mock import MagicMock
        uploader = MagicMock(config=load_config())
        uploader.upload.return_value={'uploaded_checked':80,'removed_local':0,'pass_complete':False}
        with patch('market_automation.session',return_value={'phase':'open'}):
            result=drain_uploads(self.base,uploader_factory=lambda base:uploader)
        self.assertEqual(uploader.upload.call_count,1)
        self.assertEqual(result['state'],'yielded')
        self.assertFalse(uploader.upload.call_args.kwargs['final'])

    def test_failed_pass_records_failure_without_claiming_completion(self):
        from unittest.mock import MagicMock
        uploader=MagicMock(config=load_config())
        uploader.upload.side_effect=RuntimeError('test failure')
        with patch('market_automation.session',return_value={'phase':'after'}), self.assertRaises(RuntimeError):
            drain_uploads(self.base,uploader_factory=lambda base:uploader)
        self.assertEqual(json.loads((self.base/'runtime/upload-worker.json').read_text())['state'],'failed')

    def test_final_requires_current_stop_marker_and_later_drain_confirmation(self):
        stop=self.base/'runtime/archiver.stop'
        drained=self.base/'runtime/archiver-drained.json'
        with patch('market_automation.session',return_value={'phase':'after'}):
            self.assertFalse(final_upload_ready(self.base,load_config()))
            stop.write_text('drain')
            drained.write_text(json.dumps({'drained_at':stop.stat().st_mtime-10}))
            self.assertFalse(final_upload_ready(self.base,load_config()))
            drained.write_text(json.dumps({'drained_at':stop.stat().st_mtime+10}))
            self.assertTrue(final_upload_ready(self.base,load_config()))
        with patch('market_automation.session',return_value={'phase':'open'}):
            self.assertFalse(final_upload_ready(self.base,load_config()))


@unittest.skipUnless((BASE / "tools" / "rclone.exe").exists(), "portable rclone required")
class UploadTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.remote = self.base / "drive"
        config = load_config()
        config["drive_remote"] = str(self.remote)
        config.pop("expected_drive_email", None)
        (self.base / "automation.json").write_text(json.dumps(config))
        (self.base / "tools").mkdir()
        # Run the actual binary against a local destination, without Google credentials.
        shutil.copyfile(BASE / "tools" / "rclone.exe", self.base / "tools" / "rclone.exe")
        self.uploader = DriveUploader(self.base)
        self.uploader.ready = lambda: True

    def old_file(self):
        path = self.base / "data_lake" / "stream=test" / "dt=2026-01-01" / "part.parquet"
        path.parent.mkdir(parents=True)
        path.write_bytes(b"test archive")
        os.utime(path, (time.time() - 4 * 86400,) * 2)
        return path

    def test_verified_upload_then_local_cleanup(self):
        path = self.old_file()
        result = self.uploader.upload(final=True)
        self.assertFalse(path.exists())
        self.assertEqual((self.remote / path.relative_to(self.base)).read_bytes(), b"test archive")
        self.assertEqual(result["removed_local"], 1)

    def test_today_upload_is_verified_before_historical_backfill(self):
        historical = self.old_file()
        today = datetime.now(ZoneInfo('Asia/Kolkata')).date().isoformat()
        live = self.base / 'data_lake' / 'stream=live' / ('dt=' + today) / 'part.parquet'
        live.parent.mkdir(parents=True)
        live.write_bytes(b'live data')
        os.utime(live, (time.time() - 300,) * 2)
        result = self.uploader.upload(max_batches=1, cleanup=False)
        self.assertEqual(result['uploaded_checked'], 1)
        self.assertFalse(result['pass_complete'])
        self.assertTrue((self.remote / live.relative_to(self.base)).exists())
        self.assertFalse((self.remote / historical.relative_to(self.base)).exists())
        self.assertTrue((self.base / 'runtime' / 'upload-progress.json').exists())
        self.assertFalse((self.base / 'runtime' / 'upload-status.json').exists())

    def test_configured_batch_limit_bounds_each_upload_run(self):
        self.old_file()
        self.uploader.config['upload_max_batches'] = 1
        result = self.uploader.upload(final=True, cleanup=False)
        self.assertFalse(result['pass_complete'])
        self.assertEqual(result['uploaded_checked'], 1)
        self.assertTrue((self.base / 'runtime' / 'upload-progress.json').exists())

    def test_verified_files_skipped_but_later_retention_still_runs(self):
        path = self.old_file()
        first = self.uploader.upload(final=True, cleanup=False)
        self.assertEqual(first['uploaded_checked'], 1)
        again = self.uploader.upload(final=True, cleanup=False)
        self.assertEqual(again['uploaded_checked'], 0)
        last = self.uploader.upload(final=True, cleanup=True)
        self.assertEqual(last['removed_local'], 1)
        self.assertFalse(path.exists())

    def test_failed_verification_keeps_source(self):
        path = self.old_file()
        command = self.uploader.command
        def fail_check(*args):
            if args[0] == "check":
                raise RuntimeError("simulated network failure")
            command(*args)
        with patch.object(self.uploader, "command", side_effect=fail_check):
            with self.assertRaises(RuntimeError):
                self.uploader.upload(final=True)
        self.assertTrue(path.exists())
        self.assertFalse((self.base / "runtime" / "upload-status.json").exists())

    def test_changed_source_is_not_deleted(self):
        path = self.old_file()
        command = self.uploader.command
        def change_after_check(*args):
            command(*args)
            if args[0] == "check":
                path.write_bytes(b"new contents")
        with patch.object(self.uploader, "command", side_effect=change_after_check):
            result = self.uploader.upload(final=True)
        self.assertTrue(path.exists())
        self.assertEqual(result["removed_local"], 0)

    def test_no_cleanup_and_excludes_temporary_files(self):
        path = self.old_file()
        (path.parent / ".tmp-part.parquet").write_bytes(b"unfinished")
        (self.base / ".env").write_text("SECRET=never-upload")
        result = self.uploader.upload(final=True, cleanup=False)
        self.assertTrue(path.exists())
        self.assertEqual(result["uploaded_checked"], 1)
        self.assertFalse((self.remote / ".env").exists())
        self.assertFalse((self.remote / path.parent.relative_to(self.base) / ".tmp-part.parquet").exists())

    def test_drain_really_uploads_more_files_than_single_pass_limit(self):
        path=self.old_file()
        for i in range(4):
            (path.parent/f'extra-{i}.parquet').write_bytes(b'extra archive')
        self.uploader.config.update(upload_batch_files=2,upload_max_batches=1)
        with patch('market_automation.session',return_value={'phase':'after'}), patch('market_automation.final_upload_ready',return_value=True):
            result=drain_uploads(self.base,cleanup=False,uploader_factory=lambda base:self.uploader)
        self.assertEqual(result['uploaded_checked'],5)
        self.assertEqual(result['state'],'caught_up')
        self.assertEqual(len(list(self.remote.rglob('*.parquet'))),5)

    def test_coverage_only_counts_current_unchanged_files_verified_for_this_target(self):
        today=datetime.now(ZoneInfo('Asia/Kolkata')).date().isoformat()
        path=self.base/'data_lake'/'stream=live'/('dt='+today)/'part.parquet'
        path.parent.mkdir(parents=True)
        path.write_bytes(b'archive')
        self.assertFalse(upload_coverage(self.base)['today_complete'])
        self.uploader.upload(final=True,cleanup=False)
        coverage=upload_coverage(self.base)
        self.assertEqual(coverage['verified_unchanged'],1)
        self.assertTrue(coverage['today_complete'])
        path.write_bytes(b'changed archive')
        self.assertEqual(upload_coverage(self.base)['awaiting_verification'],1)


if __name__ == "__main__":
    unittest.main()

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

from market_automation import BASE, DriveUploader, load_config, session, verify_drive_account


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
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            config = load_config()
            config["expected_drive_email"] = "target@example.com"
            (base / "automation.json").write_text(json.dumps(config))
            (base / "runtime").mkdir()
            (base / "runtime" / "rclone.conf").write_text('[gdrive]\ntoken = {"access_token":"test-only"}\n')
            response = MagicMock(ok=True)
            response.json.return_value = {"user": {"emailAddress": "TARGET@example.com"}}
            with patch("requests.get", return_value=response):
                result = verify_drive_account(base, run=MagicMock(return_value=MagicMock(returncode=0)))
            self.assertTrue(result["verified"])


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


if __name__ == "__main__":
    unittest.main()

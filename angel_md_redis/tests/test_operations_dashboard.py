from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
from http.server import ThreadingHTTPServer

from market_automation import BASE, load_config, upload_coverage
from app.operations_status import OperationsStatus, archive_summary, safe_batch, batch_includes_day
from operations_server import make_handler, LocalServer


class OperationsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.day = datetime.now(ZoneInfo('Asia/Kolkata')).date().isoformat()
        self.config = load_config()
        self.config.update(expected_drive_email='owner@example.com', drive_remote='gdrive:SmartAPI')
        (self.base/'automation.json').write_text(json.dumps(self.config))
        (self.base/'runtime').mkdir()
        self.day_logs = self.base/'logs'/self.day
        self.day_logs.mkdir(parents=True)
        self.status = OperationsStatus(self.base)
        self.status.tasks = lambda: {}

    def part(self):
        p=self.base/'data_lake'/'stream=md_greeks_change_signal'/('dt='+self.day)/'part.parquet'
        p.parent.mkdir(parents=True,exist_ok=True)
        p.write_bytes(b'archive data')
        return p

    def checkpoint(self,p):
        db=sqlite3.connect(self.base/'runtime/verified-uploads.sqlite3')
        db.execute('CREATE TABLE verified (target TEXT,folder TEXT,path TEXT,size INTEGER,mtime INTEGER)')
        s=p.stat()
        db.execute('INSERT INTO verified VALUES (?,?,?,?,?)',('gdrive:SmartAPI|owner@example.com','data_lake',p.relative_to(self.base/'data_lake').as_posix(),s.st_size,s.st_mtime_ns))
        db.commit(); db.close()

    def test_no_files_does_not_claim_verified_backup(self):
        with patch('app.operations_status.workers',return_value=[]):
            d=self.status.snapshot(self.day)
        self.assertFalse(d['uploads']['complete'])
        self.assertEqual(d['uploads']['percent'],0)

    def test_running_state_requires_actual_process_evidence(self):
        (self.base/'runtime/upload-worker.json').write_text(json.dumps({'state':'uploading','pid':123,'updated_at':datetime.now().astimezone().isoformat()}))
        with patch('app.operations_status.workers',return_value=[]),patch('app.operations_status.process_matches',return_value=False):
            self.assertFalse(self.status.snapshot(self.day)['uploads']['running'])

    def test_stream_with_underscore_preserves_real_name_and_row_count(self):
        self.part()
        (self.day_logs/'arch_layers.log').write_text('[ARCHIVER] md:greeks_change:signal rows=20 files=1 flush_sec=1\n')
        with patch('app.operations_status.workers',return_value=[]):
            d=self.status.snapshot(self.day)
        self.assertEqual(len(d['streams']),1)
        self.assertEqual(d['streams'][0]['name'],'md:greeks_change:signal')
        self.assertEqual(d['streams'][0]['rows'],20)

    def test_archive_restarts_add_records_instead_of_overwriting_day(self):
        (self.day_logs/'arch_layers.log.120000.previous').write_text('[ARCHIVER] md:ticks:eq rows=10 files=2\n[ARCHIVER] md:ticks:eq rows=20 files=3\n')
        (self.day_logs/'arch_layers.log').write_text('[ARCHIVER] md:ticks:eq rows=5 files=1\n')
        values=archive_summary(self.base,self.day,ZoneInfo('Asia/Kolkata'))
        self.assertEqual(values['md:ticks:eq'],{'rows':25,'parts':4})

    def test_verified_archives_still_count_after_local_retention_cleanup(self):
        p=self.part(); self.checkpoint(p); p.unlink()
        d=upload_coverage(self.base,self.day,include_streams=True)
        self.assertEqual(d['tracked_files'],1)
        self.assertEqual(d['tracked_verified'],1)
        self.assertEqual(d['verified_without_local_copy'],1)

    def test_changed_local_file_is_pending_even_with_old_checkpoint(self):
        p=self.part(); self.checkpoint(p); p.write_bytes(b'changed data')
        with patch('app.operations_status.workers',return_value=[]):
            d=self.status.snapshot(self.day)
        self.assertFalse(d['uploads']['complete'])
        self.assertEqual(d['uploads']['pending'],1)

    def test_prior_day_drain_is_not_shown_as_todays_success(self):
        stamp=(datetime.now(ZoneInfo('Asia/Kolkata'))-timedelta(days=1)).timestamp()
        (self.base/'runtime/archiver-drained.json').write_text(json.dumps({'drained_at':stamp,'streams':34}))
        with patch('app.operations_status.workers',return_value=[]):
            self.assertIsNone(self.status.snapshot(self.day)['session']['drain'])

    def test_raw_log_secrets_are_not_returned(self):
        folder=self.base/'logs/automation';folder.mkdir()
        (folder/'2026-11-03-drive-worker.log').write_text('Authorization: Bearer SUPER_SECRET_TOKEN\nTraceback (most recent call last)\nhttps://example.com/?code=SUPER_SECRET_TOKEN')
        self.assertNotIn('SUPER_SECRET_TOKEN',json.dumps(safe_batch(self.base)))

    def test_incomplete_verification_never_rounds_to_one_hundred_percent(self):
        coverage={'tracked_files':100000,'tracked_verified':99999,'awaiting_verification':1,
                  'verified_without_local_copy':0,'streams':{}}
        with patch('app.operations_status.workers',return_value=[]),patch('app.operations_status.upload_coverage',return_value=coverage):
            result=self.status.snapshot(self.day)
        self.assertEqual(result['uploads']['percent'],99.9)
        self.assertFalse(result['uploads']['complete'])

    def test_missing_tick_metrics_are_unknown_when_tick_files_exist(self):
        p=self.base/'data_lake'/'stream=md_ticks_eq'/('dt='+self.day)/'part.parquet'
        p.parent.mkdir(parents=True);p.write_bytes(b'archive')
        with patch('app.operations_status.workers',return_value=[]):
            result=self.status.snapshot(self.day)
        self.assertIsNone(result['archive']['equity_ticks'])
        self.assertEqual(result['collection_state'],'recorded')

    def test_live_batch_is_only_attributed_to_dates_in_its_manifest(self):
        path=self.base/'runtime/upload-data_lake.txt'
        path.write_text('stream=md_ticks_eq/dt='+self.day+'/part.parquet\n')
        worker={'updated_at':datetime.now().astimezone().isoformat()}
        self.assertTrue(batch_includes_day(self.base,self.day,worker))
        previous=(datetime.fromisoformat(self.day)-timedelta(days=1)).date().isoformat()
        self.assertFalse(batch_includes_day(self.base,previous,worker))

    def test_dashboard_port_cannot_be_bound_by_a_second_server(self):
        first=LocalServer(('127.0.0.1',0),make_handler(self.status,BASE/'operations'))
        try:
            with self.assertRaises(OSError):
                second=LocalServer(('127.0.0.1',first.server_port),make_handler(self.status,BASE/'operations'))
                second.server_close()
        finally:
            first.server_close()

    def test_daily_summary_survives_cleaned_worker_logs(self):
        (self.day_logs/'arch_layers.log').write_text('[ARCHIVER] md:ticks:eq rows=20 files=1\n')
        with patch('app.operations_status.workers',return_value=[]):
            self.status.snapshot(self.day)
            (self.day_logs/'arch_layers.log').unlink()
            other=OperationsStatus(self.base);other.tasks=lambda:{}
            self.assertEqual(other.snapshot(self.day)['archive']['equity_ticks'],20)

    def test_http_routes_are_read_only_and_do_not_expose_private_files(self):
        self.status.snapshot=lambda selected=None:{'date':self.day,'ok':True} if selected is None else (_ for _ in ()).throw(ValueError())
        server=ThreadingHTTPServer(('127.0.0.1',0),make_handler(self.status,BASE/'operations'))
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            root=f'http://127.0.0.1:{server.server_port}'
            with urlopen(root+'/api/health') as response:
                self.assertEqual(json.load(response)['app'],'smartapi-operations')
            for path,headers,method,code in [('/.env',{},'GET',404),('/runtime/rclone.conf',{},'GET',404),('/api/status?date=../.env',{},'GET',400),('/api/status',{'Host':'evil.example'},'GET',403),('/api/status',{},'POST',405)]:
                with self.subTest(path=path,method=method),self.assertRaises(HTTPError) as error:
                    urlopen(Request(root+path,headers=headers,method=method))
                self.assertEqual(error.exception.code,code)
        finally:
            server.shutdown();server.server_close();thread.join(3)

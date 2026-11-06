"""Small read-only operations snapshots. Never imports pandas, Arrow or Streamlit."""
from __future__ import annotations

import ctypes
from contextlib import closing
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import threading
import time
from zoneinfo import ZoneInfo

from market_automation import load_config, session, upload_coverage


ROW = re.compile(r'\[ARCHIVER\]\s+(\S+)\s+rows=(\d+)\s+files=(\d+)')
DAY = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text(encoding='utf-8-sig'))
    except (OSError, ValueError):
        return {} if default is None else default


def creation_ticks(pid):
    if os.name != 'nt':
        return False
    from ctypes import wintypes
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
    try:
        handle = kernel.OpenProcess(0x1000, False, int(pid))
        if not handle:
            return False
        try:
            values = [wintypes.FILETIME() for _ in range(4)]
            if not kernel.GetProcessTimes(handle, *[ctypes.byref(v) for v in values]):
                return None
            start = (values[0].dwHighDateTime << 32) + values[0].dwLowDateTime + 504911232000000000
            return start
        finally:
            kernel.CloseHandle(handle)
    except (ValueError, TypeError, OSError):
        return False


def process_matches(pid, ticks=None, before=None):
    start = creation_ticks(pid)
    if not start:
        return False
    if ticks is not None:
        return str(start) == str(ticks)
    if before:
        try:
            return (start - 621355968000000000) / 10_000_000 <= datetime.fromisoformat(before).timestamp() + 1
        except (TypeError, ValueError):
            return False
    return True


def workers(base):
    running = []
    for path in (base / 'logs').glob('*/pids/*.process.json'):
        record = read_json(path)
        if process_matches(record.get('Id'), record.get('StartTicks')):
            running.append(str(record.get('Name', 'worker')))
    return sorted(set(running))


def stamp_ticks(ticks, zone):
    try:
        return datetime.fromtimestamp((int(ticks) - 621355968000000000) / 10_000_000, zone).isoformat()
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def archive_summary(base, day, zone):
    """Accumulate per-run progress; preserved previous logs cover worker restarts."""
    folder = base / 'logs' / day
    previous = sorted(folder.glob('arch_layers.log.*.previous'))
    paths = previous + ([folder / 'arch_layers.log'] if (folder / 'arch_layers.log').exists() else [])
    totals = {}
    current = {}
    for path in paths:
        current = {}
        try:
            with path.open(encoding='utf-8-sig', errors='replace') as file:
                for line in file:
                    match = ROW.search(line)
                    if not match:
                        continue
                    name, rows, parts = match.group(1), int(match.group(2)), int(match.group(3))
                    old = current.get(name, (0, 0))
                    total = totals.setdefault(name, {'rows': 0, 'parts': 0})
                    total['rows'] += rows - old[0] if rows >= old[0] else rows
                    total['parts'] += parts - old[1] if parts >= old[1] else parts
                    current[name] = (rows, parts)
        except OSError:
            continue
    for path in (base / 'runtime' / 'archive_metrics').glob('*.json'):
        value = read_json(path)
        try:
            if datetime.fromisoformat(value['updated_at']).astimezone(zone).date().isoformat() != day:
                continue
            name = value['stream']
            rows, parts = int(value['rows_archived_this_run']), int(value['files_written_this_run'])
            total = totals.setdefault(name, {'rows': 0, 'parts': 0})
            old = current.get(name, (0, 0))
            total['rows'] += max(0, rows - old[0])
            total['parts'] += max(0, parts - old[1])
            total['updated_at'] = value['updated_at']
        except (KeyError, ValueError, TypeError):
            continue
    return totals


def tail(path, limit=20000):
    try:
        with path.open('rb') as file:
            file.seek(0, 2)
            file.seek(max(0, file.tell() - limit))
            return file.read(limit).decode('utf-8', errors='replace')
    except OSError:
        return ''


def safe_batch(base):
    paths = sorted((base / 'logs' / 'automation').glob('*drive-worker.log'), reverse=True)
    text = tail(paths[0]) if paths else ''
    copies = list(re.finditer(r'Transferred:\s*(\d+)\s*/\s*(\d+),\s*\d+%', text))
    result = {'copied': None, 'total': None}
    if copies:
        result.update(copied=int(copies[-1][1]), total=int(copies[-1][2]))
    # Return only allowlisted descriptions; never raw logs, URLs or exceptions.
    if re.search(r'lookup .*no such host|resolve host', text, re.I):
        result['issue'] = 'Network name lookup failed. Completed checkpoints are retained.'
    elif re.search(r'Error 403|insufficientPermissions|access_denied', text, re.I):
        result['issue'] = 'Drive access was refused. Check the authorized account.'
    elif 'Traceback (most recent call last)' in text:
        result['issue'] = 'An upload attempt failed. The scheduled worker will retry.'
    return result


def batch_includes_day(base, day, uploader):
    """Attribute a live batch to a session, rather than every historical date."""
    paths = list((base / 'runtime').glob('upload-*.txt'))
    try:
        path = max(paths, key=lambda p: p.stat().st_mtime)
        if path.stat().st_mtime < datetime.fromisoformat(uploader['updated_at']).timestamp() - 1:
            return False
        with path.open(encoding='utf-8-sig', errors='replace') as file:
            text = file.read(262144)
        return '/dt=' + day + '/' in text or text.startswith(day + '/') or '\n' + day + '/' in text
    except (OSError, ValueError, KeyError):
        return False


def live_feed(zone):
    """Only called while collection workers are active; never starts Redis."""
    from app.config import REDIS_URL
    import redis
    client = redis.Redis.from_url(REDIS_URL, socket_connect_timeout=.5, socket_timeout=.5)
    try:
        stamps = []
        for stream in ('md:ticks:eq', 'md:ticks:opt'):
            latest = client.xrevrange(stream, count=1)
            if latest:
                stamps.append(int(latest[0][0].split(b'-')[0]) / 1000)
        return {'reachable': True, 'latest_at': datetime.fromtimestamp(max(stamps), zone).isoformat() if stamps else None}
    except (redis.RedisError, OSError, ValueError):
        return {'reachable': False, 'latest_at': None}
    finally:
        client.close()


class OperationsStatus:
    def __init__(self, base):
        self.base = Path(base)
        self.lock = threading.Lock()
        self.cache = {}
        self.task_cache = (0, {})

    def tasks(self):
        if time.monotonic() - self.task_cache[0] < 60:
            return self.task_cache[1]
        result = {}
        if os.name == 'nt':
            command = "[Console]::OutputEncoding=[Text.UTF8Encoding]::new(); @('SmartAPI-MarketHours','SmartAPI-DriveUploads') | ForEach-Object { $t=Get-ScheduledTask -TaskName $_ -ErrorAction SilentlyContinue; if($t){$i=Get-ScheduledTaskInfo -TaskName $_; [pscustomobject]@{name=$t.TaskName;state=$t.State.ToString();next=$i.NextRunTime.ToString('o')}} } | ConvertTo-Json -Compress"
            try:
                value = subprocess.run(['powershell', '-NoProfile', '-NonInteractive', '-Command', command],
                                       capture_output=True, encoding='utf-8', timeout=10,
                                       creationflags=subprocess.CREATE_NO_WINDOW)
                if value.returncode == 0 and value.stdout.strip():
                    data = json.loads(value.stdout)
                    result = {item['name']: item for item in (data if isinstance(data, list) else [data])}
            except (OSError, ValueError, subprocess.TimeoutExpired):
                pass
        self.task_cache = (time.monotonic(), result)
        return result

    def dates(self, today):
        dates = {today}
        for path in (self.base / 'logs').glob('*'):
            if path.is_dir() and DAY.fullmatch(path.name):
                dates.add(path.name)
        for stream in (self.base / 'data_lake').glob('stream=*'):
            for part in stream.glob('dt=*'):
                if DAY.fullmatch(part.name[3:]):
                    dates.add(part.name[3:])
        return sorted(dates, reverse=True)

    def history(self, day, streams, start, drain):
        path = self.base / 'runtime' / 'operations-history.sqlite3'
        path.parent.mkdir(exist_ok=True)
        with closing(sqlite3.connect(path)) as db:
            try:
                db.execute('CREATE TABLE IF NOT EXISTS sessions (date TEXT PRIMARY KEY, data TEXT NOT NULL)')
                old = db.execute('SELECT data FROM sessions WHERE date=?', (day,)).fetchone()
                saved = json.loads(old[0]) if old else {}
                if not streams:
                    streams = saved.get('streams', {})
                start = start or saved.get('start')
                drain = drain or saved.get('drain')
                data = {'streams': streams, 'start': start, 'drain': drain}
                db.execute('INSERT OR REPLACE INTO sessions VALUES (?,?)', (day, json.dumps(data)))
                db.commit()
                return data
            finally:
                db.close()

    def snapshot(self, selected=None):
        with self.lock:
            config = load_config(self.base)
            zone = ZoneInfo(config['timezone'])
            now = datetime.now(zone)
            day = selected or now.date().isoformat()
            if not DAY.fullmatch(day) or datetime.strptime(day, '%Y-%m-%d').date().isoformat() != day:
                raise ValueError('Choose a valid session date')
            cached = self.cache.get(day)
            if cached and time.monotonic() - cached[0] < 12:
                return cached[1]
            coverage = upload_coverage(self.base, day, include_streams=True)
            streams = archive_summary(self.base, day, zone)
            running = workers(self.base)
            state = read_json(self.base / 'runtime' / 'market-state.json')
            progress = read_json(self.base / 'runtime' / 'upload-progress.json')
            uploader = read_json(self.base / 'runtime' / 'upload-worker.json')
            current_session = session(config, now)
            selected_session = session(config, datetime.fromisoformat(day + 'T12:00').replace(tzinfo=zone))
            launch = read_json(self.base / 'logs' / day / 'pids' / 'producer.process.json')
            started = stamp_ticks(launch.get('StartTicks'), zone)
            drained = read_json(self.base / 'runtime' / 'archiver-drained.json')
            drain = None
            try:
                when = datetime.fromtimestamp(float(drained['drained_at']), zone)
                if when.date().isoformat() == day:
                    drain = {'at': when.isoformat(), 'streams': int(drained['streams'])}
            except (KeyError, ValueError, TypeError, OSError):
                pass
            history = self.history(day, streams, started, drain)
            streams, started, drain = history['streams'], history['start'], history['drain']
            today = now.date().isoformat()
            current = day == today
            paused = (self.base / 'runtime' / 'uploads.paused').exists()
            uploading = bool(uploader.get('pid') and process_matches(uploader['pid'], before=uploader.get('updated_at')))
            tasks = self.tasks()
            upload_task = tasks.get('SmartAPI-DriveUploads', {}).get('state', 'Unknown')
            uploading = uploading or upload_task == 'Running'
            total, verified = coverage['tracked_files'], coverage['tracked_verified']
            pending = coverage['awaiting_verification']
            if current and running:
                collection_state = 'running'
                headline = 'Collection is running'
                detail = f'{len(running)} tracked workers are active. Archives and uploads update independently.'
            elif current and current_session['phase'] == 'open':
                collection_state, headline = 'attention', 'Collection needs attention'
                detail = 'The market session is open, but no tracked collection workers are active.'
            elif drain:
                collection_state, headline = 'stopped', 'Collection finished for this session'
                detail = f"All {drain['streams']} archive streams drained. " + ('Drive verification is still in progress.' if pending else 'Check the verified archive coverage below.')
            elif started or streams or coverage['tracked_files']:
                collection_state, headline = 'recorded', 'Collection activity recorded'
                detail = 'Archive activity exists for this date. A complete shutdown confirmation is not available.'
            else:
                collection_state, headline = 'scheduled', 'No collection recorded for this date'
                detail = 'This may be a non-trading day, a session that has not started, or a day without local records.'
            files = coverage['streams']
            table = []
            names = {'stream=' + name.replace(':', '_'): name for name in streams}
            for folder in files:
                names.setdefault(folder, folder.removeprefix('stream='))
            for name in sorted(names.values()):
                # Match the actual folder spelling; stream names can themselves contain underscores.
                folder = 'stream=' + name.replace(':', '_')
                item = files.get(folder, {})
                metric = streams.get(name, {})
                table.append({'name': name, 'rows': metric.get('rows'), 'files': item.get('files', 0),
                              'verified': item.get('verified', 0), 'bytes': item.get('bytes', 0),
                              'updated_at': metric.get('updated_at')})
            next_open = None
            for offset in range(15):
                candidate = (now + timedelta(days=offset)).replace(hour=12, minute=0, second=0, microsecond=0)
                proposed = session(config, candidate)
                if proposed['phase'] in ('closed', 'calendar_expired'):
                    continue
                opening = datetime.fromisoformat(candidate.date().isoformat() + 'T' + proposed['open']).replace(tzinfo=zone)
                if opening > now:
                    next_open = opening.isoformat()
                    break
            events = []
            if started:
                events.append({'at': started, 'title': 'Collection launched', 'detail': 'Producer process identity recorded for this session.', 'kind': 'good'})
            if drain:
                events.append({'at': drain['at'], 'title': 'Archive drain complete', 'detail': f"{drain['streams']} streams confirmed flushed.", 'kind': 'good'})
            if progress.get('last_verified_at') and datetime.fromisoformat(progress['last_verified_at']).astimezone(zone).date().isoformat() == day:
                events.append({'at': progress['last_verified_at'], 'title': 'Drive batch verified', 'detail': f"{progress.get('uploaded_checked', 0):,} files verified in the latest upload chunk.", 'kind': 'good'})
            archive_errors = read_json(self.base / 'runtime' / 'archiver-errors.json')
            alerts = []
            if archive_errors and current:
                alerts.append('An archiver reported a data gap. Review its recovery report before clearing it.')
            if paused:
                alerts.append('Uploads are paused for Google account verification.')
            if tasks.get('SmartAPI-MarketHours', {}).get('state') == 'Disabled':
                alerts.append('The collection schedule is disabled.')
            if upload_task == 'Disabled':
                alerts.append('The Drive upload schedule is disabled. Pending files will not resume automatically.')
            if progress.get('account') and progress['account'].casefold() != config.get('expected_drive_email', '').casefold():
                alerts.append('The latest upload receipt belongs to a different configured account.')
            batch = safe_batch(self.base)
            if not uploading and uploader.get('state') == 'failed' and not paused:
                alerts.append(batch.get('issue', 'The previous upload worker failed. The next scheduled check will retry.'))
            feed = None
            if current and 'producer' in running and current_session['phase'] == 'open':
                feed = live_feed(zone)
                stale = not feed['latest_at'] or (now - datetime.fromisoformat(feed['latest_at'])).total_seconds() > 120
                if not feed['reachable'] or stale:
                    collection_state, headline = 'attention', 'Market feed needs attention'
                    detail = 'Workers are active, but fresh Redis ticks could not be confirmed.'
                    alerts.append(detail)
            disk = shutil.disk_usage(self.base)
            percent = round(100 * verified / total, 1) if total else 0
            if pending:
                percent = min(percent, 99.9)
            equity = streams.get('md:ticks:eq', {}).get('rows')
            options = streams.get('md:ticks:opt', {}).get('rows')
            if equity is None and 'stream=md_ticks_eq' not in files:
                equity = 0
            if options is None and 'stream=md_ticks_opt' not in files:
                options = 0
            result = {'observed_at': now.isoformat(), 'today': today, 'date': day, 'dates': self.dates(today),
                      'headline': headline, 'detail': detail, 'collection_state': collection_state,
                      'workers': running, 'tasks': tasks, 'next_open': next_open, 'alerts': alerts, 'feed': feed,
                      'session': {'prepare': selected_session.get('open') and config.get('special_sessions', {}).get(day, config)['prepare_time'],
                                  'open': selected_session.get('open'), 'close': selected_session.get('close'),
                                  'phase': current_session['phase'] if current else selected_session['phase'],
                                  'started_at': started, 'drain': drain, 'trading_day': selected_session['phase'] not in ('closed', 'calendar_expired')},
                      'archive': {'rows': sum(x.get('rows', 0) for x in streams.values()) if streams else None,
                                  'equity_ticks': equity,
                                  'option_ticks': options,
                                  'stream_count': len(streams), 'files': total,
                                  'bytes': sum(x['bytes'] for x in files.values())},
                      'uploads': {'total': total, 'verified': verified, 'pending': pending,
                                  'percent': percent,
                                  'complete': bool(total and pending == 0), 'running': uploading, 'paused': paused,
                                  'working_on_session': uploading and batch_includes_day(self.base, day, uploader),
                                  'account': config.get('expected_drive_email', ''), 'folder': config['drive_remote'].split(':', 1)[-1],
                                  'last_verified_at': progress.get('last_verified_at'), 'batch': batch,
                                  'remote_only': coverage['verified_without_local_copy']},
                      'streams': table, 'events': sorted(events, key=lambda x: x['at'], reverse=True),
                      'disk_free_bytes': disk.free}
            self.cache[day] = (time.monotonic(), result)
            return result

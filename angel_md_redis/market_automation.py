"""Market calendar and rclone uploader. Credentials stay in runtime/rclone.conf."""
from __future__ import annotations

import argparse
import configparser
from contextlib import closing
import hashlib
import json
import os
import sys
from pathlib import Path
import subprocess
import sqlite3
import time
from datetime import datetime
from zoneinfo import ZoneInfo

BASE = Path(__file__).resolve().parent
RUNTIME = BASE / "runtime"


def rclone_process_options() -> dict:
    """Avoid console allocation/input waits under the pythonw scheduled task."""
    options = {"stdin": subprocess.DEVNULL}
    if os.name == "nt":
        options["creationflags"] = subprocess.CREATE_NO_WINDOW
    return options


def load_config(base: Path = BASE) -> dict:
    config = json.loads((base / "automation.json").read_text(encoding="utf-8-sig"))
    override = base / "automation.local.json"
    if override.exists():
        config.update(json.loads(override.read_text(encoding="utf-8-sig")))
    if int(config["local_retention_days"]) < 1:
        raise ValueError("Keep at least one day locally")
    return config


def session(config: dict, now: datetime | None = None) -> dict:
    now = (now or datetime.now(ZoneInfo(config["timezone"]))).astimezone(ZoneInfo(config["timezone"]))
    date = now.date().isoformat()
    special = config.get("special_sessions", {}).get(date)
    if special is not None:
        times = special
    elif now.year not in config["calendar_years"]:
        return {"phase": "calendar_expired", "date": date}
    elif now.weekday() >= 5 or date in config["holidays"]:
        return {"phase": "closed", "date": date}
    else:
        times = config
    prepare, opening, closing = (times[k] for k in ("prepare_time", "open_time", "close_time"))
    if not prepare <= opening < closing:
        raise ValueError("Expected prepare_time <= open_time < close_time")
    clock = now.strftime("%H:%M")
    phase = "before" if clock < prepare else "prepare" if clock < opening else "open" if clock < closing else "after"
    return {"phase": phase, "date": date, "open": opening, "close": closing}


def md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_drive_account(base: Path = BASE, run=subprocess.run) -> dict:
    """Check Google identity without exposing OAuth credentials in output."""
    import requests
    config = load_config(base)
    expected = config.get("expected_drive_email", "").strip().casefold()
    if not expected:
        raise RuntimeError("Set expected_drive_email before verifying the Google account")
    credentials = base / "runtime" / "rclone.conf"
    remote = config["drive_remote"].split(":", 1)[0]
    # Refresh an expired access token through rclone before reading the token.
    result = run([str(base / "tools" / "rclone.exe"), "about", remote + ":",
                  "--config", str(credentials), "--json", "--contimeout", "10s", "--timeout", "30s",
                  "--retries", "2", "--low-level-retries", "2"],
                 capture_output=True, text=True, check=False, timeout=90,
                 **rclone_process_options())
    if result.returncode:
        raise RuntimeError("Cannot access Google Drive; reconnect the remote before continuing")
    saved = configparser.ConfigParser(interpolation=None)
    saved.read(credentials, encoding="utf-8-sig")
    token = json.loads(saved.get(remote, "token"))
    # Transient TLS/network disconnects must not abort an otherwise healthy pass.
    # Retry only the read; keep certificate verification and account checks intact.
    for attempt in range(3):
        try:
            response = requests.get("https://www.googleapis.com/drive/v3/about",
                                    params={"fields": "user(emailAddress,displayName)"},
                                    headers={"Authorization": "Bearer " + token["access_token"]}, timeout=20)
            break
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    if not response.ok:
        raise RuntimeError(f"Cannot verify Drive identity (HTTP {response.status_code}); uploads remain blocked")
    identity = response.json().get("user", {})
    actual = identity.get("emailAddress", "").strip().casefold()
    if actual != expected:
        raise RuntimeError(f"Wrong Google account: connected {actual or 'unknown'}, expected {expected}. Uploads blocked.")
    return {"email": actual, "verified": True}


class DriveUploader:
    def __init__(self, base: Path = BASE, run=subprocess.run):
        self.base, self.run = base.resolve(), run
        self.runtime = self.base / "runtime"
        self.runtime.mkdir(exist_ok=True)
        self.config = load_config(self.base)
        self.exe = self.base / "tools" / "rclone.exe"
        self.credentials = self.runtime / "rclone.conf"

    def command(self, *args: str) -> None:
        if (self.runtime / "uploads.paused").exists():
            raise RuntimeError("Drive uploads paused pending confirmation of the correct Google account")
        result = self.run([str(self.exe), *args, "--config", str(self.credentials)],
                          cwd=str(self.base), check=False, timeout=1800,
                          **rclone_process_options())
        if result.returncode:
            raise RuntimeError(f"rclone {args[0]} failed ({result.returncode}); local files retained")

    def ready(self) -> bool:
        if not self.exe.exists() or not self.credentials.exists():
            return False
        result = self.run([str(self.exe), "listremotes", "--config", str(self.credentials)],
                          capture_output=True, text=True, check=False, timeout=30,
                          **rclone_process_options())
        name = self.config["drive_remote"].split(":", 1)[0] + ":"
        return result.returncode == 0 and name in result.stdout.splitlines()

    def _files(self, source: Path, root: Path, now: float, today: str, final: bool):
        """Walk one partition lazily; avoid resolving/hashing the entire lake."""
        for directory, dirs, files in os.walk(root, followlinks=False):
            dirs[:] = sorted((name for name in dirs if not name.startswith('.') and name not in ('pids', 'automation')
                              and not (getattr(os.lstat(Path(directory) / name), 'st_file_attributes', 0) & 0x400)
                              and not (Path(directory) / name).is_symlink()), reverse=True)
            if not Path(directory).resolve().is_relative_to(source.resolve()):
                dirs[:] = []
                continue
            for name in sorted(files, reverse=True):
                path = Path(directory) / name
                relative = path.relative_to(source).as_posix()
                if name.startswith('.') or path.suffix in ('.tmp', '.pid') or '\n' in relative or '\r' in relative:
                    continue
                stat = path.lstat()
                if path.is_symlink() or getattr(stat, 'st_file_attributes', 0) & 0x400:
                    continue
                if not final and (stat.st_mtime > now - 120 or (source.name == 'logs' and relative.startswith(today + '/'))):
                    continue
                yield relative, stat

    def _groups(self, source: Path, today: str, now: float, final: bool):
        if source.name == 'data_lake':
            streams = sorted((p for p in source.iterdir() if p.is_dir() and not p.is_symlink()), key=lambda p: p.name)
            live = [stream / ('dt=' + today) for stream in streams if (stream / ('dt=' + today)).is_dir()]
            # Yield the live generators first, without scanning any historical files.
            yield [self._files(source, root, now, today, final) for root in live]
            historical = [date for stream in streams for date in sorted(stream.iterdir(), reverse=True)
                          if date.is_dir() and date.name != 'dt=' + today]
            yield [self._files(source, root, now, today, final) for root in historical]
        else:
            yield [self._files(source, source, now, today, final)]

    def upload(self, final: bool = False, cleanup: bool = True, max_batches: int | None = None) -> dict:
        if max_batches is None:
            max_batches = int(self.config.get('upload_max_batches', 8))
        if max_batches < 1:
            raise ValueError('upload_max_batches must be at least 1')
        if (self.runtime / "uploads.paused").exists():
            raise RuntimeError("Drive uploads paused pending confirmation of the correct Google account")
        print('Checking local rclone configuration', flush=True)
        if not self.ready():
            raise RuntimeError("Google Drive not connected. Run setup_drive.ps1 once on the laptop.")
        if self.config.get("expected_drive_email"):
            print('Verifying Google Drive account', flush=True)
            verify_drive_account(self.base, self.run)
        print('Drive ready; selecting archive batch', flush=True)
        remote = self.config["drive_remote"].rstrip("/")
        if ":" not in remote or not remote.split(":", 1)[1].strip("/"):
            raise ValueError("drive_remote must name a dedicated folder, e.g. gdrive:SmartAPI")
        report = {"uploaded_checked": 0, "removed_local": 0, "final": final}
        report['pass_complete'] = False
        report['remote'] = remote
        report['account'] = self.config.get('expected_drive_email', '')
        now = time.time()
        cutoff = now - int(self.config["local_retention_days"]) * 86400
        today = datetime.now(ZoneInfo(self.config["timezone"])).date().isoformat()
        database = sqlite3.connect(self.runtime / 'verified-uploads.sqlite3')
        database.execute('CREATE TABLE IF NOT EXISTS verified (target TEXT, folder TEXT, path TEXT, size INTEGER, mtime INTEGER, PRIMARY KEY(target,folder,path))')
        target = remote + '|' + report['account']
        batch_count = 0
        try:
          for folder in ("data_lake", "logs", "client_output"):
            source = self.base / folder
            if not source.is_dir():
                continue
            known = {path: (size, mtime) for path, size, mtime in database.execute(
                'SELECT path,size,mtime FROM verified WHERE target=? AND folder=?', (target, folder))}
            for group in self._groups(source, today, now, final):
              iterators = list(group)
              while iterators:
                candidates = {}
                while iterators and len(candidates) < int(self.config.get('upload_batch_files', 128)):
                  for iterator in list(iterators):
                    for relative, stat in iterator:
                        if known.get(relative) != (stat.st_size, stat.st_mtime_ns) or (cleanup and stat.st_mtime < cutoff):
                            old = stat.st_mtime < cutoff
                            candidates[relative] = (stat.st_size, stat.st_mtime_ns, md5(source / relative) if old and cleanup else None)
                            break
                    else:
                        iterators.remove(iterator)
                    if len(candidates) >= int(self.config.get('upload_batch_files', 128)):
                        break
                if not candidates:
                    continue
                manifest = self.runtime / f"upload-{folder}.txt"
                manifest.write_text("".join(name + "\n" for name in candidates), encoding="utf-8")
                matched = self.runtime / f"verified-{folder}.txt"
                matched.unlink(missing_ok=True)
                destination = remote + "/" + folder
                common = (str(source), destination, "--files-from-raw", str(manifest))
                print(f'Uploading {len(candidates)} files to {destination}; today first', flush=True)
                transfers = str(int(self.config.get('upload_transfers', 8)))
                checkers = str(int(self.config.get('upload_checkers', 16)))
                network = ("--retries", "3", "--low-level-retries", "3", "--contimeout", "20s", "--timeout", "2m")
                self.command("copy", *common, "--checksum", "--transfers", transfers, "--checkers", checkers,
                             *network, "--stats", "15s", "--stats-log-level", "NOTICE")
                self.command("check", *common, "--one-way", "--match", str(matched), "--checkers", checkers, *network)
                verified = set(matched.read_text(encoding="utf-8-sig").splitlines())
                if verified != set(candidates):
                    raise RuntimeError("Verification report incomplete; local files retained")
                report["uploaded_checked"] += len(verified)
                for relative, (size, mtime, digest) in candidates.items():
                    if (self.runtime / "uploads.paused").exists():
                        raise RuntimeError("Uploads paused; local cleanup stopped")
                    known[relative] = (size, mtime)
                    database.execute('INSERT OR REPLACE INTO verified VALUES (?,?,?,?,?)', (target, folder, relative, size, mtime))
                    if digest is None:
                        continue
                    path = source / relative
                    stat = path.stat()
                    if path.is_symlink() or not path.resolve().is_relative_to(source.resolve()):
                        continue
                    if stat.st_size == size and stat.st_mtime_ns == mtime and md5(path) == digest:
                        path.unlink()
                        report["removed_local"] += 1
                database.commit()
                report['last_verified_at'] = datetime.now().astimezone().isoformat()
                report['last_verified_files'] = [folder + '/' + name for name in candidates][:40]
                progress = self.runtime / 'upload-progress.json'
                progress.with_suffix('.tmp').write_text(json.dumps(report, indent=2), encoding='utf-8')
                progress.with_suffix('.tmp').replace(progress)
                batch_count += 1
                # Reopen partition iterators on the next run so an overnight
                # backfill cannot keep serving yesterday's snapshot forever.
                current_day = datetime.now(ZoneInfo(self.config['timezone'])).date().isoformat()
                if current_day != today:
                    return report
                if batch_count >= max_batches:
                    return report
        finally:
            database.close()
        report['pass_complete'] = True
        report["completed_at"] = datetime.now().astimezone().isoformat()
        receipt = self.runtime / "upload-status.json"
        temp = receipt.with_suffix(".tmp")
        temp.write_text(json.dumps(report, indent=2), encoding="utf-8")
        temp.replace(receipt)
        return report


def final_upload_ready(base: Path, config: dict) -> bool:
    """Include recent parts/current logs only after a confirmed graceful drain."""
    if session(config)['phase'] in ('prepare', 'open'):
        return False
    stop = base / 'runtime' / 'archiver.stop'
    drained = base / 'runtime' / 'archiver-drained.json'
    try:
        return float(json.loads(drained.read_text())['drained_at']) >= stop.stat().st_mtime
    except (OSError, ValueError, KeyError, TypeError):
        return False


def drain_uploads(base: Path = BASE, *, cleanup=True, max_batches=None, uploader_factory=DriveUploader) -> dict:
    """Refresh priorities between chunks; after close, keep going until caught up."""
    total_checked = total_removed = passes = 0
    state_path = base / 'runtime' / 'upload-worker.json'
    while True:
        uploader = uploader_factory(base)
        final = final_upload_ready(base, uploader.config)
        passes += 1
        state = {'state': 'uploading', 'pid': os.getpid(), 'passes': passes,
                 'uploaded_checked': total_checked, 'removed_local': total_removed,
                 'updated_at': datetime.now().astimezone().isoformat()}
        state_path.parent.mkdir(exist_ok=True)
        temporary = state_path.with_suffix('.tmp')
        temporary.write_text(json.dumps(state, indent=2), encoding='utf-8')
        temporary.replace(state_path)
        try:
            result = uploader.upload(final=final, cleanup=cleanup, max_batches=max_batches)
        except Exception as error:
            state.update(state='failed', error_type=type(error).__name__,
                         updated_at=datetime.now().astimezone().isoformat())
            # Persist only error type; library exceptions can contain request URLs.
            temporary.write_text(json.dumps(state, indent=2), encoding='utf-8')
            temporary.replace(state_path)
            raise
        total_checked += result['uploaded_checked']
        total_removed += result['removed_local']
        live = session(uploader.config)['phase'] in ('prepare', 'open')
        state.update(uploaded_checked=total_checked, removed_local=total_removed,
                     pass_complete=result['pass_complete'], final=final,
                     state='caught_up' if result['pass_complete'] else 'yielded' if live else 'uploading',
                     updated_at=datetime.now().astimezone().isoformat())
        temporary.write_text(json.dumps(state, indent=2), encoding='utf-8')
        temporary.replace(state_path)
        if result['pass_complete'] or live:
            return state
        # The existing upload lock remains held for the whole drain. A fresh pass
        # re-evaluates the date, shutdown marker, credentials and file priorities.
        print(f'Continuing backlog: {total_checked} files verified in this worker', flush=True)


def upload_coverage(base: Path = BASE, date: str | None = None, *, include_streams: bool = False) -> dict:
    """Read local checksum checkpoints for today's published archive files."""
    config = load_config(base)
    today = date or datetime.now(ZoneInfo(config['timezone'])).date().isoformat()
    if datetime.strptime(today, '%Y-%m-%d').date().isoformat() != today:
        raise ValueError('Expected an ISO calendar date')
    target = config['drive_remote'].rstrip('/') + '|' + config.get('expected_drive_email', '')
    known = {}
    checkpoint = base / 'runtime' / 'verified-uploads.sqlite3'
    if checkpoint.exists():
        with closing(sqlite3.connect('file:' + checkpoint.resolve().as_posix() + '?mode=ro', uri=True)) as db:
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='verified'").fetchone():
                known = {path: (size, mtime) for path, size, mtime in db.execute(
                    'SELECT path,size,mtime FROM verified WHERE target=? AND folder=? AND path LIKE ?',
                    (target, 'data_lake', '%/dt=' + today + '/%'))}
    total = checked = 0
    streams = {}
    seen = set()
    source = base / 'data_lake'
    if source.is_dir():
        for stream in source.iterdir():
            partition = stream / ('dt=' + today)
            if stream.is_symlink() or not partition.is_dir() or partition.is_symlink():
                continue
            for directory, dirs, files in os.walk(partition, followlinks=False):
                dirs[:] = [name for name in dirs if not (Path(directory) / name).is_symlink()]
                for name in files:
                    path = Path(directory) / name
                    if name.startswith('.') or path.suffix != '.parquet' or path.is_symlink():
                        continue
                    try:
                        stat = path.stat()
                    except FileNotFoundError:
                        continue  # An old verified file can be cleaned during this read.
                    total += 1
                    relative = path.relative_to(source).as_posix()
                    seen.add(relative)
                    match = known.get(relative) == (stat.st_size, stat.st_mtime_ns)
                    checked += match
                    if include_streams:
                        item = streams.setdefault(stream.name, {'files': 0, 'verified': 0, 'bytes': 0})
                        item['files'] += 1
                        item['verified'] += match
                        item['bytes'] += stat.st_size
    result = {'date': today, 'account': config.get('expected_drive_email', ''),
            'remote': config['drive_remote'], 'local_archive_files': total,
            'verified_unchanged': checked, 'awaiting_verification': total - checked,
            'today_complete': total > 0 and total == checked}
    if include_streams:
        remote_only = set(known) - seen
        for relative in remote_only:
            item = streams.setdefault(relative.split('/')[0], {'files': 0, 'verified': 0, 'bytes': 0})
            item['files'] += 1
            item['verified'] += 1
            item['bytes'] += known[relative][0]
        result.update(tracked_files=total + len(remote_only),
                      tracked_verified=checked + len(remote_only),
                      verified_without_local_copy=len(remote_only), streams=streams)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["session", "upload", "status", "verify-drive", "coverage"])
    parser.add_argument("--final", action="store_true")
    parser.add_argument("--no-cleanup", action="store_true")
    parser.add_argument("--max-batches", type=int)
    parser.add_argument("--drain", action="store_true", help="Continue after-hours upload chunks until caught up")
    args = parser.parse_args()
    if args.command == "coverage":
        result = upload_coverage()
    elif args.command == "verify-drive":
        result = verify_drive_account()
    elif args.command == "session":
        result = session(load_config())
    elif args.command == "status":
        result = {**session(load_config()), "drive_ready": DriveUploader().ready()}
    else:
        if sys.platform == "win32":
            import ctypes
            ctypes.windll.kernel32.SetThreadExecutionState(0x80000001)
        # A byte-range lock is released by Windows even after a crash.
        import msvcrt
        RUNTIME.mkdir(exist_ok=True)
        with (RUNTIME / "upload.lock").open("a+b") as lock:
            lock.seek(0)
            try:
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                print(json.dumps({"skipped": "upload already running"}))
                return
            if args.drain:
                result = drain_uploads(cleanup=not args.no_cleanup, max_batches=args.max_batches)
            else:
                result = DriveUploader().upload(final=args.final, cleanup=not args.no_cleanup, max_batches=args.max_batches)
    print(json.dumps(result))


if __name__ == "__main__":
    main()

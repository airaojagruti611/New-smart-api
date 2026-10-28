"""Market calendar and rclone uploader. Credentials stay in runtime/rclone.conf."""
from __future__ import annotations

import argparse
import configparser
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
                  "--config", str(credentials), "--json"], capture_output=True, text=True, check=False)
    if result.returncode:
        raise RuntimeError("Cannot access Google Drive; reconnect the remote before continuing")
    saved = configparser.ConfigParser(interpolation=None)
    saved.read(credentials, encoding="utf-8-sig")
    token = json.loads(saved.get(remote, "token"))
    response = requests.get("https://www.googleapis.com/drive/v3/about",
                            params={"fields": "user(emailAddress,displayName)"},
                            headers={"Authorization": "Bearer " + token["access_token"]}, timeout=20)
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
                          cwd=str(self.base), check=False)
        if result.returncode:
            raise RuntimeError(f"rclone {args[0]} failed ({result.returncode}); local files retained")

    def ready(self) -> bool:
        if not self.exe.exists() or not self.credentials.exists():
            return False
        result = self.run([str(self.exe), "listremotes", "--config", str(self.credentials)],
                          capture_output=True, text=True, check=False)
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
        if (self.runtime / "uploads.paused").exists():
            raise RuntimeError("Drive uploads paused pending confirmation of the correct Google account")
        if not self.ready():
            raise RuntimeError("Google Drive not connected. Run setup_drive.ps1 once on the laptop.")
        if self.config.get("expected_drive_email"):
            verify_drive_account(self.base, self.run)
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
                self.command("copy", *common, "--checksum", "--transfers", "4", "--retries", "3", "--contimeout", "20s", "--timeout", "2m", "--stats", "15s", "--stats-log-level", "NOTICE")
                self.command("check", *common, "--one-way", "--match", str(matched))
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
                if max_batches is not None and batch_count >= max_batches:
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["session", "upload", "status", "verify-drive"])
    parser.add_argument("--final", action="store_true")
    parser.add_argument("--no-cleanup", action="store_true")
    parser.add_argument("--max-batches", type=int)
    args = parser.parse_args()
    if args.command == "verify-drive":
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
            result = DriveUploader().upload(final=args.final, cleanup=not args.no_cleanup, max_batches=args.max_batches)
    print(json.dumps(result))


if __name__ == "__main__":
    main()

# Drive upload backlog recovery

Collection and uploads have separate scheduled tasks:

- `SmartAPI-MarketHours` starts/stops the collectors on the market calendar.
- `SmartAPI-DriveUploads` runs an upload-only, windowless worker every five
  minutes and at sign-in, independent of the market window. It never starts
  Docker, Redis or collection. It does not wake the laptop every five minutes.

The laptop must remain powered on, signed in and online to upload. A live upload
worker requests that Windows stay awake while it works; lid/power policies may
still suspend the machine. Reboot/interruption resumes from verified checkpoints.

## Why the 3 November backlog happened

The local configuration limited each run to two batches of 40 files, five minutes
apart. That permits roughly 960 files/hour even with a healthy network, while this
session generated 15,061 archive files. Upload checks also depended on the limited
collection-task window, so they stopped after the market session.

Defaults and this machine now use 256 files per batch, eight batches per chunk,
eight transfers and sixteen checkers. During market hours the worker yields after
a bounded chunk to refresh today's priorities. Outside market hours it immediately
continues the next chunk until a complete eligible-file pass succeeds. The date,
configuration and shutdown state are rechecked between chunks. No deliberate
five-minute idle gap is inserted while draining an after-hours backlog.

Current-day files come first; the worker then handles historical backfill. A
complete current-day archive is distinct from a complete historical upload pass.
Many small files can take substantial time even with little total byte volume.

## Install and start

From `angel_md_redis`:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\install_market_schedule.ps1
Start-ScheduledTask -TaskName SmartAPI-DriveUploads
Get-ScheduledTask -TaskName SmartAPI-DriveUploads
```

The installer registers both tasks. `-Remove` removes both. The upload task uses
IgnoreNew and the Python uploader holds its existing file lock throughout draining,
so repeated triggers/manual requests do not create concurrent upload writers.
The task has a 24-hour execution ceiling; a later trigger resumes interrupted work.

## Observe verified progress

```powershell
& .\.venv\Scripts\python.exe market_automation.py coverage
Get-Content .\runtime\upload-progress.json
Get-Content .\runtime\upload-worker.json
Get-Content .\runtime\upload-status.json -ErrorAction SilentlyContinue
```

- `coverage` compares today's existing completed Parquet parts against the local
  checksum-verification checkpoint, including size and modification time. Check
  `awaiting_verification` and `today_complete`. It does not perform a fresh remote
  audit or detect later manual deletion of a previously verified Drive file.
- `upload-progress.json` records each successfully verified batch in the current
  chunk. Counters reset on a new chunk; an old timestamp is not current progress.
- `upload-worker.json` records total verified counts for the current worker across
  chunks, state (`uploading`, `yielded`, `caught_up`, `failed`), and PID. Its totals
  update between chunks. A crash can leave stale `uploading` state: check actual
  task/process status and logs, not just that JSON value.
- `upload-status.json` records the last complete eligible-file pass and can be
  absent/stale while a large backlog is still draining.
- Worker logs: `logs/automation/YYYY-MM-DD-HHMMSS-drive-worker.log`.

To run in an interactive terminal instead of the task:

```powershell
& .\.venv\Scripts\python.exe market_automation.py upload --drain
```

The existing `upload --no-cleanup --max-batches 1` remains a bounded diagnostic
command. `market_automation.ps1 -UploadOnly` remains a bounded manual upload;
use the independent task or `--drain` to clear an after-hours backlog.

## Safeguards

Every chunk verifies the configured Google account. Every copied batch must pass
`rclone check` before checkpointing or retention cleanup. Local files are removed
only after that check, the retention age, and a final unchanged-file MD5 check.
No unchecked files or remote archives are deleted to speed up the backlog.

Final mode includes recent parts/current-day worker logs only outside the open
session and after a drain marker newer than the stop request. Network operations
have connection/idle/retry and outer process time limits. A failed worker exits;
the next scheduled check retries from completed checkpoints. The upload-pause
marker used during Google account changes still blocks upload and cleanup.

To pause future upload launches, disable `SmartAPI-DriveUploads`. This alone does
not stop a running uploader; inspect and stop only its known task/process if an
immediate stop is needed. Collection scheduling remains a separate decision.

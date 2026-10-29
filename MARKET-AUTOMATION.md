# Laptop collection and Google Drive storage

For installation on another developer's machine, start with
[DATA-COLLECTION-SETUP.md](DATA-COLLECTION-SETUP.md).

The Windows task `SmartAPI-MarketHours` checks the Indian market calendar every
minute on weekdays from 09:10 through 16:10 IST, plus at sign-in and during
configured special sessions. It runs in your signed-in Windows session without saving your Windows
password. You can lock the screen, but do not sign out, shut down, or close the
lid during collection. The task starts through `pythonw.exe` and a console-free
PowerShell child so Windows Terminal does not flash a window on each check.
Wake timers must be supported/enabled for waking from
sleep. Keep the laptop plugged in and connected to the internet.

## Daily operation

- 09:10 IST: start Docker Desktop and this project's Redis; seed candle history.
  History warm-up has a two-minute limit so slow backfill cannot block collection.
- 09:15 IST: start collection, analysis, dashboard and the Parquet archiver.
  During the session, the controller restarts stopped tracked workers without
  duplicating workers that are still running. Previous error logs are retained.
- 15:40 IST: stop ingestion, give analytics 15 seconds, then stop analytics and
  request the archiver to drain Redis and flush all remaining buffered rows.
- After the archiver confirms completion: stop this project's Redis container
  and upload the final files. Docker Desktop and other projects are left alone.
- At the configured interval (30 minutes by default): upload completed files in the background. Slow uploads do
  not delay the close controller. Failed uploads retain local data and retry
  while the controller is scheduled, or on the next signed-in market day.
- After checksum verification, remove local files older than two days. Keep
  recent files as an outage buffer. Remote files are never deleted by cleanup.

Times follow the NSE derivatives session currently published for October 2026:
[NSE timings](https://www.nseindia.com/resources/exchange-communication-holidays?article_id=432919950.0).
The calendar includes the 2026 equity/F&O holidays from
[NSE FAOP71777](https://nsearchives.nseindia.com/content/circulars/FAOP71777.pdf),
plus the January 15 election holiday. Review new exchange circulars for changes.
The configuration deliberately refuses to collect in an unconfigured year:
add the official 2027 holiday calendar before January. Special sessions such as
Muhurat require an explicit date/time entry in `special_sessions` followed by
rerunning `install_market_schedule.ps1` to add their triggers; weekends do
not automatically become trading days.

## One-time Google Drive connection

Portable rclone is installed in `angel_md_redis/tools/rclone.exe`. Its ZIP was
verified against the official SHA256SUMS. No system-wide installation is needed.

1. Open the official [rclone OAuth client guide](https://rclone.org/drive/#making-your-own-client-id).
   Create a Google Cloud project, enable Google Drive API, configure the Google
   Auth Platform consent screen for your account and create an OAuth client of
   type **Desktop app**. OAuth credentials are separate from paid Cloud Storage;
   uploads go to your existing Google Drive allocation.
2. Use Production consent status for the private app; External/Testing refresh
   tokens generally expire after seven days. An unverified-app screen may appear
   for your own client. Authorize only the client you created.
3. Open PowerShell on the laptop and run:

   ```powershell
   cd D:\New-smart-api\angel_md_redis
   powershell -NoProfile -ExecutionPolicy Bypass -File .\setup_drive.ps1 -Email you@example.com
   ```

4. In the interactive rclone menu, create remote **gdrive**, choose **drive**,
   enter the client ID and secret locally, choose **drive.file** scope (access
   to files created by the app), and authorize using the Google account that
   has the 5 TB quota. For personal My Drive, decline Shared Drive configuration.
5. The script checks quota and starts an initial verified upload without local
   deletion. Scheduled uploads then use `gdrive:SmartAPI` automatically.

Do not paste OAuth secrets into chat or commit them. Tokens live in ignored
`angel_md_redis/runtime/rclone.conf`. This connection cannot be completed without
your Google browser sign-in.

## Uploaded content

To change Google accounts, add the new account as a test user in the existing
Google Auth Platform project if it is still in Testing. Then run
`powershell -NoProfile -ExecutionPolicy Bypass -File .\switch_drive_account.ps1 -Email you@example.com`
from `angel_md_redis`. Omitting `-Email` reuses the account in your ignored local configuration.
The script leaves collection running and pauses uploads while reconnecting.
It verifies the email returned by Google before resuming. Every subsequent
upload checks the configured `expected_drive_email` again before transferring
or removing local files. No new OAuth client/project is required.

```
Google Drive / SmartAPI /
  data_lake/       # ticks, Greeks, candles and all archived signal streams
  logs/            # pipeline worker logs; current-day logs uploaded after stop
  client_output/   # exported client reports
```

The archiver now includes the capital allocation stream that the launcher
already runs. Existing Parquet files remain in their original partition layout.
Temporary parts, PID records and process identity files are excluded. Environment
files, code, OAuth tokens and active Docker/Redis persistence are not uploaded.
Automation's own operational transcripts remain local in `logs/automation` to
avoid uploading a log while the uploader itself is writing it.

Verification uses `rclone check` with size and file hashes. Cleanup requires a
matching remote report and rechecks the local file's size, timestamp and MD5
before removing it. A failed copy or verification prevents cleanup for that batch.
The archive provides at-least-once delivery; deduplicate `_redis_id` within a
stream when merging records after a crash.

## Commands

```powershell
cd D:\New-smart-api\angel_md_redis
# Status and connection state:
.\market_automation.ps1 -Status
# Manual upload (also applies verified two-day local retention):
.\market_automation.ps1 -UploadOnly
# Manual safe stop; the scheduler may restart during market hours:
.\stop_all.ps1
# Disable automation before a deliberate manual stop:
Disable-ScheduledTask -TaskName SmartAPI-MarketHours
# Enable again:
Enable-ScheduledTask -TaskName SmartAPI-MarketHours
# Reinstall or remove the task:
.\install_market_schedule.ps1
.\install_market_schedule.ps1 -Remove
```

Change `automation.json`, or put machine-specific overrides in ignored
`automation.local.json`, for example:

```json
{
  "drive_remote": "gdrive:SmartAPI",
  "local_retention_days": 2,
  "close_time": "15:40",
  "special_sessions": {}
}
```

For a confirmed special trading date, add its `prepare_time`, `open_time`, and
`close_time` inside `special_sessions` under an ISO `YYYY-MM-DD` key, then rerun
the installer. Reinstall after changing the normal session times, too. All times
use Asia/Kolkata regardless of the system clock timezone. Task installation
requires Windows to use India Standard Time so its 09:10 wake trigger agrees.

Receipts: `runtime/upload-status.json`. Session state: `runtime/market-state.json`.
Live batch progress: `runtime/upload-progress.json`, including the verified
account, `uploaded_checked`, `last_verified_at`, and `last_verified_files`.
The uploader transfers today's partitions before historical backfill, in bounded
batches (40 files on this laptop). It checkpoints verified files in SQLite so
interrupted uploads resume without hashing the full historical archive first.
Each run is bounded by `upload_max_batches` (default 8) and yields when the
calendar day changes, so the next run can prioritize the current day's files.
Set `upload_interval_minutes` and `upload_max_batches` in the local configuration
to tune how often the uploader refreshes that priority.
The progress count is for the current uploader run. `pass_complete: false` means
there is still pending work; it must not be read as a complete backup receipt.
Errors: `logs/automation/*upload.err.log` and the scheduled task's result/history.
If archiver drain times out, Redis is retained for recovery and a later tick
retries. Existing processes started by old scripts lack identity records; stop
those with their original launcher before relying on this automation.

Google Drive cannot add RAM to the laptop. Redis/Docker retain their own local
working state; the task stops this Redis after collection to release its RAM.
This does not shrink Docker Desktop's virtual disk or clean unrelated images.
Uploading many small Parquet parts may take time; the very first upload is a
backfill, and files stay local until verified. When the connection is offline
long enough to exhaust the local buffer, collection will need available disk
space or a shorter verified retention period.

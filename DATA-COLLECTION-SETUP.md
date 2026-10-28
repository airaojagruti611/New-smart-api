# Developer setup: market-hours collection with Google Drive archival

This guide installs the pipeline on a Windows laptop or desktop. Collection and
Redis run on that machine; Google Drive holds uploaded archive files. No cloud VM
is required. Use your own broker credentials, Google account, and OAuth client.

## 1. Understand the data flow

```text
Angel One WebSocket / REST
          |
          v
Redis Streams ---> analysis workers ---> derived signal streams
          |                                     |
          +----------> Parquet archiver <--------+
                              |
                              v
                      local data_lake/
                              |
                  rclone copy, then checksum check
                              |
                              v
                  Google Drive / SmartAPI /
                              |
            verified local files age past retention
                              |
                              v
                     local file removal only
```

Google Drive stores files; it does not supply RAM or run Python/Redis. Keep
enough local free space for the live working set, an internet outage, and the
initial historical backfill. Uploads run in batches and may lag collection.

## 2. Prepare the machine

Install Git, Python 3.11 or newer, and Docker Desktop with Linux containers.
Use Windows PowerShell 5.1 or newer. Configure Windows for **India Standard
Time**; the task installer checks this. Start Docker Desktop at least once and
finish its initial setup.

Clone the repository, then open PowerShell in its `angel_md_redis` directory:

```powershell
cd C:\path\to\repository\angel_md_redis
python -m venv .venv
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt
docker compose up -d
docker exec md_redis redis-cli ping
```

Redis must respond `PONG`. Docker Compose persists Redis in its named volume.
Do not delete that volume when stopping the pipeline.

Create an ignored `.env` in `angel_md_redis` using your own values:

```dotenv
ANGEL_API_KEY=replace_me
ANGEL_CLIENT_CODE=replace_me
ANGEL_PIN=replace_me
ANGEL_TOTP_SECRET=replace_me
REDIS_URL=redis://localhost:6379/0
ARCHIVE_TZ=Asia/Kolkata
LOG_LEVEL=INFO
```

Configure any broker-required IP settings described in [RUN.md](RUN.md). Edit
`symbols.txt` for the instruments to collect. Never commit `.env` or broker tokens.
The application also supports a workspace-root `.env` fallback; confirm which
file supplies credentials when troubleshooting.

## 3. Install portable rclone

The `tools` directory is ignored by Git and is absent from a fresh checkout.

1. Download the Windows ZIP matching your machine from the official
   [rclone downloads](https://rclone.org/downloads/) page.
2. Check the ZIP's SHA-256 against `SHA256SUMS` for the same release:

   ```powershell
   Get-FileHash C:\path\to\downloaded-rclone.zip -Algorithm SHA256
   ```

3. Extract the archive and place **rclone.exe** at
   `angel_md_redis\tools\rclone.exe`. Create `tools` if necessary.
4. Confirm it works:

   ```powershell
   & .\tools\rclone.exe version
   ```

Do not commit the downloaded binary or copy another developer's rclone config.

## 4. Create your Google OAuth desktop client

Follow the official [rclone client setup](https://rclone.org/drive/#making-your-own-client-id)
and [Google consent-screen guide](https://developers.google.com/workspace/guides/configure-oauth-consent).

1. Open Google Cloud Console and create/select a project for this integration.
2. Under **APIs & Services > Library**, enable **Google Drive API**.
3. Under **Google Auth Platform**, configure the app name, support email,
   audience, and contact email. For a personal Google account, choose **External**.
4. If the app is in **Testing**, add the actual destination Google account under
   **Audience > Test users**, even if another account owns the Cloud project.
5. Under **Clients > Create client**, choose **Desktop app**. Keep the resulting
   client ID and client secret locally.
6. For unattended use, review Google's publishing requirements and move the app
   out of Testing when appropriate. External/Testing refresh tokens generally
   expire after seven days when Drive access is requested, requiring reauthorization.

The Cloud project's owner and the Google Drive storage owner can be different.
The account selected in the browser authorization owns the uploaded files.
The scripts verify that email before starting uploads.

## 5. Configure local storage and authorize the correct account

Create ignored `automation.local.json` beside `automation.json`:

```json
{
  "expected_drive_email": "your-storage-account@example.com",
  "drive_remote": "gdrive:SmartAPI",
  "upload_interval_minutes": 30,
  "upload_batch_files": 40,
  "local_retention_days": 2
}
```

Use an account with enough available quota. `SmartAPI` is a dedicated folder
inside that account's My Drive. Retention must be at least one day.

Run the first-time setup, replacing the example email:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\setup_drive.ps1 -Email your-storage-account@example.com
```

Answer the interactive prompts as follows; some appear only when reconnecting:

| Prompt | Answer |
| --- | --- |
| New remote | `n` |
| Remote name | `gdrive` |
| Storage type | `drive` |
| Client ID / client secret | Your own desktop client's values |
| Scope | `3`, or the option labeled `drive.file` |
| Service account file | Leave blank; press Enter |
| Edit advanced configuration | `n` |
| Replace existing token | `y` |
| Use web browser to authenticate | `y` |
| Google account | Select the exact storage account in `expected_drive_email` |
| Configure as Shared Drive / Team Drive | `n` for this My Drive setup |
| Keep the remote | `y` |
| Back at the rclone main menu | `q` |

`drive.file` permits access to files created by this OAuth app. It does not grant
general access to unrelated existing Drive content. This setup targets My Drive;
using a Workspace Shared Drive needs a separate permissions/design review.

Return to PowerShell after the browser reports success. Setup verifies the actual
Google email, checks Drive access, and starts an initial upload with local cleanup
disabled. Keep that interactive window open until it completes. Scheduled
uploads can run silently after installation.

Credentials live in ignored `runtime/rclone.conf`. The scripts create
`runtime/uploads.paused` while authorization is in progress. A mismatched account
leaves uploads paused; data collection can continue locally.

To reconnect or change accounts later:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\switch_drive_account.ps1 -Email another-storage-account@example.com
```

Add that account as a test user first if the same OAuth app is still in Testing.
You can reuse your existing OAuth client for a different authorized Drive account.

## 6. Configure the market calendar and install automation

Review `automation.json` before enabling the task. The checked-in defaults are
for the NSE equity-derivatives session reviewed in October 2026:

| Time, IST | Action |
| --- | --- |
| 09:10 | Prepare Docker/Redis and attempt a bounded history warm-up |
| 09:15 | Start collection, analysis workers, dashboard and archiver |
| 15:40 | Begin shutdown; stop ingestion, then analytics, then drain archives |
| After successful drain | Stop this project's Redis and attempt final upload |

Confirm timings against the [NSE market calendar](https://www.nseindia.com/resources/exchange-communication-holidays?article_id=432919950.0)
for your segment. Drain completion can occur after the close trigger if buffered
data is still being written. A timeout preserves Redis for recovery.

The bundled holiday calendar covers **2026 only**. Add the official calendar for
each new year to `holidays` and `calendar_years`; unconfigured years do not start
collection. `special_sessions` supports explicit dated sessions on weekends or
holidays with `prepare_time`, `open_time`, and `close_time` fields. Reinstall the
task after changing session times or adding special sessions.

Install the task in your signed-in Windows account:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\install_market_schedule.ps1
Get-ScheduledTask -TaskName SmartAPI-MarketHours
# Optional: run the controller now; the calendar prevents out-of-hours collection.
Start-ScheduledTask -TaskName SmartAPI-MarketHours
```

If Windows denies task registration, rerun the installer from an elevated
PowerShell under the intended Windows account. No Windows password is stored.
The controller uses `pythonw.exe` and a console-free PowerShell child to avoid
flashing terminal windows. It checks once a minute during the configured window
through 30 minutes after close, and also at sign-in. It restarts stopped tracked
workers during the session without duplicating workers already running.

Keep the machine powered on, signed in, plugged in, online, and with the lid open
during collection. Locking the screen is fine. Wake timers depend on Windows and
hardware support; the task cannot power on a shut-down laptop.

## 7. Verify real data and real uploads

Check the task and connection state:

```powershell
.\market_automation.ps1 -Status
& .\.venv\Scripts\python.exe market_automation.py verify-drive
Get-ScheduledTaskInfo -TaskName SmartAPI-MarketHours
```

`drive_ready` alone means a remote exists; it is not proof of a completed upload.
Also inspect producer logs and fresh `data_lake/stream=.../dt=YYYY-MM-DD/` files.

For upload evidence:

```powershell
Get-Content .\runtime\upload-progress.json
Get-Content .\runtime\upload-status.json
```

| Evidence | Meaning |
| --- | --- |
| `upload-progress.json` | Most recent checksum-verified batch in this uploader run |
| `uploaded_checked` | Count verified so far in that run, not lifetime totals |
| `last_verified_at` / `last_verified_files` | When verification succeeded and which file paths to inspect |
| `pass_complete: false` | Pending work remains; this is not a complete backup |
| `upload-status.json` | Last complete archive pass; check its account and timestamp |
| `market-state.json` / `archiver-drained.json` | Collection session and successful stream-drain evidence |

Missing receipt files mean their corresponding completion has not been recorded.
An old receipt does not establish that today's files are uploaded.

In the **verified Google account**, browse:

```text
My Drive / SmartAPI / data_lake /
  stream=md_ticks_opt / dt=YYYY-MM-DD / underlying=SYMBOL / part-....parquet
  stream=md_ticks_eq  / dt=YYYY-MM-DD / symbol=SYMBOL     / part-....parquet
```

Other signal streams use the same date layout, sometimes without a symbol folder.
Folder creation alone is not upload completion. Open a date/symbol folder and
compare actual files with `last_verified_files`. Drive may not preview Parquet;
download it and read it with pandas or another Parquet reader.

The uploader prioritizes today's partitions, copies bounded batches, verifies
checksums, then checkpoints file metadata in `runtime/verified-uploads.sqlite3`.
An interrupted pass resumes from those checkpoints. Historical backfill and many
tiny files can take substantial time; this is batch archival, not instant mirroring.

## 8. Manual controls and troubleshooting

```powershell
# Upload manually; --no-cleanup preserves local files during initial validation.
& .\.venv\Scripts\python.exe market_automation.py upload --no-cleanup --max-batches 1
# Normal upload, including verified retention cleanup:
.\market_automation.ps1 -UploadOnly
# Deliberate stop: disable recovery first, then drain workers.
Disable-ScheduledTask -TaskName SmartAPI-MarketHours
.\stop_all.ps1
# Resume automatic operation:
Enable-ScheduledTask -TaskName SmartAPI-MarketHours
Start-ScheduledTask -TaskName SmartAPI-MarketHours
# Remove scheduling:
.\install_market_schedule.ps1 -Remove
```

Manual `stop_all.ps1` stops workers; automatic close also stops the Redis container.
Closing or interrupting an uploader terminal stops that upload, not the background
collector. Do not run legacy per-stream archivers alongside `run_archiver_layers.py`:
they share consumer groups and can divide stream messages between writers.

| Symptom | Check / action |
| --- | --- |
| Google 403, app in Testing | Add the actual storage email under Audience > Test users, then reconnect |
| Shared Drive scope error | Reconnect and answer `n` to Shared Drive; retain `drive.file` for My Drive |
| Wrong Google account | Use the account-switch script; do not remove the pause marker to bypass verification |
| `lookup www.googleapis.com: no such host` | DNS/network failure; check `Resolve-DnsName www.googleapis.com`, restore connectivity, then retry |
| Only folders visible in Drive | Look at today's nested files and batch progress; inspect copy failures |
| No completed upload receipt | Keep local files; incomplete verification is not a confirmed backup |
| Scheduled process exits | Inspect `logs/automation/*-launcher.log` and Task Scheduler history |
| Upload errors | Inspect `logs/automation/*upload.err.log` |
| Worker failures | Inspect `logs/YYYY-MM-DD/NAME.err.log` and preserved `.previous` logs |
| Cannot safely drain archiver | Preserve Redis and local data; inspect logs before retrying shutdown |

Copy/verification failures retain local files for that batch. Successful automatic
uploads may remove local files older than the retention period after verification
and an unchanged-file check. Remote archives are not deleted by local cleanup.
Budget local disk for the backlog; Drive quota does not eliminate local buffering.

## 9. Developer validation and repository hygiene

```powershell
& .\.venv\Scripts\python.exe -m unittest discover -s tests -v
# Optional integration test, with local Docker Redis available:
$env:RUN_REDIS_INTEGRATION = '1'
& .\.venv\Scripts\python.exe -m unittest discover -s tests -p test_archiver_shutdown.py -v
Remove-Item Env:\RUN_REDIS_INTEGRATION
```

The Redis integration test uses a unique temporary key in database 15 and removes
that key afterward. Uploader tests exercise the real portable rclone binary with
temporary local destinations; they do not upload test data to Google Drive.

Keep `.env`, `automation.local.json`, `runtime/`, `tools/`, logs and collected
`data_lake/` files outside Git. Never commit client secrets, tokens, cached data,
or downloaded binaries. Check staged changes before committing.

The archive is at least once: recovery after a crash can repeat rows. Deduplicate
using stream name and `_redis_id` when building downstream merged datasets.
See [MARKET-AUTOMATION.md](MARKET-AUTOMATION.md) for the operational reference and
[RUN.md](RUN.md) for the underlying broker pipeline.

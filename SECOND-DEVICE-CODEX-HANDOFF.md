# Second Windows device: Codex deployment handoff

Give this entire file to the Codex coding agent on the destination laptop.
It is an execution brief, with commands and acceptance criteria. Paths below are
examples; discover the actual checkout before executing commands.

## Objective and fixed decisions

Reproduce this repository's laptop-based Angel One collection, Redis, analysis,
Parquet archival, Google Drive upload, verified local retention, and automatic
market-hours start/stop on a second Windows device.

- Repository: `https://github.com/airaojagruti611/New-smart-api.git`
- Deployment branch: `market-hours-scheduler`.
- Required baseline: commit `14b065d` or a descendant containing its optimized
  archiver and recovery checks. This handoff may arrive separately from Git.
- Destination Google Drive account: **devarshsheladiya8084@gmail.com**.
- Existing OAuth project: `smartapi-drive-backup`; reuse its Desktop OAuth client
  if the owner can supply the current client credentials privately.
- Use personal **My Drive**, rclone remote `gdrive`, scope `drive.file`.
- Default destination for this second device: **`gdrive:SmartAPI/device-02`**.
  This uses the same account and SmartAPI parent while separating device output.
- Keep the existing pipeline behavior: prepare 09:10, collect 09:15 through
  15:40 IST, use the repository holiday/special-session calendar, drain on close.
- Upload every five minutes in bounded batches; keep two days locally and remove
  eligible files only after successful remote verification and unchanged-file checks.
- Use the existing scripts. Do not replace them with a new scheduler or mount
  Google Drive as the live Redis/data working directory.

Read applicable `AGENTS.md` files and inspect the current code first. Preserve
unrelated local edits. Execute reversible setup, diagnostics and validation;
human browser login and private credential entry require the owner's participation.
Never ask the owner to paste secrets into chat, terminal transcripts, or Git.
Report exactly which steps are complete and which await human interaction or a
market session. Do not claim a working live collector based only on process launch.

## 1. Decide replacement versus simultaneous operation

Default to a replacement collector with its own device folder. Prepare the new
machine without starting broker collection. At cutover, the owner should disable
collection recovery on the old machine and drain it before enabling live collection
on the new machine. Keep old local archives and Redis until accounted for.

If both machines are intentionally collecting, retain separate device folders.
Confirm the broker account/API's current concurrent session, WebSocket, subscription
and IP requirements before running two live producers with the same credentials.
Separate Drive sign-ins do not create additional broker session capacity.

Do not point both machines at the same remote data paths. Each has independent
Redis IDs, checkpoints and logs; identical relative filenames can conflict, and
copies of the same market events are not automatically deduplicated. When merging
device archives, retain source-device provenance; `(stream, _redis_id)` identifies
replays within one source Redis history, not duplicate market events across devices.

If the owner explicitly needs the exact existing `SmartAPI/data_lake` location,
use `gdrive:SmartAPI` only after retiring the old uploader and checking overlapping
paths. The default device folder avoids needing that migration. No existing Drive
files need moving for this deployment.

## 2. Inspect and update the checkout

Run from the repository root. If a checkout already exists, use it; do not clone
over it or discard modifications.

```powershell
Get-Location
git status --short
git remote -v
git branch --show-current
git fetch origin
git switch market-hours-scheduler
git pull --ff-only origin market-hours-scheduler
git merge-base --is-ancestor 14b065d HEAD
if ($LASTEXITCODE -ne 0) { throw 'Required optimized archiver baseline missing' }
```

If the branch only exists remotely, use
`git switch --track origin/market-hours-scheduler` instead of the normal switch.
If checkout/fast-forward fails because of local changes or divergence, preserve
those edits and resolve deliberately; do not force-reset or blindly stash them.

For a fresh clone, from the chosen parent directory:

```powershell
git clone --branch market-hours-scheduler https://github.com/airaojagruti611/New-smart-api.git New-smart-api
cd .\New-smart-api
```

Read `DATA-COLLECTION-SETUP.md`, `MARKET-AUTOMATION.md`, and the actual scripts.
Then enter the application directory, for example:

```powershell
cd D:\New-smart-api\angel_md_redis
```

All subsequent commands run there. Do not reuse the original machine's absolute
paths; the installer records this device's actual paths.

## 3. Install machine prerequisites

Required: Git, 64-bit Python (prefer Python 3.11 for parity), Docker Desktop with
Linux containers and its required WSL2/virtualization support, Windows PowerShell
5.1+, an online Windows user session, and sufficient local disk/RAM.

```powershell
git --version
py -0p
docker version
Get-TimeZone
Get-PSDrive -PSProvider FileSystem
```

Set Windows timezone to `India Standard Time` if different (may require elevation):

```powershell
Set-TimeZone -Id 'India Standard Time'
```

Ensure Windows time synchronization is enabled; broker TOTP depends on accurate
time. Launch Docker Desktop and finish its first-run setup. Resolve virtualization,
installation or permissions failures before continuing.

```powershell
py -3.11 -m venv .venv
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed' }
Test-Path .\.venv\Scripts\pythonw.exe
& .\.venv\Scripts\python.exe -c "import redis, pyarrow, requests, pyotp; from zoneinfo import ZoneInfo; print(ZoneInfo('Asia/Kolkata'))"
```

If Python 3.11 is unavailable, install it or validate another supported interpreter
against the pinned requirements before proceeding. Do not copy `.venv` between PCs.

## 4. Configure local broker credentials and instrument parity

Git pull does **not** provide `.env`, `automation.local.json`, `tools/rclone.exe`,
OAuth tokens, Docker volumes, local archives, or scheduled tasks.

Have the owner enter broker credentials privately into
`angel_md_redis/.env`, or transfer that file through a private channel and review
machine-specific settings. Keep these variable names:

```dotenv
ANGEL_API_KEY=OWNER_ENTERS_PRIVATELY
ANGEL_CLIENT_CODE=OWNER_ENTERS_PRIVATELY
ANGEL_PIN=OWNER_ENTERS_PRIVATELY
ANGEL_TOTP_SECRET=OWNER_ENTERS_PRIVATELY
REDIS_URL=redis://localhost:6379/0
ARCHIVE_TZ=Asia/Kolkata
LOG_LEVEL=INFO
ARCHIVE_BATCH_SIZE=32000
ARCHIVE_READ_COUNT=2000
ARCHIVE_FLUSH_SEC=60
ARCHIVE_MAX_BUFFER_MB=16
ARCHIVE_WRITE_CONCURRENCY=2
ARCHIVE_PARTITION_BY_SYMBOL=0
```

Preserve other intentional strategy/subscription settings from the original
device, including strike ranges, subscription mode and stream limits. Review
`app/config.py` for defaults. Check for a repository-root `.env` fallback and
inherited environment variables; they must not silently supply another account.
Inspect presence/validity without printing secret values.

Do not copy old `X_CLIENT_LOCAL_IP`, `X_CLIENT_PUBLIC_IP` or `X_MAC_ADDRESS`
overrides blindly. Leave them unset for supported auto-detection, or set correct
values for the new machine/network as required by the broker. Do not use loopback
`127.0.0.1` as a broker client IP; the Redis URL above is a separate setting.
Confirm any broker-side registered IP requirements for the new network.

The original device had uncommitted changes to `symbols.txt` and
`OpenAPIScripMaster.json` when this handoff was prepared. Pulling the branch alone
does not reproduce those edits. Obtain the intended `symbols.txt` from the owner
and compare locally; refresh the scrip master using the repository's supported
mechanism, or securely transfer the intended file and validate its freshness.
Use the same instrument list/settings if exact collection coverage is required.

Do not transfer `runtime/market-state.json`, PID records, stop/drain markers,
`verified-uploads.sqlite3`, logs, Redis volume or an old OAuth config into a fresh
deployment. They describe another device's runtime. Historical migration is a
separate operation; leave the original machine's unuploaded backlog intact.

## 5. Start and validate local Redis

```powershell
docker compose up -d
if ($LASTEXITCODE -ne 0) { throw 'Redis startup failed' }
docker exec md_redis redis-cli ping
docker inspect md_redis --format '{{json .Mounts}}'
```

Expect `PONG`, a persistent `/data` volume, and the repository Redis config mount.
The checked-in config uses AOF with `appendfsync everysec` plus snapshots; this
reduces crash loss but does not promise zero loss under abrupt power failure.
Container name `md_redis` and host port 6379 must be available on this machine.
If an unrelated Redis already uses them, investigate before changing anything.

Validate the URL actually used by Python:

```powershell
& .\.venv\Scripts\python.exe -c "from app.config import REDIS_URL; import redis; print('Configured Redis responds:', redis.Redis.from_url(REDIS_URL).ping())"
```

Never run `docker compose down -v`, `FLUSHALL`, `FLUSHDB` or manual stream trimming
as a deployment shortcut. Drive cleanup does not delete the Redis Docker volume;
monitor Redis memory and volume disk usage separately from Parquet disk usage.

## 6. Install rclone on this device

Download the Windows archive matching the new device's architecture from
https://rclone.org/downloads/ and verify its ZIP SHA-256 against the official
`SHA256SUMS` for that exact release. The original deployment used rclone 1.75.2;
use that verified release for parity, or validate a newer release deliberately.

```powershell
Get-FileHash C:\path\to\rclone-release.zip -Algorithm SHA256
New-Item -ItemType Directory -Force .\tools | Out-Null
# Extract the verified download and place rclone.exe in .\tools\.
& .\tools\rclone.exe version
```

Do not run the placeholder download path literally. The agent should select and
download the correct official release, verify it, and install the binary.

## 7. Write the per-device upload configuration BEFORE authorizing

On a fresh deployment create this ignored file. If it exists, merge the intended
properties into it instead of overwriting unrelated settings.

```powershell
@'
{
  "expected_drive_email": "devarshsheladiya8084@gmail.com",
  "drive_remote": "gdrive:SmartAPI/device-02",
  "upload_interval_minutes": 5,
  "upload_batch_files": 40,
  "upload_max_batches": 2,
  "local_retention_days": 2
}
'@ | Set-Content -LiteralPath .\automation.local.json -Encoding UTF8
```

Use a stable unique device folder if `device-02` is already occupied by another
collector. A rename of the PC does not automatically rename this folder.
Keep default market times/calendar from `automation.json` unless instructed
otherwise. Do not copy the old machine's whole local override without reviewing it.

Check the effective non-secret configuration:

```powershell
& .\.venv\Scripts\python.exe -c "from market_automation import load_config; c=load_config(); print({k:c[k] for k in ('expected_drive_email','drive_remote','upload_interval_minutes','local_retention_days')})"
```

## 8. Authorize the SAME Drive account on the NEW device

Reuse the existing Desktop OAuth client if available. The owner retrieves its
current ID/secret privately from project `smartapi-drive-backup`; do not retrieve
them from old screenshots. If unavailable, follow the official setup guide:
https://rclone.org/drive/#making-your-own-client-id . Enable Drive API, configure
Google Auth Platform branding/audience and create a Desktop app OAuth client.

If the app remains External/Testing, add **devarshsheladiya8084@gmail.com** under
Google Auth Platform > Audience > Test users before login. Drive refresh tokens
for an External/Testing app generally expire after seven days; review publishing
requirements and use Production for long-term unattended operation. Reusing the
same OAuth client avoids introducing another app's file-access boundary. With a
different client and `drive.file`, do not assume existing app-created folders are
visible; verify the actual destination and watch for duplicate named folders.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\setup_drive.ps1 -Email devarshsheladiya8084@gmail.com
```

Human interaction is required for credentials/browser consent. Prompt answers:

| Prompt | Answer |
| --- | --- |
| New remote | `n` |
| Name | `gdrive` |
| Storage | `drive`, or the number explicitly labeled Google Drive |
| client_id | Privately enter the Desktop client's current ID |
| client_secret | Privately enter its current secret |
| Scope | `3` if labeled `drive.file`; select by label if numbering differs |
| service_account_file | Empty, press Enter |
| Advanced configuration | `n` |
| Replace token, if asked | `y` |
| Use browser automatically | `y` |
| Browser account | **devarshsheladiya8084@gmail.com**, use another account if needed |
| Shared Drive / Team Drive | **`n`** |
| Keep remote | `y` |
| Main config menu after saving | `q` |

Browser "Success" only completes the authorization callback. Return to PowerShell
and wait for account verification and the initial no-cleanup upload to finish.
An empty new lake may produce a successful zero-file pass; that does not prove
live data upload. Credentials stay in ignored `runtime/rclone.conf`.

The setup script's final printed message currently mentions `gdrive:SmartAPI`
literally; the uploader uses `drive_remote` from the configuration. Confirm the
effective `gdrive:SmartAPI/device-02` value and receipt rather than that message.

```powershell
& .\.venv\Scripts\python.exe market_automation.py verify-drive
& .\tools\rclone.exe about gdrive: --config .\runtime\rclone.conf
Test-Path .\runtime\uploads.paused
```

Require verified email matching the requested account; `about` shows quota, not
identity. The pause marker should be absent after successful setup. If verification
fails, keep the marker and fix login; do not manually remove it to bypass the check.
For a configured remote use:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\switch_drive_account.ps1 -Email devarshsheladiya8084@gmail.com
```

## 9. Check calendar and tests before enabling live collection

```powershell
& .\.venv\Scripts\python.exe market_automation.py session
& .\.venv\Scripts\python.exe -m unittest discover -s tests -v
if ($LASTEXITCODE -ne 0) { throw 'Resolve validation failures before scheduling' }
```

Check `automation.json` calendar years, holidays and special sessions. The bundled
calendar covers 2026 only and fails closed for unknown years. Update future years
from official exchange publications before relying on automation. Reinstall the
task after schedule or special-session changes. Do not modify holidays or clock
times to make a weekend appear open for testing.

The 15:40 close is this repository's configured collection cutoff. Verify the
exchange segment's current published hours if deploying on a later date. This
guide was prepared on 30 October 2026, IST; it is not an evergreen exchange calendar.

Optional Redis integration validation, with local Redis available:

```powershell
$env:RUN_REDIS_INTEGRATION = '1'
try {
    & .\.venv\Scripts\python.exe -m unittest discover -s tests -p test_archiver_shutdown.py -v
    if ($LASTEXITCODE -ne 0) { throw 'Redis integration validation failed' }
} finally {
    Remove-Item Env:\RUN_REDIS_INTEGRATION -ErrorAction SilentlyContinue
}
```

Inspect current test isolation first: these tests use unique temporary keys in
local Redis database 15. Do not repoint them to a production/shared Redis server.

## 10. Cut over and install the silent scheduler

For replacement deployment, the owner runs the following on the OLD device
before live start on the new one. The new agent cannot infer it happened:

```powershell
# OLD DEVICE, from its angel_md_redis directory:
Disable-ScheduledTask -TaskName SmartAPI-MarketHours
powershell -NoProfile -ExecutionPolicy Bypass -File .\stop_all.ps1
```

Verify old ingestion is stopped and the archiver drained; a timeout means keep
Redis alive for recovery and investigate. Disabling the task does not stop an
already-running uploader. For exclusive reuse of the old remote root, let that
uploader finish or stop only the positively identified uploader process before
enabling the new uploader. With separate device folders, old backlog uploads may
continue without writing into the new device's paths.

On the NEW device, while signed in as the intended daily Windows user:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\install_market_schedule.ps1
Get-ScheduledTask -TaskName SmartAPI-MarketHours | Select-Object TaskName,State,Actions
Get-ScheduledTaskInfo -TaskName SmartAPI-MarketHours
Start-ScheduledTask -TaskName SmartAPI-MarketHours
```

If registration is denied, elevate under the same intended user and retry. Do not
install under an unrelated administrator account. The installer uses the actual
`.venv/Scripts/pythonw.exe` and `run_market_hidden.py`; it stores no Windows password.
Check that an older deployment task or legacy scheduler on THIS device is not
starting another collector. Do not run `install_scheduler.ps1` as well.

Starting this task during market hours starts collection. Outside hours it obeys
the calendar and can service uploads without starting live ingestion. Do not use
`run_all.ps1` to bypass the calendar during deployment verification.

The normal task checks every minute from 09:10 until 30 minutes after the cutoff
(16:10 with present settings), plus at sign-in. The machine must remain powered
on, signed in, online, with Docker available. Locking the screen is fine. Configure
plugged-in sleep/lid behavior so the PC stays awake; wake timers cannot power on
a shut-down laptop. Do not assume collection starts if the user has signed out.

## 11. Prove live collection and archive accuracy

During an actual open market session, inspect:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\market_automation.ps1 -Status
. .\automation_common.ps1
Get-PipelineWorkers | Select-Object Name,Record
Get-Content .\runtime\market-state.json
$marketDate = Get-Date -Format 'yyyy-MM-dd'
Get-Content (Join-Path "logs\$marketDate" 'producer.log') -Tail 30
Get-Content (Join-Path "logs\$marketDate" 'producer.err.log') -Tail 30
Get-Content .\runtime\archive_metrics\md_ticks_opt.json
```

Require broker login/subscriptions with no repeated authentication errors,
fresh ticks arriving, fresh completed Parquet parts, and progressing archiver
metrics. Compare two observations rather than a single old timestamp. Low-volume
signals need not all produce files at once.

Read configured Redis without exposing secrets:

```powershell
@'
import json, redis
from datetime import datetime
from zoneinfo import ZoneInfo
from app.config import REDIS_URL
r = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=8)
for stream in ('md:ticks:eq', 'md:ticks:opt'):
    latest = r.xrevrange(stream, count=1)
    stamp = (datetime.fromtimestamp(int(latest[0][0].split('-')[0])/1000,
             ZoneInfo('Asia/Kolkata')).isoformat() if latest else None)
    print(json.dumps({'stream':stream, 'length':r.xlen(stream), 'last_entry_ist':stamp}))
'@ | & .\.venv\Scripts\python.exe -
```

Redis entry time indicates ingestion time; compare the tick payload's exchange
timestamp as well when diagnosing delayed broker data.

Archive path on disk:
`data_lake/stream=md_ticks_opt/dt=YYYY-MM-DD/part-....parquet`.
The optimized default combines symbols in parts, retains symbol columns, flushes
at a row/memory/time threshold, and ACKs only after publishing valid files.
Do not run legacy per-stream archivers alongside `run_archiver_layers.py`.
If `runtime/archiver-errors.json` exists, investigate its recorded gaps and keep
source evidence. Deleting this marker does not recover lost data.

## 12. Prove the NEW device's files are in the correct Drive

Allow completed files to age at least two minutes, plus a scheduled upload interval.
For a bounded manual verification pass that preserves local files:

```powershell
& .\.venv\Scripts\python.exe market_automation.py upload --no-cleanup --max-batches 1
Get-Content .\runtime\upload-progress.json
if (Test-Path .\runtime\upload-status.json) { Get-Content .\runtime\upload-status.json }
& .\tools\rclone.exe lsf 'gdrive:SmartAPI/device-02' --config .\runtime\rclone.conf
```

Do not start concurrent manual uploaders; a "upload already running" result means
the other uploader holds the lock, not that this invocation verified files.
Use the actual chosen device folder if different from `device-02`.

Require fresh progress containing the correct `account`, `remote`, nonzero
`uploaded_checked` for actual collected files, and `last_verified_files`.
`pass_complete:false` is a partial verified batch. `upload-status.json` is the
last complete pass and can be absent or stale. A zero-file pass is not proof
that today's lake reached Drive. Current day's active worker logs are generally
deferred until final upload, so archive receipts are the better daytime evidence.

In a browser signed into **devarshsheladiya8084@gmail.com**, open:

```text
My Drive / SmartAPI / device-02 / data_lake /
  stream=md_ticks_opt / dt=YYYY-MM-DD / part-....parquet
```

Compare an exact receipt path with an actual remote file; folder names and folder
modified times alone prove nothing about contents. Optionally download one
verified part to a temporary location, compare its checksum with the local file,
and read it with PyArrow. Drive need not preview Parquet in the browser.
If using rclone to list more deeply, limit the query to today's stream/date folder
instead of recursively traversing the entire shared archive.

## 13. Verify local cleanup after retention, and shutdown after close

Normal upload with retention cleanup uses:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\market_automation.ps1 -UploadOnly
```

This is authorized to remove eligible local archive files after verification.
With retention two days, today's files remain local; no removals on a new device
is expected. Do not lower retention, edit mtimes, or delete real data merely to
demonstrate cleanup. After files age beyond 48 hours, compare `removed_local`,
the exact local path's absence, and the remote file's continued presence.
Copy/check failures retain the affected batch locally; already verified earlier
batches may have been cleaned. This is not transactional across the whole lake.

At close, verify separately: ingestion stopped, analysis stopped, archiver drained,
and uploads verified. Inspect worker records, `runtime/archiver-drained.json`,
`runtime/market-state.json`, archiver errors and upload receipts.
For Redis group lag/pending checks, start from the configured `REDIS_URL` and
`STREAMS` in `run_archiver_layers.py`; do not assume an unrelated Redis DB.

A drain timeout preserves the archiver and Redis. The current task window ends
30 minutes after close; a drain extending beyond it can finish without same-evening
controller finalization or final upload. Report this condition honestly and handle
the remaining controller/upload pass after confirming all workers drained.
Do not mark the whole flow complete because live ingestion is off.

## 14. Recovery and operational commands

```powershell
# Status and account check:
powershell -NoProfile -ExecutionPolicy Bypass -File .\market_automation.ps1 -Status
& .\.venv\Scripts\python.exe market_automation.py verify-drive

# Deliberate pause: stop automatic recovery before stopping workers.
Disable-ScheduledTask -TaskName SmartAPI-MarketHours
powershell -NoProfile -ExecutionPolicy Bypass -File .\stop_all.ps1

# Resume calendar-controlled operation:
Enable-ScheduledTask -TaskName SmartAPI-MarketHours
Start-ScheduledTask -TaskName SmartAPI-MarketHours
```

| Problem | Action |
| --- | --- |
| Google 403 / testing app | Add the destination email as a test user; reconnect |
| Team Drive scope failure | Reconnect; answer `n` to Shared Drive |
| Wrong account | Run account-switch script with exact email; preserve upload pause until verified |
| DNS / `no such host` | Check `Resolve-DnsName www.googleapis.com`, network and DNS; retry after repair |
| Browser success but script waiting | Return to rclone, answer remaining prompts and exit its config menu |
| Flashing console windows | Check task points to `pythonw.exe` and hidden launcher, not a legacy task |
| Task Ready but no ticks | Check actual calendar phase, broker logs, worker records and Redis freshness |
| Only folders / stale receipts | Inspect upload error logs and actual nested Parquet files; keep local backlog |
| Duplicate SmartAPI folders | Confirm OAuth app visibility and actual folder IDs; do not delete either blindly |
| Drain gap / timeout | Preserve Redis and archives; inspect archiver logs/errors before restarting |
| Drive full or slow uploads | Preserve local backlog, check quota/network, monitor disk; do not assume cleanup frees everything |

Logs: `logs/automation/*-launcher.log`, date controller logs,
`*upload.log`, `*upload.err.log`, and `logs/YYYY-MM-DD/*.log` / `*.err.log`.
Uploads are batched and today-first, not instantaneous streaming to Drive.
Two devices share account storage and API/network capacity.

## 15. Final acceptance report the agent must give the owner

Provide concrete evidence with timestamps in IST:

1. Checkout path, branch and full commit; any retained local modifications.
2. Python version, dependency validation and test results/skips.
3. Intended symbol coverage and relevant settings, without secret values.
4. Local Redis health/persistence; no destructive Redis operations performed.
5. Verified Drive email and exact device destination.
6. Scheduled task principal, enabled state, correct executable/path and next run.
7. Calendar coverage, configured session times and the machine's sleep/sign-in requirements.
8. Live collection evidence and actual readable Parquet parts, or explicitly
   "awaiting next market session" if deployment happened outside hours.
9. Exact fresh checksum-verified remote file, receipt timestamp and account/path;
   separate partial batches from a complete upload pass.
10. Retention configured; whether removal was actually observed or awaits file age.
11. Close/drain observed or awaiting first market close; any backlog or unresolved errors.
12. Whether the old collector was retired or concurrent collection was explicitly arranged.

Keep secrets, `.venv`, runtime, tools and collected data out of Git. Check
`git status --short` and ignored-file coverage before committing any setup fixes.
Do not automatically push local configuration or credentials. This handoff
authorizes deployment and verification, not unrelated repository changes.

Local references: [general setup](DATA-COLLECTION-SETUP.md),
[operations and archiver guarantees](MARKET-AUTOMATION.md), [pipeline basics](RUN.md).

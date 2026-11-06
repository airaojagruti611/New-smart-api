# Local collection operations dashboard

Open **http://127.0.0.1:8502** on this laptop. Bookmark the page for daily checks.
The dashboard is read-only, refreshes every 15 seconds while visible, and keeps
running after the market collector stops. Docker/Redis do not need to be running
to inspect saved sessions or Drive verification progress.

## What it shows

- Session date picker, actual collection state, and market calendar timing in IST.
- Archived equity/options tick counts, archive file totals and size.
- Drive checksum-verification coverage for the selected date, pending files,
  configured account, and latest verified batch activity.
- Start/stop/drain timeline and next scheduled market session.
- Per-stream records, parts, size and verification, with a stream search filter.
- Current tracked worker count, schedule state and available local disk space.
- During an open session with active workers, a lightweight Redis freshness check
  warns if no recent tick can be confirmed. It never starts Redis itself.

Collection stopped, archives drained, and uploads verified are different states.
A live upload task does not mean all files are backed up. The current batch is
attributed only to dates listed in its manifest; a historical session is not shown
as uploading just because another date's upload worker is active.

Verification uses the existing checksum checkpoints and checks that local files
are unchanged. It is not a new remote audit on every refresh: later manual deletion
or editing of Drive files is not detected here. Verified checkpoint records for
files removed by local retention still count as previously verified archives.
Empty days never show 100%; missing row metrics are shown as unavailable.

## Startup and removal

From `angel_md_redis`, after creating the Python environment:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\install_operations_dashboard.ps1
```

This installs and starts **SmartAPI-OperationsDashboard** under the signed-in
Windows user. It runs at future sign-ins without a terminal window. It is separate
from `SmartAPI-MarketHours` and `SmartAPI-DriveUploads`, so collection shutdown
does not stop the UI. No Python data-science framework or new dependency is needed.

To stop/remove only this dashboard:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\install_operations_dashboard.ps1 -Remove
```

The server stores its process ID and exact start ticks in ignored
`runtime/operations.process.json`, so removal checks its identity before stopping
it. Exclusive port binding prevents duplicate servers. If port 8502 belongs to
another application, investigate rather than terminating that application.

To run manually for development instead of the installed task:

```powershell
& .\.venv\Scripts\python.exe operations_server.py --port 8502
```

Logs are in `logs/automation/YYYY-MM-DD-operations.log`.

## History and access

Daily counters are reconstructed from archive progress logs/metrics and preserved
in ignored `runtime/operations-history.sqlite3`. The background monitor saves a
snapshot each minute, even with the browser closed. Older sessions are best effort
where logs were already removed before installation; missing measurements are not
invented. Process status, schedule state and latest system Drive activity are
current observations; the timeline/archive table refer to the chosen session.

The service binds only to `127.0.0.1` and checks the HTTP host. It serves a fixed
allowlist of assets and read-only JSON, not arbitrary files, raw logs, `.env`,
broker credentials or OAuth configuration. There are no start/stop/delete web
actions and no remote font/script dependencies. It is intended for this laptop,
not public hosting or remote access from another device.

The layout supports desktop and narrow screens; the archive table scrolls
horizontally when needed and is keyboard-focusable. If refresh fails, current
activity badges become stale/unavailable and the last snapshot is identified.
Switching dates clears the previous session before the new request is shown.

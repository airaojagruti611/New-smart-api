# PowerShell Helper to Stop All Pipeline Workers (Windows)
#
# run_all.ps1 writes one pid file per worker to logs\<yyyy-MM-dd>\pids\<name>.pid.
# This script searches EVERY date folder (so it still works after midnight),
# stops each live worker, waits for it to exit, and deletes a pid file only once
# its process is gone. A pid whose process started after the pid file was written
# is treated as a recycled pid and is NOT killed.
#
# Usage:  .\stop_all.ps1 [-TimeoutSec 20] [-Check]
#   -Check   only report running workers; exit 1 if any (nothing is stopped)
param (
    [int]$TimeoutSec = 20,
    [switch]$Check
)

Set-Location $PSScriptRoot

$logRoot = if ($env:LOG_DIR) { $env:LOG_DIR } else { Join-Path $PSScriptRoot "logs" }

function Get-WorkerProcess {
    param ([System.IO.FileInfo]$File)
    $raw = (Get-Content -LiteralPath $File.FullName -ErrorAction SilentlyContinue | Select-Object -First 1)
    if (-not $raw) { return $null }
    $pidText = ($raw -replace '[^0-9]', '')
    if (-not $pidText) { return $null }
    $proc = Get-Process -Id ([int]$pidText) -ErrorAction SilentlyContinue
    if (-not $proc) { return $null }
    # Guard against pid reuse (e.g. after a reboot): our worker was started
    # right before run_all.ps1 wrote its pid file.
    try {
        if ($proc.StartTime -gt $File.LastWriteTime.AddSeconds(10)) { return $null }
    } catch {
        # StartTime not readable (access denied) -> not one of our workers
        return $null
    }
    return $proc
}

$pidFiles = @()
if (Test-Path -LiteralPath $logRoot) {
    $pidFiles = @(Get-ChildItem -Path $logRoot -Recurse -File -Filter "*.pid" -ErrorAction SilentlyContinue |
        Where-Object { $_.Directory.Name -eq "pids" })
}

if ($pidFiles.Count -eq 0) {
    Write-Host "No PID files found under $logRoot\*\pids." -ForegroundColor Yellow
    exit 0
}

$targets = @()
$stale = 0
foreach ($file in $pidFiles) {
    $proc = Get-WorkerProcess -File $file
    if ($proc) {
        $targets += [pscustomobject]@{ Name = $file.BaseName; Id = $proc.Id; File = $file.FullName }
    } else {
        $stale++
        if (-not $Check) { Remove-Item -LiteralPath $file.FullName -Force -ErrorAction SilentlyContinue }
    }
}

if ($Check) {
    if ($targets.Count -gt 0) {
        Write-Host "Pipeline workers still running ($($targets.Count)):" -ForegroundColor Yellow
        foreach ($t in $targets) { Write-Host "  $($t.Name) pid=$($t.Id) ($($t.File))" }
        exit 1
    }
    exit 0
}

if ($targets.Count -eq 0) {
    Write-Host "No running pipeline workers found (removed $stale stale pid file(s))." -ForegroundColor Yellow
    exit 0
}

Write-Host "Stopping $($targets.Count) background pipeline workers..." -ForegroundColor Yellow
foreach ($t in $targets) {
    # venv\Scripts\python.exe is a launcher that spawns the real interpreter as a
    # child, so kill the whole tree (/T); fall back to Stop-Process.
    & taskkill.exe /PID $t.Id /T /F 2>&1 | Out-Null
    if (Get-Process -Id $t.Id -ErrorAction SilentlyContinue) {
        try {
            Stop-Process -Id ([int]$t.Id) -Force -ErrorAction Stop
        } catch {
            Write-Host "  -> Stop-Process failed for $($t.Name) (PID $($t.Id)): $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    Write-Host "  -> Stop requested: $($t.Name) (PID $($t.Id))" -ForegroundColor Gray
}

# Wait for the processes to actually exit
$deadline = (Get-Date).AddSeconds($TimeoutSec)
do {
    $alive = @($targets | Where-Object { Get-Process -Id $_.Id -ErrorAction SilentlyContinue })
    if ($alive.Count -eq 0) { break }
    Start-Sleep -Milliseconds 500
} while ((Get-Date) -lt $deadline)

$stopped = 0
$failed = 0
foreach ($t in $targets) {
    if (Get-Process -Id $t.Id -ErrorAction SilentlyContinue) {
        Write-Host "  FAILED to stop $($t.Name) (PID $($t.Id)); pid file kept: $($t.File)" -ForegroundColor Red
        $failed++
    } else {
        Remove-Item -LiteralPath $t.File -Force -ErrorAction SilentlyContinue
        Write-Host "  Stopped $($t.Name) (PID $($t.Id))" -ForegroundColor Gray
        $stopped++
    }
}

$color = if ($failed -eq 0) { "Green" } else { "Red" }
Write-Host "Stopped $stopped worker(s); $failed failed; $stale stale pid file(s) removed." -ForegroundColor $color
if ($failed -gt 0) { exit 1 }
exit 0

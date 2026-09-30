# Market-hours supervisor for the Option Rider pipeline (Windows).
#
# Keeps the pipeline running Mon-Fri between StartTime and StopTime (IST) and
# stops it outside that window. Checks every CheckSec seconds, so it also
# starts the pipeline if the PC is switched on in the middle of the session.
#
# Normally launched hidden by the "OptionRider Market Scheduler" task that
# install_scheduler.ps1 registers. Can also be run by hand in a terminal.
#
# Optional: list NSE holidays (one yyyy-MM-dd per line, # for comments) in
# market_holidays.txt next to this script; those days are skipped.

param(
    [string]$StartTime = "09:15",
    [string]$StopTime = "15:30",
    [int]$CheckSec = 30,
    [int]$MaxStartsPerDay = 3
)

Set-Location $PSScriptRoot

$LogFile = Join-Path $PSScriptRoot "logs\scheduler.log"
New-Item -ItemType Directory -Force -Path (Split-Path $LogFile) | Out-Null

function Write-Log([string]$msg) {
    $line = "{0}  {1}" -f (Get-IstNow).ToString("yyyy-MM-dd HH:mm:ss"), $msg
    Add-Content -Path $LogFile -Value $line -Encoding utf8
    Write-Host $line
}

function Get-IstNow {
    # Independent of the PC's own timezone setting.
    [TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]::UtcNow, "India Standard Time")
}

function Get-Holidays {
    $f = Join-Path $PSScriptRoot "market_holidays.txt"
    if (-not (Test-Path $f)) { return @() }
    Get-Content $f | ForEach-Object { ($_ -split '#')[0].Trim() } | Where-Object { $_ }
}

function Test-MarketWindow([DateTime]$now) {
    if ($now.DayOfWeek -in @([DayOfWeek]::Saturday, [DayOfWeek]::Sunday)) { return $false }
    if ((Get-Holidays) -contains $now.ToString("yyyy-MM-dd")) { return $false }
    $t = $now.ToString("HH:mm")
    return ($t -ge $StartTime -and $t -lt $StopTime)
}

function Get-LiveWorkerCount {
    $n = 0
    Get-ChildItem -Path (Join-Path $PSScriptRoot "logs") -Filter "*.pid" -Recurse -ErrorAction SilentlyContinue |
        Where-Object { $_.Directory.Name -eq "pids" } |
        ForEach-Object {
            $pidVal = (Get-Content $_.FullName -ErrorAction SilentlyContinue | Select-Object -First 1)
            if ($pidVal -and ($pidVal.Trim() -match '^\d+$') -and (Get-Process -Id ([int]$pidVal.Trim()) -ErrorAction SilentlyContinue)) {
                $n++
            }
        }
    return $n
}

function Wait-Docker([int]$timeoutSec = 300) {
    # At logon Docker Desktop may not be up yet; start it and wait for the engine.
    & docker info *> $null
    if ($LASTEXITCODE -eq 0) { return $true }

    $dd = Join-Path $env:ProgramFiles "Docker\Docker\Docker Desktop.exe"
    if ((Test-Path $dd) -and -not (Get-Process -Name "Docker Desktop" -ErrorAction SilentlyContinue)) {
        Write-Log "Docker not running - starting Docker Desktop"
        Start-Process -FilePath $dd | Out-Null
    }
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 5
        & docker info *> $null
        if ($LASTEXITCODE -eq 0) { return $true }
    }
    return $false
}

# Only one supervisor at a time (logon trigger + daily trigger can overlap).
$mutex = New-Object System.Threading.Mutex($false, "Global\OptionRiderMarketScheduler")
if (-not $mutex.WaitOne(0)) {
    Write-Host "Another market_scheduler instance is already running. Exiting."
    exit 0
}

Write-Log "Scheduler started (window $StartTime-$StopTime IST, Mon-Fri, check every ${CheckSec}s)"

$startsToday = 0
$startsDay = ""

try {
    while ($true) {
        $now = Get-IstNow
        $today = $now.ToString("yyyy-MM-dd")
        if ($today -ne $startsDay) { $startsDay = $today; $startsToday = 0 }

        $inWindow = Test-MarketWindow $now
        $live = Get-LiveWorkerCount

        if ($inWindow -and $live -eq 0) {
            if ($startsToday -ge $MaxStartsPerDay) {
                # Avoid a crash loop hammering Angel login; wait for tomorrow.
            } elseif (-not (Wait-Docker)) {
                Write-Log "Docker engine not reachable after 5 min - will retry"
            } else {
                $startsToday++
                Write-Log "Market open and pipeline not running - starting (attempt $startsToday/$MaxStartsPerDay)"
                & (Join-Path $PSScriptRoot "run_all.ps1") *>&1 | Out-File -FilePath $LogFile -Append -Encoding utf8
                Set-Location $PSScriptRoot
                Write-Log "run_all.ps1 finished; live workers: $(Get-LiveWorkerCount)"
            }
        } elseif (-not $inWindow -and $live -gt 0) {
            Write-Log "Outside market window - stopping $live live workers"
            & (Join-Path $PSScriptRoot "stop_all.ps1") *>&1 | Out-File -FilePath $LogFile -Append -Encoding utf8
            Set-Location $PSScriptRoot
        }

        Start-Sleep -Seconds $CheckSec
    }
} finally {
    $mutex.ReleaseMutex()
}

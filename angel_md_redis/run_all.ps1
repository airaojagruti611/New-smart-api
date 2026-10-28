# PowerShell Runner for Angel One Market Data Pipeline (Windows)
param([switch]$Resume)
Set-Location $PSScriptRoot
. (Join-Path $PSScriptRoot 'automation_common.ps1')
if (-not $Resume -and @(Get-PipelineWorkers).Count -gt 0) {
    throw 'Pipeline workers are already running. Stop them before starting another session.'
}
if (@(Get-PipelineWorkers | Where-Object Name -eq 'arch_layers').Count -eq 0) {
    Remove-Item -LiteralPath (Join-Path $RuntimeDir 'archiver.stop') -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath (Join-Path $RuntimeDir 'archiver-drained.json') -Force -ErrorAction SilentlyContinue
}

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " Starting Angel One Market Data Pipeline " -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# 1. Determine Python executable (active venv, .venv, venv, or system)
$VenvPython = ""

if ($env:VIRTUAL_ENV -and (Test-Path "$env:VIRTUAL_ENV\Scripts\python.exe")) {
    $VenvPython = "$env:VIRTUAL_ENV\Scripts\python.exe"
    Write-Host "Using active virtualenv Python: $VenvPython" -ForegroundColor Gray
} elseif (Test-Path "$PSScriptRoot\.venv\Scripts\python.exe") {
    $VenvPython = "$PSScriptRoot\.venv\Scripts\python.exe"
    Write-Host "Using local .venv Python: $VenvPython" -ForegroundColor Gray
} elseif (Test-Path "$PSScriptRoot\venv\Scripts\python.exe") {
    $VenvPython = "$PSScriptRoot\venv\Scripts\python.exe"
    Write-Host "Using local venv Python: $VenvPython" -ForegroundColor Gray
} elseif (Test-Path "$PSScriptRoot\..\.venv\Scripts\python.exe") {
    $VenvPython = "$PSScriptRoot\..\.venv\Scripts\python.exe"
    Write-Host "Using workspace .venv Python: $VenvPython" -ForegroundColor Gray
} else {
    $VenvPython = "python"
    Write-Host "WARNING: No virtualenv python executable found! Falling back to system 'python'." -ForegroundColor Yellow
}

# 2. Environment Variables
$env:PYTHONUNBUFFERED = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUTF8 = "1"
$env:LOG_LEVEL = "INFO"
$env:ARCHIVE_TZ = "Asia/Kolkata"
# Match run_all.sh: Start-Worker already redirects stdout into logs\<date>\<name>.log
$env:LOG_TO_FILE = "0"

# 3. Start Redis Container
Write-Host "`n[1/4] Ensuring Redis Docker Container is running..." -ForegroundColor Yellow
if (-not $Resume -or @(Get-PipelineWorkers).Count -eq 0) {
    docker compose up -d
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Failed to start Redis Docker container. Make sure Docker Desktop is running." -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host 'Reusing running Redis and collection workers.'
}

# 4. Setup Logging and PID directories
$date = Get-Date -Format "yyyy-MM-dd"
$logDir = Join-Path $PSScriptRoot "logs\$date"
$pidDir = Join-Path $logDir "pids"
New-Item -ItemType Directory -Force -Path $pidDir | Out-Null

Write-Host "`n[2/4] Seeding candle history for Indicator Signals..." -ForegroundColor Yellow
$histLog = Join-Path $logDir "history_bootstrap.log"
$histErr = Join-Path $logDir "history_bootstrap.err.log"
if ($env:SKIP_HISTORY_BOOTSTRAP -ne '1') {
& $VenvPython "run_history_bootstrap.py" 1> $histLog 2> $histErr
if ($LASTEXITCODE -ne 0) {
    Write-Host "WARNING: history bootstrap failed (see $histErr). Supertrend/EMA/HTF/pivots may stay NEUTRAL until live bars accumulate." -ForegroundColor Yellow
} else {
    Write-Host "  History seed complete." -ForegroundColor Green
}
Write-Host "Waiting 8s so the next Angel login uses a fresh TOTP..." -ForegroundColor Gray
Start-Sleep -Seconds 8
}

Write-Host "`n[3/4] Spawning Background Workers..." -ForegroundColor Yellow

function Start-Worker {
    param (
        [string]$Name,
        [string]$Script,
        [string]$WorkerArgs = ""   # not $Args: that is a PowerShell automatic variable and arrives empty
    )
    if ($Resume -and @(Get-PipelineWorkers | Where-Object Name -eq $Name).Count -gt 0) {
        Write-Host "  -> Already running: $Name"
        return
    }
    Write-Host "  -> Launching worker: $Name" -ForegroundColor Green
    $stdoutFile = Join-Path $logDir "$Name.log"
    $stderrFile = Join-Path $logDir "$Name.err.log"
    $pidFile = Join-Path $pidDir "$Name.pid"
    # Preserve the previous failure log when restarting a stopped worker.
    if ($Resume) {
        foreach ($oldLog in @($stdoutFile, $stderrFile)) {
            if (Test-Path -LiteralPath $oldLog) {
                Move-Item -LiteralPath $oldLog -Destination ($oldLog + '.' + (Get-Date -Format 'HHmmssfff') + '.previous')
            }
        }
    }
    
    if ($WorkerArgs) {
        $proc = Start-Process -FilePath $VenvPython -ArgumentList "$Script $WorkerArgs" -WorkingDirectory $PSScriptRoot -RedirectStandardOutput $stdoutFile -RedirectStandardError $stderrFile -PassThru -WindowStyle Hidden
    } else {
        $proc = Start-Process -FilePath $VenvPython -ArgumentList "$Script" -WorkingDirectory $PSScriptRoot -RedirectStandardOutput $stdoutFile -RedirectStandardError $stderrFile -PassThru -WindowStyle Hidden
    }
    
    $proc.Id | Out-File -FilePath $pidFile -Encoding ascii
    @{ Id = $proc.Id; Name = $Name; Script = $Script; StartTicks = $proc.StartTime.ToUniversalTime().Ticks.ToString() } |
        ConvertTo-Json | Set-Content -LiteralPath (Join-Path $pidDir "$Name.process.json") -Encoding UTF8
}

# --- Core Pipeline Workers ---
Start-Worker "arch_layers" "run_archiver_layers.py" "all"
Start-Worker "producer" "run_producer.py"
Start-Sleep -Seconds 8
Start-Worker "greeks" "run_greeks_only.py"
Start-Worker "joiner" "run_joiner.py"
Start-Worker "greeks_phase" "run_greeks_analyzer.py"

# --- Candles & Pivots ---
Start-Worker "candles_pub" "run_candles_publisher.py"
Start-Worker "candles_rs" "run_candles_resampler.py"
Start-Worker "pivots" "run_daily_pivots.py"

# --- Signal & Analytics ---
Start-Worker "htf_trend" "run_htf_trend_filter.py"
Start-Worker "level_entry" "run_level_entry.py"
Start-Worker "st_bias" "run_supertrend_mtf_bias.py"
Start-Worker "ema_cross" "run_ema_cross.py"
Start-Worker "momentum" "run_momentum_confirm.py"
Start-Worker "volume" "run_volume_analyzer.py"
Start-Worker "regime" "run_market_regime.py"
Start-Worker "bidask" "run_bidask_analyzer.py"
Start-Worker "smartmoney" "run_smart_money.py"
Start-Worker "orderflow" "run_order_flow.py"
Start-Worker "strikeflow" "run_strike_flow.py"
Start-Worker "stockflow" "run_stock_entry_exit.py"
Start-Worker "optexit" "run_option_liquidity_exit.py"
Start-Worker "imbalance" "run_bidask_imbalance.py"
Start-Worker "composite" "run_composite.py"
Start-Worker "oi_analysis" "run_oi_analysis.py"
Start-Worker "liquidity_score" "run_liquidity_score.py"

# --- Strategy Decision & Strike Selection ---
Start-Worker "entry_trigger" "run_entry_trigger.py"
Start-Worker "strike_select" "run_strike_select.py"
Start-Worker "capital_alloc" "run_capital_alloc.py"

# --- Volatility (Modules 8-9) ---
Start-Worker "expected_move" "run_expected_move.py"
Start-Worker "greeks_change" "run_greeks_change.py"

# --- Archiver: all Angel One + layer streams -> data_lake/*.parquet ---
# Started before ingestion above.

# --- Copy dashboard inputs to a cloud Redis (for Streamlit Community Cloud) ---
# Only when CLOUD_REDIS_URL is set (environment or .env).
$HasCloudRedis = [bool]$env:CLOUD_REDIS_URL
if (-not $HasCloudRedis -and (Test-Path "$PSScriptRoot\.env")) {
    $HasCloudRedis = [bool](Select-String -Path "$PSScriptRoot\.env" -Pattern '^\s*CLOUD_REDIS_URL\s*=\s*\S' -Quiet)
}
if ($HasCloudRedis) {
    Start-Worker "cloud_mirror" "run_cloud_mirror.py"
}

# --- Live Streamlit UI — set START_DASHBOARD=0 to skip ---
# Binds to 127.0.0.1 by default; share it via Cloudflare Tunnel, not the LAN.
$DashPort = if ($env:DASHBOARD_PORT) { $env:DASHBOARD_PORT } else { "8501" }
$DashAddr = if ($env:DASHBOARD_ADDR) { $env:DASHBOARD_ADDR } else { "127.0.0.1" }
if ($env:START_DASHBOARD -ne "0") {
    Start-Worker "dashboard" "-m streamlit" "run streamlit_app.py --server.port $DashPort --server.address $DashAddr --server.headless true --browser.gatherUsageStats false"
}

Write-Host "`n[4/4] Success! Pipeline is running." -ForegroundColor Cyan
Write-Host "Logs are being recorded in: $logDir" -ForegroundColor Gray
Write-Host "To stop all processes, run: .\stop_all.ps1" -ForegroundColor Yellow
if ($env:START_DASHBOARD -ne "0") {
    Write-Host "Dashboard: http://127.0.0.1:$DashPort" -ForegroundColor Gray
}

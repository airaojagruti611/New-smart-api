# PowerShell Runner for Angel One Market Data Pipeline (Windows)
Set-Location $PSScriptRoot

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " Starting Angel One Market Data Pipeline " -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# 1. Activate Virtual Environment if available
if (Test-Path "$PSScriptRoot\venv\Scripts\Activate.ps1") {
    Write-Host "Activating local venv..." -ForegroundColor Gray
    & "$PSScriptRoot\venv\Scripts\Activate.ps1"
} elseif (Test-Path "$PSScriptRoot\..\.venv\Scripts\Activate.ps1") {
    Write-Host "Activating workspace .venv..." -ForegroundColor Gray
    & "$PSScriptRoot\..\.venv\Scripts\Activate.ps1"
}

# 2. Environment Variables
$env:PYTHONUNBUFFERED = "1"
$env:LOG_LEVEL = "INFO"

# 3. Start Redis Container
Write-Host "`n[1/3] Ensuring Redis Docker Container is running..." -ForegroundColor Yellow
docker compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to start Redis Docker container. Make sure Docker Desktop is running." -ForegroundColor Red
    exit 1
}

# 4. Setup Logging and PID directories
$date = Get-Date -Format "yyyy-MM-dd"
$logDir = Join-Path $PSScriptRoot "logs\$date"
$pidDir = Join-Path $logDir "pids"
New-Item -ItemType Directory -Force -Path $pidDir | Out-Null

Write-Host "`n[2/3] Spawning Background Workers..." -ForegroundColor Yellow

function Start-Worker {
    param (
        [string]$Name,
        [string]$Script,
        [string]$Args = ""
    )
    Write-Host "  -> Launching worker: $Name" -ForegroundColor Green
    $stdoutFile = Join-Path $logDir "$Name.log"
    
    if ($Args) {
        $proc = Start-Process -FilePath "python" -ArgumentList "$Script $Args" -RedirectStandardOutput $stdoutFile -RedirectStandardError $stdoutFile -PassThru -NoNewWindow
    } else {
        $proc = Start-Process -FilePath "python" -ArgumentList "$Script" -RedirectStandardOutput $stdoutFile -RedirectStandardError $stdoutFile -PassThru -NoNewWindow
    }
    
    $proc.Id | Out-File -FilePath (Join-Path $pidDir "$Name.pid") -Encoding ascii
}

# --- Core Pipeline Workers ---
Start-Worker "producer" "run_producer.py"
Start-Worker "greeks" "run_greeks_only.py"
Start-Worker "joiner" "run_joiner.py"

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
Start-Worker "entry_trigger" "run_entry_trigger.py"
Start-Worker "strike_select" "run_strike_select.py"
Start-Worker "capital_alloc" "run_capital_alloc.py"

# --- Archivers ---
Start-Worker "arch_eq" "run_archiver_all.py" "eq"
Start-Worker "arch_opt" "run_archiver_all.py" "opt"
Start-Worker "arch_greeks" "run_archiver_all.py" "greeks"
Start-Worker "arch_features" "run_archiver_all.py" "features"

Start-Worker "arch_candles_1m" "run_archiver_candles_1m.py"
Start-Worker "arch_candles_5m" "run_archiver_candles_5m.py"
Start-Worker "arch_candles_10m" "run_archiver_candles_10m.py"
Start-Worker "arch_candles_30m" "run_archiver_candles_30m.py"
Start-Worker "arch_candles_1d" "run_archiver_candles_1d.py"

Start-Worker "arch_regime_csv" "run_archiver_signals_csv.py" "regime"
Start-Worker "arch_volume_csv" "run_archiver_signals_csv.py" "volume"

Write-Host "`n[3/3] Success! Pipeline is running." -ForegroundColor Cyan
Write-Host "Logs are being recorded in: $logDir" -ForegroundColor Gray
Write-Host "To stop all processes, run: .\stop_all.ps1" -ForegroundColor Yellow

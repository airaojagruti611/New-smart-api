# PowerShell runner script for Angel One MD Pipeline on Windows

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

$VenvPython = "$ScriptDir\venv\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    $VenvPython = "python"
}

# Start Redis
Write-Host "Starting Redis via Docker Compose..." -ForegroundColor Green
docker compose up -d

$Day = Get-Date -Format "yyyy-MM-dd"
$LogDir = "$ScriptDir\logs\$Day"
$PidDir = "$LogDir\pids"

if (-not (Test-Path $PidDir)) {
    New-Item -ItemType Directory -Force -Path $PidDir | Out-Null
}

$env:PYTHONUNBUFFERED = "1"
$env:LOG_LEVEL = "INFO"

function Start-Worker {
    param (
        [string]$Name,
        [string]$Script,
        [string]$Args = ""
    )
    Write-Host "Starting $Name..." -ForegroundColor Cyan
    $LogFile = "$LogDir\$Name.log"
    $PidFile = "$PidDir\$Name.pid"
    
    if ($Args) {
        $proc = Start-Process -FilePath $VenvPython -ArgumentList "$Script $Args" -RedirectStandardOutput $LogFile -RedirectStandardError $LogFile -PassThru -NoNewWindow
    } else {
        $proc = Start-Process -FilePath $VenvPython -ArgumentList "$Script" -RedirectStandardOutput $LogFile -RedirectStandardError $LogFile -PassThru -NoNewWindow
    }
    
    $proc.Id | Out-File -FilePath $PidFile -Encoding ascii
}

# 1) Core Data Producers & Streams
Start-Worker "producer" "run_producer.py"
Start-Sleep -Seconds 3
Start-Worker "greeks" "run_greeks_only.py"
Start-Worker "joiner" "run_joiner.py"

# 2) Candles & Pivots
Start-Worker "candles_pub" "run_candles_publisher.py"
Start-Worker "candles_rs" "run_candles_resampler.py"
Start-Worker "pivots" "run_daily_pivots.py"

# 3) Analysis & Microstructure
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

# 4) Strategy Decision & Strike Selection
Start-Worker "entry_trigger" "run_entry_trigger.py"
Start-Worker "strike_select" "run_strike_select.py"
Start-Worker "capital_alloc" "run_capital_alloc.py"

# 5) Archivers
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

Write-Host ""
Write-Host "All background pipeline workers started successfully!" -ForegroundColor Green
Write-Host "Logs directory: $LogDir" -ForegroundColor Yellow
Write-Host "PIDs directory: $PidDir" -ForegroundColor Yellow
Write-Host ""
Write-Host "To stop all workers, run:" -ForegroundColor Magenta
Write-Host "  Get-Content $PidDir\*.pid | ForEach-Object { Stop-Process -Id $_ -ErrorAction SilentlyContinue }" -ForegroundColor White

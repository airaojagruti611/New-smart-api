# PowerShell script to stop all running pipeline services

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

$Day = Get-Date -Format "yyyy-MM-dd"
$PidDir = "$ScriptDir\logs\$Day\pids"

if (Test-Path $PidDir) {
    Get-ChildItem "$PidDir\*.pid" | ForEach-Object {
        $pidVal = Get-Content $_.FullName -ErrorAction SilentlyContinue
        if ($pidVal) {
            Write-Host "Stopping process ID $pidVal ($($_.BaseName))..." -ForegroundColor Yellow
            Stop-Process -Id $pidVal -Force -ErrorAction SilentlyContinue
        }
    }
    Write-Host "All workers stopped." -ForegroundColor Green
} else {
    Write-Host "No active PID files found for today ($Day)." -ForegroundColor Red
}

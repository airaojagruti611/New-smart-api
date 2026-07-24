# PowerShell Helper to Stop All Pipeline Workers
Set-Location $PSScriptRoot

$date = Get-Date -Format "yyyy-MM-dd"
$pidDir = Join-Path $PSScriptRoot "logs\$date\pids"

if (Test-Path $pidDir) {
    $pidFiles = Get-ChildItem -Path $pidDir -Filter "*.pid"
    if ($pidFiles.Count -gt 0) {
        Write-Host "Stopping $($pidFiles.Count) background pipeline workers..." -ForegroundColor Yellow
        foreach ($file in $pidFiles) {
            $pidVal = Get-Content $file.FullName
            try {
                Stop-Process -Id [int]$pidVal -Force -ErrorAction SilentlyContinue
                Write-Host "  -> Stopped PID $pidVal ($($file.BaseName))" -ForegroundColor Gray
            } catch {
                # Already stopped
            }
            Remove-Item -Path $file.FullName -Force
        }
        Write-Host "All background workers stopped." -ForegroundColor Green
    } else {
        Write-Host "No active PID files found in $pidDir." -ForegroundColor Yellow
    }
} else {
    Write-Host "No PID directory found for today ($date)." -ForegroundColor Yellow
}

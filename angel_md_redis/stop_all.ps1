# PowerShell Helper to Stop All Pipeline Workers
Set-Location $PSScriptRoot

# Look at every day's pid folder, not just today's, so a pipeline started
# before midnight (or on a previous day) is still stopped.
$pidFiles = @(Get-ChildItem -Path (Join-Path $PSScriptRoot "logs") -Filter "*.pid" -Recurse -ErrorAction SilentlyContinue |
    Where-Object { $_.Directory.Name -eq "pids" })

if ($pidFiles.Count -eq 0) {
    Write-Host "No active PID files found under logs\*\pids." -ForegroundColor Yellow
    return
}

Write-Host "Stopping $($pidFiles.Count) background pipeline workers..." -ForegroundColor Yellow
foreach ($file in $pidFiles) {
    $pidVal = (Get-Content $file.FullName -ErrorAction SilentlyContinue | Select-Object -First 1)
    if ($pidVal -and ($pidVal.Trim() -match '^\d+$')) {
        # venv python.exe is a launcher that spawns the real interpreter as a
        # child, so kill the whole process tree (/T), not just the recorded PID.
        & taskkill.exe /PID $pidVal.Trim() /T /F 2>$null | Out-Null
        Write-Host "  -> Stopped PID $($pidVal.Trim()) ($($file.BaseName))" -ForegroundColor Gray
    }
    Remove-Item -Path $file.FullName -Force -ErrorAction SilentlyContinue
}
Write-Host "All background workers stopped." -ForegroundColor Green

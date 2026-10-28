$ErrorActionPreference = 'Stop'
$ProjectRoot = $PSScriptRoot
$RuntimeDir = Join-Path $ProjectRoot 'runtime'
New-Item -ItemType Directory -Force -Path $RuntimeDir | Out-Null
$PipelinePython = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $PipelinePython)) { $PipelinePython = (Get-Command python.exe).Source }

function Get-PipelineWorkers {
    $files = @(Get-ChildItem -LiteralPath (Join-Path $ProjectRoot 'logs') -Filter '*.process.json' -Recurse -ErrorAction SilentlyContinue)
    foreach ($file in $files) {
        $record = Get-Content -LiteralPath $file.FullName -Raw | ConvertFrom-Json
        $process = Get-Process -Id $record.Id -ErrorAction SilentlyContinue
        if ($process -and $process.StartTime.ToUniversalTime().Ticks.ToString() -eq $record.StartTicks) {
            [pscustomobject]@{ Name = $record.Name; Process = $process; Record = $file.FullName }
        }
    }
}

function Stop-PipelineWorker($Worker) {
    # Re-check process identity immediately before taskkill to avoid stale PID reuse.
    $record = Get-Content -LiteralPath $Worker.Record -Raw | ConvertFrom-Json
    $process = Get-Process -Id $record.Id -ErrorAction SilentlyContinue
    if ($process -and $process.StartTime.ToUniversalTime().Ticks.ToString() -eq $record.StartTicks) {
        & taskkill.exe /PID $record.Id /T /F 2>$null | Out-Null
        if ($LASTEXITCODE -ne 0 -and (Get-Process -Id $record.Id -ErrorAction SilentlyContinue)) {
            throw "Could not stop $($Worker.Name)"
        }
    }
}

function Start-PipelineDocker {
    try { & docker.exe info --format '{{.ServerVersion}}' 2>$null | Out-Null } catch { $LASTEXITCODE = 1 }
    if ($LASTEXITCODE -ne 0) {
        $desktop = Join-Path $env:ProgramFiles 'Docker\Docker\Docker Desktop.exe'
        if (-not (Test-Path -LiteralPath $desktop)) { throw 'Docker Desktop not installed' }
        Start-Process -FilePath $desktop -WindowStyle Hidden
        $deadline = (Get-Date).AddMinutes(3)
        do {
            Start-Sleep -Seconds 5
            try { & docker.exe info --format '{{.ServerVersion}}' 2>$null | Out-Null } catch { $LASTEXITCODE = 1 }
            if ($LASTEXITCODE -eq 0) { break }
        } while ((Get-Date) -lt $deadline)
        if ($LASTEXITCODE -ne 0) { throw 'Docker Desktop did not become ready' }
    }
    & docker.exe compose --project-directory $ProjectRoot -f (Join-Path $ProjectRoot 'docker-compose.yml') up -d
    if ($LASTEXITCODE -ne 0) { throw 'Could not start project Redis' }
    $deadline = (Get-Date).AddMinutes(2)
    do {
        try { $pong = & docker.exe exec md_redis redis-cli ping 2>$null } catch { $LASTEXITCODE = 1; $pong = '' }
        if ($LASTEXITCODE -eq 0 -and $pong -eq 'PONG') { return }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw 'Redis did not become ready'
}

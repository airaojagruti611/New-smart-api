$ErrorActionPreference = 'Stop'
$ProjectRoot = $PSScriptRoot
$RuntimeDir = Join-Path $ProjectRoot 'runtime'
New-Item -ItemType Directory -Force -Path $RuntimeDir | Out-Null
$PipelinePython = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $PipelinePython)) { $PipelinePython = (Get-Command python.exe).Source }

# We already parallelize across many workers. Avoid allocating a machine-wide
# native numerical thread pool in each process (hundreds of MiB per importer).
# Honor explicitly configured process environment overrides.
foreach ($threadSetting in @('OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS')) {
    if (-not [Environment]::GetEnvironmentVariable($threadSetting, 'Process')) {
        [Environment]::SetEnvironmentVariable($threadSetting, '1', 'Process')
    }
}

function Get-MatchingPipelineProcess($Record) {
    # Windows reuses PIDs. An old worker PID may now belong to a protected
    # system process whose StartTime is null or throws access denied.
    # Unverifiable identity must never count as a worker or become a kill target.
    try {
        $candidate = Get-Process -Id $Record.Id -ErrorAction SilentlyContinue
        if (-not $candidate) { return }
        $started = $candidate.StartTime
        if ($null -eq $started) { return }
        if ($started.ToUniversalTime().Ticks.ToString() -eq $Record.StartTicks) {
            return $candidate
        }
    } catch {
        # A process can also exit between lookup and reading its start time.
        return
    }
}

function Get-PipelineWorkers {
    $files = @(Get-ChildItem -LiteralPath (Join-Path $ProjectRoot 'logs') -Filter '*.process.json' -Recurse -ErrorAction SilentlyContinue)
    foreach ($file in $files) {
        $record = Get-Content -LiteralPath $file.FullName -Raw | ConvertFrom-Json
        $process = Get-MatchingPipelineProcess $record
        if ($process) {
            [pscustomobject]@{ Name = $record.Name; Process = $process; Record = $file.FullName }
        }
    }
}

function Stop-PipelineWorker($Worker) {
    # Re-check process identity immediately before taskkill to avoid stale PID reuse.
    $record = Get-Content -LiteralPath $Worker.Record -Raw | ConvertFrom-Json
    $process = Get-MatchingPipelineProcess $record
    if ($process) {
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
        if ($LASTEXITCODE -eq 0 -and $pong -eq 'PONG') {
            Assert-PipelineRedis
            return
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw 'Redis did not become ready'
}

function Assert-PipelineRedis {
    $serverInfo = & docker.exe exec md_redis redis-cli INFO server
    if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect project Redis identity' }
    $runIdLine = @($serverInfo | Where-Object { $_ -match '^run_id:' })
    if ($runIdLine.Count -ne 1) { throw 'Cannot determine project Redis identity' }
    $runId = ($runIdLine[0] -replace '^run_id:', '').Trim()
    & $PipelinePython (Join-Path $ProjectRoot 'scripts\check_redis_target.py') --expected-run-id $runId
    if ($LASTEXITCODE -ne 0) { throw 'Configured Redis does not match the project container; collection blocked.' }
}

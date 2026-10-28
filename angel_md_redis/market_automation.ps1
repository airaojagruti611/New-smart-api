param([switch]$Status, [switch]$UploadOnly)
. (Join-Path $PSScriptRoot 'automation_common.ps1')
Set-Location $PSScriptRoot
if ($Status) {
    & $PipelinePython (Join-Path $ProjectRoot 'market_automation.py') status
    Get-ScheduledTask -TaskName 'SmartAPI-MarketHours' -ErrorAction SilentlyContinue | Select-Object TaskName,State
    return
}
if ($UploadOnly) {
    & $PipelinePython (Join-Path $ProjectRoot 'market_automation.py') upload
    if ($LASTEXITCODE -ne 0) { throw 'Upload incomplete; local files retained.' }
    return
}
$mutex = New-Object System.Threading.Mutex($false, 'Local\SmartAPI-MarketHours')
try { $acquired = $mutex.WaitOne(0) } catch [System.Threading.AbandonedMutexException] { $acquired = $true }
if (-not $acquired) { $mutex.Dispose(); return }
$logFolder = Join-Path $ProjectRoot 'logs\automation'
New-Item -ItemType Directory -Force -Path $logFolder | Out-Null
Start-Transcript -Path (Join-Path $logFolder ((Get-Date -Format 'yyyy-MM-dd') + '.log')) -Append | Out-Null
try {
    $config = Get-Content -LiteralPath (Join-Path $ProjectRoot 'automation.json') -Raw | ConvertFrom-Json
    $override = Join-Path $ProjectRoot 'automation.local.json'
    if (Test-Path -LiteralPath $override) {
        $local = Get-Content -LiteralPath $override -Raw | ConvertFrom-Json
        foreach ($property in $local.PSObject.Properties) { $config | Add-Member -NotePropertyName $property.Name -NotePropertyValue $property.Value -Force }
    }
    $sessionText = & $PipelinePython (Join-Path $ProjectRoot 'market_automation.py') session
    if ($LASTEXITCODE -ne 0) { throw 'Could not read market calendar' }
    $session = $sessionText | ConvertFrom-Json
    $statePath = Join-Path $RuntimeDir 'market-state.json'
    $state = if (Test-Path -LiteralPath $statePath) { Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json } else {
        [pscustomobject]@{ Prepared = ''; Started = ''; Stopped = ''; FinalUpload = ''; LastUpload = ''; LastAttempt = '' }
    }
    function Save-State {
        $state | ConvertTo-Json | Set-Content -LiteralPath ($statePath + '.tmp') -Encoding UTF8
        Move-Item -LiteralPath ($statePath + '.tmp') -Destination $statePath -Force
    }
    if (-not $UploadOnly) {
        if ($session.phase -eq 'calendar_expired') { Write-Warning 'Calendar year not configured; collection disabled. Update automation.json.' }
        if ($session.phase -in @('prepare', 'open') -and $state.Prepared -ne $session.date) {
            Start-PipelineDocker
            $env:ARCHIVE_TZ = 'Asia/Kolkata'
            $prepLog = Join-Path $logFolder ($session.date + '-history.log')
            $prepErr = Join-Path $logFolder ($session.date + '-history.err.log')
            $prep = Start-Process -FilePath $PipelinePython -ArgumentList ('"' + (Join-Path $ProjectRoot 'run_history_bootstrap.py') + '"') -WorkingDirectory $ProjectRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput $prepLog -RedirectStandardError $prepErr
            if (-not $prep.WaitForExit(120000)) {
                & taskkill.exe /PID $prep.Id /T /F 2>$null | Out-Null
                Write-Warning 'History seed exceeded two minutes; continuing with live data.'
            } elseif ($prep.ExitCode -ne 0) {
                Write-Warning "History seed failed; see $prepErr. Live collection will still start."
            }
            $state.Prepared = $session.date
            Save-State
            $session = (& $PipelinePython (Join-Path $ProjectRoot 'market_automation.py') session) | ConvertFrom-Json
        }
        if ($session.phase -eq 'open' -and $state.Stopped -ne $session.date) {
            $workers = @(Get-PipelineWorkers)
            $recordFolder = Join-Path $ProjectRoot ('logs\' + $session.date + '\pids')
            $expectedWorkers = @(Get-ChildItem -LiteralPath $recordFolder -Filter '*.process.json' -ErrorAction SilentlyContinue).Count
            if ($workers.Count -eq 0 -or $workers.Count -lt $expectedWorkers) {
                # Recheck after Docker/history initialization in case it crossed the close.
                $latest = (& $PipelinePython (Join-Path $ProjectRoot 'market_automation.py') session) | ConvertFrom-Json
                if ($latest.phase -eq 'open') {
                    $env:SKIP_HISTORY_BOOTSTRAP = '1'
                    & (Join-Path $ProjectRoot 'run_all.ps1') -Resume
                    $state.Started = $session.date
                    Save-State
                }
            }
        } else {
            # Also stops leftover workers after reboot, on holidays, or after manual runs.
            if ($session.phase -ne 'prepare' -and @(Get-PipelineWorkers).Count -gt 0) {
                & (Join-Path $ProjectRoot 'stop_all.ps1')
                $state.Stopped = $session.date
                Save-State
                & docker.exe compose --project-directory $ProjectRoot -f (Join-Path $ProjectRoot 'docker-compose.yml') stop redis
                if ($LASTEXITCODE -ne 0) { throw 'Redis did not stop cleanly' }
            }
        }
    }
    $final = $session.phase -notin @('prepare', 'open') -and @(Get-PipelineWorkers).Count -eq 0
    if (Test-Path -LiteralPath (Join-Path $RuntimeDir 'uploads.paused')) {
        Write-Host 'Drive uploads paused pending confirmation of the correct Google account. Collection scheduling remains active.'
        return
    }
    $receiptPath = Join-Path $RuntimeDir 'upload-status.json'
    if (Test-Path -LiteralPath $receiptPath) {
        $receipt = Get-Content -LiteralPath $receiptPath -Raw | ConvertFrom-Json
        $receiptTime = [datetime]$receipt.completed_at
        $afterClose = $session.phase -in @('closed', 'calendar_expired')
        if ($session.close) {
            $afterClose = $receiptTime -ge [datetime]::Parse(($session.date + ' ' + $session.close))
        }
        if ($receipt.final -and $afterClose -and $receiptTime.ToString('yyyy-MM-dd') -eq $session.date) {
            $state.FinalUpload = $session.date
            Save-State
        }
    }
    $due = $UploadOnly -or -not $state.LastAttempt -or ((Get-Date) - [datetime]$state.LastAttempt).TotalMinutes -ge $config.upload_interval_minutes
    if ($final -and $state.FinalUpload -ne $session.date) { $due = $true }
    if ($due) {
        # Persist failures too, so a missing connection is retried at a bounded rate.
        if ($state.LastAttempt -and ((Get-Date) - [datetime]$state.LastAttempt).TotalMinutes -lt 5 -and -not $UploadOnly) { return }
        $state.LastAttempt = (Get-Date).ToString('o')
        Save-State
        $uploadArgs = '"' + (Join-Path $ProjectRoot 'market_automation.py') + '" upload'
        if ($final) { $uploadArgs += ' --final' }
        $stamp = Get-Date -Format 'yyyy-MM-dd-HHmmss'
        # Slow uploads must never block the market-close controller.
        Start-Process -FilePath $PipelinePython -ArgumentList $uploadArgs -WorkingDirectory $ProjectRoot -WindowStyle Hidden `
            -RedirectStandardOutput (Join-Path $logFolder "$stamp-upload.log") `
            -RedirectStandardError (Join-Path $logFolder "$stamp-upload.err.log") | Out-Null
    }
} finally {
    Stop-Transcript | Out-Null
    $mutex.ReleaseMutex()
    $mutex.Dispose()
}

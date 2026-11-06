$ErrorActionPreference = 'Stop'
. (Join-Path (Split-Path -Parent $PSScriptRoot) 'automation_common.ps1')
$fixture = Join-Path ([IO.Path]::GetTempPath()) ('smartapi-workers-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path (Join-Path $fixture 'logs') -Force | Out-Null
$ProjectRoot = $fixture
$started = [datetime]::UtcNow
$record = [pscustomobject]@{Id=123; Name='producer'; StartTicks=$started.Ticks.ToString()}
$recordPath = Join-Path $fixture 'logs\producer.process.json'
$record | ConvertTo-Json | Set-Content -LiteralPath $recordPath
$script:killCalls = 0
function taskkill.exe { $script:killCalls++; $global:LASTEXITCODE = 0 }
function Get-Process {
    param($Id, $ErrorAction)
    if ($script:lookupThrows) { throw 'Access denied or process exited' }
    return $script:fakeProcess
}
try {
    foreach ($case in @('missing', 'null-start', 'wrong-start', 'lookup-error', 'property-error', 'matching')) {
        $script:lookupThrows = $false
        switch ($case) {
            'missing' { $script:fakeProcess = $null }
            'null-start' { $script:fakeProcess = [pscustomobject]@{StartTime=$null} }
            'wrong-start' { $script:fakeProcess = [pscustomobject]@{StartTime=$started.AddDays(-1)} }
            'lookup-error' { $script:lookupThrows = $true; $script:fakeProcess = $null }
            'property-error' {
                $script:fakeProcess = New-Object psobject
                $script:fakeProcess | Add-Member -MemberType ScriptProperty -Name StartTime -Value { throw 'Access denied' }
            }
            'matching' { $script:fakeProcess = [pscustomobject]@{StartTime=$started} }
        }
        $expected = if ($case -eq 'matching') { 1 } else { 0 }
        $workers = @(Get-PipelineWorkers)
        if ($workers.Count -ne $expected) { throw "Wrong worker count for $case" }
        $script:killCalls = 0
        Stop-PipelineWorker ([pscustomobject]@{Name='producer'; Record=$recordPath})
        if ($script:killCalls -ne $expected) { throw "Unsafe kill behavior for $case" }
    }
    # Identity must be rechecked if the PID is reused after initial enumeration.
    $script:fakeProcess = [pscustomobject]@{StartTime=$started}
    $worker = @(Get-PipelineWorkers)[0]
    $script:fakeProcess = [pscustomobject]@{StartTime=$null}
    $script:killCalls = 0
    Stop-PipelineWorker $worker
    if ($script:killCalls) { throw 'Killed a PID whose identity changed' }
    Write-Output 'Worker identity regression checks passed (7 scenarios).'
} finally {
    # Only remove this test's known files/directories; never production records.
    Remove-Item -LiteralPath $recordPath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath (Join-Path $fixture 'logs') -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $fixture -ErrorAction SilentlyContinue
}

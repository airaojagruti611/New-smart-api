# Stop ingestion, allow analytics to finish, then drain/flush the archiver.
param([int]$DrainTimeoutSeconds = 240)
. (Join-Path $PSScriptRoot 'automation_common.ps1')
Set-Location $PSScriptRoot
$workers = @(Get-PipelineWorkers)
if ($workers.Count -eq 0) {
    Write-Host 'No tracked pipeline workers are running.'
    return
}
foreach ($worker in $workers | Where-Object { $_.Name -in @('producer', 'greeks') }) {
    Stop-PipelineWorker $worker
}
Start-Sleep -Seconds 15
foreach ($worker in @(Get-PipelineWorkers) | Where-Object { $_.Name -ne 'arch_layers' }) {
    Stop-PipelineWorker $worker
}
$archivers = @(Get-PipelineWorkers | Where-Object { $_.Name -eq 'arch_layers' })
if ($archivers.Count -eq 0) {
    throw 'Archiver was not running; Redis retained. Check its logs before marking the session complete.'
}
Set-Content -LiteralPath (Join-Path $RuntimeDir 'archiver.stop') -Value 'drain' -Encoding ASCII
$deadline = (Get-Date).AddSeconds($DrainTimeoutSeconds)
while (@(Get-PipelineWorkers | Where-Object { $_.Name -eq 'arch_layers' }).Count -gt 0) {
    if ((Get-Date) -ge $deadline) {
        throw 'Archiver has not finished draining. It and Redis remain running; retry stop_all.ps1.'
    }
    Start-Sleep -Seconds 2
}
if (-not (Test-Path -LiteralPath (Join-Path $RuntimeDir 'archiver-drained.json'))) {
    throw 'Archiver exited without confirming a successful flush. Redis retained for recovery.'
}
Write-Host 'Pipeline stopped; all archiver buffers were flushed.'

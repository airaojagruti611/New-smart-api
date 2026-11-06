param([switch]$Remove)
. (Join-Path $PSScriptRoot 'automation_common.ps1')
$taskName = 'SmartAPI-OperationsDashboard'
if ($Remove) {
    if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
        Disable-ScheduledTask -TaskName $taskName | Out-Null
        Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
        $record = Join-Path $RuntimeDir 'operations.process.json'
        if (Test-Path -LiteralPath $record) {
            Stop-PipelineWorker ([pscustomobject]@{Name='operations-dashboard';Record=$record})
        }
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    }
    return
}
$windowless = Join-Path (Split-Path -Parent $PipelinePython) 'pythonw.exe'
if (-not (Test-Path -LiteralPath $windowless)) { throw 'The project Python environment needs pythonw.exe' }
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$action = New-ScheduledTaskAction -Execute $windowless -Argument ('"' + (Join-Path $ProjectRoot 'run_operations_hidden.py') + '"') -WorkingDirectory $ProjectRoot
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew -ExecutionTimeLimit ([timespan]::Zero) -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Description 'SmartAPI local read-only operations UI on 127.0.0.1:8502. Independent of collection and uploads.' -Force | Out-Null
Start-ScheduledTask -TaskName $taskName
Write-Host 'Operations dashboard: http://127.0.0.1:8502'
Get-ScheduledTask -TaskName $taskName | Select-Object TaskName,State

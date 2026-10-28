param([switch]$Remove)
. (Join-Path $PSScriptRoot 'automation_common.ps1')
$taskName = 'SmartAPI-MarketHours'
if ($Remove) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    return
}
# Windows triggers use local time. Refuse a silent timezone mismatch.
if ((Get-TimeZone).Id -ne 'India Standard Time') { throw 'Set Windows time zone to India Standard Time before installing.' }
$windowlessPython = Join-Path (Split-Path -Parent $PipelinePython) 'pythonw.exe'
if (-not (Test-Path -LiteralPath $windowlessPython)) { throw 'pythonw.exe is required for the silent scheduler launcher.' }
$arguments = '"' + (Join-Path $ProjectRoot 'run_market_hidden.py') + '"'
$action = New-ScheduledTaskAction -Execute $windowlessPython -Argument $arguments -WorkingDirectory $ProjectRoot
$config = Get-Content -LiteralPath (Join-Path $ProjectRoot 'automation.json') -Raw | ConvertFrom-Json
$localConfig = Join-Path $ProjectRoot 'automation.local.json'
if (Test-Path -LiteralPath $localConfig) {
    $overrides = Get-Content -LiteralPath $localConfig -Raw | ConvertFrom-Json
    foreach ($property in $overrides.PSObject.Properties) { $config | Add-Member -NotePropertyName $property.Name -NotePropertyValue $property.Value -Force }
}
$normalStart = [datetime]::Today.Add([timespan]::Parse($config.prepare_time))
$normalEnd = [datetime]::Today.Add([timespan]::Parse($config.close_time)).AddMinutes(30)
if ($normalEnd -le $normalStart) { throw 'Invalid market times' }
$daily = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At $normalStart
# Check through 30 minutes after close; avoid overnight/weekend wakeups.
$template = New-ScheduledTaskTrigger -Once -At $normalStart -RepetitionInterval (New-TimeSpan -Minutes 1) -RepetitionDuration ($normalEnd - $normalStart)
$daily.Repetition = $template.Repetition
$triggers = @($daily)
foreach ($property in $config.special_sessions.PSObject.Properties) {
    $start = [datetime]::ParseExact(($property.Name + ' ' + $property.Value.prepare_time), 'yyyy-MM-dd HH:mm', [cultureinfo]::InvariantCulture)
    $end = [datetime]::ParseExact(($property.Name + ' ' + $property.Value.close_time), 'yyyy-MM-dd HH:mm', [cultureinfo]::InvariantCulture).AddMinutes(30)
    if ($end -gt (Get-Date)) {
        $triggers += New-ScheduledTaskTrigger -Once -At $start -RepetitionInterval (New-TimeSpan -Minutes 1) -RepetitionDuration ($end - $start)
    }
}
$logon = New-ScheduledTaskTrigger -AtLogOn -User ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name)
$settings = New-ScheduledTaskSettingsSet -WakeToRun -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 12)
$principal = New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger ($triggers + @($logon)) -Settings $settings -Principal $principal -Description 'SmartAPI: NSE market-hours collection, graceful close, Google Drive uploads and verified local retention.' -Force | Out-Null
Get-ScheduledTask -TaskName $taskName | Select-Object TaskName,State
Write-Host "Installed: prepare $($config.prepare_time), collect $($config.open_time)-$($config.close_time) IST, holidays excluded. Requires your Windows session to stay signed in."

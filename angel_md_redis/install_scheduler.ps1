# Registers the "OptionRider Market Scheduler" Windows scheduled task.
#
#   .\install_scheduler.ps1              # install / update
#   .\install_scheduler.ps1 -Uninstall   # remove the task (and stop the pipeline)
#
# Triggers:
#   - At logon of the current user (covers "whenever the PC is started")
#   - Daily at 09:05 IST, waking the PC from sleep, in case it was left on
# The task runs market_scheduler.ps1 hidden; that script decides when the
# pipeline should actually be up (Mon-Fri 09:15-15:30 IST).

param([switch]$Uninstall)

$TaskName = "OptionRider Market Scheduler"
$Script = Join-Path $PSScriptRoot "market_scheduler.ps1"

if ($Uninstall) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Get-CimInstance Win32_Process -Filter "Name='powershell.exe'" |
        Where-Object { $_.CommandLine -like "*market_scheduler.ps1*" } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
    & (Join-Path $PSScriptRoot "stop_all.ps1")
    Write-Host "Removed task '$TaskName'." -ForegroundColor Green
    return
}

# 09:05 IST expressed in this PC's local time (identical if the PC is on IST).
$ist = [TimeZoneInfo]::FindSystemTimeZoneById("India Standard Time")
$istToday = [TimeZoneInfo]::ConvertTimeFromUtc([DateTime]::UtcNow, $ist).Date
$dailyLocal = [TimeZoneInfo]::ConvertTime($istToday.AddHours(9).AddMinutes(5), $ist, [TimeZoneInfo]::Local)

$action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$Script`"" `
    -WorkingDirectory $PSScriptRoot

$user = "$env:USERDOMAIN\$env:USERNAME"
$triggers = @(
    (New-ScheduledTaskTrigger -AtLogOn -User $user),
    (New-ScheduledTaskTrigger -Daily -At $dailyLocal)
)

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -StartWhenAvailable -WakeToRun `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -MultipleInstances IgnoreNew `
    -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)

$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $triggers `
    -Settings $settings -Principal $principal -Force | Out-Null

Write-Host "Installed task '$TaskName'." -ForegroundColor Green
Write-Host "  Runs at logon and daily at $($dailyLocal.ToString('HH:mm')) local; pipeline up Mon-Fri 09:15-15:30 IST." -ForegroundColor Gray
Write-Host "  Log: $(Join-Path $PSScriptRoot 'logs\scheduler.log')" -ForegroundColor Gray

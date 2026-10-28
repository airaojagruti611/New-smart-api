param([string]$Email)
. (Join-Path $PSScriptRoot 'automation_common.ps1')
$rclone = Join-Path $ProjectRoot 'tools\rclone.exe'
if (-not (Test-Path -LiteralPath $rclone)) { throw 'Portable rclone is missing from tools\rclone.exe' }
$credentials = Join-Path $RuntimeDir 'rclone.conf'
$localPath = Join-Path $ProjectRoot 'automation.local.json'
$localConfig = if (Test-Path -LiteralPath $localPath) { Get-Content -LiteralPath $localPath -Raw | ConvertFrom-Json } else { [pscustomobject]@{} }
if (-not $Email) { $Email = $localConfig.expected_drive_email }
if (-not $Email) { throw 'Specify the destination Google account: .\setup_drive.ps1 -Email you@example.com' }
$localConfig | Add-Member -NotePropertyName expected_drive_email -NotePropertyValue $Email -Force
$localConfig | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $localPath -Encoding UTF8
$pauseFile = Join-Path $RuntimeDir 'uploads.paused'
Set-Content -LiteralPath $pauseFile -Value "Waiting for verified connection to $Email"
Write-Host "Required Google Drive account: $Email"
Write-Host 'One-time Google Drive setup. Use the Google account holding your 5 TB.'
Write-Host 'Create a Google OAuth Desktop client first, using the official guide:'
Write-Host 'https://rclone.org/drive/#making-your-own-client-id'
Write-Host 'Enable Google Drive API, configure OAuth consent for your account, and use Production status to avoid 7-day Testing token expiry.'
Write-Host 'In rclone config: new remote name gdrive; storage drive; enter your own client ID/secret; scope drive.file (files created by this app).'
Write-Host 'Use browser authorization; personal My Drive: do not select a Shared Drive.'
Write-Host 'Credentials are stored only in ignored runtime\rclone.conf. Do not paste them into chat.'
& $rclone config --config $credentials
if ($LASTEXITCODE -ne 0) { throw 'Drive sign-in failed' }
& $rclone about 'gdrive:' --config $credentials
if ($LASTEXITCODE -ne 0) { throw 'Could not access gdrive. Check the remote name and authorization.' }
& $PipelinePython (Join-Path $ProjectRoot 'market_automation.py') verify-drive
if ($LASTEXITCODE -ne 0) { throw 'Google account did not match; uploads remain paused.' }
Remove-Item -LiteralPath $pauseFile -Force
Write-Host 'Drive connected. Starting the first verified upload (local cleanup disabled for this initial run).'
& $PipelinePython (Join-Path $ProjectRoot 'market_automation.py') upload --no-cleanup
if ($LASTEXITCODE -ne 0) { throw 'Initial upload incomplete. Local data remains safe; scheduled uploads will retry.' }
Write-Host 'Connected. The scheduled uploader will now use gdrive:SmartAPI.'

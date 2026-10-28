param([string]$Email)
. (Join-Path $PSScriptRoot 'automation_common.ps1')
$rclone = Join-Path $ProjectRoot 'tools\rclone.exe'
$credentials = Join-Path $RuntimeDir 'rclone.conf'
$localPath = Join-Path $ProjectRoot 'automation.local.json'
$localConfig = if (Test-Path -LiteralPath $localPath) { Get-Content -LiteralPath $localPath -Raw | ConvertFrom-Json } else { [pscustomobject]@{} }
if (-not $Email) { $Email = $localConfig.expected_drive_email }
if (-not $Email) { throw 'Specify the destination Google account: .\switch_drive_account.ps1 -Email you@example.com' }
$pauseFile = Join-Path $RuntimeDir 'uploads.paused'
Set-Content -LiteralPath $pauseFile -Value "Uploads paused until Google account is verified: $Email"
$localConfig | Add-Member -NotePropertyName expected_drive_email -NotePropertyValue $Email -Force
$localConfig | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $localPath -Encoding UTF8
Write-Host "Connect Google Drive for $Email. Data collection continues; uploads remain paused."
Write-Host 'Reuse the existing OAuth client; you do not need another Google Cloud project.'
Write-Host 'If the OAuth app is in Testing, add this email under Google Auth Platform > Audience > Test users first:'
Write-Host 'https://console.cloud.google.com/auth/audience (select your OAuth client project)'
Write-Host 'In the Google browser prompt choose Use another account, then sign in with the exact email above.'
& $rclone config reconnect 'gdrive:' --config $credentials
if ($LASTEXITCODE -ne 0) { throw 'Reconnect failed; uploads remain paused.' }
& $PipelinePython (Join-Path $ProjectRoot 'market_automation.py') verify-drive
if ($LASTEXITCODE -ne 0) { throw 'Google account did not match; uploads remain paused.' }
# Only the verified target may receive future uploads and retention cleanup.
Remove-Item -LiteralPath $pauseFile -Force
Write-Host "Account verified: $Email. Uploads resumed to its SmartAPI folder."
Write-Host 'Starting the first upload to the verified account without local deletion.'
& $PipelinePython (Join-Path $ProjectRoot 'market_automation.py') upload --no-cleanup
if ($LASTEXITCODE -ne 0) { throw 'Account verified, but first upload incomplete. Local files retained; scheduled uploads will retry.' }

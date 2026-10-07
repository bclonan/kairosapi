param(
    [string]$BaseURL = 'http://127.0.0.1:8075',
    [string]$DemoAPI = 'http://127.0.0.1:9081'
)

$ErrorActionPreference = 'Stop'
if (-not $env:KAIROS_API_TOKEN -or -not $env:KAIROS_ADMIN_TOKEN) {
    throw 'Set both tokens to the values used by the running Kairos instance.'
}
$runner = @{ Authorization = 'Bearer ' + $env:KAIROS_API_TOKEN }
$admin = @{ Authorization = 'Bearer ' + $env:KAIROS_ADMIN_TOKEN }
$workspace = Split-Path -Parent $PSScriptRoot
$evidence = Join-Path $workspace ('bin/verification/files-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $evidence -Force | Out-Null
$inputPath = Join-Path $evidence 'payload.bin'
$outputPath = Join-Path $evidence 'returned.bin'
$backupPath = Join-Path $evidence 'kairos-backup.db'
$bytes = New-Object byte[] 196613
[System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
[System.IO.File]::WriteAllBytes($inputPath, $bytes)
$digest = (Get-FileHash -LiteralPath $inputPath -Algorithm SHA256).Hash.ToLowerInvariant()
$uploadHeaders = @{
    Authorization = $runner.Authorization
    'X-File-Name' = 'payload.bin'
    'X-Content-SHA256' = $digest
    'Idempotency-Key' = [guid]::NewGuid().ToString()
}
$uploaded = Invoke-RestMethod -Uri ($BaseURL + '/v1/files') -Method Post -Headers $uploadHeaders -ContentType application/octet-stream -InFile $inputPath
$duplicate = Invoke-RestMethod -Uri ($BaseURL + '/v1/files') -Method Post -Headers $uploadHeaders -ContentType application/octet-stream -InFile $inputPath
if (-not $duplicate.duplicate -or $duplicate.file.id -ne $uploaded.file.id) { throw 'File retry created another object.' }
$spec = Get-Content -Raw -LiteralPath (Join-Path $workspace 'workflows/file-relay.json')
Invoke-RestMethod -Uri ($BaseURL + '/v1/workflows') -Method Post -Headers $admin -ContentType application/json -Body $spec | Out-Null
$request = @{ workflow_id = 'file_relay'; input = @{ file = $uploaded.reference; api_base = $DemoAPI } } | ConvertTo-Json -Depth 15
$run = Invoke-RestMethod -Uri ($BaseURL + '/v1/runs') -Method Post -Headers $runner -ContentType application/json -Body $request
if ($run.state -ne 'succeeded' -or $run.output.receipt.sha256 -ne $digest -or $run.output.receipt.size -ne $bytes.Length) { throw ('File chain failed: ' + ($run | ConvertTo-Json -Depth 20)) }
$fileID = $run.output.file.'$artifact'
Invoke-WebRequest -Uri ($BaseURL + '/v1/files/' + $fileID + '/content') -Headers $runner -OutFile $outputPath | Out-Null
if ((Get-FileHash -LiteralPath $outputPath -Algorithm SHA256).Hash.ToLowerInvariant() -ne $digest) { throw 'Downloaded bytes differ.' }
$backup = Invoke-WebRequest -Uri ($BaseURL + '/v1/admin/backup') -Headers $admin -OutFile $backupPath -PassThru
$backupDigest = (Get-FileHash -LiteralPath $backupPath -Algorithm SHA256).Hash.ToLowerInvariant()
if ($backupDigest -ne [string]$backup.Headers['X-Content-SHA256']) { throw 'Backup digest differs.' }
if ($run.id -notmatch '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' -or $fileID -cle $run.id) { throw 'ULID sequence did not advance.' }
$result = @{
    run_id = $run.id
    file_id = $fileID
    state = $run.state
    bytes = $bytes.Length
    sha256 = $digest
    duplicate_upload_reused_file = $duplicate.duplicate
    multipart_receipt = $run.output.receipt
    backup_path = $backupPath
    backup_sha256 = $backupDigest
}
$result | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $evidence 'result.json') -Encoding utf8
$result | ConvertTo-Json -Depth 10

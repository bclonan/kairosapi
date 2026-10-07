param(
    [string]$BaseURL = 'http://127.0.0.1:8075'
)

$ErrorActionPreference = 'Stop'
if (-not $env:KAIROS_API_TOKEN -or -not $env:KAIROS_ADMIN_TOKEN) {
    throw 'Set both tokens to the values used by the running Kairos instance.'
}
$runner = @{ Authorization = 'Bearer ' + $env:KAIROS_API_TOKEN }
$admin = @{ Authorization = 'Bearer ' + $env:KAIROS_ADMIN_TOKEN }
$workspace = Split-Path -Parent $PSScriptRoot
$fixtures = Join-Path $workspace 'docs/examples/resources'
$evidence = Join-Path $workspace ('bin/verification/typed-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $evidence -Force | Out-Null

function Send-ResourceFile {
    param([string]$FileName, [string]$ContentType)
    $path = Join-Path $fixtures $FileName
    $digest = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
    $headers = @{
        Authorization = $runner.Authorization
        'X-File-Name' = $FileName
        'X-Content-SHA256' = $digest
        'Idempotency-Key' = 'kairos-demo:typed-resources:v1:' + $FileName + ':' + $digest
    }
    return Invoke-RestMethod -Uri ($BaseURL + '/v1/files') -Method Post -Headers $headers -ContentType $ContentType -InFile $path
}

$people = Send-ResourceFile -FileName 'people.json' -ContentType 'application/json'
$descriptor = Send-ResourceFile -FileName 'person.desc' -ContentType 'application/protobuf'
$spec = Get-Content -Raw -LiteralPath (Join-Path $fixtures 'typed-chain.json') | ConvertFrom-Json -AsHashtable
$spec.resources.people = @{ '$artifact' = $people.file.id }
$spec.resources.person = @{ '$artifact' = $descriptor.file.id }
$definition = $spec | ConvertTo-Json -Depth 20
Invoke-RestMethod -Uri ($BaseURL + '/v1/workflows') -Method Post -Headers $admin -ContentType application/json -Body $definition | Out-Null
$runHeaders = @{
    Authorization = $runner.Authorization
    'Idempotency-Key' = [guid]::NewGuid().ToString()
}
$request = @{ workflow_id = $spec.id; version = $spec.version } | ConvertTo-Json
$run = Invoke-RestMethod -Uri ($BaseURL + '/v1/runs') -Method Post -Headers $runHeaders -ContentType application/json -Body $request
$deadline = [DateTime]::UtcNow.AddSeconds(30)
while ($run.state -in @('queued', 'running') -and [DateTime]::UtcNow -lt $deadline) {
    Start-Sleep -Milliseconds 100
    $run = Invoke-RestMethod -Uri ($BaseURL + '/v1/runs/' + $run.id) -Headers $runner
}
if ($run.state -ne 'succeeded') { throw ('Typed workflow failed: ' + ($run | ConvertTo-Json -Depth 20)) }
if ($run.output.person.name -ne 'Ada' -or $run.output.person.customerId -cne '9007199254740993' -or $run.output.person.payload -cne 'AAH/') {
    throw 'Dictionary or protobuf transformation changed the expected values.'
}
$fileID = $run.output.file.'$artifact'
$outputPath = Join-Path $evidence 'person.pb'
Invoke-WebRequest -Uri ($BaseURL + '/v1/files/' + $fileID + '/content') -Headers $runner -OutFile $outputPath | Out-Null
$wireHex = [Convert]::ToHexString([System.IO.File]::ReadAllBytes($outputPath))
if ($wireHex -cne '0A034164611081808080808080101A030001FF') {
    throw ('The encoded protobuf bytes differ: ' + $wireHex)
}
$duplicate = Invoke-RestMethod -Uri ($BaseURL + '/v1/runs') -Method Post -Headers $runHeaders -ContentType application/json -Body $request
if ($duplicate.id -ne $run.id -or $duplicate.output.file.'$artifact' -ne $fileID) {
    throw 'Repeating the run submission created another run or output file.'
}
$result = @{
    run_id = $run.id
    state = $run.state
    dictionary_file = $people.file.id
    descriptor_file = $descriptor.file.id
    output_file = $fileID
    wire_hex = $wireHex
    person = $run.output.person
    duplicate_submission_reused_run = $duplicate.id -eq $run.id
}
$result | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $evidence 'result.json') -Encoding utf8
$result | ConvertTo-Json -Depth 10

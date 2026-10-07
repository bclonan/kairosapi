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
foreach ($file in @('reusable-http.json', 'customer-events.json')) {
    $body = Get-Content -Raw -LiteralPath (Join-Path $workspace ('workflows/' + $file))
    Invoke-RestMethod -Uri ($BaseURL + '/v1/workflows') -Method Post -Headers $admin -ContentType application/json -Body $body | Out-Null
}
$before = Invoke-RestMethod -Uri ($DemoAPI + '/stats') -TimeoutSec 3
$event = @{
    specversion = '1.0'
    id = [guid]::NewGuid().ToString()
    source = '/kairos-demo'
    type = 'customer.requested'
    data = @{ name = 'Ada'; notify = $true; api_base = $DemoAPI }
} | ConvertTo-Json -Depth 10
$delivery = Invoke-RestMethod -Uri ($BaseURL + '/v1/events') -Method Post -Headers $runner -ContentType 'application/cloudevents+json' -Body $event
if ($delivery.runs.Count -ne 1) { throw 'Expected one matching customer workflow.' }
$runID = $delivery.runs[0].id
$deadline = [DateTime]::UtcNow.AddSeconds(10)
do {
    $run = Invoke-RestMethod -Uri ($BaseURL + '/v1/runs/' + $runID) -Headers $runner -TimeoutSec 3
    if ($run.state -notin @('queued','running')) { break }
    Start-Sleep -Milliseconds 100
} while ([DateTime]::UtcNow -lt $deadline)
if ($run.state -ne 'succeeded' -or -not $run.output.receipt.notified) { throw ('Run failed: ' + ($run | ConvertTo-Json -Depth 20)) }
$repeat = Invoke-RestMethod -Uri ($BaseURL + '/v1/events') -Method Post -Headers $runner -ContentType 'application/cloudevents+json' -Body $event
$after = Invoke-RestMethod -Uri ($DemoAPI + '/stats') -TimeoutSec 3
if (-not $repeat.duplicate -or $repeat.runs[0].id -ne $runID -or ($after.customers - $before.customers) -ne 1 -or ($after.notifications - $before.notifications) -ne 1) {
    throw 'Duplicate delivery or API call counts did not match the expected result.'
}
@{
    run_id = $runID
    state = $run.state
    receipt = $run.output.receipt
    steps = @($run.steps | Select-Object id,state)
    duplicate_reused_run = $repeat.duplicate
    customer_calls = $after.customers - $before.customers
    notification_calls = $after.notifications - $before.notifications
} | ConvertTo-Json -Depth 10

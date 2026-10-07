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
$definition = Get-Content -Raw -LiteralPath (Join-Path $workspace 'workflows/async-operation.json')
Invoke-RestMethod -Uri ($BaseURL + '/v1/workflows') -Method Post -Headers $admin -ContentType application/json -Body $definition | Out-Null
$before = Invoke-RestMethod -Uri ($DemoAPI + '/stats') -TimeoutSec 3
$request = @{
    workflow_id = 'async_operation'
    version = 1
    async = $true
    input = @{ api_base = $DemoAPI; value = 'Complete this before creating the receipt' }
} | ConvertTo-Json -Depth 10
$submissionHeaders = @{
    Authorization = $runner.Authorization
    'Idempotency-Key' = [guid]::NewGuid().ToString()
}
$accepted = Invoke-RestMethod -Uri ($BaseURL + '/v1/runs') -Method Post -Headers $submissionHeaders -ContentType application/json -Body $request
$deadline = [DateTime]::UtcNow.AddSeconds(15)
$observedPhases = [System.Collections.Generic.HashSet[string]]::new()
do {
    $run = Invoke-RestMethod -Uri ($BaseURL + '/v1/runs/' + $accepted.id) -Headers $runner -TimeoutSec 3
    foreach ($step in $run.steps) {
        if ($step.phase) { [void]$observedPhases.Add($step.phase) }
    }
    if ($run.state -notin @('queued', 'running')) { break }
    Start-Sleep -Milliseconds 50
} while ([DateTime]::UtcNow -lt $deadline)
if ($run.state -ne 'succeeded' -or $run.output.operation.status -ne 'completed' -or -not $run.output.receipt.accepted) {
    throw ('Asynchronous chain did not complete: ' + ($run | ConvertTo-Json -Depth 20))
}
if ($run.steps[1].started_at -lt $run.steps[0].finished_at) {
    throw 'Dependent receipt started before the asynchronous operation completed.'
}
$repeat = Invoke-RestMethod -Uri ($BaseURL + '/v1/runs') -Method Post -Headers $submissionHeaders -ContentType application/json -Body $request
$after = Invoke-RestMethod -Uri ($DemoAPI + '/stats') -TimeoutSec 3
if ($repeat.id -ne $run.id -or ($after.operations - $before.operations) -ne 1 -or ($after.operation_polls - $before.operation_polls) -ne 2 -or ($after.operation_receipts - $before.operation_receipts) -ne 1) {
    throw 'Expected one provider operation, two status polls, one receipt, and a deduplicated run.'
}
@{
    run_id = $run.id
    state = $run.state
    provider_operation_id = $run.output.operation.id
    provider_operations = $after.operations - $before.operations
    provider_polls = $after.operation_polls - $before.operation_polls
    dependent_receipts = $after.operation_receipts - $before.operation_receipts
    duplicate_reused_run = $repeat.id -eq $run.id
    observed_phases = @($observedPhases)
    checkpoint = $run.steps[0].checkpoint
    receipt = $run.output.receipt
} | ConvertTo-Json -Depth 10

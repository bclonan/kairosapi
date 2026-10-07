param([string]$BaseURL = 'http://127.0.0.1:8075')

$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($env:KAIROS_API_TOKEN)) {
    throw 'Set KAIROS_API_TOKEN to the running local service token first.'
}
$headers = @{ Authorization = 'Bearer ' + $env:KAIROS_API_TOKEN }
$health = Invoke-RestMethod -Uri ($BaseURL + '/healthz') -TimeoutSec 2
$catalog = Invoke-RestMethod -Uri ($BaseURL + '/v1/workflows') -Headers $headers -TimeoutSec 2
$body = @{workflow_id='welcome'; input=@{name='Smoke Test'}} | ConvertTo-Json -Depth 10
$result = Invoke-RestMethod -Uri ($BaseURL + '/v1/runs') -Method Post -Headers $headers -ContentType application/json -Body $body -TimeoutSec 5
if ($health.status -ne 'ok' -or $result.state -ne 'succeeded' -or $result.steps.Count -ne 3 -or $result.steps[2].output.profile.name -ne 'Smoke Test') {
    throw 'The bundled example did not produce the expected result.'
}
$denied = $false
try {
    Invoke-RestMethod -Uri ($BaseURL + '/v1/workflows') -TimeoutSec 2 | Out-Null
} catch {
    $denied = [int]$_.Exception.Response.StatusCode -eq 401
}
if (-not $denied) { throw 'The service did not reject an unauthenticated request.' }
@{
    health=$health.status
    catalog_count=$catalog.workflows.Count
    state=$result.state
    step_states=@($result.steps.state)
    unauthorized_rejected=$denied
    run_id=$result.id
} | ConvertTo-Json -Depth 5

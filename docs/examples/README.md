# Runnable examples

[Documentation index](../README.md)

Run commands from the repository root with Kairos already started. The client terminal needs the same runner and admin token values as the server. Never generate new client-only tokens and expect the running server to accept them.

PowerShell examples require PowerShell 7. The Bash walkthrough uses curl, OpenSSL, and Python 3 to read JSON responses. The README's basic quick start does not need Python.

| Example | Files | What it exercises |
| --- | --- | --- |
| Welcome | [example.json](../../workflows/example.json) | Seed loading, independent steps, dependency join |
| Greeting chain | [greeting.json](greeting.json), [greeting-chain.json](greeting-chain.json), [greeting-run.json](greeting-run.json) | Input schemas, runtime publication, pinned subflow, declared output |
| Customer event | [reusable-http.json](../../workflows/reusable-http.json), [customer-events.json](../../workflows/customer-events.json) | Event intake, HTTP chaining, optional notification, deduplication |
| File relay | [file-relay.json](../../workflows/file-relay.json) | Raw upload, file response, multipart forwarding, exact-byte download |

Only `workflows/example.json` is seeded by default. Other files contain one definition each and must be published. Publish children before parents. Repeating an identical publication is safe; changing an existing version returns 409.

## Client setup

PowerShell:

```powershell
$base = 'http://127.0.0.1:8075'
$runner = @{ Authorization = 'Bearer ' + $env:KAIROS_API_TOKEN }
$admin = @{ Authorization = 'Bearer ' + $env:KAIROS_ADMIN_TOKEN }
Invoke-RestMethod "$base/readyz"
```

Bash, in a dedicated example shell:

```bash
set -euo pipefail
base=http://127.0.0.1:8075
: "${KAIROS_API_TOKEN:?Use the running server's runner token}"
: "${KAIROS_ADMIN_TOKEN:?Use the running server's admin token}"
curl --fail-with-body "$base/readyz"
```

## Greeting chain

This example needs no outbound origin. Publish `greeting` version 1, then `greeting_chain` version 1.

### PowerShell

```powershell
foreach ($file in @('greeting.json', 'greeting-chain.json')) {
    $spec = Get-Content -Raw -LiteralPath "docs/examples/$file"
    Invoke-RestMethod "$base/v1/workflows" -Method Post -Headers $admin -ContentType application/json -Body $spec
}
$request = Get-Content -Raw -LiteralPath docs/examples/greeting-run.json
$key = [guid]::NewGuid().ToString()
$runHeaders = $runner + @{ 'Idempotency-Key' = $key }
$run = Invoke-RestMethod "$base/v1/runs" -Method Post -Headers $runHeaders -ContentType application/json -Body $request
$again = Invoke-RestMethod "$base/v1/runs" -Method Post -Headers $runHeaders -ContentType application/json -Body $request
if ($run.state -ne 'succeeded' -or $run.output.receipt.message -ne 'Hello Ada') {
    throw 'Greeting result did not match'
}
if ($again.id -ne $run.id) { throw 'Retry created a different run' }
$run.output.receipt
```

### Bash

```bash
for spec in docs/examples/greeting.json docs/examples/greeting-chain.json; do
  curl --fail-with-body "$base/v1/workflows" \
    -H "Authorization: Bearer $KAIROS_ADMIN_TOKEN" \
    -H 'Content-Type: application/json' --data-binary "@$spec"
done
key=$(openssl rand -hex 16)
run=$(curl --fail-with-body "$base/v1/runs" \
  -H "Authorization: Bearer $KAIROS_API_TOKEN" \
  -H "Idempotency-Key: $key" \
  -H 'Content-Type: application/json' \
  --data-binary @docs/examples/greeting-run.json)
again=$(curl --fail-with-body "$base/v1/runs" \
  -H "Authorization: Bearer $KAIROS_API_TOKEN" \
  -H "Idempotency-Key: $key" \
  -H 'Content-Type: application/json' \
  --data-binary @docs/examples/greeting-run.json)
python3 -c 'import json, sys
run, again = map(json.loads, sys.argv[1:])
assert run["state"] == "succeeded"
assert run["output"]["receipt"]["message"] == "Hello Ada"
assert again["id"] == run["id"]
print(json.dumps(run["output"], indent=2))' "$run" "$again"
```

The receipt includes `Hello Ada` and the root run ID. The root has a `greet` step with child results, followed by `receipt`. Its reference `/steps/greet/message` reads the child's declared output.

To try input rejection, submit `{"workflow_id":"greeting_chain","input":{}}`. It returns 422 before queue admission. To change the greeting text, increment the child's version, publish it, and publish a new parent version that pins that child.

## Async submission and inspection

Synchronous waiting is convenient for these short local examples. For a client that manages its own polling, submit with `async: true`:

```powershell
$request = @{
    workflow_id = 'greeting_chain'
    version = 1
    input = @{ name = 'Ada' }
    async = $true
} | ConvertTo-Json -Depth 10
$asyncHeaders = $runner + @{ 'Idempotency-Key' = [guid]::NewGuid().ToString() }
$run = Invoke-RestMethod "$base/v1/runs" -Method Post -Headers $asyncHeaders -ContentType application/json -Body $request
for ($attempt = 0; $attempt -lt 60 -and $run.state -in @('queued', 'running'); $attempt++) {
    Start-Sleep -Seconds 1
    $run = Invoke-RestMethod "$base/v1/runs/$($run.id)" -Headers $runner
}
$run
```

If still active after the polling window, keep the ID and inspect it later. Ending this loop does not cancel it. Use `POST /v1/runs/{id}/cancel` only when cancellation is the intended operation.

For pagination, use the returned cursor without deriving it from timestamps:

```powershell
$page = Invoke-RestMethod "$base/v1/runs?limit=2" -Headers $runner
$page.runs
if ($page.next_before) {
    Invoke-RestMethod "$base/v1/runs?limit=2&before=$($page.next_before)" -Headers $runner
}
```

## Customer event chain

Start the separate fixture in another terminal:

```sh
go run ./cmd/demo-api
```

It listens on `127.0.0.1:9081`. Stop Kairos and restart it with the same tokens and database, plus the exact local origin.

PowerShell, in the Kairos server terminal:

```powershell
$env:KAIROS_HTTP_ALLOWED_ORIGINS = 'http://127.0.0.1:9081'
$env:KAIROS_ALLOW_PRIVATE_NETWORKS = 'true'
go run .
```

Bash, in the Kairos server terminal:

```bash
export KAIROS_HTTP_ALLOWED_ORIGINS=http://127.0.0.1:9081
export KAIROS_ALLOW_PRIVATE_NETWORKS=true
go run .
```

The PowerShell client can execute the complete checked-in demonstration:

```powershell
./scripts/demo-api-chain.ps1 -BaseURL $base -DemoAPI http://127.0.0.1:9081
```

It publishes the reusable HTTP child and its parent, creates an event, waits for completion, and checks fixture counters before and after redelivery. It verifies one customer call and one notification call for that event.

For Bash, publish the same definitions and send an event:

```bash
for spec in workflows/reusable-http.json workflows/customer-events.json; do
  curl --fail-with-body "$base/v1/workflows" \
    -H "Authorization: Bearer $KAIROS_ADMIN_TOKEN" \
    -H 'Content-Type: application/json' --data-binary "@$spec"
done
event_id="docs-$(openssl rand -hex 12)"
event=$(cat <<JSON
{
  "specversion": "1.0",
  "id": "$event_id",
  "source": "/kairos-demo",
  "type": "customer.requested",
  "data": {
    "name": "Ada",
    "notify": true,
    "api_base": "http://127.0.0.1:9081"
  }
}
JSON
)
delivery=$(curl --fail-with-body "$base/v1/events" \
  -H "Authorization: Bearer $KAIROS_API_TOKEN" \
  -H 'Content-Type: application/cloudevents+json' --data-binary "$event")
run_id=$(printf '%s' "$delivery" | python3 -c 'import json, sys; print(json.load(sys.stdin)["runs"][0]["id"])')
for attempt in $(seq 1 60); do
  result=$(curl --fail-with-body "$base/v1/runs/$run_id" \
    -H "Authorization: Bearer $KAIROS_API_TOKEN")
  state=$(printf '%s' "$result" | python3 -c 'import json, sys; print(json.load(sys.stdin)["state"])')
  case "$state" in
    queued|running) sleep 1 ;;
    *) break ;;
  esac
done
printf '%s\n' "$result"
test "$state" = succeeded
duplicate=$(curl --fail-with-body "$base/v1/events" \
  -H "Authorization: Bearer $KAIROS_API_TOKEN" \
  -H 'Content-Type: application/cloudevents+json' --data-binary "$event")
python3 -c 'import json, sys
receipt = json.loads(sys.argv[1])
assert receipt["duplicate"] is True
assert receipt["runs"][0]["id"] == sys.argv[2]
print("Duplicate event retained run " + sys.argv[2])' "$duplicate" "$run_id"
```

The receipt output includes the returned customer ID, `notified: true`, and the root run ID. No commercial API is involved. Setting `notify` to false under a new event ID skips notification and returns `notified: false`. Changing the body under the old event ID returns 409.

This fixture does not implement provider idempotency. Event deduplication prevents a second admission, but it cannot make resubmission after an uncertain external effect safe.

## File relay

Keep both services and the localhost origin settings from the customer example.

PowerShell:

```powershell
./scripts/demo-file-chain.ps1 -BaseURL $base -DemoAPI http://127.0.0.1:9081
```

The script generates 196,613 random bytes, checks upload deduplication, runs a raw-body and multipart chain, downloads the result, compares SHA-256 digests, and verifies a database snapshot download. Its evidence stays under ignored `bin/verification`.

The following Bash example uses this README's bytes. It creates no document parser or format conversion:

```bash
curl --fail-with-body "$base/v1/workflows" \
  -H "Authorization: Bearer $KAIROS_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  --data-binary @workflows/file-relay.json
source_file=README.md
source_hash=$(openssl dgst -sha256 -r "$source_file" | awk '{print $1}')
upload_key=$(openssl rand -hex 16)
upload=$(curl --fail-with-body "$base/v1/files" \
  -H "Authorization: Bearer $KAIROS_API_TOKEN" \
  -H 'Content-Type: application/octet-stream' \
  -H 'X-File-Name: README.md' \
  -H "X-Content-SHA256: $source_hash" \
  -H "Idempotency-Key: $upload_key" \
  --data-binary "@$source_file")
file_id=$(printf '%s' "$upload" | python3 -c 'import json, sys; print(json.load(sys.stdin)["file"]["id"])')
request=$(printf '{"workflow_id":"file_relay","version":1,"input":{"api_base":"http://127.0.0.1:9081","file":{"$artifact":"%s"}}}' "$file_id")
run_key=$(openssl rand -hex 16)
run=$(curl --fail-with-body "$base/v1/runs" \
  -H "Authorization: Bearer $KAIROS_API_TOKEN" \
  -H "Idempotency-Key: $run_key" \
  -H 'Content-Type: application/json' --data-binary "$request")
output_id=$(printf '%s' "$run" | python3 -c 'import json, sys; print(json.load(sys.stdin)["output"]["file"]["$artifact"])')
mkdir -p bin/verification
output_file="bin/verification/$output_id.bin"
curl --fail-with-body "$base/v1/files/$output_id/content" \
  -H "Authorization: Bearer $KAIROS_API_TOKEN" --output "$output_file"
output_hash=$(openssl dgst -sha256 -r "$output_file" | awk '{print $1}')
test "$source_hash" = "$output_hash"
printf 'Verified file: %s\nSHA-256: %s\n' "$output_id" "$output_hash"
```

The local fixture caps requests at 1 MiB even though Kairos's default file limit is 32 MiB. Use a small file for this demonstration. The file guide explains [staged visibility](../files-and-atomicity.md#file-steps-and-visibility) and [reference retention](../files-and-atomicity.md#limits-and-retention).

For a snapshot you can restore, follow the [backup and restore commands](../operations.md#download-and-verify-a-backup). A file transfer check and a snapshot digest check are separate from an actual restoration test.

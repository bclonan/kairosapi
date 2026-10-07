# Operations guide

[Documentation index](README.md)

The supported deployment is one Kairos process with one database on a local persistent filesystem. All callers share the instance's runner or admin role. Use separate instances for separate trust groups.

The [configuration reference](configuration.md) lists defaults and limits. The [verification record](verification.md) distinguishes native tests from container and hosted deployment work that remains unverified.

## Build and run

Build from the repository root:

```sh
go mod download
go build -trimpath -o bin/kairos .
```

On Windows, build `bin/kairos.exe` instead. The executable needs the seed JSON file at its configured path. It does not embed that file, read `.env`, or accept command-line configuration flags.

Supply at least `KAIROS_API_TOKEN`. Use a separate `KAIROS_ADMIN_TOKEN` for publication and backups. Set absolute paths for `KAIROS_DB_PATH` and `KAIROS_WORKFLOWS_FILE` in a service manager so its working directory cannot change their meaning.

Run under a dedicated operating-system account. Give it write access to the database directory and its temporary spool directory. The spool directory is named `<database-path>.uploads` beside the database. Keep the seed file readable.

The database and snapshots contain workflow input, provider output, and files in plaintext. Protect their directory permissions and use host or volume encryption where required. Environment-backed provider secrets are not intentionally copied into the database, but a workflow can retain a secret sent through its ordinary input or output.

## Container deployment

The checked-in [Dockerfile](../Dockerfile) builds a static executable and packages the welcome seed with CA certificates. It runs as a non-root user. [Compose](../compose.yaml) supplies a persistent `/data` volume and a read-only root filesystem.

After supplying token values in the host environment:

```sh
docker compose config --quiet
docker compose up --build -d
docker compose logs -f kairos
```

Use `--quiet` for configuration validation so the rendered configuration does not print credentials. The host listener binds to `127.0.0.1:8075`. Compose does not publish it to every host interface.

Only the runner token, admin token, and allowed origins currently use Compose interpolation. Other limits are explicit values in the YAML. Add provider secrets and configuration changes through a reviewed override or deployment secret facility. A host environment variable is not forwarded merely because it starts with `KAIROS_`.

The local fixture uses the host's loopback interface. A container's `127.0.0.1` refers to the container itself, so the native demo's origin will not reach a fixture on the host. Run the documented demonstrations natively, or configure a reachable fixture address and exact origin for your container environment.

The container healthcheck calls `/readyz`. It does not restart an unhealthy container by itself. Arrange external supervision for a persistent readiness failure. Compose's `restart: unless-stopped` handles process exit.

Keep the named data volume during ordinary replacement. Do not use `docker compose down --volumes` as a restart command. A local volume is not a backup. Image build, container execution, healthcheck behavior, and resource limits still need deployment verification.

## Ingress and outbound requests

Kairos serves HTTP. Put it behind a TLS gateway before exposing it beyond the local host. Set request-body limits and timeouts that accommodate the configured upload limit and file transfer deadlines. Forward the bearer header and disable caching. The service sets `Cache-Control: no-store` and does not provide browser CORS configuration.

There is no login service, token issuance endpoint, per-user audit identity, or credential scoping by workflow. The admin credential can download the whole database. Restrict its distribution.

Outbound HTTP is disabled until origins are configured. Prefer exact HTTPS origins for known providers. Public wildcard mode still applies connection-time address checks. The HTTP action neither uses proxy environment variables nor follows redirects.

Use deployment network restrictions as a second boundary when needed. Removing an origin from configuration can stop a retained fixed-target definition from compiling at startup. Secret rotation also requires a restart. See [upgrades and policy changes](#upgrades-and-policy-changes).

## Startup, shutdown, and queue behavior

Startup opens the database exclusively, checks its structure and file integrity, loads and compiles the catalog, recovers old active runs as `interrupted`, and starts queue workers. It performs full stored-file digest checks, so startup work grows with retained bytes.

The seed file must exist even when the database already has a catalog. Use a file containing `[]` if no new seeds are needed. A seed that conflicts with an existing immutable version prevents startup.

Queued work can execute as soon as the service opens. There is no startup pause, maintenance-only mode, or API to hold the queue. Do not start a restored copy beside the original with the same external destinations enabled.

On Ctrl+C or SIGTERM, the HTTP server stops accepting work and allows up to 15 seconds for HTTP shutdown. Service closure then cancels active execution and records interrupted outcomes. Queued runs remain for restart. The built-in actions honor cancellation; custom handlers that ignore their context can delay process shutdown.

For a planned upgrade, stop new submissions at the gateway and allow known active work to finish before signaling the process. If a run does not finish, record its ID and review its external effects after restart. Cancellation and interruption never reverse an accepted provider request.

## Health, logs, and capacity

`GET /healthz` reports that the HTTP process is responding. `GET /readyz` reports whether the orchestrator is available. Neither endpoint checks provider credentials, provider health, free disk space, or future write success.

The process emits JSON logs to standard output. Startup, shutdown, admissions, completion, and storage failures supply operational evidence, but Kairos has no metrics endpoint, trace exporter, or filtered run-search API yet. Use `GET /v1/runs` and individual run details with a runner token to inspect states.

Watch the process exit state, readiness, error logs, request status rates, local disk space, database size, and transfer failures. Use the returned run ID when correlating a caller report with a stored outcome. Avoid logging bearer tokens or complete workflow bodies at the gateway.

| Signal | Interpretation and next action |
| --- | --- |
| 429 on admission | Active-run and queue capacity is occupied. Retry the same logical submission with the same key after `Retry-After`. |
| 429 on file routes | All transfer slots are occupied. Slow transfers and backups share those slots. |
| 507 on admission | Retained runs or receipts have reached their configured bound. Review retention and capacity together. |
| 507 on upload | File count or byte quota is exhausted. Delete only unreferenced files or increase measured capacity. |
| Readiness 503 | The service is closing or has detected a storage failure. Check logs and the storage device before restarting. |
| Increasing interrupted runs | Inspect restarts, shutdown timeouts, and storage errors. Reconcile provider effects before resubmission. |

Pruning runs frees reusable database pages without shrinking the database file. It runs during successful new admission, not on a timer. Files have no TTL. Deleting a run through retention releases its file pins but does not delete the published files themselves.

Reserve disk space beyond the payload quota for database overhead, retained history, transfer spools, and snapshot files. A backup creates a local temporary copy before streaming it. Quotas do not measure all of that space.

## Download and verify a backup

`GET /v1/admin/backup` creates a consistent database snapshot. Prefer it over copying a live database file. It includes queued work, catalog versions, runs, receipts, files, indexes, and persistent ULID state.

These commands store a local development backup under ignored `bin/backups`. For deployment, use a directory with restricted permissions and a separate backup destination. Keep the response digest alongside the snapshot.

### PowerShell

The example uses the client variables from the README:

```powershell
$backupDir = Join-Path $PWD 'bin/backups'
New-Item -ItemType Directory -Path $backupDir -Force | Out-Null
$backupPath = Join-Path $backupDir ("kairos-{0}.db" -f [guid]::NewGuid().ToString('N'))
$response = Invoke-WebRequest "$base/v1/admin/backup" -Headers $admin -OutFile $backupPath -PassThru
$expected = [string]($response.Headers['X-Content-SHA256'] | Select-Object -First 1)
$actual = (Get-FileHash -LiteralPath $backupPath -Algorithm SHA256).Hash.ToLowerInvariant()
if (-not $expected -or $actual -cne $expected) {
    throw 'Backup digest verification failed'
}
Set-Content -LiteralPath "$backupPath.sha256" -Value $actual -Encoding ascii
$backupPath
```

### Bash

This example uses curl and OpenSSL:

```bash
mkdir -p bin/backups
backup="bin/backups/kairos-$(openssl rand -hex 12).db"
headers="$backup.headers"
curl --fail-with-body "$base/v1/admin/backup" \
  -H "Authorization: Bearer $KAIROS_ADMIN_TOKEN" \
  -D "$headers" --output "$backup"
expected=$(awk 'tolower($1) == "x-content-sha256:" {gsub("\r", "", $2); digest=$2} END {print digest}' "$headers")
actual=$(openssl dgst -sha256 -r "$backup" | awk '{print $1}')
if [ -z "$expected" ] || [ "$actual" != "$expected" ]; then
  echo 'Backup digest verification failed' >&2
  exit 1
fi
printf '%s\n' "$actual" > "$backup.sha256"
printf 'Verified backup: %s\n' "$backup"
```

Transport integrity proves the downloaded bytes match the snapshot. It does not prove that an external provider's state matches the snapshot. Retain configuration, the matching executable or image, and secret references separately.

## Restore into a new path

Stop the target service and keep its current database unchanged. For replacement recovery, also keep the original instance stopped. Restoring resets state to the snapshot, including deduplication receipts and sequence allocation.

With the verified PowerShell `$backupPath` from above:

```powershell
$restorePath = Join-Path $PWD ("bin/restored-{0}.db" -f [guid]::NewGuid().ToString('N'))
if (Test-Path -LiteralPath $restorePath) { throw 'Restore path already exists' }
Copy-Item -LiteralPath $backupPath -Destination $restorePath
$env:KAIROS_DB_PATH = $restorePath
# Supply the same seed, action registry, origins, and required secret names.
.\bin\kairos.exe
```

In Bash, with the verified `$backup` from above:

```bash
restore="$PWD/bin/restored-$(openssl rand -hex 12).db"
if [ -e "$restore" ]; then
  echo 'Restore path already exists' >&2
  exit 1
fi
cp "$backup" "$restore"
export KAIROS_DB_PATH="$restore"
# Supply the same seed, action registry, origins, and required secret names.
./bin/kairos
```

Startup validates the snapshot and recovers active records as interrupted. Check readiness, expected catalog versions, representative completed runs, and file digests. Queued records may start immediately. If inspecting the restore must not cause provider effects, block outbound destinations at the deployment network layer before startup. This can make queued work fail, so it is not a read-only inspection mode.

Never edit bbolt records by hand to reset a run. For an interrupted effect, inspect provider records using its business identity or idempotency key. Record the reconciliation decision outside Kairos, which has no recovery-decision API yet. Submitting a new run creates new operation keys.

A restored snapshot cannot undo provider effects that happened after the snapshot. It can also lack newer deduplication receipts. Account for those effects before reopening intake.

## Upgrades and policy changes

Before replacement, stop new intake, record unfinished run IDs, and download a verified backup. Preserve the current executable or image and its configuration. Validate the new binary with the same registry, seed definitions, secrets, and origin policy against a copy in an isolated environment.

The current database format is 2. Startup migrates format 1 in one transaction, preserves legacy public run IDs, and adds ordered indexes and file storage. It does not rewrite the bytes of completed legacy result bodies. Do not assume an older executable can read a newer database. Rollback may require the pre-upgrade binary and snapshot together.

Every retained definition must compile. Publishing a replacement version does not remove the old version. Removing an action, a required secret, or a fixed destination can therefore stop startup even when no new requests use that old version.

There is no catalog deletion or migration endpoint. Plan policy changes with retained definitions in mind. If a destination must be revoked immediately, block it at the deployment network layer and test a compatible catalog transition before changing startup policy. Do not broaden outbound access to get a failing catalog to load.

Use one running process during replacement. There is no rolling upgrade across replicas sharing the same file, distributed lease protocol, or worker failover.

## Release checks still required

Local unit tests and native demonstrations have passed as recorded in [verification](verification.md). A deployment release still needs a built and scanned image, verified volume ownership, container start and stop tests, gateway configuration, measured resource limits, a restore in a separate environment, and provider outcomes checked independently.

Exercise disk exhaustion, cancellation during transfer, interrupted runs, a nonempty queue at restart, and credential rotation in that deployment. The [roadmap](product-roadmap.md) defines later work on durable replay, tenant permissions, metrics, and operator recovery.

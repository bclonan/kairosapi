# Configuration reference

[Documentation index](README.md) · [Quick start](../README.md#quick-start)

The process reads its environment once at startup through [internal/config/config.go](../internal/config/config.go). It does not load `.env`. Use [.env.example](../.env.example) as a list of settings and supply values through your shell or deployment manager.

Relative paths resolve against the process working directory. Restart after changing settings or environment-backed secrets. There is no live configuration reload.

## Server and credentials

| Variable | Default | Rule |
| --- | --- | --- |
| `KAIROS_API_TOKEN` | Required | At least 32 bytes, without surrounding whitespace |
| `KAIROS_ADMIN_TOKEN` | Empty | When set, at least 32 bytes and different from the runner token |
| `KAIROS_ADDR` | `127.0.0.1:8075` | A `host:port` listen address |
| `KAIROS_DB_PATH` | `data/kairos.db` | One local database path owned by this process |
| `KAIROS_WORKFLOWS_FILE` | `workflows/example.json` | Path to a JSON array of seed definitions, at most 1 MiB |
| `KAIROS_SECRET_*` | Unset | Operator-defined complete header values referenced by fixed-target HTTP actions |

Generate 32 random bytes and encode them as 64 hexadecimal characters for each token, as shown in the [quick start](../README.md#quick-start). The runner can execute published workflows, deliver events, inspect all runs, upload files, download files, and cancel runs. The admin can also publish definitions, read full definitions, execute inline specifications, delete unreferenced files, and download the entire database.

An empty admin token disables those admin operations. It does not disable runner access. There are no per-user keys, workflow permissions, or tenant-specific credentials.

The seed file must exist even when the database already contains definitions. Use `[]` for an empty seed catalog. A stored ID/version and its seed must have identical normalized content. Removing a seed does not remove its stored version. The catalog permits 128 retained versions in total.

## Execution and history

| Variable | Default | Accepted value |
| --- | --- | --- |
| `KAIROS_WORKERS` | `8` | 1 through 128 active leaf actions |
| `KAIROS_MAX_RUNS` | `16` | 1 through 128 active root runs |
| `KAIROS_QUEUE_SIZE` | `128` | 0 through 4,096 additional queued runs |
| `KAIROS_MAX_RECORDS` | `1000` | Up to 100,000 runs and 100,000 receipts, counted separately |
| `KAIROS_RETENTION` | `168h` | Go duration from `1m` through `8760h` |
| `KAIROS_RUN_TIMEOUT` | `60s` | Go duration from `1s` through `5m` |

`KAIROS_MAX_RECORDS` must be at least `KAIROS_MAX_RUNS + KAIROS_QUEUE_SIZE`. Setting queue size to zero still permits work within active-run capacity. The scheduler owns a durable queue even when a caller requests synchronous waiting.

The worker limit applies across the process. A parent awaiting a child workflow holds no leaf worker slot. A delay action occupies a leaf slot; retry backoff does not. These settings do not cap HTTP client connections to Kairos. Apply connection limits at the gateway.

Definitions and steps can set shorter `timeout_seconds` values. An omitted or zero value inherits the applicable limit. A step's deadline includes waiting for an action slot and retry backoff. The root run deadline begins when execution starts, so time spent queued is outside that deadline.

Terminal-run retention starts at completion. A receipt remains while an associated run is active or still retained. Pruning occurs during a successful new admission transaction; there is no background retention sweep. Capacity can return 507 when no records qualify for pruning.

## Files

| Variable | Default | Accepted value |
| --- | --- | --- |
| `KAIROS_FILE_MAX_BYTES` | `33554432` | 1 byte through 268,435,456 bytes, or 256 MiB |
| `KAIROS_FILE_TOTAL_BYTES` | `268435456` | At least the per-file limit, up to 1,099,511,627,776 bytes, or 1 TiB |
| `KAIROS_FILE_MAX_COUNT` | `1000` | 1 through 100,000 files |
| `KAIROS_FILE_TRANSFERS` | `2` | 1 through 16 simultaneous file operations |

These settings take integer byte counts, not values such as `32MB`. Staged and published files both consume quota. Uploads, downloads, multipart assembly, and backups share transfer slots.

Multipart requests accept at most 16 files. Combined file bytes and form-field values must fit the per-file limit. Form values also have a 256 KiB aggregate bound.

Payload quotas exclude database overhead, retained run history, temporary spool files, and backup copies. Large file transactions also consume memory. The Compose memory limit does not increase when you raise file limits.

File records have no TTL. Admin deletion succeeds only when no retained run or published definition references the file. See [files and atomicity](files-and-atomicity.md) for publication, deletion, and backup behavior.

## Outbound network policy

| Variable | Default | Accepted value |
| --- | --- | --- |
| `KAIROS_HTTP_ALLOWED_ORIGINS` | Empty | Comma-separated exact origins, or `*` for public HTTPS destinations |
| `KAIROS_ALLOW_PRIVATE_NETWORKS` | `false` | Exactly `true` or `false` |

An empty origin list disables outbound HTTP actions. Origins include scheme, host, and explicit port. Do not include endpoint paths, query strings, credentials, or fragments. `https://partner.example` and `https://partner.example:443` are separate entries.

For fixed provider destinations:

```powershell
$env:KAIROS_HTTP_ALLOWED_ORIGINS = 'https://api.partner.example,https://files.partner.example'
$env:KAIROS_ALLOW_PRIVATE_NETWORKS = 'false'
```

Those example domains are placeholders. Replace them with the origins required by your workflow.

For the local fixture:

```powershell
$env:KAIROS_HTTP_ALLOWED_ORIGINS = 'http://127.0.0.1:9081'
$env:KAIROS_ALLOW_PRIVATE_NETWORKS = 'true'
```

`*` cannot be combined with private-network access. The transport checks resolved addresses when connecting and rejects restricted networks in public mode. It does not use proxy environment variables or follow redirects.

Fixed-target URLs are checked when a definition compiles. Input-supplied URLs are checked at execution. A policy change can therefore prevent startup if a stored fixed-target definition is no longer permitted. See [upgrades and policy changes](operations.md#upgrades-and-policy-changes).

## Provider secret headers

A fixed-target HTTP action can reference a server environment variable:

```json
{
  "url": "https://api.partner.example/messages",
  "method": "POST",
  "secret_headers": {
    "Authorization": "KAIROS_SECRET_PARTNER_AUTH"
  }
}
```

The variable name must match `KAIROS_SECRET_[A-Z0-9_]+`. Its value is the complete header, such as `Bearer ` followed by the provider token. The factory requires the value to exist and be nonempty when compiling the definition. Publishing fails when the secret is missing, and startup fails if an existing definition cannot compile.

Compiled handlers retain their header values. Restart after rotation so every retained definition uses the new value. `url_from_input` cannot forward server-held `secret_headers`. Fixed credential headers belong in `secret_headers`, not in the literal `headers` configuration.

Input fields and successful provider outputs are retained without automatic secret redaction. Passing credentials in ordinary workflow input can therefore store them in the database and backups.

## Fixed protocol and graph limits

These limits come from the implementation and are not environment settings.

| Limit | Value |
| --- | --- |
| Structured API request body | 1 MiB |
| Serialized definition | 1 MiB |
| Input schema | 32 KiB |
| Workflow input, action input, action output, automatic HTTP response | 256 KiB each, subject to copy-budget checks |
| Accumulated run result | 4 MiB |
| Direct steps per definition | 1 through 32 |
| Expanded steps across subflows | 128, including each subflow wrapper |
| Definition nesting | 8 levels, including the root |
| Event triggers per definition | 16 |
| Retry attempts | 5 total, with an explicit idempotence declaration above one |
| Delay action | 1 through 60,000 milliseconds |
| JSON document depth | 64 |
| Template and copied data depth | 32 |
| Idempotency key | 256 bytes |
| HTTP action URL | 8,192 bytes |
| Configurable input or captured response header names | 32 of each |

JSON numbers retain their decimal representation. Numeric text is bounded to 1,024 characters and exponents to absolute value 1,024. Use file references for binary or larger data.

## Transport deadlines

| Boundary | Limit |
| --- | --- |
| Incoming request headers | 5 seconds |
| Ordinary request read / response write | 10 / 45 seconds |
| Incoming idle connection | 60 seconds |
| Incoming header budget | 16 KiB |
| Synchronous run wait | 30 seconds, then a stored nonterminal run returns 202 |
| File-route read / write deadlines | 1 / 2 minutes |
| Outbound dial / TLS handshake | 10 / 10 seconds |
| Outbound response headers | 15 seconds |
| Outbound response header budget | 32 KiB |
| HTTP shutdown grace | 15 seconds |

Step and run contexts can impose shorter outbound deadlines. A provider that accepts a request but returns headers too late can leave its effect uncertain.

## Docker Compose settings

Docker Compose reads its own environment and optional `.env` for interpolation. This differs from the Go executable, which reads only the process environment.

The checked-in Compose file interpolates the runner token, admin token, and allowed origins. Other values, including limits and private-network access, are set explicitly in [compose.yaml](../compose.yaml). Set them in that file or an override. An arbitrary host `KAIROS_*` variable is not automatically forwarded.

Compose sets `GOMEMLIMIT=384MiB` and a 512 MiB container memory limit. `GOMEMLIMIT` is a Go runtime setting, not a retained-file quota. The service also has a one-CPU limit and a 128-process limit. Validate actual load before choosing different values.

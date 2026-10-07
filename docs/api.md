# HTTP API contract

[Documentation index](README.md) · [Runnable client examples](examples/README.md)

Kairos has two shared bearer credentials. `KAIROS_API_TOKEN` can execute published workflows, deliver events, inspect and cancel all runs, and upload and read files. The separate `KAIROS_ADMIN_TOKEN` also permits publishing, reading full definitions, submitting inline specifications, deleting unreferenced files, and downloading backups. Admin credentials can call runner endpoints too. This is a single trust group, with no per-user or tenant authorization.

Send `Authorization: Bearer YOUR_TOKEN`. Structured requests use `application/json`. Event intake also accepts `application/cloudevents+json`. File upload bodies contain raw bytes with their own media type. The service publishes no browser CORS policy.

| Endpoint | Role | Behavior |
| --- | --- | --- |
| `GET /healthz` | Public | HTTP process is responding |
| `GET /readyz` | Public | Catalog loaded and orchestrator has not stopped or recorded a storage failure |
| `GET /v1/workflows` | Runner | Retained version metadata, publication ULID, and latest-version flag |
| `GET /v1/workflows/{id}` | Admin | Full latest definition |
| `GET /v1/workflows/{id}/versions/{version}` | Admin | Full pinned definition |
| `POST /v1/workflows` | Admin | Validate and persist an immutable version |
| `POST /v1/runs` | Runner or admin | Queue a published workflow; inline definitions require admin |
| `GET /v1/runs/{id}` | Runner | Current state, steps, attempts, child results, and declared output |
| `GET /v1/runs?limit=25&before=ULID` | Runner | Indexed newest-first run metadata; limit 1 to 100; exclusive optional cursor |
| `POST /v1/runs/{id}/cancel` | Runner | Request cancellation, or return an already completed run |
| `POST /v1/events` | Runner | Deduplicate an event and atomically admit its matching workflows |
| `POST /v1/files` | Runner | Store raw bytes and metadata atomically; optional upload idempotency key |
| `GET /v1/files?limit=25&before=ULID` | Runner | Newest-first published file metadata with an optional exclusive cursor |
| `GET /v1/files/{id}` | Runner | Published file metadata |
| `GET /v1/files/{id}/content` | Runner | Verified original bytes as an attachment; supports byte ranges |
| `DELETE /v1/files/{id}` | Admin | Delete an unreferenced published file and its upload receipt |
| `GET /v1/admin/backup` | Admin | Consistent database snapshot with a SHA-256 response header |

Readiness does not probe provider availability or guarantee future disk writes. Run listing reads a ULID index and bounded summaries. Both lists return `next_before` when a full page was read. Use it for the next request; an empty next page is possible. File listing omits staged workflow outputs. See [file upload headers, limits, and recovery](files-and-atomicity.md).

## Response shapes and headers

The default local base URL is `http://127.0.0.1:8075`. All structured responses use `application/json`. File content and backup downloads return binary bodies.

| Request | Response body |
| --- | --- |
| Health / readiness | `{"status":"ok"}` or `{"status":"ready"}`; unavailable readiness is `{"status":"unavailable"}` |
| List workflows | `{"workflows":[...]}` with `id`, `version`, optional `description`, `latest`, and `publication_id` per entry |
| Read definition | The definition object itself |
| Publish definition | `{"id":"greeting","version":1,"created":true}` for a new version |
| Submit, retrieve, or cancel a run | The run object itself |
| List runs | `{"runs":[...],"next_before":"..."}` with metadata summaries |
| Deliver event | `{"event_id":"...","receipt_id":"...","runs":[...],"duplicate":false}` |
| Upload file | `{"file":{...},"reference":{...},"duplicate":false}` |
| List files | `{"files":[...],"next_before":"..."}` |
| Read file metadata | The metadata object itself |
| Delete file | No body on 204 |

The workflow catalog sorts by ID ascending and version descending. It has no cursor because the total retained catalog is bounded. Run lists omit outputs and include `steps: null` in their summaries. Retrieve an individual run for step history. Run and file lists sort newest first and end with `next_before: ""` when fewer than the requested count remain. Neither list is a fixed snapshot while new records arrive.

| Header | When to use it |
| --- | --- |
| `Location` | Publication, run submission, and file upload identify their stored resource |
| `Idempotency-Replayed: true` | A direct submission or file upload returned an existing receipt |
| `Retry-After: 1` | A 429 response asks the client to back off |
| `WWW-Authenticate: Bearer` | A 401 response needs a valid credential |
| `X-Content-SHA256` | Verify the downloaded file or database snapshot |
| `ETag` | File content uses the quoted stored digest |
| `X-File-Content-Encoding` | Describes the original file encoding without instructing a client to decode it |

The service sets `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`. File content supports byte-range requests and ordinary HTTP conditional retrieval. Verify complete downloaded bytes against the digest; a partial range will not hash to the whole-file digest.

## Publish

The request body is one workflow definition, not an array. See the [specification guide](event-specifications.md).

A new version returns 201. Identical content returns 200. Different content for an existing ID and version returns 409. JSON object-key order does not affect identity. Omitted `spec_version` normalizes to `"1.0"`. Definitions must reference already published subflow versions. Versions cannot be edited or deleted through this API.

At startup, `KAIROS_WORKFLOWS_FILE` provides a JSON array of seed definitions. Use `[]` for an empty seed catalog. Stored versions and seed versions must agree. Startup can compile seed dependencies regardless of array order. Removing a seed from the file does not delete its stored version.

## Submit a run

```json
{
  "workflow_id": "welcome",
  "version": 1,
  "input": {"name": "Ada"},
  "async": true
}
```

Omitting `version` selects the latest version at admission and stores the resolved version. `input` can be omitted or null and behaves as an empty object. `async` defaults to false for compatibility with the welcome example.

| Field | Rule |
| --- | --- |
| `workflow_id` | Required for a published run; omit for an inline specification |
| `version` | Positive pinned version, or omitted/zero for latest; omit for an inline specification |
| `specification` | Optional complete definition instead of a catalog ID; admin only |
| `input` | JSON object, defaulting to empty; cannot be an array or scalar |
| `async` | Boolean; false waits for completion, true returns after admission |

An admin can replace `workflow_id` and `version` with `specification` containing a complete definition. An inline definition executes without joining the published catalog or registering event triggers. Its subflows must still reference published versions.

An asynchronous request returns 202 and a stored run snapshot. `Location` identifies `/v1/runs/{id}`. A synchronous request waits at most 30 seconds, then returns either a terminal result or 202. Inspect the stored run after a client timeout. Disconnecting the request does not cancel work.

Set `Idempotency-Key` to a stable string of at most 256 bytes to deduplicate direct submissions. The key is scoped to this instance. Repeating the same request returns the same run and adds `Idempotency-Replayed: true`. The choice to wait does not affect identity. Different content under the same key returns 409. Omitting a key creates a new run every time.

If an original request omitted its version, retry with the same omitted version and key. The receipt returns the original run even after a newer definition is published. Changing the request to an explicit version changes its content hash.

## Deliver an event

```json
{
  "specversion": "1.0",
  "id": "customer-request-482",
  "source": "/kairos-demo",
  "type": "customer.requested",
  "subject": "customer/482",
  "data": {
    "name": "Ada",
    "notify": true,
    "api_base": "http://127.0.0.1:9081"
  }
}
```

The structured envelope follows the JSON form of [CloudEvents 1.0](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/formats/json-format.md). `id`, `source`, and `type` are required nonempty strings of at most 1,024 bytes each. `source` must parse as a URI reference. `specversion` must be `"1.0"`. Optional `subject`, `time`, `datacontenttype`, and `dataschema` must be strings of at most 1,024 bytes. A supplied `time` must use RFC3339. Binary `data_base64` is unsupported. JSON `data` can be an object, array, scalar, or null. The complete event has a 256 KiB data budget in addition to the transport body limit.

Kairos matches triggers on the latest published version of each workflow. One workflow starts at most once per event, even if multiple triggers match. Each run receives `{"event": completeEnvelope, "data": envelope.data}` as its input. Include this wrapper in its input schema. The duplicated data counts toward the input size limit.

A successful intake returns 202 with `event_id`, a Kairos `receipt_id` ULID, `runs`, and `duplicate: false`. No matches produces an empty run list and still records the event receipt. Publishing a matching workflow later does not replay an already accepted event.

The deduplication identity is the pair of `source` and `id`. An identical repeat returns 200, the original run IDs, and `duplicate: true`. Different content with the same identity returns 409. Fanout admission and its receipt commit together. Capacity exhaustion or one invalid workflow input rejects the entire fanout, with no accepted subset.

This endpoint uses Kairos bearer authentication. It does not verify Stripe, Slack, GitHub, or other provider webhook signatures. Put a verifying adapter in front of it before receiving those webhooks. There is no ordering guarantee between distinct events and no correlation of later events with an existing run.

## Results and failure states

Run snapshots contain `id`, `sequence_id`, `workflow_id`, `version`, `state`, `created_at`, and `steps`. New runs use the same ULID for `id` and `sequence_id`. Migrated runs retain their old public IDs and receive a sortable `sequence_id`. Execution timestamps appear after the corresponding transition. A successful workflow with declared `output` includes that result. An accepted cancellation can include `cancellation_requested: true` before a running handler exits. Stored submission input is not a separate field in the public run response, though steps may copy it into their outputs.

Step results stay in definition order. They include state, attempt count, timestamps, output, and an error when applicable. A subflow step includes its completed child step results under `children` and its declared output under `output`. Child results are attached on completion; they are not durable checkpoints for replay.

Run states are `queued`, `running`, `succeeded`, `failed`, `timed_out`, `canceled`, and `interrupted`. Steps also use `pending` and `skipped`. A false condition skips a step. Downstream steps may still run and use reference defaults.

Queued cancellation prevents execution. Running cancellation is cooperative and cannot retract a completed external operation. A step timeout fails its workflow; expiry of the overall deadline produces `timed_out`. Failure stops new scheduling and cancels active siblings.

Queued runs survive restart. Runs that were executing become `interrupted` and never restart automatically. Review provider records before deciding whether to submit new work. Kairos does not promise exactly-once external effects, distributed failover, rollback, or execution checkpoint replay.

| Status | Meaning |
| --- | --- |
| 200 | Successful or retrieved result, repeated publication, duplicate event |
| 201 / 204 | New published version or file; successful file deletion |
| 202 | Accepted and still queued or running, or new event receipt |
| 400 | Invalid JSON, duplicate keys, unknown fields, or invalid request structure |
| 401 / 403 | Missing credential or insufficient role |
| 404 | Unknown workflow, version, run, or publicly available file |
| 408 | Synchronous result is canceled |
| 409 | Immutable content conflict, idempotency conflict, referenced file deletion, or interrupted synchronous result |
| 413 / 415 | Structured request exceeds 1 MiB, file exceeds its configured limit, or structured content type is unsupported |
| 422 | Invalid definition, event, input schema, or data limits |
| 424 | Synchronous workflow failed |
| 429 | Run or file transfer capacity exhausted; `Retry-After: 1` |
| 503 | Orchestrator is stopping or storage has failed |
| 504 | Synchronous workflow deadline expired |
| 507 | Run history, receipt capacity, or file storage quota exhausted |

Retrieving a failed run returns HTTP 200 with its actual state. Error responses do not echo rejected upstream bodies. Successful outputs can contain sensitive provider data.

Ordinary application errors use `{"error":"message"}`. A terminal failure returned by `POST /v1/runs` instead contains the run object with its state and step evidence. Handle both shapes. The HTTP router can return ordinary 404 or 405 responses for unknown paths or unsupported methods, and file transfers can return standard range and conditional-response statuses.

There is no delete-run, replay-run, live progress stream, queue-pause, or definition-update endpoint. Use [polling examples](examples/README.md#async-submission-and-inspection) for active work. Preserve the original run ID when reconciling failures instead of creating a new operation automatically.

## Retention and recovery

`KAIROS_RETENTION` defaults to 168 hours and accepts 1 minute through 8,760 hours. Terminal runs age from completion. Receipts remain while any associated run is active or still within retention. Old records are pruned during successful admission.

A fanout receipt can outlive an earlier-finishing run's detailed history. Its duplicate response then includes that original ID with state `expired`. It does not execute that run again. Once the receipt itself expires, the same key or event identity can start new work.

`KAIROS_MAX_RECORDS` bounds runs and receipts independently. The default is 1,000 of each. When history is full and no records qualify for pruning, admission returns 507. Choose capacity and retention together, monitor disk usage, and keep backups. Pruning frees reusable database pages without shrinking the file.

One process holds an exclusive lock on the database. Network filesystems and multiple replicas sharing the file are outside the supported deployment model. A failed progress write cancels execution and makes readiness fail. Restart only after fixing storage and inspecting interrupted outcomes.

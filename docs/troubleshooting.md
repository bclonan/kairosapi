# Troubleshooting

[Documentation index](README.md)

Keep the HTTP status, response body, run ID, and server log near the failure. Do not start a second operation merely because the first caller timed out. Retrieve the original run or repeat its exact request with the original idempotency key.

## Server startup

| Symptom | Check |
| --- | --- |
| Token validation fails | The runner token needs at least 32 bytes without surrounding whitespace. A configured admin token must be distinct and meet the same rules. |
| Changing `.env` has no effect | The Go process does not load it. Set environment variables in the server process. Compose uses `.env` only for configured interpolation. |
| Cannot open the workflow file | Start from the repository root or set an absolute `KAIROS_WORKFLOWS_FILE`. The file must exist even with a populated database. |
| Seed decoding fails | The startup file is a JSON array. `POST /v1/workflows` accepts one object instead. Both reject unknown fields, duplicate keys, and trailing JSON. |
| Immutable version conflict | A seed and stored ID/version differ. Restore the original seed or publish a new version. Removing a seed does not delete stored content. |
| Missing subflow or missing child output | Publish dependencies before their parent, pin a positive child version, and declare child `output`. Seed dependencies can resolve across their array regardless of order. |
| Missing secret or rejected origin | Every retained definition compiles on startup. Restore the required configuration after reviewing the intended policy. A newer version does not remove an older requirement. |
| Database open times out | Another process may own the database, or its path may be inaccessible. Find the owner and stop it normally. Do not remove a database or lock file to force access. |
| Integrity, index, or sequence checks fail | Keep the database for diagnosis. Correct the storage problem and restore a verified snapshot to a new path. Do not delete internal records to bypass validation. |
| Startup takes longer with more files | Startup verifies every stored file digest. Measure retained bytes and storage speed; readiness is unavailable until initialization finishes. |

See [configuration](configuration.md) for exact limits and [operations](operations.md#restore-into-a-new-path) for restoration.

## Authentication and publication

401 means no valid instance credential reached the server. Confirm the client has the same token as the server, includes the `Bearer ` prefix, and reaches the intended instance. A second terminal does not inherit environment changes made in the first.

403 means the operation requires the admin credential. Publishing, full definition reads, inline specifications, file deletion, and backups require admin access. An empty admin token disables these operations.

409 during publication means that ID/version already holds different normalized content. Increment `version` and publish again. Object-key order is irrelevant, but description and specification content are part of the version. There is no update-in-place endpoint.

## Requests and run results

| Symptom | Meaning and response |
| --- | --- |
| 400 | Check strict JSON structure, field spelling, duplicate keys, and trailing values. |
| 415 | Send `application/json` for structured requests or `application/cloudevents+json` for events. File uploads have their own media type. |
| 413 | The structured body exceeds 1 MiB or a file exceeds its configured limit. Store large bytes as files and pass references. |
| 422 | Inspect definition validation, the input schema, event fields, references, and data limits. A smaller wire body can still exceed copied data limits. |
| 202 | The durable request is accepted. Poll the returned `Location` or run ID. |
| 424 from a synchronous run | The workflow failed. Read the returned run's error and failed step. Successful earlier effects can remain. |
| 504 from a synchronous run | The overall run deadline expired. Inspect its stored state and provider records. |
| 408 or 409 from a synchronous run | The stored result is canceled or interrupted. Retrieval still returns HTTP 200. |
| 429 | Wait for `Retry-After` and retry the same operation identity. See the capacity settings. |
| 507 | Retained history, receipt capacity, or file quota is full. Time alone does not trigger a retention sweep. |
| 503 or failed readiness | Check shutdown and storage errors. Resolve storage before restarting or admitting more work. |

PowerShell's `Invoke-RestMethod` and curl with `--fail-with-body` treat error statuses as command failures. A synchronous 424 still contains a run result. Retrieve `GET /v1/runs/{id}` to inspect it with a normal 200 response. Do not assume every non-2xx body has the generic `{"error":"..."}` shape.

If an accepted request's response is lost, retry with the same key and identical logical body. Changing `async` does not change direct-run identity; changing a version from omitted to explicit does. A different body under the same key returns 409.

## Workflow behavior

| Symptom | Check |
| --- | --- |
| Steps execute in an unexpected order | Array order does not create dependencies. Add `depends_on` to every required ordering relationship. |
| A reference to an earlier step is rejected | The referenced step must be a direct dependency, even if it is already an ancestor through another step. Root declared output can reference any step. |
| A join cannot read a skipped branch | Give the reference a literal `default`. Defaults apply to missing data, not present null values. |
| A reused workflow returns no useful data | A child must declare `output`. Its declared output becomes the parent step output; inspect completed nested step history under `children`. |
| A new child version has no effect | Parents pin child versions. Publish a new parent version that references the new child. |
| A workflow has no root `output` | Inspect step outputs, or publish a version with declared output. The bundled welcome workflow demonstrates the former. |
| An action is never retried | Only transient failures qualify. More than one attempt also requires `idempotent: true`. Whole-subflow retries are rejected. |
| The timeout occurs before a step dispatches | Its deadline includes waiting for an action worker and retry backoff. Root queue time is outside the execution deadline. |
| Cancellation takes too long | Cancellation is cooperative. Inspect blocking custom code and ensure every call honors its context. |
| Restart does not resume an active chain | This is the recovery contract. Previously active work becomes interrupted. Queued work can start; partial effects are not replayed. |

The engine has no arbitrary loop, array map, future-event wait, or exception branch. See the [workflow reference](event-specifications.md) before encoding one indirectly through dependencies or sleeps.

## HTTP actions

An empty origin list disables HTTP dispatch. In public mode, use HTTPS and permitted public destinations. Local HTTP fixtures need both an exact origin and `KAIROS_ALLOW_PRIVATE_NETWORKS=true`. Origin matching includes explicit ports. In a container, localhost points to the container.

Redirect responses fail. Configure the final intended URL within the allowed origin policy. Proxy environment variables do not change the transport's route.

Only 2xx responses succeed. If a provider reports failure inside a 200 JSON response, configure `expect_json` for a known success field or use a provider-specific action. A successful transport response does not establish business success.

Automatic response mode decodes JSON or returns text and caps the body at 256 KiB. For binary or larger responses, choose `response_mode: "file"`. It stores bytes unchanged and cannot use `expect_json`.

For credentials, fixed destinations can use `secret_headers` with a `KAIROS_SECRET_` environment name. Input-selected destinations cannot forward server-held secret headers. Restart after secret rotation because compiled handlers retain their values.

## Events and files

A new event returns 202 even when nothing matches. Inspect `runs` in the receipt. Trigger matching uses only the latest version of each workflow, and a newer version without triggers stops new event starts for that ID.

An identical source/ID pair returns the original receipt. Publishing a workflow later does not make that event run again. Use a new event ID only for a new intended operation. The service does not verify provider webhook signatures.

File upload bodies are raw bytes, not multipart envelopes. Multipart applies to an outgoing HTTP action's `body.fields` and `body.files`. Check `X-Content-SHA256` against the original file and preserve the same metadata when retrying an upload key.

A file response produced inside an active workflow is hidden from public reads until the final run transaction. Same-run dependent steps can read it. A different run cannot use it yet.

409 on deletion means a retained run or published definition pins the file. Run pruning can release a run pin. Published definitions have no deletion endpoint, so a literal file reference in a definition remains pinned. Files have no automatic TTL.

If a download fails integrity validation, preserve the database and restore a verified snapshot after diagnosing storage. Never relabel corrupted bytes with a new checksum.

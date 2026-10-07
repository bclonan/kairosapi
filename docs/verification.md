# Verification evidence

[Documentation index](README.md)

Verified October 7, 2026 on the local branch `codex/architecture-cleanup`. These results cover runtime specifications, reusable events, atomic file storage, ordered ULIDs, recovery, and the earlier cleanup. The historical baseline is commit `6726dbc93e6e2ae8d4a8c1bc32b2b248fb09cd80`. Changes remain uncommitted and have not been pushed or deployed to a hosted environment.

| Check | Environment | Result |
| --- | --- | --- |
| Historical `go test ./...` | Original Windows checkout | Failed before compilation because the original repository had no Go module |
| `go test -count=1 -coverprofile=coverage.out ./...` | Windows amd64, Go 1.26.8 | Passed, 55 top-level tests plus subtests across seven internal packages |
| `go vet ./...` | Windows amd64, Go 1.26.8 | Passed |
| `go test -race -count=1 ./...` | Ubuntu under WSL, Linux amd64, Go 1.26.8, GCC and CGO enabled | Passed |
| `go build -trimpath -o bin/kairos.exe .` | Windows amd64 | Passed |
| `CGO_ENABLED=0 go build -trimpath -o bin/kairos-linux .` | Ubuntu under WSL | Passed |
| `go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...` | Windows amd64 | No vulnerabilities found after updating the pinned indirect text and system packages |
| `scripts/smoke.ps1` | Native Windows Kairos process on localhost | Health, catalog, three-step workflow, and unauthenticated 401 checks passed |
| `scripts/demo-api-chain.ps1` | Native Kairos and separate native localhost demo API | Runtime publication, nested HTTP requests, event delivery, result inspection, and duplicate suppression passed |
| `scripts/demo-file-chain.ps1` | Native Kairos and separate native localhost demo API | Raw upload, upload deduplication, file response, multipart transfer, digest comparison, ordered ULIDs, and online backup passed |
| Native backup restoration | A separate database copied from the online snapshot, then opened by a fresh Kairos process | Four catalog versions, the completed file run, and its exact output bytes were available |
| Forced process termination | Go test executable started as a child process on Windows and Linux | Recovery preserved a completed step file, removed unreferenced staged bytes, resumed queued work, and did not replay the interrupted run |
| Corruption and rollback checks | Local bbolt databases | Partial publication rolled back; corrupted file bytes, missing indexes, and missing ULID state prevented startup |
| Completed-run restart check | Native Kairos stopped and restarted against its existing database | All three catalog versions and the completed run remained available; provider fixture counters did not increase |
| `docker compose config --quiet` | Windows Docker Compose | Passed |
| Container build and execution | Local Docker daemon | Unverified, `docker version` did not return and the probe was stopped |
| Formatting and `git diff --check` | Working tree | Passed |
| Credential-pattern scan | Current action, API, workflow, configuration, examples, and scripts | No matching live-key, Slack-token, or private-key patterns; Git history was excluded |

Whole-module statement coverage was 71.1%. Package coverage was 70.7% for actions, 79.2% for the API, 71.6% for file storage, 89.4% for configuration, 65.9% for identity, 74.5% for orchestration, and 73.9% for workflow execution. Default package coverage does not credit an imported package for behavior exercised by another package's tests. Both executable entry points have no instrumented unit coverage; the native smoke and demos exercised them directly. Coverage does not measure provider compatibility or throughput.

The Linux race run used the official Go 1.26.8 distribution downloaded and checksum-verified for this workspace, with GCC and CGO enabled. Local toolchain artifacts stay under ignored `bin` and do not enter Git or the Docker build.

## Executed behaviors

- A runtime-published parent calls a reusable HTTP child with one leaf worker, transfers the returned customer ID to a dependent notification request, and returns a declared receipt.
- Independent actions overlap within the shared worker limit. Cancellation releases capacity, stops dependent effects, and waits for cooperative handlers to exit.
- Immutable versions reject content changes. Child references remain pinned after a newer child version is published. Definitions and admission receipts survive closing and reopening the database.
- Event fanout starts the latest matching versions, admits the batch atomically, and rejects the entire batch if capacity or schema validation fails.
- Concurrent direct submissions with the same idempotency key produce one run. Duplicate events return their original run IDs without more upstream calls.
- Conditional steps skip, joins use literal reference defaults, and string composition keeps referenced data literal.
- JSON Schema validates input before admission. External schema references, duplicate JSON keys, excessive numeric exponents, invalid graphs, and excessive nesting fail validation.
- Retryable HTTP failures repeat only when a step declares idempotence. A simulated 503 followed by success uses the same generated provider idempotency key. Permanent errors receive one engine attempt.
- Dynamic URL and method input, custom HTTP methods, repeated query and form values, selected headers, and response predicates work against local HTTP fixtures.
- Origin rules, private-address checks, secret-header restrictions, admin authorization, request limits, and cancellation endpoints have assertions.
- A scalar result from a root step-input template produces a failed step and does not panic the scheduler.
- A forced storage failure stops the next external action, cancels workers, fails readiness, and leaves the persisted active run interrupted on reopening.
- Queued cancellation prevents execution. A queued run resumes after a graceful service restart; the previously active run remains interrupted and is not executed again.
- An injected persisted crash state and a forcibly killed child process recover as interrupted without replay. These do not cover every network acceptance boundary.
- Retention releases admission capacity in the same transaction. A fanout receipt can return an expired earlier result's original ID without executing it again.
- Multi-chunk binary files retain every byte and their SHA-256 digest. API fixtures cover PDF, PNG, WAV, ZIP, CSV, opaque bytes, and empty payloads. These are transport checks, not format-parser validation.
- Staged outputs remain hidden during execution and become available with the terminal run transaction. Injected transaction failure rolls back file publication, run state, quota changes, and cleanup together.
- Incomplete, oversized, canceled, and digest-mismatched uploads leave no file records or allocated IDs. Quotas and transfer slots reject excess work. Referenced files cannot be deleted; run retention releases their pins.
- File response bytes pass through a dependent raw HTTP request and multipart request. The local receiver compares exact bytes and repeated form fields.
- Corrupted file chunks fail before a download reader is returned. Startup rejects file corruption, a missing run index, a missing file index, and missing persistent ULID state.
- Persistent ULIDs remain ordered across database reopen and backward clock adjustment. Concurrent allocation produces distinct canonical IDs, and an aborted transaction does not advance the committed sequence.
- Format 1 migration preserves old public run IDs and completed result bytes. The new indexed cursor lists them in historical creation order.
- Backup restoration preserves files, catalog entries, results, and deduplication receipts. Restoring does not replay completed or interrupted effects.

## Native demo evidence

The final API-chain demo run was `01M4BW1FSDAB0P074MN4VXXEPQ`. Its `create`, `notify`, and `receipt` steps succeeded. The separate demo API returned `customer_1`. The script measured one new customer call and one new notification call, then confirmed the duplicate event reused that run.

The file-chain run was `01M4BW1FH0NQYJQN274G9H03H6`. It transferred 196,613 bytes and published file `01M4BW1FJDJVSPH8R0J5H1QSS0`. The input, multipart receiver, downloaded output, and restored output all had SHA-256 `1995571d3b74ff514cef27acc5a3043df9bb343e6ea8d2810abbac1362dedafb`. The duplicate upload returned its original file ID. The downloaded backup's SHA-256 was `2a92cf16e8be4b86cff21745d95b9ccf2cec4e3169af99bb5b433c89644f5058`.

A fresh native server opened a copy of that snapshot. It returned the completed file run, all four catalog versions, and the same file digest. The fixture's customer and notification counts remained at one each. The native run also opened and migrated the database retained from the earlier format 1 demo.

An earlier run, `3e2e0256b745c3345f50bbee149fb2d4`, remained successful after a real service stop and restart. The catalog still contained the welcome version and both runtime-published versions. The fixture stayed at one customer and one notification through that restart.

The demonstrations used localhost fixtures, temporary test credentials, and databases under ignored `bin/verification`. They did not send real messages or call a commercial provider. The test services were stopped after verification.

## Remaining release evidence

The Docker daemon did not answer the version probe. Image build, image scanning, container startup, volume ownership, healthcheck behavior, and resource-limit behavior remain unverified. The updated GitHub Actions workflow has not run remotely.

No live Stripe, Slack, OAuth, payment, or other provider integration was tested. No historical credential was used or revoked. No hosted deployment, sustained load benchmark, multi-tenant isolation test, network filesystem test, actual disk-full experiment, power-loss experiment, or process-kill test around a real provider's acceptance was performed. Backup restoration was verified locally; disaster recovery on another host remains a release gate.

The implementation does not resume partially completed workflows, wait for future signals, or coordinate multiple worker hosts. Its active-run interruption policy is explicit. Those capabilities and their release gates remain in the [product roadmap](product-roadmap.md).

## Developer documentation checks

The expanded README and guides were checked on October 7, 2026 against this working tree. The update changed documentation and added three runnable greeting JSON files. It did not change runtime behavior.

| Check | Result |
| --- | --- |
| Local Markdown links and heading anchors | Every local target in the README and documentation resolved |
| Fenced JSON and example files | 13 JSON examples parsed |
| PowerShell and Bash syntax | 18 PowerShell blocks and 18 shell blocks parsed |
| Configuration and endpoint coverage | All 17 explicit configuration variables and all 17 registered HTTP routes appeared in the reference docs; secret-header configuration was documented separately |
| Custom Go action | The complete source example compiled and ran with its documented specification; it returned `Welcome Ada`, rejected unknown config, and honored a canceled context |
| README and PowerShell walkthroughs | Welcome, greeting publication, pinned reuse, direct deduplication, async polling, pagination, terminal cancellation, file upload, and the API/file demonstration scripts passed |
| Documented rejection cases | Missing greeting input returned 422; changed immutable content returned 409 |
| Bash walkthroughs on Ubuntu under WSL | Welcome, greeting reuse, direct deduplication, event delivery and redelivery, raw and multipart file transfer, and byte comparison passed |
| Backup and restore commands | PowerShell and Bash downloaded snapshots, verified their digests, restored copies into new paths, and retrieved six catalog versions and unchanged file bytes |
| Repository tests and builds | `go test -count=1 ./...` passed again; Windows and Linux executables built |

The PowerShell greeting run was `01M4BY76HWAA6T3M0D2VN685SP`. Its restored result still returned `Hello Ada`. The Bash file run was `01M4BYD2JVGJ49M4JYHHDEA85Z` and published `01M4BYD2NV5SCR5C5JDKTGNRTP`. Its output and restored output matched the README bytes used during that test, with SHA-256 `269299e51472b13a164f91a02a3efb48c52697c897f8ce2876cbe089eb023bdc`.

Both restored instances retained six definitions. Each local provider fixture still reported one customer and one notification, so the restore caused no additional calls in these completed-run examples. Linux used its own service and fixture because this host's WSL environment could not reach the Windows loopback listener.

Local command transcripts and validation results remain under ignored `bin/verification/_docs-6f0c918f`. Snapshots remain under ignored `bin/backups`. All documentation test services were stopped. The earlier race and vulnerability results above were not rerun for this documentation-only update. Container and hosted release gaps remain open.

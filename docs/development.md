# Developer guide

[Documentation index](README.md)

Kairos is one Go executable. It has no frontend build, generated client, external queue, or database server to start. The [quick start](../README.md#quick-start) runs a workflow without outbound network access.

## Local setup

Install the toolchain selected in [go.mod](../go.mod), then run these commands from the repository root:

```sh
go version
go mod download
go test -count=1 ./...
go build -trimpath -o bin/kairos .
```

On Windows, use `bin/kairos.exe` as the build output. Set the runner token and optional admin token as shown in the README, then run `go run .` or the built executable. Relative seed and database paths resolve against the working directory.

For isolated development, set `KAIROS_DB_PATH` to a new path under ignored `bin/verification`. Do not use a production database for action development. Stored definitions compile with the current registry at every startup, so removing an action can make an existing catalog impossible to load.

The [local API fixture](../cmd/demo-api/main.go) listens on port 9081. Its customer, notification, and file endpoints are for development. It keeps counters in memory and has no durable provider state.

## Package responsibilities

| Package | Owns | Keep out |
| --- | --- | --- |
| [server.go](../server.go) | Environment wiring, registry construction, process signals, HTTP listener | Workflow business logic |
| [internal/config](../internal/config) | Environment defaults and range validation | Network requests |
| [internal/api](../internal/api) | Authentication, HTTP decoding, status mapping, file transfer responses | Direct database edits |
| [internal/orchestrator](../internal/orchestrator) | Immutable catalog, admission, receipts, queue claims, persisted results, retention, recovery | Provider-specific requests |
| [internal/workflow](../internal/workflow) | Schema validation, graph compilation, templates, conditions, bounded scheduling, retries | Database ownership |
| [internal/action](../internal/action) | Compiled action factories and context-bound execution | Admission or run-state commits |
| [internal/artifact](../internal/artifact) | Binary chunks, digests, quotas, reference pins, staging, snapshots | File format parsing |
| [internal/identity](../internal/identity) | Monotonic ULID allocation, including transaction-backed sequence state | Business ordering |

The packages under `internal/` are implementation code, not a separately supported Go SDK. External clients use the HTTP API.

## How a run executes

1. Publication compiles the definition, resolves pinned subflows, and validates configuration. Publication stores the immutable definition and its file pins together.
2. Admission resolves the requested version, validates input, checks capacity, and commits the root run, queue entry, indexes, input file pins, and optional receipt in one transaction. Event admission performs this for every matched workflow as one batch.
3. A root worker claims a queued run and records `running` before executing its plan.
4. The engine resolves dependencies and conditions. Ready leaf actions share the process-wide worker semaphore. Subflow wrappers wait for their children without holding a leaf slot.
5. The engine records progress before dispatch and after results. If a progress write fails, the service stops dispatch and fails readiness.
6. The terminal transaction stores the final run, updates its indexes, publishes files referenced by recorded results, and removes unreferenced staged files.

Admission and execution are separate. A client timeout does not erase a committed run. A database transaction also cannot retract a request already accepted by a provider.

The [transaction table](files-and-atomicity.md#what-commits-together) is the reference for changes to persistence. Keep related records in the same transaction. Allocate persistent IDs through the transaction-backed allocator. Do not replace it with a process-local ID generator for stored records.

Startup validates stored files and indexes, compiles the catalog, recovers formerly active runs as `interrupted`, and starts queued work. Recovery does not replay a partially completed graph.

## Add a Go action

An action has a factory and a handler:

```go
type Handler func(context.Context, map[string]any) (any, error)
type Factory func(json.RawMessage) (Handler, error)
type Registry map[string]Factory
```

The factory runs during compilation. It must validate configuration without causing a provider effect. The returned handler can be reused concurrently by many runs. Capture immutable configuration and use context-bound clients.

For a small working example, create `internal/action/greeting.go` with this complete file:

```go
package action

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/bclonan/kairosapi/internal/workflow"
)

func Greeting(raw json.RawMessage) (workflow.Handler, error) {
	cfg := struct {
		Prefix string `json:"prefix"`
	}{Prefix: "Hello "}
	if len(raw) > 0 {
		if err := workflow.Decode(raw, &cfg); err != nil {
			return nil, err
		}
	}
	if len(cfg.Prefix) > 128 {
		return nil, errors.New("greeting prefix exceeds 128 bytes")
	}
	return func(ctx context.Context, input map[string]any) (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name, ok := input["name"].(string)
		if len(input) != 1 || !ok || len(name) == 0 || len(name) > 128 {
			return nil, errors.New("greeting requires only a name of 1 to 128 bytes")
		}
		return map[string]any{"message": cfg.Prefix + name}, nil
	}, nil
}
```

Add the factory to the registry in [server.go](../server.go), retaining the existing entries:

```go
registry := workflow.Registry{
	"value":    action.Value,
	"http":     httpAction.Prepare,
	"delay":    action.Delay,
	"greeting": action.Greeting,
}
```

Rebuild and restart. Then publish this definition with an admin token:

```json
{
  "spec_version": "1.0",
  "id": "custom_greeting",
  "version": 1,
  "steps": [
    {
      "id": "message",
      "action": "greeting",
      "config": {"prefix": "Welcome "},
      "input": {"name": {"$ref": "/input/name"}}
    }
  ],
  "output": {"message": {"$ref": "/steps/message/message"}}
}
```

Submit `{"workflow_id":"custom_greeting","input":{"name":"Ada"}}` to `POST /v1/runs`. Its declared output is `{"message":"Welcome Ada"}`. The custom action validates its own input, so malformed input fails the step. Add an `input_schema` to the definition when rejection must happen before admission.

This action is a source extension example. The checked-in server does not register `greeting`. The runnable [greeting workflow](examples/greeting.json) uses the existing `value` action and needs no source changes.

### Handler rules

- Honor cancellation on every blocking operation. Use `http.NewRequestWithContext` for network requests and context-aware timers. Do not start detached work after returning.
- Keep execution bounded. The engine limits invocations, not goroutines created inside a custom handler. Go cannot forcibly stop a handler that ignores its context.
- Return JSON-compatible data within the action budget. Decoded numbers use `json.Number`. Avoid assuming every numeric value is a `float64`.
- Do not return raw provider error bodies, credentials, or headers in errors. The error message becomes part of the stored run. Successful outputs also remain visible to all runners.
- Use `workflow.OperationKey(ctx)` for a stable root-run and full-step-path identity across retries. `workflow.RunID(ctx)` returns the root run ID. A new run has a new identity and does not automatically deduplicate a previous uncertain effect.
- Return `*workflow.ActionError` with `Transient: true` only for a failure that permits another attempt under the provider's contract. Plain errors are permanent. The workflow still needs `idempotent: true` and more than one configured attempt for retries.
- Let the orchestrator own database changes. For file actions, use the configured artifact store with the root run ID as owner. See [internal/action/files.go](../internal/action/files.go) and [internal/action/http.go](../internal/action/http.go) for staged reads and writes. Close readers and release transfer slots on every path.

A panic becomes a failed action, but that does not undo an effect the handler already caused. Treat custom actions as trusted server code.

### Keep old definitions executable

Published workflow versions are immutable, but action implementations belong to the binary. Changing an action can change how an old definition executes after an upgrade. For incompatible semantics, register a new action name or accept an explicit configuration version. Retain support for every version still present in the catalog.

The `workflow` action is compiler-managed. Add a leaf action through the registry; do not replace subflow resolution with recursive HTTP calls to Kairos.

## Test a change

Run the affected package first, then the repository checks:

```sh
go test -count=1 ./internal/action
go test -count=1 ./...
go vet ./...
go test -race -count=1 ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...
go build -trimpath -o bin/kairos .
```

The race detector needs CGO and a supported C compiler. The recorded Windows-host race run used Linux under WSL with GCC. Run `go test -race -count=1 ./...` inside that Linux checkout with its Go toolchain. A Windows test run without `-race` does not replace that check.

For the Linux HTTP examples, run Kairos, its fixture, and the client inside the same Linux environment. A Windows loopback listener may not be reachable through WSL's localhost. The documentation verification used separate native Windows and Linux instances with separate databases.

Format changed Go code with `gofmt`. For the current source tree:

```sh
gofmt -w server.go internal cmd
git diff --check
```

To inspect coverage in PowerShell:

```powershell
go test -count=1 '-coverprofile=coverage.out' ./...
go tool cover '-func=coverage.out'
```

The quotes preserve the complete flag argument in PowerShell. The same flags work without quotes in Bash. Coverage identifies unexecuted statements; it does not establish crash tolerance or provider compatibility.

| Change | Useful evidence |
| --- | --- |
| New action | Config rejection, input/output contract, canceled context, concurrent calls, permanent and transient errors |
| Compiler or templates | Invalid graph rejection, nested limits, literal data preservation, missing references, numeric precision |
| Scheduler | Shared concurrency bound, dependent effects, cancellation, retry identities, nested workflows with one worker |
| Admission or storage | Concurrent deduplication, rollback, capacity failure, close/reopen, queue recovery, index consistency |
| Files | Exact bytes and digest, interrupted upload, quota rollback, cross-run visibility, reference pins, restore |
| API or authorization | Role checks, body limits, status and header contract, duplicate retries, terminal retrieval |

Tests use temporary databases and local HTTP fixtures. The durability suite includes a child process that the test kills, then reopens its database. It must never use a live service's path. The [verification record](verification.md) explains the limits of those tests.

After changing an end-to-end contract, run the relevant [example](examples/README.md) against the native executable. The PowerShell scripts need tokens in the client environment. The API and file demos also need the localhost fixture and an explicit private origin in Kairos.

## Contribution and release expectations

Keep specifications and runtime behavior documented together. A new environment variable belongs in configuration, `.env.example`, and the deployment configuration where applicable. A new HTTP field needs validation, API documentation, and an example. A persistence change needs a format migration and rollback tests.

Do not commit database files, backup snapshots, generated executables, logs, secrets, or local toolchains. These belong outside source control. Never use historical committed credentials in tests.

[CI](../.github/workflows/ci.yml) checks formatting, vet, the race suite, dependency vulnerabilities, native build, and Docker build. A local pass does not imply that remote CI or a container deployment passed. Record the commit, toolchain, commands, results, and any unverified deployment boundary with a release.

The repository currently has no license file. Resolve distribution rights before publishing a software release.

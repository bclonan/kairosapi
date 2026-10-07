# Reusable event and workflow specifications

[Documentation index](README.md) · [No-network greeting example](examples/README.md#greeting-chain)

A Kairos specification describes a finite graph of actions. It is data, not executable source code. Publish it through `POST /v1/workflows`, call it through `POST /v1/runs`, or attach triggers for `POST /v1/events`.

The runnable [HTTP request specification](../workflows/reusable-http.json) and [customer event chain](../workflows/customer-events.json) demonstrate reuse across API calls. The [local demo](../README.md#run-the-complete-local-api-demo) publishes and executes them.

## Definition

```json
{
  "spec_version": "1.0",
  "id": "welcome_message",
  "version": 1,
  "description": "Build a welcome message from validated input.",
  "input_schema": {
    "type": "object",
    "required": ["name"],
    "properties": {"name": {"type": "string", "minLength": 1}},
    "additionalProperties": false
  },
  "steps": [
    {
      "id": "message",
      "action": "value",
      "input": {
        "text": {"$concat": ["Welcome ", {"$ref": "/input/name"}]}
      }
    }
  ],
  "output": {"text": {"$ref": "/steps/message/text"}}
}
```

`id` and every step ID must start with a letter, contain only letters, digits, underscores, or hyphens, and fit in 64 characters. `version` is a positive integer. `spec_version` currently accepts only `"1.0"` and defaults to that value.

| Definition field | Required | Meaning |
| --- | --- | --- |
| `spec_version` | No | Specification format, currently `"1.0"` |
| `id` | Yes | Logical workflow name; the service assigns a separate publication ULID |
| `version` | Yes | Positive immutable version number |
| `description` | No | Text shown in catalog metadata |
| `input_schema` | No | JSON Schema for the submitted input object |
| `triggers` | No | Up to 16 event-matching rules |
| `timeout_seconds` | No | Positive integer within the server limit; zero or omission inherits |
| `steps` | Yes | Array of 1 through 32 action steps |
| `output` | No for roots | Object template for the final result; a reusable child needs a nonempty declared output |

`input_schema` uses JSON Schema, with draft 2020-12 as the default. Local `$defs` and fragment references work. External file and network schema references are disabled. Schemas are limited to 32 KiB. Validation happens before top-level queue admission and again before a subflow executes. Invalid subflow input fails that step; earlier parent actions may already have completed. Formats have the validator's default annotation behavior, so use patterns or a provider action for stricter business validation.

The compiler rejects unknown actions, invalid references, duplicate step IDs, dependency cycles, missing subflows, and unsupported retry policies. JSON decoding rejects duplicate object keys and trailing values. Definitions may contain at most 32 direct steps and 1 MiB of JSON. The catalog retains at most 128 versions in total.

## Chaining, parallel execution, and reuse

Use `depends_on` to order effects. Without it, independent steps may run together. Array order controls result order and ready-step preference, but does not establish dependencies.

| Step field | Required | Meaning |
| --- | --- | --- |
| `id` | Yes | Unique name within this definition |
| `action` | Yes | Registered action name, or compiler-managed `workflow` |
| `depends_on` | No | Direct predecessor IDs; repeats, self-references, and cycles are rejected |
| `input` | No | Object template resolved for this invocation; defaults to an empty object |
| `config` | Depends on action | Static factory configuration; it is not an input template |
| `when` | No | Predicate evaluated after dependencies are complete |
| `timeout_seconds` | No | Positive integer within the definition limit; zero or omission inherits |
| `retry` | No | `max_attempts`, `backoff_ms`, and `idempotent` |

```json
{
  "id": "greeting",
  "action": "workflow",
  "config": {"workflow_id": "welcome_message", "version": 1},
  "input": {"name": {"$ref": "/input/customer/name"}}
}
```

The `workflow` action calls a published, pinned version. A child must declare `output`. That output becomes `/steps/greeting` in its parent. The child keeps its own input schema, graph, timeouts, and result history.

Subflows can contain subflows. Expansion is limited to 128 total steps and 8 definition levels, including the root. Compilation counts every referenced instance, including steps that might later be skipped. Repeated use of the same child gets separate input and operation identities.

The shared worker semaphore applies to leaf actions across all runs. A parent waiting for a subflow holds no leaf worker slot. One worker can therefore execute a nested chain. Goroutines waiting for dependency completion, retry timers, and subflows are bounded by the admitted runs and expanded graph limits.

Custom Go actions register a factory with this contract:

```go
type Handler func(context.Context, map[string]any) (any, error)
type Factory func(json.RawMessage) (Handler, error)
type Registry map[string]Factory
```

Factories validate configuration. Handlers honor cancellation and return bounded JSON. A panic becomes a failed action. Go cannot forcibly stop a handler that ignores its context, so untrusted code belongs in a separate process.

The [developer guide](development.md#add-a-go-action) contains a complete action implementation and registry wiring. The `value` action returns its resolved input and accepts no configuration fields. The `delay` action also returns its resolved input after its timer. HTTP and subflow actions have the contracts below.

## Passing data

Step input is an object template. It can contain objects, arrays, scalar values, and the following operators.

| Expression | Meaning |
| --- | --- |
| `{"$ref":"/input/name"}` | Copy submitted input |
| `{"$ref":"/steps/create/body/id"}` | Copy output from a declared dependency |
| `{"$ref":"/run/id"}` | Root run identity, unchanged within nested workflows |
| `{"$ref":"/run/workflow_id"}` | ID of the current workflow, including a child |
| `{"$ref":"/run/version"}` | Version of the current workflow |
| `{"$ref":"/steps/optional/body","default":null}` | Use a literal fallback when the reference is missing |
| `{"$concat":["Bearer ",{"$ref":"/input/access_token"}]}` | Join strings, numbers, or booleans |
| `{"$literal":{"$ref":"this-is-data"}}` | Copy reserved-looking data without interpreting it |

Paths use JSON Pointer. `~1` represents a slash and `~0` a tilde. Array indices use ordinary nonnegative decimal numbers without leading zeroes. A step output reference must name a direct dependency. Declared workflow output may reference any step.

Referenced values and reference defaults are copied as literal data. A caller cannot inject another template through its input. Missing references fail unless a default exists. A default does not replace a present JSON null. Step input must resolve to an object. Its individual fields can have any supported JSON type.

`$concat` accepts at most 64 pieces. There is no general expression language, JavaScript evaluation, or arbitrary string-to-code conversion.

Input and output data have a 32-level nesting limit and a 256 KiB per-action budget. Copy bookkeeping can reject data before its serialized JSON reaches that size. The compiler and decoder bound numeric text to 1,024 characters and exponents to absolute value 1,024. Numeric conditions preserve integer precision and compare equivalent decimal values numerically.

Declared `output` defines what a reusable workflow returns. Step history still appears in the run result. Accumulated results have a 4 MiB limit; a run can fail that limit after earlier external effects have occurred.

## Conditions and joins

```json
{
  "id": "notify",
  "action": "workflow",
  "depends_on": ["create"],
  "when": {
    "all": [
      {"path": "/input/data/notify", "op": "eq", "value": true},
      {"path": "/steps/create/body/id", "op": "exists"}
    ]
  },
  "config": {"workflow_id": "send_notification", "version": 1},
  "input": {"customer_id": {"$ref": "/steps/create/body/id"}}
}
```

Predicates support `eq`, `ne`, `exists`, `not_exists`, `gt`, `gte`, `lt`, `lte`, and `in`. Ordered comparisons require numbers. `in` requires an array. Groups use `all`, `any`, or `not`, with one operator form per condition, at most 16 immediate children, and bounded nesting. Groups short-circuit in document order.

A false condition marks the step `skipped`. A join can proceed after succeeded or skipped dependencies. Use reference defaults when reading a branch that might be skipped. Missing data in an ordinary comparison fails the step; use an existence check first when absence is expected.

The executor has no loop action, dynamic map over an arbitrary array, exception-catching branch, or compensation behavior. Those need explicit future contracts rather than accidental cycles.

## HTTP actions

```json
{
  "id": "create",
  "action": "http",
  "timeout_seconds": 15,
  "retry": {"max_attempts": 3, "backoff_ms": 500, "idempotent": true},
  "config": {
    "url": "https://partner.example/customers",
    "method": "POST",
    "encoding": "json",
    "secret_headers": {"Authorization": "KAIROS_SECRET_PARTNER_AUTH"},
    "idempotency_header": "Idempotency-Key",
    "response_headers": ["X-Request-ID"],
    "expect_json": {"/ok": true}
  },
  "input": {"body": {"name": {"$ref": "/input/name"}}}
}
```

This example assumes the provider supports that idempotency header and response contract. The declaration alone cannot make a non-idempotent provider safe to retry.

| Configuration | Behavior |
| --- | --- |
| `url` | Fixed endpoint, with optional whole-segment `{name}` path placeholders |
| `method` | Uppercase HTTP method, including custom methods; CONNECT is rejected |
| `url_from_input` | Read `input.url` instead of `config.url` and recheck origin policy |
| `method_from_input` | Read `input.method` instead of `config.method` |
| `encoding` | `json` by default, `form`, `raw` text, `file` bytes, or `multipart` |
| `response_mode` | `auto` by default for JSON or text; `file` stores the exact response bytes |
| `response_name` | File name to retain when `response_mode` is `file`; defaults to `file.bin` |
| `headers` | Fixed non-secret headers |
| `secret_headers` | Header names mapped to `KAIROS_SECRET_` environment names |
| `input_headers` | Exact header names the input may supply |
| `response_headers` | Selected headers to expose in the output |
| `idempotency_header` | Header receiving a stable hash of root run ID and full nested step path |
| `expect_json` | Response-body JSON pointers and expected values |

When a `*_from_input` option is true, omit its fixed configuration value. A dynamic URL cannot use server-held `secret_headers`. Headers supplied by input cannot replace fixed headers, secret headers, or the generated idempotency header. Host and connection-control headers are forbidden. Response cookie capture is disabled.

The input object supports `body`, `path`, `query`, and `headers`, plus the explicitly enabled `url` and `method`. Query and form objects accept scalar values or arrays of scalars for repeated keys. Path values fill whole segments and cannot contain slash, backslash, or dot segments. Supply a Content-Type header for XML, GraphQL text, or another raw text body as needed.

HTTP output contains `status` and `body`, plus selected `headers` when configured. In `auto` mode, JSON bodies decode into JSON values and other bodies become strings. Use `response_mode: "file"` for binary or larger responses so every byte survives. Its `body` contains a file reference. It cannot be combined with `expect_json`. Only 2xx statuses succeed. Redirects fail rather than change the destination. `expect_json` can detect an API returning HTTP 200 with a business error.

The transport ignores proxy environment variables, checks DNS results, and dials an approved IP address. Public mode blocks private, loopback, link-local, reserved, and unsupported transition addresses. Exact origin matching includes scheme and explicit port. `https://example.com` and `https://example.com:443` are different entries.

The generic adapter covers bounded HTTP request/response exchanges, raw file bodies, and multipart uploads. REST, HTTP webhooks, GraphQL POST requests, and text-based protocols can use it. OAuth refresh, cryptographic signing, streaming, gRPC, WebSockets, and automatic pagination need additional actions or explicit orchestration. Successful HTTP transport alone does not prove provider business success.

For `encoding: "file"`, set `input.body` to a file reference such as `{"$ref":"/input/file"}`. For `encoding: "multipart"`, use `body.fields` for scalar or repeated form values and `body.files` for an array of `{"field":"document","file":{"$ref":"/input/file"}}`. Multipart supports up to 16 file parts; their combined bytes and form values must fit the per-file limit. The action generates the boundary and Content-Type header. It verifies and spools all parts before sending the request.

`{"$artifact":"ULID"}` is reserved for a stored file. Input, literal definition, and result references must identify existing files. Kairos pins referenced files while the owning definition or run remains retained. A missing result reference fails its run without stopping the service. Passing a reference copies metadata; the file bytes stay outside the JSON size budget. See the [file contract](files-and-atomicity.md) and [complete relay example](../workflows/file-relay.json).

## Retry, timeouts, and delays

Without a retry block, each action gets one engine attempt. `max_attempts` accepts up to 5 total attempts. More than one requires `idempotent: true`. Repeating an entire subflow is rejected; configure retries on its individual external actions.

`max_attempts: 0` also means one attempt. `backoff_ms` accepts an integer from 0 through 30,000. A retry block does not change permanent failures into transient ones.

HTTP marks connection and response-read failures, plus 408, 429, 500, 502, 503, and 504, as retryable. Other statuses, rejected response predicates, invalid inputs, and oversized output fail immediately. Custom actions can return a typed `workflow.ActionError` with `Transient: true`.

Backoff starts at the larger of 100 ms and `backoff_ms`, doubles between attempts, and caps at 30 seconds. Provider Retry-After can extend that wait within the step deadline. The same configured idempotency header value is reused for every attempt. Backoff releases the action worker slot. There is no jitter or per-provider rate limiter yet.

The default run timeout is 60 seconds. `KAIROS_RUN_TIMEOUT` permits 1 second through 5 minutes. Definitions and steps may choose shorter `timeout_seconds` values. Step deadlines include waiting for a worker and retry backoff. Child execution respects both parent and child deadlines.

`delay` accepts `{"milliseconds":1000}` as configuration, up to 60 seconds, and returns its resolved input after a cancellable timer. It occupies an action slot. It does not survive a restart or wait for a future event. Use it only for short, explicit pauses.

## Event triggers

```json
{
  "triggers": [
    {
      "type": "orders.*",
      "source": "/checkout",
      "subject": "orders/priority",
      "match": {"/data/paid": true}
    }
  ]
}
```

A trigger matches an exact type or a prefix ending in `*`. Optional source and subject are exact matches. `match` applies equality checks at JSON pointers into the complete event envelope. Up to 16 triggers may belong to one definition.

The latest version controls new event routing. Publishing a newer version with no triggers stops new automatic starts for that workflow ID. Already admitted runs keep their pinned version. Use a new event ID when intentionally starting another operation.

Persistent event receipts prevent duplicate admission within the retention window. They do not impose ordering between different events, provide a broker subscription, or replace a provider's signature verification.

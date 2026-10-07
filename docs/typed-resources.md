# Dictionaries and Protocol Buffers

Kairos can load a reusable JSON dictionary or Protocol Buffers descriptor when a step first needs it. Upload the bytes once, then reference their immutable file ULID in a workflow version. Different runs share a bounded cache of parsed resources.

This guide uses the [typed workflow example](examples/resources/typed-chain.json), its [dictionary](examples/resources/people.json), the [source schema](examples/resources/person.proto), and a ready [compiled descriptor](examples/resources/person.desc).

## Run the example

Start Kairos with the runner and administrator tokens described in the [README](../README.md). In a second PowerShell 7 terminal, set the same token values and run:

```powershell
./scripts/demo-typed-resources.ps1
```

The script uploads the dictionary and descriptor, fills their file IDs into the workflow, publishes it, and runs these steps in order:

1. Select Ada's record from the dictionary.
2. Encode the record as a `demo.Person` protobuf message.
3. Decode that file back into ProtoJSON.
4. Download the encoded file and compare all 19 bytes with the expected wire message.
5. Repeat the submission with the same idempotency key and check that it returns the same run and output file.

The result includes `name: "Ada"`, `customerId: "9007199254740993"`, and `payload: "AAH/"`. ProtoJSON uses a string for this 64-bit integer and base64 for the three payload bytes `00 01 FF`. The encoded file is binary protobuf, not JSON text or a base64 file.

The script writes its downloaded file and result JSON under `bin/verification/typed-<unique-id>`. It needs no external API origin and no local `protoc` installation. Repeating it against the same database reuses the resource uploads and workflow version. If you edit the fixture bytes or workflow, choose a new workflow version.

## Declare resources once

The `resources` object names immutable artifact references. `$resource` is a compile-time alias used inside step configuration. It does not fetch a URL or interpret input data.

```json
{
  "resources": {
    "people": {"$artifact": "<DICTIONARY_FILE_ULID>"},
    "person": {"$artifact": "<DESCRIPTOR_FILE_ULID>"}
  }
}
```

Replace the placeholders with file IDs returned by `POST /v1/files`. The example script does this. Step configuration can also contain a literal artifact reference without an alias.

Publication pins these files in the same database transaction as the workflow definition. A pinned file cannot be deleted. A file ID always identifies the same bytes; publish a new workflow version to select a different resource. Kairos validates the reference at publication and parses the resource when the action first runs. A malformed dictionary or descriptor therefore fails the action even if its file reference passed publication.

Only published files can supply dictionaries or descriptors. Binary message input can also be a staged file from an earlier step in the same root run.

## Dictionary lookup

```json
{
  "id": "lookup",
  "action": "dictionary",
  "config": {
    "resource": {"$resource": "people"},
    "pointer": "/people"
  },
  "input": {"key": "ada"}
}
```

The uploaded dictionary must be a JSON object. `pointer` is an optional JSON pointer into that object; its default is the root. If `input.key` is present, Kairos appends it as one escaped pointer segment. A key containing `/` or `~` remains a literal key.

The output is `{"value": <selected JSON>}`. Omitting `input.key` returns the value at `pointer`. Missing entries fail the step. Duplicate JSON keys, malformed JSON, and null or array roots fail resource loading. Numbers retain their JSON spelling and precision within Kairos's existing numeric limits.

Kairos copies the selected value before returning it. A step cannot change another run's cached dictionary.

## Encode and decode protobuf

An encode step accepts ProtoJSON in `input.value`:

```json
{
  "id": "encode",
  "action": "protobuf",
  "config": {
    "descriptor": {"$resource": "person"},
    "message": "demo.Person",
    "operation": "encode",
    "name": "person.pb"
  },
  "input": {
    "value": {"$ref": "/steps/lookup/value"}
  }
}
```

The output contains `file`, an artifact reference, and `message_type`. The file has media type `application/protobuf`. Kairos stages it under the root run ID and publishes it with the final retained run result. A downstream HTTP action can send it with `encoding: "file"`. A multipart request can attach it beside other files.

A decode step accepts an artifact reference in `input.file`:

```json
{
  "id": "decode",
  "action": "protobuf",
  "config": {
    "descriptor": {"$resource": "person"},
    "message": "demo.Person",
    "operation": "decode"
  },
  "input": {
    "file": {"$ref": "/steps/encode/file"}
  }
}
```

The output is `{"value": <ProtoJSON>, "message_type": "demo.Person"}`. Use `depends_on` for these references in DAG mode. The full example uses `execution.mode: "sequential"`, so each listed step waits for the steps before it.

The `message` value must be a fully qualified message name. `name` applies only to encoding. Kairos rejects unknown JSON fields and invalid field types when encoding. Binary protobuf can contain unknown fields, but the JSON output contains the fields defined by the selected descriptor. Decoding and re-encoding through JSON does not preserve unknown wire fields or original field ordering. Keep the original artifact when byte preservation matters.

Kairos uses the official Go protobuf runtime to build a separate dynamic message per invocation. It shares immutable descriptors between concurrent runs. Encoding is deterministic for the pinned descriptor and this runtime version, but deterministic protobuf encoding is not a cross-language canonicalization promise. See the official [dynamic message API](https://pkg.go.dev/google.golang.org/protobuf/types/dynamicpb), [ProtoJSON format](https://protobuf.dev/programming-guides/json/), and [serialization guidance](https://protobuf.dev/programming-guides/serialization-not-canonical/).

## Prepare your own descriptor

Kairos reads a binary `google.protobuf.FileDescriptorSet`. It does not compile uploaded `.proto` source.

Run the Protocol Buffers compiler where your schema files live:

```bash
protoc \
  --proto_path=. \
  --include_imports \
  --descriptor_set_out=person.desc \
  person.proto
```

Include every imported schema. Kairos does not search a global type registry, local include directory, package repository, or network service. Types used by `google.protobuf.Any` must also exist in the descriptor set. Upload the resulting descriptor as a regular file and reference its returned ULID.

The [example descriptor](examples/resources/person.desc) declares the same fields as [person.proto](examples/resources/person.proto). The action integration test checks the committed descriptor against those expected message fields and verifies the exact encoded wire bytes.

This codec handles protobuf messages. gRPC framing, service discovery, and streaming require a transport adapter. A protobuf body sent to an ordinary HTTP endpoint works through the existing HTTP action.

## Limits and recovery

| Item | Limit or behavior |
| --- | --- |
| Dictionary or descriptor source | 1 MiB, uncompressed |
| Cached resource variants | 32 entries across both action types |
| Cached source bytes | 8 MiB |
| Concurrent cache misses | One load at a time, waiting callers can cancel |
| Descriptor files | 64 |
| Descriptor message objects | 8,192, including fields and descriptor options |
| Descriptor and message recursion | 32 levels |
| Dictionary key input | 1,024 bytes |
| Protobuf wire input or output | 256 KiB |
| Action JSON input or output | Existing 256 KiB limit |

Cache byte accounting measures source bytes, not exact Go heap size. Entry, source, and descriptor complexity limits bound parsed data separately. A cache hit still checks that the file is published and exists. The first load verifies the stored file digest before parsing.

Resource load errors do not poison the cache. Missing imports, invalid messages, and oversized selections fail the step with a recorded error. Concurrent action outputs use independent maps and protobuf message instances.

With `execution.recovery: "resume"`, these built-in transformations can safely run again after an interruption because they have no external provider effect. A checkpointed successful step reuses its saved result. An encoding attempt that produced bytes before its checkpoint may create a new output ULID after recovery. The bytes remain deterministic and file finalization discards unreferenced staged output. A repeated completed run submission with the same idempotency key returns the original run and file IDs.

## Go utility functions

The internal packages support actions compiled into Kairos. Go's `internal` rule restricts imports to this module tree; these are not a separately versioned public SDK.

| Utility | Use |
| --- | --- |
| `action.NewResources` | Create the shared dictionary and protobuf action factories. |
| `Resources.SetFiles` | Attach the service's file store once during startup. |
| `Resources.Dictionary` | Compile validated dictionary action configuration. |
| `Resources.Protobuf` | Compile validated protobuf action configuration. |
| `resource.New` | Create a bounded loader over an existing artifact store. |
| `Loader.Lookup` | Resolve a JSON pointer and return an independent dictionary value. |
| `Loader.Message` | Resolve a message descriptor and an immutable dynamic type resolver. |
| `artifact.ReferenceID` | Validate and extract the ULID from an artifact reference. |
| `artifact.Metadata.Reference` | Return a file reference suitable for workflow data. |
| `workflow.RunID` | Obtain the root run ID for staged file ownership. |

Register both factories alongside the existing actions, then configure their shared file store before the service compiles workflow definitions:

```go
resources := action.NewResources()
registry := workflow.Registry{
    "value":      action.Value,
    "dictionary": resources.Dictionary,
    "protobuf":   resources.Protobuf,
}
options.ConfigureFiles = resources.SetFiles
service, err := orchestrator.Open(options, registry, seeds)
```

The server already registers these actions. If your custom server also uses the HTTP action's file support, call both `SetFiles` methods from the same `ConfigureFiles` callback, as [server.go](../server.go) does.

The [developer guide](development.md) explains custom factories, context cancellation, and registry wiring. The [file guide](files-and-atomicity.md) covers upload, ownership, integrity, and backup.

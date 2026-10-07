# Kairos documentation

Start with the [project README](../README.md) to run the welcome workflow and publish a reusable chain. These guides describe the current Go implementation. Proposed capabilities live in the roadmap.

| Guide | What it answers |
| --- | --- |
| [Developer guide](development.md) | How the code is organized, how to test a change, and how to add an action |
| [Configuration reference](configuration.md) | Every environment setting, default, supported range, and fixed limit |
| [Workflow reference](event-specifications.md) | Definition fields, dependencies, reuse, templates, schemas, conditions, HTTP, retries, and triggers |
| [HTTP API contract](api.md) | Authentication, endpoints, request identity, responses, states, and errors |
| [Runnable examples](examples/README.md) | Publication order and commands for local, HTTP, event, and file chains |
| [Files and atomicity](files-and-atomicity.md) | Binary storage, staging, reference pins, ULIDs, commit boundaries, and fault behavior |
| [Operations guide](operations.md) | Deployment, database ownership, shutdown, monitoring, backup, restore, and upgrades |
| [Troubleshooting](troubleshooting.md) | How to interpret startup failures, HTTP errors, and unexpected run behavior |
| [Architecture review](architecture-review.md) | Original intent, historical findings, current design, and architectural direction |
| [Product roadmap](product-roadmap.md) | Remaining product work and its release acceptance gates |
| [Verification record](verification.md) | Tests and demonstrations that ran, their environments, and gaps in the evidence |

For client integration, read the API contract and workflow reference, then run an example. For runtime changes, start with the developer guide and the relevant package tests. For deployment, read configuration, operations, and the transaction boundaries before choosing limits or a recovery procedure.

The API uses one runner credential and an optional admin credential for the whole instance. There is no tenant boundary within an instance. A local atomic commit does not imply an atomic effect at an external provider.

The implementation uses Go packages under `internal/`. It has no separately versioned Go SDK, generated OpenAPI file, browser application, or configuration reload endpoint.

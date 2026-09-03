# Protocol Contracts

This document owns WDL's cross-tier protocol contract posture. Module docs remain the
owner for individual endpoints and Redis key families; this page explains how metadata,
Redis payloads, control APIs, binding materialization, and state-machine protocols
should become explicit instead of living as scattered switch statements and implicit
object shapes.

## Scope

Protocol contracts include:

- control/admin request and response bodies;
- bundle metadata, binding metadata, route projections, lifecycle indexes, and other
  Redis DB 0 records;
- data-plane Redis payloads such as KV metadata, queue entries, delayed messages, and
  log-tail events;
- binary internal envelopes such as runtime-load, D1 query, D1 actor query, DO invoke,
  and D1 query response payloads;
- Rust service state-machine records for scheduler, workflows, D1, and DO;
- logs and metrics when consumers rely on stable event names, fields, metric families,
  or bounded labels.

The current code and tests are authoritative. This document defines the direction for
new work and for refactors that touch existing protocol surfaces.

## Ownership Rules

Every protocol shape needs one owning module and one current written source:

| Surface | Owner |
|---|---|
| Control/admin HTTP bodies and error codes | `docs/modules/control-auth.md` plus `control/` helpers |
| Bundle metadata and binding metadata | `docs/modules/runtime.md`, `docs/modules/control-auth.md`, `control/`, `runtime/load/` |
| Redis keys and logical DB split | `docs/redis-key-layout.md` plus shared key helpers |
| Route and pattern projections | `shared/route-projection.js`, `control/routing.js`, `gateway/` |
| D1 query/facade protocol | `docs/modules/d1.md`, `shared/d1-*`, `d1-runtime/`, runtime D1 binding |
| Runtime KV host responses | `tests/fixtures/kv-host-response.json` owns the bounded batch, metadata, and list response shapes emitted by Rust redis-proxy and consumed by Runtime KV. |
| AI provider records and resolver snapshots | `docs/modules/ai.md`, `shared/ai-contract.js`, `control/handlers/ai.js`, `rust/redis-proxy/src/ai.rs`, `tests/fixtures/ai-contract.json` |
| AI tenant/provider HTTP, SSE, WebSocket, and facade protocol | `docs/modules/ai.md`, `runtime/bindings/ai*.js`, `runtime/ai-client.js` |
| Internal WebSocket backend reconnect policy | `docs/modules/gateway.md`, `shared/worker-contract.js`, `gateway/websocket.js` |
| Durable Object invoke/connect and owner-shard protocol | `docs/modules/durable-objects.md`, `runtime/_wdl-do-scoped-request.js`, `runtime/_wdl-do-transport.js`, `do-runtime/protocol.js`, `do-runtime/protocol/wire-grammar.js`, `tests/fixtures/do-owner-shards.json` |
| Session policy projection and lifecycle notifications | `docs/modules/control-auth.md`, `docs/modules/gateway.md`, `shared/worker-contract.js`, `control/routing.js`, `control/handlers/delete-plan.js`, `gateway/runtime.js`, `gateway/websocket-lifecycle.js`, `do-runtime/owner-registry.js`, `rust/workflows/src/api/do_alarms/dispatch.rs` |
| Queue, cron, and delayed queue records | `docs/modules/queues-cron.md`, `shared/queue-keys.js`, scheduler/proxy Rust modules |
| Queue Runtime outcomes | `tests/fixtures/queue-runtime-response.json` owns the cross-language outer/inner outcome literals and required result fields consumed by Runtime dispatch and Rust Scheduler; `rust/scheduler/src/queue/delivery/outcome.rs` owns Queue decision grammar validation before destructive acknowledgement. |
| Workflow definitions and instance state | `docs/modules/workflows.md`, `rust/workflows/`, runtime workflow dispatch |
| Workflow Runtime requests, results, and Workflow-owned retryable backend errors | `tests/fixtures/workflow-runtime-request.json` owns the Rust-to-JavaScript run request shape, including the absolute dispatch deadline. `tests/fixtures/workflow-runtime-response.json` owns the outcome literals, required terminal payload fields, and Workflow-owned retryable error literals consumed by Runtime dispatch and Rust Workflows. Internal-auth failure uses the shared `shared/internal-auth.js` and `tests/fixtures/internal-auth-contract.json` contract. |
| Workflow service lifecycle errors | `tests/fixtures/workflow-service-errors.json` owns the migration-pending status and error code emitted by Rust Workflows and preserved by Control lifecycle checks. |
| Workflow step replay identity and terminal responses | `tests/fixtures/workflow-step-response.json` owns backend-written operation kinds and the required claim/replay/register-wait terminal payload fields consumed by Runtime and emitted by Rust Workflows. |
| Workflow tick response | `tests/fixtures/workflow-tick-response.json` and `rust/common/src/workflow_tick.rs` own the executable shape; `rust/workflows/` and `rust/scheduler/` consume it, while `docs/modules/workflows.md` documents the surrounding behavior. |
| Durable Object alarm responses | `tests/fixtures/do-alarm-response.json` owns the 16 KiB cap, 5-second body deadline, exact actor, dispatch, set/delete, and cleanup success variants, and the failed/result-unknown dispatch errors consumed by JavaScript and Rust; `shared/do-alarm-response.js` and `rust/workflows/src/api/do_alarms/dispatch.rs` own their runtime readers. |
| Observability event and metric shape | `docs/modules/log-tail-observability.md`, `shared/observability.js`, `wdl-rust-common` |

If one tier writes a shape and another tier reads it, the same change must update the
writer, reader, owning doc, and a drift-catching test. Do not create a second parser or
fallback reader unless an external rollout requirement exists.

## Schema Direction

WDL should move high-risk object shapes toward explicit schemas or schema-like
normalizers. "Schema" here means a single canonical definition that:

- names required fields, optional fields, defaults, caps, and enum values;
- rejects malformed persisted state fail-closed instead of lossy-normalizing it;
- produces a normalized value with a small, typed surface;
- is referenced by all writers and readers for the same protocol domain;
- carries fixtures or behavior tests for success, malformed, legacy-rejected, and
  boundary-size cases.

The first candidates are:

- deploy request body and emitted bundle metadata;
- binding metadata for KV, R2, AI, D1, DO, workflows, service bindings, platform
  bindings, queues, assets, vars, and secrets;
- Redis projections for active routes, pattern routes, cron/queue/workflow lifecycle
  indexes, and workflow definitions;
- runtime-load and DO/D1 binary envelope metadata;
- workflow instance state and scheduler discovery indexes.

Do not schema-ize by adding a large generic framework first. Prefer small local
validators that can later be grouped behind a registry.

## Binding Registry Direction

Binding work should converge on a registry-shaped pipeline. Each binding kind should
have one entry that owns:

- deploy-time input validation and normalization;
- Redis/bundle metadata materialization;
- runtime `env` materialization;
- hidden backend binding requirements;
- host-wrapper and raw-export hiding requirements;
- tenant-visible facade behavior;
- docs and test fixture names.

Large switch statements are acceptable only as temporary dispatch over registry entries.
Do not add a new binding by updating deploy validation, runtime env construction, host
wrapper generation, and docs as unrelated local edits. The review unit should make it
obvious which registry entry owns the new kind.

## State-Machine Protocol Tests

State-machine correctness should be protected by model-ish tests and failure injection,
not only by happy-path integration tests. A good state-machine test names:

- the authoritative record and derived projection;
- the generation, lease, token, or WATCH fence that prevents stale writers;
- the injected failure or interleaving;
- the expected repair, retry, or fail-closed behavior.

Priority state machines:

- deploy, promote, rollback, version delete, whole-worker delete, and S3 cleanup
  intent;
- D1/DO owner claim, forward, renew, drain, release, and stale owner hints;
- workflow run-token, step dependency, ready/due, event, callback, retention, and
  lifecycle delete blockers;
- scheduler queue consumer discovery, delayed due indexes, retry/DLQ, and orphan
  migration;
- log-tail activation leases and bounded stream behavior.

Failure-injection tests should prefer pure unit or service-local tests when they can
exercise the protocol deterministically. Integration tests should cover the smallest
end-to-end path that proves the distributed boundary still holds.

## Known Constraint Runbook

Known constraints must be explicit contracts, not reviewer folklore:

- Gateway route invalidations are non-durable hints. Gateway clears caches on
  subscriber connect/disconnect, then re-reads Redis on the next lookup.
- Runtime cold-load tolerates a torn read between immutable bundle metadata and current
  namespace/worker secrets. Per-version secret snapshots would be a new protocol.
- Admin host must stay outside `PLATFORM_DOMAIN`; gateway's `ADMIN_HOST` branch is a
  control-plane ingress shortcut, not tenant routing.
- Delete locks cover the Redis lifecycle critical section. Cleanup after a committed
  delete is represented by durable cleanup intent, not by holding the lock through S3
  work.
- Streaming responses, WebSocket upgrades, empty responses, `HEAD`, and result
  envelopes are explicit error-contract exceptions.
- Main queue streams are durable and intentionally unbounded; auxiliary diagnostic or
  activation streams may be bounded.

When a review finds a suspected issue in one of these areas, first decide whether the
current behavior violates the contract above. If the behavior is accepted, update the
owning active doc or add a style-contract guard instead of landing a cosmetic "fix".

## PR Review Gate

Protocol-affecting PRs need both local and integration validation:

- control/admin protocol changes: unit tests for request/response shape, plus targeted
  control/auth/gateway integration when route, auth, deploy, or lifecycle behavior
  changes;
- runtime binding metadata changes: runtime/load unit tests, binding facade tests, and
  targeted integration for the affected binding;
- Redis key or payload changes: source-scan drift guard plus targeted integration for
  every writer/reader pair;
- Rust service state-machine changes: crate tests, Rust check/clippy, and targeted
  integration for the service boundary;
- cross-tier wire changes: update the accepting side, sending side, docs, and tests in
  one deployable boundary unless an external rollout requires a staged plan.

Full integration remains required before calling a code/runtime/config boundary
commit-ready when protocol behavior, runtime config, Redis shape, or state-machine
logic changed.

# Source Map

This document records the core service source tree ownership map. It is intentionally
more concrete than the architecture overview and less semantic than the module docs.
Repository meta files, CI definitions, downstream tenant-facing docs, and docs sources
are outside this map unless they own runtime or deployable service behavior.

## Runtime And Control Workerd Tiers

| Path | Responsibility |
|---|---|
| `docker-compose.yml` | Local developer stack: Valkey, s3mock, gateway, user-runtime, system-runtime, d1-runtime, do-runtime, scheduler, workflows, redis-proxy sidecars, supervisors, and optional `d1-multi` / `do-multi` profiles. |
| `docker-compose.images.yml` | Local stack override that pulls published `docker.io/getwdl/wdl-workerd` and `docker.io/getwdl/wdl-rust` images instead of relying on locally built image tags. |
| `Dockerfile.workerd` | Workerd-side image. Builds supervisors, compiles workerd configs, extracts `workerd` from npm, and ships compiled `dist/workerd-configs/*.bin` without `node_modules`. |
| `Dockerfile.rust` | Unified Rust image for redis-proxy, scheduler, and workflows binaries; runtime command chooses the service. |
| `envoy/envoy.yaml` | Local Compose private mesh proxy used by integration tests and local development. |
| `gateway/config.capnp` | Gateway workerd config: public `:8080`, `RUNTIME_USER`, `RUNTIME_SYSTEM`, and `CONTROL` externals. |
| `gateway/config-local.capnp` | Local gateway workerd config compiled for Docker Compose with Envoy-backed private service routes. |
| `gateway/index.js` | Gateway worker dispatch branches: admin-host short-circuit, subdomain routing, and pattern routing. |
| `gateway/dispatch.js` | Pure gateway dispatch decision tree and route target selection. |
| `gateway/holder.js`, `gateway/websocket.js` | WebSocket holder, reconnect forwarding, and `101` upgrade preservation. |
| `gateway/runtime.js` | Gateway route/pattern caches, Redis subscriber invalidation, logging, metrics, and health/metrics snapshots. |
| `gateway/lib.js` | Pure routing helpers used by workerd and Node tests. |
| `runtime/config-user.capnp` | User runtime config: loader `:8081`, internal `:8088`, public-only loaded-worker outbound. |
| `runtime/config-system.capnp` | System runtime config: loader `:8081`, internal `:8088`, control `:8082`, auth worker, private+public outbound. |
| `runtime/config-user-local.capnp`, `runtime/config-system-local.capnp` | Local runtime workerd configs compiled for Docker Compose with Envoy-backed private service routes. |
| `runtime/index.js` | Runtime loader socket entrypoint. |
| `runtime/internal.js` | Private `:8088` runtime dispatch surface for scheduled, queue, workflow run/notify, and other platform-only events. |
| `runtime/runtime.js` | Service-name binding, loaded-worker registry, sibling eviction, logger, metrics, and request-scope setup. |
| `runtime/metrics.js` | Runtime Prometheus snapshot helpers and bounded metric aggregation. |
| `runtime/dispatch.js` and `runtime/dispatch/*` | Fetch, scheduled, queue, workflow dispatch, workflow step facade, replay cache, and deterministic workflow JSON helpers. |
| `runtime/load.js` and `runtime/load/*` | Bundle decode, module rewrite, env construction, wrapper generation, injected runtime source ownership, and hidden binding stripping. |
| `runtime/bindings/` | Host-side binding adapters for KV, D1, R2, Durable Objects, ASSETS, service, and queue. |
| `runtime/workflows-client.js`, `runtime/dispatch/workflow-*.js`, `runtime/load/env-build.js` | Workflow binding materialization, backend client, dispatch facade, replay cache, and step semantics. |
| `runtime/tail-worker.js` / `runtime/tail-forwarder.js` | Workerd tail capture plus activation-gated append path for `wdl tail`. |
| `runtime/lib.js` | Pure runtime helpers such as bundle-to-worker-code, byte normalization, and dispatch body normalization. |
| `control/index.js` | Thin HTTP dispatcher on system-runtime `:8082`; delegates to handlers after auth. |
| `control/handlers/` | Endpoint handlers for deploy, promote, versions, workers, delete, secrets, hosts, reload, auth tokens, D1, R2, workflows, and log tail. |
| `control/shared.js` | Control singletons, auth wrapper, Redis publish helpers, state-bound workflow transport wiring, and shared lifecycle/delete helpers. Direct `state.*` access belongs here or in the dispatcher. |
| `control/errors.js`, `control/json-body.js`, `control/optimistic.js` | Pure Control error-response and bounded JSON request-body contracts plus the strict `WatchError`/Redis-session adapter over the shared optimistic retry loop, re-exported by `control/shared.js`. |
| `control/workflows-client.js` | Internal Control-to-Workflows POST transport with explicit caller-owned timeout selection; callers own endpoint-specific response interpretation. |
| `control/lib.js` | Pure Control data-shaping: route-to-action classification, key helpers, canonical bundle `__meta__` parsing, and referrer redaction. |
| `control/bundle.js` | Bundle/module normalization, compatibility metadata, vars, and emitted module manifest construction. |
| `control/bindings.js` | Service/platform binding parsers, ACL evaluation, and linker helpers. |
| `control/topology.js` | Route, pattern, cron, queue consumer, and workflow declaration parsing for deploy metadata. |
| `control/routing.js`, `control/routing/route-plan.js` | Promote, secret bump/promote, host reconcile WATCH/MULTI loops, and pure route/pattern planning helpers. |
| `control/lifecycle-indexes.js` | Redis mutation helpers for worker lifecycle, cron, queue consumer, and referrer indexes. |
| `control/env-budget.js` | Control-plane estimate of workerd `workerLoader` env size for deploy and secret mutation guards. |
| `control/worker-code-budget.js` | Control-plane final WorkerCode size estimate for deploy guards, sharing runtime and do-runtime wrapper/module injection rules. |
| `control/d1-*` | D1 control metadata, store, lifecycle, migration, and d1-runtime client modules. |
| `control/r2.js` | Control-plane R2 bucket/object API client for the configured S3-compatible store. |
| `control/s3.js` | S3-compatible ASSETS upload helper. |
| `control/cron-index.js` | Cron identity and diff helpers shared by promote logic. |
| `auth/index.js`, `auth/lib.js`, `auth/runtime.js` | Static socket-less auth worker, pure auth helpers, bootstrap token upsert, role evaluation, and Redis-backed token store. |

## Shared JavaScript

| Path | Responsibility |
|---|---|
| `shared/redis.js`, `shared/redis-*.js` | Public Redis import surface plus split RESP codec, per-call client, WATCH/MULTI session, and subscriber loop modules. Runtime hot paths prefer the Rust redis-proxy sidecar. |
| `shared/redis-lock.js` | Token-fenced Redis lock creation, acquire, renewal, and best-effort token-scoped release shared by Control and Auth. |
| `shared/optimistic-retry.js` | Generic bounded optimistic retry loop used by Control, Auth, and the D1/DO owner-lease adapters. |
| `shared/owner-endpoint.js`, `shared/owner-lease.js`, `shared/owner-protocol.js`, `shared/owner-forwarder.js` | Shared owner endpoint grammar, lease parsing, generation counters, key derivation, fence matching, staged Redis owner writes, and authenticated forwarding mechanics used by Control and the D1/DO runtimes. |
| `shared/auth-roles.js` | Role table, principal validation, reserved namespace policy, and auth action capabilities. |
| `shared/auth-token.js` | Shared `x-admin-token` sanitizer used by control and auth. |
| `shared/internal-auth.js` | Shared internal mesh auth header and token helpers used by JS callers and receivers. |
| `shared/secret-envelope.js`, `shared/secret-keys.js` | Secret envelope encryption/decryption, canonical base64/JSON handling, AAD binding helpers, and secret Redis key construction. |
| `shared/base64.js` | Dependency-free byte/text base64 codec shared by workerd tiers, with a `Buffer` fast path under `nodejs_compat`. |
| `shared/hex.js`, `shared/random-id.js`, `shared/errors.js` | Small dependency-free primitives for byte-to-hex rendering, random hex ids, and string-only error message extraction. |
| `shared/observability.js` | Structured logger, metrics registry, request-id helpers, and log-level handling for JS tiers. |
| `shared/respond.js` | Shared HTTP response, JSON error, Prometheus text, best-effort response body discard, and `x-request-id` echo helpers. |
| `shared/bounded-body.js` | Shared bounded byte-stream and request-body readers; each tier maps limit errors to its own contract. |
| `shared/ns-pattern.js` | Platform-domain normalization plus namespace, worker, binding, queue, KV/D1/R2 id, module path, reserved object-key, and reserved namespace grammars. |
| `shared/worker-contract.js` | Worker version grammar plus worker, route-plane, lifecycle, DO owner-scope key, and route-invalidation channel helpers. |
| `shared/workerd-compat-flags.js` | Pinned upstream mirror of experimental enable flags plus WDL-owned dynamic-worker date and error-serialization policy. |
| `shared/queue-keys.js` | JavaScript queue key helpers used by tests and cross-tier key-shape checks. |
| `shared/route-projection.js` | Compact pattern-route projection encoding shared by control writers, delete checks, and gateway readers. |
| `shared/d1-*.js`, `shared/sql-splitter.js` | D1 parameter, data-field, transport, timeout, query-wire, and SQL splitting utilities shared by runtime, d1-runtime, control, and tests. |
| `shared/fnv1a32.js` | Shared JavaScript FNV-1a helpers for runtime-side shard and slot hashing. |
| `shared/s3-query.js` | S3 query encoder used by the s3-cleanup system worker; runtime R2 keeps the same standalone helper in `runtime/r2-utils.js` because that file is injected as worker source. |
| `shared/s3-retry.js` | Bounded transient retry policy for idempotent S3 POST operations, shared by runtime R2 and the s3-cleanup worker. |
| `shared/s3-xml.js` | Shared S3 XML parsing helpers used by control R2, runtime R2, and system cleanup paths. |
| `shared/worker-id.js` | Shared `x-worker-id` formatting, parsing, and runtime-load identity grammar used by gateway, runtime, DO runtime, and tests. |
| `shared/cron-time.js` | Control-side cron parsing and slot-alignment helpers; scheduler advancement uses Rust `croner`. |
| `shared/vendor/` | Pre-bundled third-party dependencies regenerated by `npm run build:vendor`. |
| `types/workerd-embedded.d.ts` | Ambient TypeScript declarations for workerd-embedded module specifiers such as `*-source` aliases used by embedded runtime bundles. |

## Stateful Workerd Tiers

| Path | Responsibility |
|---|---|
| `d1-runtime/` | D1 workerd service. Supervisor is PID 1, spawns workerd, renews leases, and drains on SIGTERM. Router/actor/owner modules implement per-database ownership, forwarding, read cache, and SQLite localDisk execution. |
| `do-runtime/` | Durable Object workerd service. Supervisor is PID 1, spawns workerd, renews owned shards, drains on SIGTERM, and SIGKILLs workerd after successful drain to avoid the half-dead 504 window. Owner/actor/load/alarm modules implement owner scopes, native facet execution, SQLite storage, Workflows alarm client/shim/dispatch endpoint, and WebSocket connect. |
| `do-runtime/config-local.capnp` | Local Durable Objects runtime workerd config compiled for Docker Compose with Envoy-backed private service routes. |

## Rust Workspace

| Path | Responsibility |
|---|---|
| `rust/redis-proxy/` | Runtime sidecar for cold-load, secret decrypt, KV, queue producer, and log-tail sidecar APIs. |
| `rust/scheduler/` | Cron, queue, delayed queue, orphan migration, and workflow tick scheduler. |
| `rust/workflows/` | Workflows service, DB 2 state machine, and internal DO alarm backend jobs. |
| `rust/supervisor/` | D1/DO supervisor binaries. |
| `rust/common/` | Shared Rust utilities such as worker-contract grammar and keys, time, logging, internal-auth matching, Redis connection primitives, and metrics primitives. |

## System Workers, Fixtures, And Examples

| Path | Responsibility |
|---|---|
| `system-workers/s3-cleanup/` | Permanent `__system__` worker for post-delete ASSETS cleanup. It consumes `worker-delete-s3-cleanup`, persists task state in D1, and uses cron for replay. |
| `test-workers/` | Integration-owned worker fixtures. Tests may depend on their exact shape. |
| `examples/` | Manual demos and reference projects. Tests should not silently depend on them unless the fixture graduates to `test-workers/`. |
| `scripts/run-integration-tests.js` | Integration worker-pool runner. |
| `scripts/compile-workerd-configs.js` | Compiles workerd Cap'n Proto configs into `dist/workerd-configs/*.bin`. |
| `scripts/extract-workerd-experimental-compat-flags.mjs` | Pin-bump experimental-flag extractor. |

## Infrastructure

| Path | Responsibility |
|---|---|
| `terraform/` | AWS ECS-shaped environment: ECS, Valkey, EFS, S3/R2, and ALB rules. |
| `deploy/kubernetes/` | Kustomize-based local and portable Kubernetes manifests. |

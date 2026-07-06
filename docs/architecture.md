# Architecture Overview

This document is the high-level map of WDL. It explains the deployment units, trust
boundaries, state ownership, and request paths that the module docs expand in detail.
Current code and tests remain authoritative; use this document to choose the right
module doc before changing behavior.

For the concise repository-wide invariants used by coding agents, see
[CLAUDE.md](../CLAUDE.md).

## System Shape

WDL is a self-hosted multi-tenant Workers platform built on stock Cloudflare workerd. It
does not patch workerd. User bundles are stored in Valkey/Redis and loaded dynamically
through workerd's `workerLoader` API. Every tenant request is scoped to a namespace,
which is also the account boundary used by routing, bindings, secrets, and lifecycle
indexes.

The platform is split into these service families:

- Gateway: public ingress and admin-host ingress.
- Runtime pools: user-runtime and system-runtime workerd services.
- Stateful runtimes: d1-runtime and do-runtime workerd services with localDisk storage
  on EFS.
- Rust services: scheduler, workflows, redis-proxy sidecars, and supervisors.
- Control/auth: static workerd workers hosted through system-runtime.
- Data stores: Valkey logical DBs, EFS localDisk for D1/DO, and S3-compatible object
  storage for assets/R2 and cleanup.

The deployable app service inventory is:

- `gateway`: one workerd container on public ingress. It routes tenant and admin-host
  traffic but does not authorize control-plane actions.
- `user-runtime`: workerd runtime pool for tenant workers. It has a local `redis-proxy`
  sidecar and exposes the loader socket plus the private internal dispatch socket.
- `system-runtime`: workerd runtime pool for control/auth/static system workers and
  privileged `__system__` loaded workers. It also has a local `redis-proxy` sidecar.
- `d1-runtime`: workerd service for D1 SQLite execution. A `supervisor` process is PID
  1, owns local drain/renew orchestration, and spawns workerd as a child.
- `do-runtime`: workerd service for Durable Object facet execution. A `supervisor`
  process is PID 1, owns local drain/renew orchestration, and spawns workerd as a child.
- `scheduler`: Rust service for cron, queue, and workflow tick dispatch.
- `workflows`: Rust service that owns workflow instance state in Valkey DB 2.

In local compose these services run with Valkey/Redis and `s3mock`. Production-shaped
environments replace those dependencies with managed or provisioned equivalents. Each
service can run multiple replicas when the owning module's concurrency contract allows
it; D1/DO require per-replica storage identity when scaled beyond one task.

The production HA model is single-region and replica-oriented. WDL does not provide a
global edge control plane or cross-region replication, but it does provide explicit
recovery contracts inside one operator-owned region: stateless service families can be
replicated behind service discovery, stateful owners are protected by Redis leases and
generation fences, scheduler projections are repairable, and workflows progress is
guarded by DB 2 leases and run tokens. A task or pod replacement should therefore be a
recoverable event rather than a metadata mutation.

Important sockets and ports:

- Gateway public/admin ingress: `:8080`.
- Runtime loader socket: `:8081`.
- Runtime internal socket for scheduler/workflows dispatch: `:8088`.
- Control worker on system-runtime: `:8082`.
- D1 runtime: `:8787`.
- DO runtime: `:8788`.
- Workflows service: `:9120`.

Source layout follows the service boundaries:

- JavaScript/workerd tiers: `gateway/`, `runtime/`, `d1-runtime/`, `do-runtime/`,
  `control/`, `auth/`, `shared/`, `system-workers/`, `test-workers/`, and
  `examples/`.
- Rust workspace: `rust/redis-proxy/`, `rust/scheduler/`, `rust/supervisor/`,
  `rust/workflows/`, and `rust/common/`.

Workerd tiers use `index.js` as entrypoint, `config*.capnp` as the workerd config, and
pure `lib.js` helpers where unit testing is practical. Tier-local `runtime.js` files own
Redis, cache, subscriber, or logging mechanics so entry files stay focused on dispatch.
`shared/` is embedded into workerd configs with `embed "../shared/*"`; pre-bundled npm
dependencies live under `shared/vendor/` so workerd does not resolve `node_modules`.

The module docs are the current detailed references for each family:

- [Gateway](modules/gateway.md)
- [Runtime loader and bindings](modules/runtime.md)
- [Control and auth](modules/control-auth.md)
- [Durable Objects](modules/durable-objects.md)
- [D1](modules/d1.md)
- [Queues and cron](modules/queues-cron.md)
- [Workflows](modules/workflows.md)
- [Log tail and observability](modules/log-tail-observability.md)
- [Infra and deployment](modules/infra.md)

For a feature-by-feature view of Cloudflare Workers compatibility, read the [Workers
compatibility matrix](compatibility.md). For a cross-cutting view of trust zones and
internal mesh assumptions, read the [Security model](security.md). The architecture
overview describes the service shape; those documents record compatibility and security
posture.

## Main Request Paths

Tenant HTTP/WebSocket traffic:

1. Client reaches gateway on the public socket.
2. Gateway resolves subdomain or pattern routes from Redis.
3. Gateway forwards to the runtime loader socket with `x-worker-id`.
4. Runtime loads the immutable worker bundle through `workerLoader`.
5. Runtime materializes bindings and invokes the worker.

WebSocket upgrades first land on gateway's WebSocket holder Durable Object before
backend forwarding, so long-lived `101` responses do not stay on the ordinary gateway
request IoContext.

Admin/control traffic:

1. Client reaches the control URL.
2. Gateway's `ADMIN_HOST` branch forwards to control in system-runtime.
3. Control asks auth to verify the token and action.
4. Control handlers mutate Redis metadata, object storage, or service-specific control
   APIs.

Cron and queue dispatch:

1. Control promotes cron and queue consumer projections into Redis.
2. Scheduler discovers due work.
3. Scheduler posts to runtime's private internal socket on `:8088`.
4. Runtime invokes `scheduled()` or `queue()` on the loaded worker.

Stateful binding calls:

- D1 facades call d1-runtime. D1 ownership is per physical database and fenced by owner
  generation.
- Durable Object facades call do-runtime. DO ownership is per owner scope and native
  facet storage is protected by owner lease plus generation.
- Workflow facades call workflows. The workflows service owns DB 2 instance state and
  dispatches runs back to runtime through `:8088`.
- Queue producers write through redis-proxy to DB 1 after runtime-side caps.
- R2 uses platform S3-compatible credentials at runtime for mutable tenant object data.
- ASSETS are immutable deploy artifacts uploaded by control; runtime only constructs
  tokenized CDN URLs from bundle metadata and `ASSETS_CDN_BASE`.
- KV uses redis-proxy DB 1 key families. Service and platform bindings are in-isolate
  JSRPC surfaces whose ACL and target metadata are resolved by control/runtime.

## Trust Boundaries

Gateway is routing, not authorization. Control/auth own control-plane authorization.
Runtime wrappers own tenant env shaping and hidden binding stripping.

Privileged runtime entrypoints are separated by socket, not by public path reservation.
Tenant traffic goes through the runtime loader socket. Scheduler and workflows dispatch
use the private runtime internal socket on `:8088`.

Hidden Fetcher bindings such as DO, D1, and workflows backends are platform plumbing.
They must not be exposed to user code. Runtime wrappers remove those bindings before
tenant code observes `env`.

Tenant-running Fargate task roles must stay least-privilege; tenant code must not
receive broad cloud credentials through task metadata.

## State Ownership

Valkey logical DBs are split by authority:

- DB 0: control-plane metadata, route state, lifecycle indexes, secrets, referrer
  indexes, D1/DO metadata, cron config, and queue consumer projections.
- DB 1: data-plane KV, queue streams, delayed queues, log-tail streams, and cleanup
  queues.
- DB 2: workflows workflow instance state.

Indexes are usually rebuildable projections. Authority lives in the owning hash, stream,
or lifecycle record named in each module doc. Do not add fallback SCAN paths or
secondary writers without documenting whether the index is authoritative or repairable.

## Failure Model

WDL prefers explicit fences over implicit ordering:

- Control route and lifecycle writes use WATCH/MULTI boundaries.
- D1 and DO owner records include task identity plus monotonic generation.
- DO alarms distinguish SQLite row tokens from Workflows DB2 run tokens.
- Workflows execution commits use generation/run-token fences while lifecycle commits
  rotate generation.
- Queue and cron scheduler indexes are non-authoritative and repairable.

Replica failover follows those same fences. Gateway/runtime replacement is stateless
apart from local caches and loaded isolates. D1 failover is per physical database; DO
failover is per owner scope. A new owner claims only after the previous lease is gone or
released by drain, then advances the generation so stale owners fail later owner-side
checks. Scheduler replicas may race to observe due work, but dispatch paths use Redis
claiming or repairable projections rather than local process memory as authority.

Transport failure and user-code failure are separate. Runtime handler errors should not
become scheduler transport retries unless the transport contract says so.

## Rollout Order

Roll the side that adds a new endpoint or accepts a new body shape before the side that
calls it. Common cases:

- Runtime internal `:8088` route changes: roll runtime before scheduler or workflows
  starts calling the new route.
- Workflows API shape changes called by runtime/control: roll workflows first, then
  callers.
- Binding facade protocol changes: roll runtime with the affected stateful runtime or
  Rust service.
- Redis key ownership changes: update writers, readers, and style-contract tests in the
  same deployable boundary.

Use [Infra and deployment](modules/infra.md) for environment-specific rollout rules.

## Development Standards

- [Project standards](project-standards.md) cover cross-language contracts, security
  boundaries, observability, JS, Rust, tests, docs, and deployment code.
- [Workerd JavaScript standards](workerd-js-standards.md) cover gateway, runtime,
  control, auth, d1-runtime, do-runtime, shared JS, and JS tests.
- [Rust service and sidecar standards](rust-sidecar-standards.md) cover scheduler,
  workflows, redis-proxy, supervisor, and shared `rust/common/` primitives in the
  `rust/` Cargo workspace.

Both standards follow the same refactor discipline: define one deployable boundary, keep
tests tied to real contracts, stage for review, and run the smallest checks that protect
the touched behavior.

# Infrastructure And Deployment

## Purpose

Infrastructure docs describe how local compose, Terraform-managed environments, and
Kubernetes manifests map the platform services onto Redis, local Envoy mesh, ECS
Service Connect, EFS, S3/R2-compatible storage, and release pipelines.

## Current Implementation

There are two main infrastructure families:

- `terraform/`: the reusable AWS ECS/Fargate template that also backs the WDL hosted
  preview.
- `deploy/kubernetes/`: a Kustomize self-hosting template and cluster smoke baseline.

Terraform is both a checked-in reference deployment and the implementation used for the
hosted preview; operators apply it from their own environment-specific inputs.
Kubernetes manifests are release artifacts and a starting template for cluster-shaped
self-hosting. In either path, the target environment's operator workflow owns final
capacity, regions, and rollout policy.

Local development uses `docker-compose.yml` and profiles such as `d1-multi` and
`do-multi`.

Service families:

- workerd pools: gateway, user-runtime, system-runtime
- stateful runtimes: d1-runtime, do-runtime
- Rust services: scheduler, workflows, redis-proxy sidecars, supervisors
- data plane storage with Redis-compatible logical DBs, S3-compatible object
  buckets, and EFS localDisk for D1/DO

Local compose is the developer convenience environment, not the production delivery
contract. It starts the same service families with local ports, Valkey, `s3mock`, and
an Envoy mesh for private service hops; profiles such as `d1-multi` and
`do-multi` exercise local multi-replica behavior for targeted tests. The
production-shaped delivery examples are Terraform and the Kubernetes manifests under
`deploy/kubernetes/`.

These paths preserve the service boundaries, private mesh assumptions, image contracts,
health/metrics endpoints, and ownership/failover rules that the runtime modules depend
on. The Kubernetes tree is a user-facing template and smoke baseline; Terraform records
a reusable ECS profile and backs the hosted preview. Neither defines a universal WDL
production-capacity contract. Operators choose concrete capacity, managed Redis/Valkey
durability, object storage, EFS or equivalent localDisk persistence, ingress
protection, and regional backup/restore policy.

The app services intentionally keep one container boundary per deployable service,
except for co-located sidecars:

- user-runtime and system-runtime co-locate `redis-proxy`.
- d1-runtime and do-runtime run through the Rust `supervisor`, which is PID 1 and owns
  local SIGTERM drain/renew before stopping the child workerd process.
- scheduler, workflows, gateway, and the stateless runtime pools otherwise run as their
  own service tasks/pods.

## Interfaces

- Public ingress through ALB/gateway. Terraform-managed ALB ingress can terminate the
  admin host, platform wildcard, canonical site host, and additional exact public
  hosts on the same gateway service.
- Admin-host ingress through gateway's `ADMIN_HOST` branch to system-runtime control.
- Local Compose private service hops use `envoy/envoy.yaml` and the `*-local.capnp`
  workerd configs compiled into `dist/workerd-configs`.
- ECS Service Connect covers runtime, D1, DO, and workflows service targets.
- Service Connect targets that carry WebSockets must keep HTTP semantics that preserve
  HTTP/1.1 Upgrade. Do not silently downgrade gateway/runtime/DO traffic to a plain L4
  path unless the 101 upgrade path has been revalidated.
- `redis-proxy` runs as a local sidecar beside runtime/DO tasks.
- AI provider traffic leaves user-runtime, system-runtime, and do-runtime through a
  dedicated public-only `AI_NETWORK` service. It does not use system-runtime's broader
  private-and-public default outbound service.
- Scheduler joins the Service Connect namespace as a client for runtime internal
  dispatch and workflows tick, while Valkey/Redis access uses its own connection
  configuration. Workflows delivers Durable Object alarms to do-runtime through
  `/internal/do/alarms/dispatch`.
- GitHub release workflow publishes release images from `wdl.*` tag pushes,
  validates `VERSION` and `CHANGELOG.md` against the tag, and can also run
  manually for validation or publish reruns.
- Terraform apply for Terraform-managed infrastructure.

GitHub Actions is the pull-request and `main` validation gate for JavaScript,
Rust, and hygiene checks. The Docker Compose integration suite needs Docker Hub
and Build Cloud credentials, so it only runs on trusted pushes. The release
workflow under `.github/workflows/` builds and publishes Docker Hub/GHCR images
from `wdl.*` tag pushes after checking `VERSION` and `CHANGELOG.md`; manual runs
can validate or publish the same build path, and it is not a PR validation gate.

Gateway and runtime use stable internal port contracts across environments:

- `gateway :8080` is the only public HTTP/WebSocket socket.
- `runtime :8081` is the loader socket used by gateway.
- `runtime :8088` is private and is used by scheduler and workflows dispatch.
- `system-runtime :8082` hosts control behind gateway's admin-host branch.
- `d1-runtime :8787`, `do-runtime :8788`, and `workflows :9120` are private mesh
  endpoints.
- `redis-proxy :7070` is a local sidecar socket for colocated runtime/DO tasks.

Do not expose private mesh endpoints through public ingress. K8s and ECS delivery must
make the same assumption true with Service Connect, ClusterIP Services, NetworkPolicy,
or equivalent controls.

Private mesh callers and receivers share `WDL_INTERNAL_AUTH_TOKEN` as the current
internal token. Calls between runtime, d1-runtime, do-runtime, scheduler,
workflows, and redis-proxy sidecars carry it as `x-wdl-internal-auth`. Receivers
also accept optional `WDL_INTERNAL_AUTH_PREVIOUS_TOKEN` during rotation; callers
always send only the current token. Receivers require exactly one auth header.
Both token values must contain only visible ASCII bytes, with no whitespace or commas,
so Fetch header normalization preserves them exactly and header joining cannot make
repeated headers look like one configured token. Health and metrics endpoints are the
only unauthenticated service endpoints. The token is platform plumbing,
not a tenant binding: runtime wrapper code strips it from tenant-visible `env`, host-owned
DO proxies and host-side backend capabilities add it for DO forwarding, and spoofed tenant
headers are removed before forwarding.

AI runtime limits are environment inputs on user-runtime, system-runtime, and
do-runtime. Defaults and runtime hard maxima are owned by
`shared/ai-runtime-config.js`. The Terraform profile uses those code defaults for
user-runtime and do-runtime and only pins the smaller system-runtime capacity override.
Local Compose and the Kubernetes template repeat the defaults so operators can override
them and tests pin those surfaces to the code owner:

| Variable | Default | Scope |
| --- | ---: | --- |
| `AI_REQUEST_MAX_IN_FLIGHT` | `64` (`32` on system-runtime) | Model-list and HTTP body-admission calls per runtime replica; non-streaming calls remain here through completion. |
| `AI_STREAM_MAX_IN_FLIGHT` | `64` (`16` on system-runtime) | Open SSE streams after request-pool body admission per runtime replica. |
| `AI_WS_MAX_SESSIONS` | `32` (`8` on system-runtime) | Open provider WebSockets per runtime replica. |
| `AI_REQUEST_BUDGET_MS` | `120000` | Model-list and HTTP setup deadline; non-streaming inference remains covered through completion, while SSE switches to its stream-duration bound after response headers. |
| `AI_STREAM_IDLE_TIMEOUT_MS` | `30000` | Maximum SSE interval while actively waiting for provider bytes; downstream backpressure pauses this clock. |
| `AI_STREAM_MAX_DURATION_MS` | `300000` | Absolute SSE lifetime. |
| `AI_WS_HANDSHAKE_BUDGET_MS` | `15000` | Provider WebSocket handshake deadline. |
| `AI_WS_IDLE_TIMEOUT_MS` | `120000` | Maximum WebSocket interval with no provider frame. |
| `AI_WS_MAX_DURATION_MS` | `1440000` | Operator WebSocket lifetime cap before adapter-specific clamping. |

These are replica-local admission pools, not namespace quota, billing controls, or a
reservation of each request's maximum byte allowance. User and DO workloads use the
higher defaults; system-runtime keeps lower defaults because AI there is an exceptional
operator-controlled workload. The Kubernetes and Terraform profiles are deployment
starting points, not a universal WDL production capacity guarantee. Operators can
override the settings per runtime service without redefining the binding contract.
User-runtime and do-runtime have independent pools because they are separate services.
Workerd exposes no JavaScript queued-byte or WebSocket backpressure signal, so operators
must size the session pool and runtime memory together for their frame and
receiver-speed distribution. The code-owned request, response, frame, and aggregate
byte caps are documented in the [AI module](ai.md).

AI client source is embedded only by the three base module owners:
`runtime/config-user.capnp`, `runtime/config-system.capnp`, and
`do-runtime/config.capnp`. Their local and evictable configs import those worker graphs.
Cap'n Proto service lists do not inherit, so every concrete user-runtime,
system-runtime, and do-runtime config declares the `ai-public-network` service it binds.
Production configs use a public-only network service; local configs route the same
binding to the deterministic fake provider Worker.

## Redis / Storage Contracts

Logical DB split:

- DB 0 on the Control endpoint: control-plane metadata.
- DB 0 on the Workflows endpoint: only the Workflows-owned schema-reset state. It is
  the same logical database when Control and Workflows share an endpoint, but a different
  server when `WORKFLOWS_REDIS_URL` selects a dedicated endpoint.
- DB 1: data-plane KV, queue, log-tail streams.
- DB 2: workflows instance state.
- DB 15: inactive schema-2 Workflow archive while migration is pending.

Stateful storage:

- D1 localDisk on EFS.
- DO localDisk on separate EFS.
- Assets/R2 in S3-compatible bucket namespace.
- Workflow payload refs remain in DB 2 under caps unless application stores external
  references.

## Ownership / Failure Semantics

- Scheduler defaults to one replica in deployment, and current dispatch paths are
  multi-replica safe. Terraform starts its Fargate replacement before draining the old
  task, so a routine rollout does not intentionally pause scheduling.
- Workflows is a separate Rust service.
- By default, Scheduler and Workflows drain in-flight work for up to 25 seconds.
  Compose, Kubernetes, and Terraform ECS pin 30-second stop windows so the platform
  does not terminate either process before its default application drain closes.
  Deployments that override `SCHEDULER_SHUTDOWN_DRAIN_MS` or
  `WORKFLOWS_SHUTDOWN_DRAIN_MS` should review the corresponding stop window.
- Gateway, user-runtime, and system-runtime can be horizontally replicated behind the
  environment's load balancing and service discovery layer. Local route caches, loaded
  isolates, and owner hints are optimizations, not authority.
- D1/DO use owner leases, monotonic generation fences, and local drain/renew. Scaling
  them beyond one task requires stable per-replica storage identity and private local
  supervisor access so drain/renew never targets another replica by service alias.
- Ordinary D1/DO task loss falls back to lease expiry and takeover by another replica;
  graceful rollout should prefer supervisor drain so ownership is released before the
  child workerd process exits.
- Terraform runs this application stack's services on ECS Fargate. Capacity policy
  changes should document which services can use `FARGATE_SPOT`; stateful runtimes and
  singleton control loops should stay on on-demand Fargate unless their interruption
  semantics are re-reviewed.
- In addition to the Fargate task memory limit, D1 and DO workerd containers set
  explicit container memory hard limits. DO also reserves memory for its local
  redis-proxy sidecar.
- do-runtime defaults `DO_PREVENT_EVICTION` to `true`, keeping host actors resident so
  current workerd actor eviction cannot interrupt in-flight hibernatable WebSocket
  operations. Explicit `false` enables eviction for validated workloads but is not a
  replacement for the container memory hard limit.
- All Terraform Fargate services use start-before-stop rolling replacement with
  `maximum_percent = 200` and `minimum_healthy_percent = 100`. Wire-compatible D1/DO
  releases safely overlap router tasks while owner lease, generation fencing, and
  supervisor drain control state handoff; release Changelog gates override this default
  when a wire transition cannot safely overlap. Scheduler overlap is covered by per-path
  Redis claims and fences.
- D1/DO and scheduler keep Availability Zone rebalancing disabled so ECS does not
  initiate unrelated owner or control-loop relocation. This setting does not serialize
  an explicit deployment.

## Security Boundaries

- user-runtime loaded worker outbound is public-only.
- system-runtime is privileged by design.
- Runtime internal `:8088`, d1-runtime `:8787`, do-runtime `:8788`, workflows `:9120`,
  and Redis are private mesh services. Private service calls also require
  `x-wdl-internal-auth` with the shared `WDL_INTERNAL_AUTH_TOKEN`.
- Kubernetes NetworkPolicies use per-component caller sets. ECS intentionally groups
  user-runtime, system-runtime, D1, and DO tasks in one runtime security group, so its
  network rules are coarser and are not required to match the Kubernetes caller matrix.
- Tenant-running runtime tasks use least-privilege ECS task roles, public-only workerd
  outbound bindings, and private mesh security groups as their cloud credential and
  network boundary. ECS Exec should be enabled only where operator access is intended.

## Observability

- Platform services emit structured logs.
- Gateway, user-runtime, system-runtime, d1-runtime, do-runtime, scheduler, workflows,
  and redis-proxy expose Prometheus metrics where configured; endpoint paths differ by
  service and are listed in `log-tail-observability.md`.
- Terraform enables ECS Container Insights with enhanced observability by default. That
  is AWS infrastructure telemetry for cluster, service, task, and container health and
  utilization. It is separate from WDL's own Prometheus metrics and bounded-label
  logging contracts.
- CloudWatch/EFK log ingestion beyond the Terraform defaults is deployment-configured.

## Deployment / Rollout Notes

Cross-tier protocol changes use one dependency-driven procedure rather than a permanent
service list:

1. Roll readers or receivers that accept both the current and next shape.
2. Before rolling a writer tier, pause the affected mutation surface when mixed writer
   versions could commit incompatible state.
3. Roll writers or callers that emit the new shape, wait for old writers to drain, then
   resume the paused surface.

The release changelog names the concrete services and any additional gate for each
version. Services whose contracts are unchanged do not acquire an ordering requirement.

Internal auth rotation is dual-read / single-write, but it is not rolling-safe:
callers always send `WDL_INTERNAL_AUTH_TOKEN`, and receivers accept current plus
optional previous. Rotate it during a maintenance window or after pausing
scheduler/workflows traffic. Configure the old value as
`WDL_INTERNAL_AUTH_PREVIOUS_TOKEN` and the new value as `WDL_INTERNAL_AUTH_TOKEN`
across all private services, roll/restart the private fleet together, then clear
`WDL_INTERNAL_AUTH_PREVIOUS_TOKEN` with a second roll after the fleet converges.

WDL does not generally guarantee binary, runtime, configuration, schema, or storage
downgrades. The notes below identify known hazards and best-effort recovery steps; they
are not an exhaustive or guaranteed rollback procedure.

When attempting a workerd downgrade, verify that every retained Dynamic Worker version
uses a `compatibility_date` no later than the maximum supported by the target binary.
Redeploy or remove incompatible retained versions before restoring traffic; runtime
health checks do not cold-load every retained bundle.

The workerd 2026-07-17 stateful-runtime upgrade is forward-only relative to
2026-07-01. Starting any workerd 2026-07-03 or later adds an `actor_name` column to
the native alarm scheduler's `_cf_ALARM` table in each localDisk `metadata.sqlite`;
2026-07-01 then fails during startup against that schema. Forward upgrades need no
operator action. For a best-effort downgrade of D1 or DO to 2026-07-01:

1. Stop every D1 and DO runtime that shares the affected localDisk volumes.
2. Delete only `metadata.sqlite`, `metadata.sqlite-wal`, and `metadata.sqlite-shm`
   under `/data/d1/wdl-d1-storage-v1/` and `/data/do/wdl-do-host-v1/`.
3. Start the 2026-07-01 runtimes and verify their health before restoring traffic.

Those files contain workerd's native alarm scheduler metadata, which WDL does not use;
WDL DO alarms are Workflows-backed. Do not delete the per-actor `*.sqlite` databases.
This cleanup restores process startup only; it does not make per-actor data backward
compatible. Workerd 2026-07-17 can persist `Blob` values through
`ctx.storage.put()`, while 2026-07-01 cannot deserialize them. Rewrite such values to
a 2026-07-01-compatible type or delete them before downgrade; otherwise the older
runtime cannot read the affected DO storage values.

For Terraform test, prefer Terraform-managed changes instead of manual rolling
operations unless explicitly debugging.

## Tests That Protect This Module

- Operator-driven checks: Terraform plan review and Kubernetes manifest review.
- `npm run test:integration`
- `tests/unit/style-contracts.test.js`: local compose Envoy mesh shape, D1/DO
  test-hook IaC gates, DO residency defaults, and Fargate-only Terraform launch
  contracts.
- `tests/integration/durable-objects-eviction.test.js`: resident-default selection plus
  explicit eviction, actor reconstruction, task-local session-policy fencing across an
  owner round trip, SQLite continuity, and quiescent hibernating WebSocket continuity.
- `tests/integration/ai-binding.test.js`: provider/credential lifecycle, runtime and DO
  facades, public-only HTTP/SSE/WebSocket forwarding, SDK compatibility, and watchdog
  cleanup after caller teardown.
- Smoke tests against the target deployed environment after rolling.

## Known Constraints And Non-Goals

- Cost/capacity choices are operational decisions and should not be hidden in
  application docs.

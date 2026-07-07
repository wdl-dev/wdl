# Infrastructure And Deployment

## Purpose

Infrastructure docs describe how local compose, Terraform-managed environments, and
Kubernetes manifests map the platform services onto Redis, local Envoy mesh, ECS
Service Connect, EFS, S3/R2-compatible storage, and release pipelines.

## Current Implementation

There are two main infrastructure families:

- `terraform/`: AWS ECS-shaped deployment environment.
- `deploy/kubernetes/`: Kustomize manifests for Kubernetes-shaped deployment.

Terraform is the AWS ECS-shaped deployment environment and is changed with
`terraform plan/apply` from a developer/operator machine. Kubernetes manifests
are release artifacts for cluster-shaped deployment and are rolled by the target
cluster's operator workflow.

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
production-shaped delivery paths are Terraform and the Kubernetes manifests under
`deploy/kubernetes/`.

These paths are intended for production operation, not only local demonstration. That
means they preserve the service boundaries, private mesh assumptions, image contracts,
health/metrics endpoints, and ownership/failover rules that the runtime modules depend
on. Operators still choose concrete capacity, managed Redis/Valkey durability,
object storage, EFS or equivalent localDisk persistence, ingress protection, and
regional backup/restore policy.

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
always send only the current token. Both token values must be non-empty ASCII
strings so JS and Rust compare the same header representation. Health and
metrics endpoints are the only unauthenticated service endpoints. The token is
platform plumbing, not a tenant binding: runtime wrapper code strips it from
tenant-visible `env`, host-owned DO proxies and host-side backend capabilities
add it for DO forwarding, and spoofed tenant headers are removed before
forwarding.

## Redis / Storage Contracts

Logical DB split:

- DB 0: control-plane metadata.
- DB 1: data-plane KV, queue, log-tail streams.
- DB 2: workflows instance state.

Stateful storage:

- D1 localDisk on EFS.
- DO localDisk on separate EFS.
- Assets/R2 in S3-compatible bucket namespace.
- Workflow payload refs remain in DB 2 under caps unless application stores external
  references.

## Ownership / Failure Semantics

- Scheduler defaults to one replica in deployment; current dispatch paths are
  multi-replica safe, but rollout can still pause scheduling because ECS uses
  stop-before-start replacement.
- Workflows is a separate Rust service.
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
- Terraform Fargate services should use rolling replacement where the service can
  tolerate overlapping capacity. D1/DO use sequential replacement, while scheduler
  remains stop-before-start as a singleton control loop. D1/DO and scheduler disable
  Availability Zone rebalancing so replacement follows their explicit deployment
  strategy.

## Security Boundaries

- user-runtime loaded worker outbound is public-only.
- system-runtime is privileged by design.
- Runtime internal `:8088`, d1-runtime `:8787`, do-runtime `:8788`, workflows `:9120`,
  and Redis are private mesh services. Private service calls also require
  `x-wdl-internal-auth` with the shared `WDL_INTERNAL_AUTH_TOKEN`.
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

Common ordering when runtime internal protocol changes:

1. Roll workerd pools: gateway, user-runtime, system-runtime as needed.
2. Roll scheduler if it calls new runtime internal paths.
3. Roll workflows if workflow runtime protocol changed.
4. Roll D1/DO only when their transport/config changes.

Direction matters:

- If workflows dispatches a new runtime internal path or body shape, roll runtime first,
  then workflows.
- If runtime/control/do-runtime calls a new workflows API shape, roll workflows first,
  then the callers.
- If workflows and runtime both change protocol at the same time, deploy the side that
  adds the new endpoint or body shape first, then deploy the side that calls it.

Internal auth rotation is dual-read / single-write, but it is not rolling-safe:
callers always send `WDL_INTERNAL_AUTH_TOKEN`, and receivers accept current plus
optional previous. Rotate it during a maintenance window or after pausing
scheduler/workflows traffic. Configure the old value as
`WDL_INTERNAL_AUTH_PREVIOUS_TOKEN` and the new value as `WDL_INTERNAL_AUTH_TOKEN`
across all private services, roll/restart the private fleet together, then clear
`WDL_INTERNAL_AUTH_PREVIOUS_TOKEN` with a second roll after the fleet converges.

For Terraform test, prefer Terraform-managed changes instead of manual rolling
operations unless explicitly debugging.

## Tests That Protect This Module

- Operator-driven checks: Terraform plan review and Kubernetes manifest review.
- `npm run test:integration`
- `tests/unit/style-contracts.test.js`: local compose Envoy mesh shape, D1/DO
  test-hook IaC gates, and Fargate-only Terraform launch contracts.
- Smoke tests against the target deployed environment after rolling.

## Known Constraints And Non-Goals

- Cost/capacity choices are operational decisions and should not be hidden in
  application docs.

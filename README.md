# WDL

[![CI](https://github.com/wdl-dev/wdl/actions/workflows/ci.yml/badge.svg)](https://github.com/wdl-dev/wdl/actions/workflows/ci.yml)
[![Docker release](https://github.com/wdl-dev/wdl/actions/workflows/release.yml/badge.svg)](https://github.com/wdl-dev/wdl/actions/workflows/release.yml)
[![Docker Hub: wdl-workerd](https://img.shields.io/badge/docker.io-getwdl%2Fwdl--workerd-2496ED?logo=docker&logoColor=white)](https://hub.docker.com/r/getwdl/wdl-workerd)
[![Docker Hub: wdl-rust](https://img.shields.io/badge/docker.io-getwdl%2Fwdl--rust-2496ED?logo=docker&logoColor=white)](https://hub.docker.com/r/getwdl/wdl-rust)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> Chinese version: [README.zh.md](README.zh.md)

WDL is a self-hosted multi-tenant Workers platform with multi-replica failover,
built on stock Cloudflare workerd.
It loads immutable Worker versions dynamically from Redis/Valkey through workerd's
`workerLoader` API, then layers platform services around that runtime: control/auth,
KV, R2, D1, Durable Objects, queues, cron, Workflows, AI, ASSETS, service/platform
bindings, live log tailing, Prometheus metrics, and deployment/lifecycle tooling.
The project name began as "workerd dynamic loader"; WDL is the product name now that
the platform extends well beyond dynamic loading.

## What It Provides

- Multi-tenant routing by namespace subdomain and custom host patterns.
- Immutable worker versions with explicit promote/rollback and hard-delete lifecycle
  APIs.
- Wrangler-compatible deployment through the `wdl` CLI.
- KV, R2, D1, Durable Objects, queues, cron triggers, Workflows, namespace-scoped AI,
  ASSETS, service bindings, and platform bindings.
- Secret-at-rest envelope encryption before values are written to Redis.
- Live `wdl tail` over bounded Redis streams, structured logs, and Prometheus metrics.
- Explicit failover semantics for D1/Durable Objects, plus multi-replica-safe dispatch
  for runtime, scheduler, and Workflows.
- Local Docker Compose stack plus production-shaped Terraform and Kubernetes delivery
  paths.

## Why WDL

workerd is the runtime, not the platform. It gives you Workers execution, but not
the multi-tenant routing, state, storage, scheduling, secrets, control APIs, and
lifecycle machinery needed to operate Workers as a platform. WDL provides that
layer.

WDL builds around stock workerd, without forking it. Upstream workerd remains the
runtime contract; WDL expresses the platform through workerd configs, static system
workers, Rust services, Redis/Valkey state machines, and S3-compatible object storage.
Operators get the Workers programming model while inheriting upstream workerd fixes
instead of maintaining a runtime fork.

WDL is also more than a demo stack. Its control plane, routing, stateful binding
ownership, dispatch workers, observability, release images, and infrastructure paths are
written as single-region production platform components. "Production-ready" here means
the platform has explicit recovery contracts, private mesh boundaries, release gates,
and deployable Terraform/Kubernetes shapes; operators still own capacity planning,
managed Redis/Valkey, storage durability, ingress protection, and regional disaster
recovery.

## What WDL Is Not

- **Not a global edge network.** WDL runs in a single region on infrastructure you
  operate. It is not Cloudflare's global edge, anycast network, point-of-presence
  fabric, or DDoS protection service. That tradeoff is what lets WDL provide
  strongly consistent KV and read-your-writes D1.
- **Not a 100% drop-in for Cloudflare Workers.** Compatibility is tracked
  surface-by-surface as stronger, different, or not implemented in the
  [compatibility matrix](docs/compatibility.md).

## Who It's For

- Teams that want the Workers programming model and Wrangler workflow on their own
  infrastructure.
- Internal platform teams offering multi-tenant Workers to their own developers.
- Data-residency, sovereignty, compliance, or air-gapped environments that require
  operator-owned infrastructure.
- Workloads already written to the Workers model that need an operator-owned
  deployment path without rewriting the application model.

## How It Relates to Cloudflare Workers

**WDL is not affiliated with, endorsed by, or sponsored by Cloudflare, Inc.
Cloudflare, Cloudflare Workers, Wrangler, and workerd are trademarks or registered
trademarks of Cloudflare, Inc.**

You write standard module workers (`export default { fetch }`) with a normal
`wrangler.toml` or `wrangler.jsonc`, pinned to Wrangler 4. `wdl deploy` runs
`wrangler deploy --dry-run` for local bundling only; nothing is sent to Cloudflare.
Do not use `wrangler deploy` against a WDL platform. Releases go through `wdl deploy`.

Workers serve from a path-prefixed URL on the platform domain:

```text
https://<namespace>.<platform-domain>/<worker-name>/<path>
```

The worker sees the request path with the `/<worker-name>` prefix stripped.

Compatibility differences fall into three categories: stronger, different, and not
implemented. WDL also adds platform-owned capabilities such as platform bindings.
The surface-by-surface status lives in the [compatibility matrix](docs/compatibility.md).

## Hosted Preview

WDL is infrastructure first: operators run their own platform and tenants deploy to it
with the `wdl` CLI. The WDL Team runs a hosted preview, with control at `api.wdl.dev`
and workers serving from `*.wdl.sh`, running WDL's own workloads. It is a permanent
demo, not a commercial service: it is redeployed at any time as the platform iterates,
may be unstable, and comes with no commitments — but what it shows is exactly what WDL
can do. The WDL Team has no intention of turning it into a public FaaS with service
commitments — that space is crowded enough already; the focus is self-hosted
infrastructure. It is available today; to test on it, email <hi@wdl.dev>.

## Architecture Summary

![WDL architecture overview](wdl-architecture.png)

The platform is split into seven app services plus shared state:

| Service | Role |
|---|---|
| `gateway` | Public/control ingress, host routing, custom pattern routing, and WebSocket proxy/reconnect path. |
| `user-runtime` | Tenant worker runtime pool, public-only outbound, local redis-proxy sidecar. |
| `system-runtime` | Control/auth/static system workers and privileged `__system__` runtime pool. |
| `d1-runtime` | D1 SQLite execution with owner leases and supervised workerd process. |
| `do-runtime` | Durable Object native facets, SQLite storage, alarms, owner leases, and WebSockets. |
| `scheduler` | Cron, queue, and workflow tick dispatch. |
| `workflows` | Workflows V2 state machine, DB 2 owner, and DO alarm delivery backend. |

Valkey/Redis uses a deliberate logical split:

- DB 0: control metadata, bundles, routes, auth, lifecycle, D1/DO owner state,
  workflow definitions.
- DB 1: data-plane KV, queue streams, delayed queues, orphan streams, log-tail streams.
- DB 2: workflow instance state, step records, ready/due shards, events, run leases.

S3-compatible storage backs ASSETS and R2. D1 and Durable Object SQLite files live on
workerd `localDisk` storage. In production-shaped environments, those map to managed or
provisioned storage; in local compose they map to volumes and `s3mock`.

### High Availability And Failover

WDL's HA model is single-region and service-replica based. Gateway, runtime pools,
scheduler, workflows, D1 runtime, and DO runtime are independent service families, so
operators can run multiple tasks or pods where the module's concurrency contract allows
it. Tenant worker versions are immutable and are loaded by id, so replacing a runtime
replica does not mutate routing state.

Stateful bindings use ownership protocols instead of assuming that a service discovery
target is the owner. D1 ownership is per physical database. Durable Object ownership is
per owner scope. Both owner records carry task identity, lease expiry, and a monotonic
generation fence, so stale replicas fail closed after a takeover. Supervisors drain local
D1/DO owners during rollout, and ordinary task loss falls back to lease expiry and
takeover by another replica. Scheduler projections are repairable, queues are
at-least-once, cron/queue dispatch paths are multi-replica safe, and Workflows uses DB 2
leases and generation/run-token fences for execution progress. Scheduler rollout can
still create a short dispatch gap, and missed cron slots follow the documented
best-effort cron semantics instead of being replayed.

For the full architecture, see [docs/architecture.md](docs/architecture.md).

## Quick Start

This path requires Git, Node.js 24, Docker Engine or Docker Desktop, and a recent
Docker Compose with `!reset` and `--wait` support. It was verified with Docker Compose
v5.3.0.

Clone the repository, install the standalone `wdl` CLI and repository dependencies,
compile the local workerd configs, start the stack from published Docker Hub images,
and deploy a smoke worker. The compile step is required on a fresh clone because
compose bind-mounts `./dist` over the image's built configs.

```bash
git clone https://github.com/wdl-dev/wdl.git
cd wdl
npm install -g @wdl-dev/cli@1.8.0
npm ci
npm install --ignore-scripts --prefix examples/hello-jsonc
npm run compile:workerd:local
docker compose -f docker-compose.yml -f docker-compose.images.yml up -d --pull always --no-build --wait --wait-timeout 180
export ADMIN_TOKEN=local-dev-token
export CONTROL_URL=http://admin.test:8080
export CONTROL_CONNECT_HOST=localhost

wdl deploy examples/hello-jsonc --ns demo
```

Call tenant workers through the gateway:

```bash
curl -H "Host: demo.workers.local" "http://localhost:8080/hello-jsonc/"
```

Inspect the namespace through the CLI:

```bash
wdl workers --ns demo
```

This returns the namespace worker list and should show `hello-jsonc` as the active
worker.

The control URL host must stay outside `PLATFORM_DOMAIN`; gateway uses that host
to short-circuit to the static control worker. Adding `demo.workers.local` and
`admin.test` to `/etc/hosts` is optional for this flow; it is only needed if you
want browser-style requests to those names without explicit `Host` headers or
CLI connect-host overrides.

To stop the local stack and permanently remove its Compose volumes after the smoke
test:

```bash
docker compose -f docker-compose.yml -f docker-compose.images.yml down -v
```

The `-v` flag deletes all local WDL data in this Compose project.

## Deploy a Worker

Use the same published `wdl` CLI version installed in Quick Start. Keep it aligned
with the top-level `WDL_CLI_PACKAGE` value in `.github/workflows/ci.yml`; CI uses
that pinned package for CLI integration tests.

To validate unpublished CLI changes, link or wrap the downstream checkout so `wdl` is
on `PATH`. `WDL_CLI_BIN` remains available for focused integration runs that need an
explicit executable override.

From the platform repository root, deploy a Wrangler project:

```bash
npm install --ignore-scripts --prefix test-workers/kv-demo
wdl deploy test-workers/kv-demo --ns demo
```

Exercise it through the gateway:

```bash
curl -H "Host: demo.workers.local" "http://localhost:8080/kv-demo/alice"
```

`wdl deploy` shells out to Wrangler dry-run, uploads the complete emitted bundle to
control, then promotes the new immutable version. The CLI never writes
Redis directly for ordinary operations; control remains the authority for validation,
Redis commits, routing, lifecycle, and cleanup intent.

See [docs/modules/cli.md](docs/modules/cli.md) for the full CLI and Wrangler input
contract.

## Common Commands

```bash
# Compile local workerd configs used by compose
npm run compile:workerd:local

# Recompile local configs and restart currently running compose services
# Rebuild Docker images separately after Rust or Dockerfile edits
npm run dev:rebuild

# Build local development images when changing Rust services or Dockerfiles
docker compose build

# Fast local JS gate
npm test

# Individual JS checks
npm run lint
npm run typecheck
npm run typecheck:strict
npm run typecheck:node
npm run test:unit

# Integration suite
npm run test:integration

# Rust checks from the repository root
(cd rust && cargo fmt --all --check)
(cd rust && cargo clippy --locked --workspace --all-targets -- -D warnings)
(cd rust && cargo test --locked --workspace)
(cd rust && cargo deny --manifest-path Cargo.toml check)
```

Integration runner behavior, sharding, artifacts, and debug flags are documented in
[docs/testing.md](docs/testing.md).

## Documentation Map

Start with [docs/README.md](docs/README.md). The active docs are the current design
contract:

- [Architecture](docs/architecture.md)
- [Security model](docs/security.md)
- [Compatibility matrix](docs/compatibility.md)
- [Project-wide standards](docs/project-standards.md)
- [Protocol contracts](docs/protocol-contracts.md)
- [Redis key layout](docs/redis-key-layout.md)
- [Source map](docs/source-map.md)
- [Module docs](docs/modules/README.md)
- [Testing](docs/testing.md)
- [Contributor reading path](docs/contributing.md)
- [Workerd JavaScript standards](docs/workerd-js-standards.md)
- [Rust service standards](docs/rust-sidecar-standards.md)

`CLAUDE.md` is intentionally only a short agent checklist and pointer map. It should not
be treated as the canonical module reference.

## Deployment Paths

- Local development: `docker-compose.images.yml` uses published Docker Hub images;
  plain `docker compose` builds local development images.
- Kubernetes: `deploy/kubernetes/` Kustomize base and local overlay.
- Terraform: `terraform/` for AWS ECS-shaped deployment.

The delivery paths share the same service model and image contracts. Production
deployments should keep Redis/Valkey private, keep runtime internal sockets private,
and protect object storage, D1/DO localDisk storage, and secret-envelope root material
as platform state.

Production deployments should also run replica counts and rollout policies that match
the service's ownership contract: stateless workerd pools can be scaled horizontally,
scheduler dispatch is replica-safe but may pause briefly during rollout, and D1/DO
require stable per-replica storage identity plus private supervisor drain/renew access
before scaling beyond one task.

## Project Layout

| Path | Purpose |
|---|---|
| `gateway/` | Ingress routing worker. |
| `runtime/` | User/system runtime loader, dispatch, bindings, and workflow facade. |
| `control/` | Static control worker and handlers. |
| `auth/` | JSRPC auth worker. |
| `d1-runtime/` | D1 workerd runtime. |
| `do-runtime/` | Durable Object workerd runtime. |
| `rust/` | redis-proxy, scheduler, workflows, supervisor, and shared Rust crates. |
| `shared/` | Shared JS contracts, Redis client, observability, auth, version, D1 utilities. |
| `system-workers/` | Permanent platform-loaded workers. |
| `test-workers/` | Integration fixtures. |
| `examples/` | Manual demos and reference projects. |
| `deploy/`, `terraform/` | Deployment manifests and infrastructure. |
| `docs/` | Architecture, operations, and module contracts. |
| `scripts/` | Build, compile, and test-runner implementations behind the npm scripts. |
| `tests/` | Unit and integration suites. |
| `licenses/` | Image NOTICE files and generated third-party license bundles. |

The more detailed ownership map lives in [docs/source-map.md](docs/source-map.md).

## Release Versioning

Project releases use signed annotated Git tags in the `wdl.YYYYMMDD.N` format, for
example `wdl.20260531.1`. Before publishing Docker images, the release workflow
validates that the tag matches the root `VERSION` file, the tag date matches the
locked workerd package date, `CHANGELOG.md` has release notes for the tag, the
tag points to the current repository default branch tip, and the tag commit's
`CI` workflow has completed successfully. The integration suite is part of that
required CI run. The final `.N` is the WDL project release counter for that
workerd date, so WDL patches can ship on the same bundled workerd. Docker image
tags use
`wdl.YYYYMMDD.<8-char-sha>` so an image tag names the exact released commit without
repeating the Git tag's release counter. The date suffix tracks the bundled workerd
date version, but the `wdl.` namespace and project release counter are intentionally
separate from Cloudflare's numeric package majors and patch counter. Project release
tags are independent of the CLI package version and independent of per-worker versions
such as `v1` or `v2`.

## License

Copyright 2026 Sean Consulting OÜ.

Licensed under the Apache License, Version 2.0. See [LICENSE](./LICENSE).

See [NOTICE](./NOTICE) and [AUTHORS](./AUTHORS).

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the Apache-2.0 license, is
licensed under Apache-2.0 without any additional terms or conditions.

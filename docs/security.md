# Security Model

This document describes WDL's current security boundaries. It is not a formal threat
model for every deployment, but it records the assumptions the code and infrastructure
rely on today. Current code, tests, and deployed network policy remain authoritative.

## Core Assumptions

- Tenant worker code is untrusted.
- Platform services inside the private service mesh are trusted platform components.
  Internal endpoints require the shared `WDL_INTERNAL_AUTH_TOKEN` application header
  in addition to socket, network, service, and task-placement boundaries. Health and
  metrics endpoints remain unauthenticated so orchestrators and scrapers can probe
  service liveness.
- Public ingress and admin ingress are different planes. Gateway routes public tenant
  traffic; control/auth authorize control-plane operations.
- Internal network safety is still a deployment contract. The shared internal token is
  defense in depth and caller-shape authentication for in-tree platform services; it is
  not a substitute for keeping private ports off public ingress.
- A runtime escape must not automatically become cloud credential theft. Tenant-running
  tasks must use least-privilege task roles and must not receive broad infrastructure
  credentials.

## Trust Zones

| Zone | Examples | Trust level | Main boundary |
|---|---|---|---|
| Public data plane | Gateway public socket, tenant Worker URLs | Untrusted clients | Gateway routing plus runtime worker isolation |
| Admin/control plane | Control URL/admin host, control worker, auth worker, CLI token API | Authenticated operators/tenants | `x-admin-token`, auth role table, namespace scope |
| Tenant runtime | user-runtime loaded workers | Untrusted tenant code | workerd isolate, wrapper-shaped `env`, public-only outbound |
| System runtime | system-runtime, control/auth/tail, `__system__` workers | Platform code | Reserved namespace and private+public outbound |
| Private service mesh | d1-runtime, do-runtime, workflows, scheduler, redis-proxy sidecars | Trusted platform components | Service Connect/security groups/internal sockets plus `WDL_INTERNAL_AUTH_TOKEN` |
| State stores | Valkey DBs, EFS localDisk, S3-compatible storage | Platform-owned data services | Writer ownership, Redis DB split, secret envelopes, storage credentials |
| Host/infra | ECS Fargate tasks, IAM roles, task metadata | Operator-controlled | IAM least privilege, ECS Exec policy |

## Public And Admin Ingress

Gateway is not an authorization layer for tenant applications. It resolves a public
host/path to an immutable worker version and forwards the request to the runtime loader
socket. Tenant applications must implement their own public application auth when they
need it.

Admin-host requests are different. Gateway's `ADMIN_HOST` branch only forwards to
control; it does not authorize. Control parses the route into an action and asks auth
to verify `x-admin-token` against `shared/auth-roles.js`.

Reserved namespaces are exact literals, not a broad `__*` convention. The current set is
`__system__`, `__platform__`, and `__community__`. `__system__` is the system-runtime
namespace and is the only reserved namespace with a narrow public system-route allowance
documented in the gateway module. `__platform__` is the current platform-tier namespace
for platform bindings and platform-scoped roles. `__community__` is reserved for future
community platform-tier use, but it is not currently a platform-tier role namespace.
Reserved namespaces are blocked in public subdomain routing before Redis route lookup.
Reserved namespaces are not tenant namespaces: tenant-scoped `ns` tokens must bind
ordinary tenant namespaces, while reserved namespace access requires `ops` or the
platform role shapes allowed by `shared/auth-roles.js`. Tenant namespaces use
DNS-label-compatible grammar: 1-63 lowercase alphanumeric/hyphen characters, with
alphanumeric first and last characters.

## Tenant Runtime Isolation

WDL uses stock workerd and does not patch it. Runtime isolation therefore starts with
workerd isolate boundaries and continues with WDL-specific wrapper and network rules:

- Tenant bundles are loaded as immutable worker versions through `workerLoader`.
- Runtime wrapper generation constructs the tenant-visible `env`; hidden platform
  Fetcher bindings for D1, DO, workflows, and owner-network paths stay inside runtime
  and are deleted before tenant code observes `env`.
- user-runtime loaded workers receive public-only outbound. Tenant `fetch()` and
  `cloudflare:sockets` must not reach platform-private addresses.
- system-runtime `__system__` workers intentionally have private+public outbound because
  they are platform code, not tenant code.
- Privileged runtime events use the private `:8088` internal socket. Gateway must not
  reserve tenant-visible paths like `/_scheduled`; the socket boundary is the security
  boundary.

## Internal Mesh Trust

Many internal endpoints are private platform protocols, not public APIs:

- runtime `:8088` scheduled, queue, and workflow dispatch
- d1-runtime owner, SQL, drain, and renew paths
- do-runtime invoke/connect/alarm/storage cleanup/drain/renew paths
- do-runtime diagnostic probe path
- runtime `:8088` workflow run/notify dispatch paths called by workflows
- workflows internal lifecycle, step, tick, and DO alarm mutation/cleanup paths
- redis-proxy sidecar APIs for cold-load, KV, queue, logs, and runtime support

These endpoints require the `x-wdl-internal-auth` header whose value comes from the
shared `WDL_INTERNAL_AUTH_TOKEN` secret injected into every platform service task.
During rotation, receivers also accept optional `WDL_INTERNAL_AUTH_PREVIOUS_TOKEN`;
callers still send only the current token. The header is stripped from
tenant-originated forwarding paths. The token stays in host-owned Durable Object
proxies and host-side backend capabilities; it is not embedded in generated
tenant facade code or tenant-visible `env`.

Do not expose these endpoints through gateway or an internet-facing load balancer.
The shared internal token authenticates in-tree platform callers on the private mesh; it
does not authorize arbitrary external callers. If a new caller is outside the trusted
mesh, add an explicit authentication and authorization design instead of reusing the
internal protocol directly.

This "private mesh is trusted" rule is not permission to trust tenant input. Runtime and
stateful services still validate worker ids, namespace/worker grammar, owner headers,
generation fences, content types, request sizes, and metadata shapes at the protocol
boundary.

## Control-Plane Authorization

Auth tokens are bearer tokens. Auth stores token records and token hashes in Redis;
plaintext tokens are shown only at issue time. Role evaluation is centralized in
`shared/auth-roles.js`.

Important role boundaries:

- `ops` is full-plane and bootstrap-managed.
- `ops-observer` is cross-namespace read-only but intentionally lacks secret value,
  workflow payload, arbitrary SQL, R2 object body/head, token list, and write
  capabilities.
- `ns` roles are tenant-namespace scoped.
- `platform` and `platform-observer` roles are pinned to platform-tier reserved
  namespaces.
- `token-issuer` is namespace-unbound; aside from `/whoami` self-introspection, its
  only non-diagnostic action is `auth.delegated_token.issue`. It cannot issue direct
  tokens, list/revoke tokens, or access tenant resources. Delegated issue returns
  short-lived credentials; namespace resource lifecycle remains outside auth token
  lifecycle.
- Delegated namespace safety assumes routine namespace-scoped writes use
  namespace-bound credentials. Full-plane unbound credentials can still perform
  namespace-scoped writes today, and those writes may leave no auth-visible namespace
  fact once the active worker gate is cleared; this is an accepted V1 residual risk
  until a persistent namespace fact index exists.
- Platform cross-namespace visibility requires the role kind and bound namespace to
  match the platform-tier rule; do not replace this with route-name checks.

Control handlers should not infer permission from URL prefixes. They should use
`parseControlRoute()` action classification and auth verification.

Secret PUT handlers see plaintext only long enough to validate and encrypt it. The Redis
secret stores contain `WDL-ENC:` envelope values; redis-proxy decrypts them only while
serving `/runtime/load`. There is no steady-state plaintext fallback on the runtime
cold-load path, so a missing or wrong secret-envelope provider key fails closed for
workers that need secrets.

## Binding And State Security

Bindings are the main tenant capability surface:

- KV and queue producers use redis-proxy sidecars and runtime-owned caps.
- D1 and DO facades route through stateful runtimes that own single-writer leases and
  generation fences.
- Workflows facade calls workflows; workflows owns DB 2 state and does not trust
  tenant-provided identity fields.
- R2 uses platform S3-compatible credentials in runtime; tenant code receives an R2
  binding, not raw credentials.
- ASSETS only exposes `env.ASSETS.url(path)` for tokenized CDN URLs; runtime does not
  expose S3 credentials or bytes for assets.
- Service and platform bindings are resolved from control metadata and ACLs;
  cross-namespace service calls require target-side authorization.
- Secrets are materialized as plaintext only at runtime `env` construction. At rest,
  `secrets:<ns>` and `secrets:<ns>:<worker>` hash values are envelope ciphertext; Redis
  snapshots and debug reads should not reveal tenant secret plaintext.

Redis key families have explicit owners in module docs. Indexes are usually repairable
projections, not authority. Adding a second writer or fallback SCAN path is a security
and correctness change because it can bypass lifecycle and delete fences.

## Infrastructure Boundaries

Terraform runs platform services on ECS Fargate, including tenant-executing runtime
tasks. Cloud credential exposure for tenant-running tasks is bounded by
least-privilege task roles, public-only workerd outbound bindings, and private mesh
security groups. ECS Exec should be enabled only where platform operator access is
intended.

Service Connect and security groups are part of the internal mesh boundary. The shared
`WDL_INTERNAL_AUTH_TOKEN` current value must be identical across runtime,
d1-runtime, do-runtime, scheduler, workflows, and redis-proxy sidecars; the optional
previous value is accepted only as a maintenance-window rotation bridge. Callers
always send the current value, so token rotation is not rolling-safe unless traffic is
paused or the private fleet is restarted together. Scheduler is a client of runtime
internal dispatch and workflows tick; Workflows dispatches Durable Object alarms to
do-runtime. Neither scheduler nor workflows is a public service target.

## Tenant-Facing Contract

Tenant-facing behavior is documented by the standalone CLI guide. Security-relevant
tenant rules include:

- Tenant tokens are namespace-scoped unless the operator issues a broader role.
- The default public URL shape is namespace/worker path based.
- Custom domains and Wrangler `routes` are operator-enabled, not general self-service.
- Cross-namespace service bindings require target-side authorization.
- Tenant socket/fetch access to platform-private addresses is blocked at the
  runtime/workerd network boundary.
- Live tail is best-effort debugging, not audit history.

## Observability And Sensitive Data

Logs are the primary platform audit/debug stream for many services. Do not log plaintext
tokens, token hashes, secret values, raw platform credentials, or unbounded tenant
payloads. Metrics labels must remain bounded and must not include namespace, worker,
version, path, Redis key, token id, or raw error text.

Request ids may cross service boundaries, but they are sanitized and bounded before
propagation.

## Non-Goals And Gaps

- WDL does not claim Cloudflare account API parity.
- WDL does not implement Cloudflare edge cache semantics or expose `caches.default`.
- Internal mesh protocols do not currently use per-request mTLS between every platform
  service; the current application-layer mesh control is a shared static token.
- If a future deployment puts internal endpoints on a network where callers cannot be
  assumed to be platform code, add explicit caller-specific authentication before
  reusing this model.
- Tenant public application authentication is the tenant application's responsibility.

## Tests And Checks That Protect This Model

- `tests/unit/style-contracts.test.js`: route channels, hidden Fetcher stripping,
  internal socket split, Fargate/task-role infrastructure guards, low-cardinality
  metrics, and other drift guards.
- `tests/unit/auth-lib.test.js`, `tests/unit/auth-index.test.js`,
  `tests/integration/auth-worker.test.js`, `tests/integration/auth-platform.test.js`:
  token and role boundaries.
- `tests/unit/runtime-load.test.js`, `tests/unit/runtime-binding-surface.test.js`,
  `tests/integration/service-bindings.test.js`: wrapper and binding exposure.
- `tests/unit/gateway-dispatch.test.js`, `tests/integration/gateway.test.js`,
  `tests/integration/routing-gateway.test.js`: route and reserved namespace behavior.
- `tests/integration/d1-*.test.js`, `tests/integration/durable-objects*.test.js`, and
  workflows integration tests: owner/fence behavior for stateful bindings.

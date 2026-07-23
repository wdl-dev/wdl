# Gateway

## Purpose

Gateway is the public data-plane ingress and the admin-host ingress shim. It routes
tenant HTTP/WebSocket traffic to the correct runtime pool and forwards admin-host
traffic to control without making control aware of gateway topology.

## Current Implementation

The workerd entrypoint is `gateway/index.js`. Pure route parsing lives in
`gateway/dispatch.js` and `gateway/lib.js`; Redis/cache/subscriber mechanics live in
`gateway/runtime.js`; WebSocket lifetime management lives in `gateway/holder.js` and
`gateway/websocket.js`.

Gateway has three dispatch branches:

- Normalized, lowercased host equals `env.ADMIN_HOST`: short-circuit to
  `env.CONTROL.fetch()`. This branch does not consult namespace or route Redis
  state, so admin-host requests can still reach control during route cache drift,
  route lookup outage, or DB 0 `FLUSHALL` recovery work. Auth and most control
  operations still depend on Redis and fail closed when their own Redis state is
  unavailable.
- `<ns>.<PLATFORM_DOMAIN>/<worker>/<path>`: subdomain route lookup from `routes:<ns>`.
- Pattern hosts: declared-host gate from `declared-hosts`, then lookup from
  `patterns:<host>` with longest-prefix slot matching.

The resolved `{ ns, worker, version }` becomes `x-worker-id: <ns>:<worker>:<version>`
and `x-worker-prefix` on the runtime request. Literal `__system__` routes go to
`RUNTIME_SYSTEM`; all ordinary tenant namespaces go to `RUNTIME_USER`.
Before forwarding, gateway removes client-supplied `x-worker-id`, `x-worker-prefix`,
and every `x-wdl-*` header. The same internal-header policy filters every forwarded
response, including failed and successful WebSocket upgrades, before it crosses back
onto the public socket.

The `ADMIN_HOST` branch is infrastructure traffic, not a loaded-worker request.
It does not set `x-worker-id` or `x-worker-prefix`. `PLATFORM_DOMAIN` and
`ADMIN_HOST` are environment-configurable. Routing tiers normalize `PLATFORM_DOMAIN`
as an ALB-compatible ASCII DNS hostname of at most 126 bytes, with an alphabetic final
label, and default it to `workers.local`; one trailing dot is removed and the result is
lowercased. Control's `/whoami` consumes only an explicitly configured value and never
advertises the `workers.local` default. Admin-host short-circuiting remains unset by
default.

## Interfaces

- Public HTTP socket: `:8080`.
- Health and metrics: root `/healthz` and `/_metrics` are gateway-reserved
  paths on the public listener.
- Admin-host forwarding: `ADMIN_HOST` routed to control.
- Data-plane forwarding: runtime loader socket, not runtime internal dispatch socket.
- WebSocket upgrades: moved into `GatewayWsHolder` Durable Object so long-lived 101
  responses do not live on the ordinary gateway request IoContext.

## Routing And Cache Model

Gateway has no control-plane authority. It projects Redis route state into a small local
routing cache:

- Every request starts by normalizing and lowercasing the URL host. The `ADMIN_HOST`
  branch bypasses route Redis state and forwards to control/auth through
  `env.CONTROL.fetch()`.
- Subdomain routing first rejects reserved namespaces, then checks `namespaces` and
  `routes:<ns>`. The leading worker segment is stripped before forwarding to runtime, so
  tenant code sees the path after the worker name.
- Pattern routing first checks `declared-hosts`, then reads `patterns:<host>` and
  chooses the longest matching path slot. The gate answers only "is this host declared
  by any namespace"; it does not assign host ownership. Ownership and conflict checks
  remain encoded by the active `patterns:<host>` projections.
- Runtime pool selection is exact: only literal `__system__` routes use
  `RUNTIME_SYSTEM`. Future reserved namespaces must opt in explicitly; do not replace
  this with broad reserved-prefix matching.
- Route and pattern caches are bounded per gateway isolate. They are performance caches
  only; Redis remains the current route source of truth.
- `routes:invalidate`, `patterns:invalidate`, and `routes:flush` are non-durable pub/sub
  hints. Gateway clears caches on subscriber connect/disconnect so missed messages
  repair on the next lookup.
- Pattern-host ownership moves publish `patterns:invalidate`, but the hint is still
  non-durable. A gateway that misses the pub/sub message can serve the previous
  `patterns:<host>` projection from its bounded in-memory cache until subscriber
  reconnect or process restart clears it; this is an accepted stale-cache window, not a
  durable authorization record.
- WebSocket upgrades use the same route resolution as HTTP, then transfer the public
  socket to `GatewayWsHolder`. The holder owns backend reconnect attempts and a bounded
  client-frame buffer; rolling gateway or runtime can still drop the physical client
  connection.

## Redis / Storage Contracts

Gateway reads:

```text
namespaces               Set, active namespace gate
declared-hosts           Set, custom/pattern hosts declared by any namespace
routes:<ns>              Hash, worker name -> active version
patterns:<host>          Hash, path slot -> v2 tab-separated projection
```

Gateway subscribes to:

```text
routes:invalidate        payload = namespace
routes:flush             payload ignored
patterns:invalidate      payload = host or "*"
```

Control writes Redis and publishes invalidations. Gateway never calls control to ask
whether a route changed.

## Ownership / Concurrency / Failure Semantics

- Route caches are pull-triggered and self-healing.
- Gateway clears route/pattern caches on subscriber connect and disconnect, because
  pub/sub messages are not durable.
- Subscriber reconnects clear local caches, and the next request re-reads Redis; missed
  invalidations therefore degrade to bounded stale cache, not permanent drift.
- An invalidation that arrives during a cold Redis read discards that reply. Gateway
  retries at most five snapshots, then returns `503 gateway_routing_unavailable` rather
  than letting unrelated invalidation churn hold the request indefinitely.
- Pattern-host reassignment between namespaces has the same non-durable hint window:
  ordinary control writers publish invalidation, but Redis state is authoritative only
  after the gateway drops or refreshes its local cache.
- Redis outage on data-plane route lookup surfaces as gateway failure; admin-host
  forwarding remains independent of route Redis state.
- Pattern branch leaves the request path unchanged; subdomain branch strips the leading
  worker segment.
- WebSocket backend reconnect is bounded and owns a bounded client-frame buffer.

## Security Boundaries

- Reserved namespaces are always rejected in the subdomain branch before route lookup.
- The public system-route whitelist applies only to pattern routes; currently literal
  `__system__` pattern routes are sent to `RUNTIME_SYSTEM`.
- Platform-tier namespaces are resource-shaped and should be reached through bindings,
  not public subdomains.
- Gateway must not reserve tenant paths like `/_scheduled` or `/_queued`. Privileged
  runtime endpoints live on runtime `:8088`, not behind gateway path filters.
- Gateway chooses runtime pool by exact namespace literal, not broad reserved-prefix
  matching.
- Admin-host routing only gets the request to control; authentication still happens
  inside control/auth.
- A host that matches a reserved namespace must land in the subdomain branch and be
  rejected there. Do not let reserved namespace hosts fall through to pattern routing
  as ordinary "no route matches" traffic.

## Observability

Gateway emits request logs with request id, route context, and outcome. Metrics use
bounded labels only; namespace, worker, version, path details belong in logs, not metric
labels.

`/healthz` and `/_metrics` are served from the public gateway listener before host
classification. This is intentional: load balancers need a route-independent health
probe, and gateway metrics describe the ingress process rather than a tenant worker.
Those two root paths are globally reserved by gateway, so a tenant worker named
`healthz` or `_metrics` cannot serve its root fetch path through subdomain routing.
Tenant paths below another worker name, such as `/app/_metrics`, remain ordinary worker
fetches.
Gateway metrics must therefore stay safe for a public data-plane socket: they may expose
bounded service, route-stage, outcome, binding, websocket-state, Redis-command, and cache
size signals, but must not expose namespace, worker, version, request path, token,
secret, raw host, raw error text, or other tenant-controlled labels. Deployments that
treat operational volume or cache state as sensitive should block `/_metrics` at the
ingress, load balancer, or service-mesh layer while leaving `/healthz` available for
readiness.

## Deployment / Rollout Notes

- Gateway can roll independently for route-cache or request-parsing changes that
  preserve forwarded headers.
- Changes to runtime internal socket paths do not require gateway path filtering.
- Route invalidation channel changes must stay aligned with control; style-contract
  tests protect the literal channel names.

## Tests That Protect This Module

- `tests/unit/gateway-dispatch.test.js`
- `tests/unit/gateway-lib.test.js`
- `tests/unit/gateway-runtime.test.js`
- `tests/unit/gateway-websocket.test.js`
- `tests/unit/gateway-holder.test.js`
- `tests/integration/gateway.test.js`
- `tests/integration/routing-gateway.test.js`
- `tests/unit/style-contracts.test.js`

## Known Constraints And Non-Goals

- Gateway has no synchronous per-gateway invalidation acknowledgement.
- Gateway is not the authorization layer for control APIs.
- Gateway is not responsible for D1, DO, queues, cron, or workflows routing after a
  worker has been loaded.

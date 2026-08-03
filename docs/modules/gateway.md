# Gateway

## Purpose

Gateway is the public data-plane ingress and the admin-host ingress shim. It routes
tenant HTTP/WebSocket traffic to the correct runtime pool and forwards admin-host
traffic to control without making control aware of gateway topology.

## Current Implementation

The workerd entrypoint is `gateway/index.js`. Pure route parsing lives in
`gateway/dispatch.js` and `gateway/lib.js`; static routing-option memoization plus
Redis/cache/subscriber mechanics live in `gateway/runtime.js`; WebSocket lifetime
management lives in `gateway/websocket.js`.

Gateway has three dispatch branches:

- Normalized, lowercased host equals `env.ADMIN_HOST`: short-circuit to
  `env.CONTROL.fetch()`. This branch does not consult namespace or route Redis
  state, so admin-host requests can still reach control during route cache drift,
  route lookup outage, or DB 0 `FLUSHALL` recovery work. Auth and most control
  operations still depend on Redis and fail closed when their own Redis state is
  unavailable.
- `<ns>.<PLATFORM_DOMAIN>/<worker>/<path>`: subdomain route lookup from
  `routes:<ns>`, excluding workers in `platform-domain-disabled:<ns>`.
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
- WebSocket upgrades: gateway terminates a local public `WebSocketPair` and proxies
  directly through the resolved runtime binding.

## Routing And Cache Model

Gateway has no control-plane authority. It projects Redis route state into a small local
routing cache:

- Every request starts by normalizing and lowercasing the URL host. The `ADMIN_HOST`
  branch bypasses route Redis state and forwards to control/auth through
  `env.CONTROL.fetch()`.
- Subdomain routing first rejects reserved namespaces, then checks `namespaces`,
  `routes:<ns>`, and the explicit `platform-domain-disabled:<ns>` opt-out set. The
  leading worker segment is stripped before forwarding to runtime, so tenant code sees
  the path after the worker name. Pattern routing remains active for an opted-out
  worker.
- Pattern routing first checks `declared-hosts`, then reads `patterns:<host>` and
  chooses the longest matching path slot. The gate answers only "is this host declared
  by any namespace"; it does not assign host ownership. Ownership and conflict checks
  remain encoded by the active `patterns:<host>` projections.
- Runtime pool selection is exact: only literal `__system__` routes use
  `RUNTIME_SYSTEM`. Future reserved namespaces must opt in explicitly; do not replace
  this with broad reserved-prefix matching.
- Route and pattern caches are bounded per gateway isolate. They are performance caches
  only; Redis remains the current route source of truth.
- `routes:invalidate`, `patterns:invalidate`, `routes:flush`,
  `do-rollout:restart`, and `worker:delete` are non-durable pub/sub hints. Route and
  pattern events invalidate only their caches. Exact rollout/delete events request
  worker-local WebSocket reconciliation, while subscriber reconnect reconciles every
  process-local group from Redis authority.
- Route invalidation has no cluster-wide acknowledgement barrier. After promotion, a
  warm Gateway may therefore briefly admit an ordinary request against the previous
  immutable version before it observes `routes:invalidate`. Runtime does not recheck
  active state for that request, and the bundled workerd lets admitted calls drain after
  sibling cache eviction. WebSocket lifecycle snapshots give initial upgrades and
  backend reconnects their own active-state admission points; they do not linearize
  ordinary HTTP routing.
- Pattern-host ownership moves publish `patterns:invalidate`, but the hint is still
  non-durable. A gateway that misses the pub/sub message can serve the previous
  `patterns:<host>` projection from its bounded in-memory cache until subscriber
  reconnect or process restart clears it; this is an accepted stale-cache window, not a
  durable authorization record.
- WebSocket upgrades use the same route resolution as HTTP. Gateway terminates the
  public socket locally and proxies directly through the resolved runtime binding. The
  proxy owns backend reconnect attempts and a bounded client-frame buffer. At WebSocket
  lifecycle boundaries it atomically reads the active route and
  `worker:do-rollout:<ns>:<worker>` projection. A healthy connection may keep draining
  on its original immutable version. After abnormal backend loss, Gateway reconnects
  that pinned version only while it remains active; an active-version change or a newer
  sequence in the current DO `restart` projection closes the public connection with
  `1012`. Rolling gateway or runtime can still drop the physical client connection.

## Redis / Storage Contracts

Gateway reads:

```text
namespaces               Set, active namespace gate
declared-hosts           Set, custom/pattern hosts declared by any namespace
routes:<ns>              Hash, worker name -> active version
worker:do-rollout:<ns>:<worker>
                         String, active DO rollout version/mode/sequence projection
platform-domain-disabled:<ns>
                         Set, workers hidden from the platform-domain branch
patterns:<host>          Hash, path slot -> v2 tab-separated projection
```

Gateway subscribes to:

```text
routes:invalidate        payload = namespace
routes:flush             payload ignored
patterns:invalidate      payload = host or "*"
do-rollout:restart       payload = {ns,worker,version,restartSequence}
worker:delete            payload = {ns,worker}
```

Control writes Redis and publishes invalidations. Gateway never calls control to ask
whether a route changed.

## Ownership / Concurrency / Failure Semantics

- Route caches are pull-triggered and self-healing.
- Gateway clears route/pattern caches on subscriber connect and disconnect, because
  pub/sub messages are not durable.
- Subscriber reconnects clear local caches, and the next request re-reads Redis; missed
  invalidations therefore degrade to bounded stale cache, not permanent drift.
- Gateway also keeps a process-local registry of active public WebSocket sessions grouped
  by namespace and worker. A `do-rollout:restart` event requests an authoritative
  reconciliation only for that worker; `worker:delete` does the same for a successful
  whole-worker delete. Neither event closes from its payload alone. Requests are
  coalesced per group, and transport retries retain only failed groups. If delete
  reconciliation observes the worker as inactive, its established sessions close with
  `1012`. If same-name recreation completes before a successful authoritative read, the
  latest projection wins and cannot prove the intervening deletion; eliminating that
  accepted window would require a durable incarnation fence. The subscriber settles a
  request-owned lifecycle signal; workerd resumes its continuation in that WebSocket's
  IoContext before Gateway touches either peer. A later `preserve` projection supersedes
  an unobserved restart at the same sequence. Transport failures leave healthy sessions
  registered and retry with bounded backoff; malformed or regressed authoritative state
  closes affected sessions with `1011`.
- Membership-gate reads restart when that gate changes. Once the gate is warm, a cold
  route or pattern projection read is fenced by its namespace or host plus the global
  reset generation: invalidation of that key or a full reset discards the reply, while
  unrelated key invalidations do not. Per-key generations exist only while reads are
  in flight. Gateway retries at most five snapshots, then returns
  `503 gateway_routing_unavailable`.
- Pattern-host reassignment between namespaces has the same non-durable hint window:
  ordinary control writers publish invalidation, but Redis state is authoritative only
  after the gateway drops or refreshes its local cache.
- Redis outage on data-plane route lookup surfaces as gateway failure; admin-host
  forwarding remains independent of route Redis state.
- Pattern branch leaves the request path unchanged; subdomain branch strips the leading
  worker segment.
- WebSocket backend reconnect is bounded and owns a bounded client-frame buffer.
- After route resolution, Gateway reads the route and DO rollout projection at one Redis
  linearization point. An initial route mismatch fails with
  `503 gateway_routing_unavailable` before the backend upgrade. This intentionally makes
  the client retry the full upgrade after a stale route-cache hit instead of introducing
  a second route-resolution path or admitting an inactive immutable version. A second
  check after the upgrade establishes the backend's active-state admission point. After
  abnormal backend loss, Gateway checks before retrying and validates a successful
  replacement upgrade before attaching it. Pending initial and replacement backend
  sockets remain request-owned while that check runs, so terminal lifecycle signals can
  close them immediately. A `preserve` promotion committed after the final snapshot may
  leave the just-admitted backend draining; Gateway does not add a cross-service barrier
  or run lifecycle checks for every frame. A missing rollout projection beside an active
  route means default `preserve`. A missing route and projection means an inactive
  worker: initial admission returns `503`, while an established session closes with
  `1012`.
  Torn or malformed state fails closed with `1011` for established sessions.
- Normal upstream closure propagates without another lifecycle read. After abnormal
  loss, an unchanged active version permits transparent reconnect to the same pinned
  worker id when the sequence is unchanged or the current projection is `preserve`. A
  changed active version, or a newer sequence in the current `restart` projection, closes
  the public and backend peers with `1012 service restart`.
  Lifecycle commands have a socket-closing two-second deadline. Transport failures and
  transient Redis reply codes (`BUSY`, `CLUSTERDOWN`, `LOADING`, `MASTERDOWN`,
  `READONLY`, and `TRYAGAIN`) retry within the configured reconnect schedule. Malformed
  persisted state, non-transient Redis reply errors, regressed sequence state, or
  exhausted retries close both peers with `1011` instead of reconnecting stale state.
- Gateway-owned WebSocket peers use `arraybuffer` binary delivery so text and binary
  messages can be forwarded without changing their frame type. Tenant WebSocket code
  retains workerd's normal `binaryType` contract.
- Client close and error events terminate both the public WebSocket pair and the current
  backend socket. Backend Close frames use workerd's default reciprocal Close handling
  instead of leaving old backend sockets half-open during normal closure or reconnect.
  A Close frame without a status remains status-free; abnormal codes that cannot appear
  on the wire are forwarded as `1011`, and forwarded reasons are bounded to the WebSocket
  123-byte UTF-8 limit.

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

- Deploy the Gateway, Workflows, and do-runtime rollout readers before Control can write
  `durableObjectRollout=restart`. Roll system-runtime/Control last; keep Control
  mutations paused while that tier rolls and allow API clients to send the new field
  only after mutations resume.
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
- `tests/integration/gateway.test.js`
- `tests/integration/gateway-websocket.test.js`
- `tests/integration/routing-gateway.test.js`
- `tests/unit/style-contracts.test.js`

## Known Constraints And Non-Goals

- Gateway has no synchronous per-gateway invalidation acknowledgement.
- Gateway is not the authorization layer for control APIs.
- Gateway is not responsible for D1, DO, queues, cron, or workflows routing after a
  worker has been loaded.
- WebSocket lifecycle checks are not per-frame owner fences. `preserve` lets a healthy
  old-version connection drain, but it does not reload that version after the active
  route changes and its backend detaches. `restart` additionally closes older public
  sessions immediately; stale DO facets converge lazily on their next dispatch.

# Durable Objects

## Purpose

Durable Objects provide stateful, named object execution for tenant workers while
preserving stock workerd's native Durable Object programming model, including
synchronous SQLite-backed `ctx.storage.sql`.

## Current Implementation

DO execution is isolated in `do-runtime`, a separate workerd service on `:8788`. Loaded
workers use a facade in `runtime/do-client.js`; do-runtime loads the same immutable
bundle and resolves user classes with `WorkerStub.getDurableObjectClass()`, then runs
them as native facets through a host actor.

Key files:

- `runtime/do-client.js`, `runtime/bindings/do.js`
- `do-runtime/index.js`, `do-runtime/actor.js`, `do-runtime/load.js`
- `do-runtime/owner-registry.js`, `do-runtime/owner-client.js`
- `do-runtime/alarm*.js`
- `supervisor` for drain/renew process supervision

workerd provides the native Durable Object execution model inside the host actor: class
construction, facet identity, SQLite-backed storage, synchronous `ctx.storage.sql`,
alarms as a storage-facing API surface, and the in-facet WebSocket hibernation APIs. WDL
supplies the parts that Cloudflare's platform would normally provide outside the
isolate: namespace binding materialization, owner lookup, routing to the owning task,
Redis-backed lease/fence state, gateway-held public WebSocket forwarding, alarm
scheduling through Workflows, and lifecycle cleanup metadata.

The runtime shims `ctx.storage.setAlarm()`, `getAlarm()`, and `deleteAlarm()` because
stock workerd throws for native alarms on the SQLite-backed facets WDL uses. Alarm state
lives in object SQLite; Workflows owns the backend due/retry/discard job state in DB 2.
Alarm writes are supported inside async `ctx.storage.transaction()` callbacks, where the
shim can flush backend side effects after the transaction commits. `transactionSync()`
cannot await those side effects, so `setAlarm()` and `deleteAlarm()` throw when called
from a synchronous transaction callback.

## Interfaces

- Tenant binding: Durable Object namespace facade in loaded worker env.
- Runtime -> do-runtime fetch/RPC: `/internal/do/invoke`
- Runtime -> do-runtime WebSocket: `/internal/do/connect`
- do-runtime -> workflows alarm writes: `/internal/workflows/do-alarms/set`,
  `/internal/workflows/do-alarms/delete`
- workflows -> do-runtime alarm dispatch: `/internal/do/alarms/dispatch`
- Internal storage cleanup: `/internal/do/storage/delete`,
  `/internal/do/storage/delete-worker`
- Local supervisor endpoints: `/internal/do/drain`, `/internal/do/renew`
- Owner/diagnostic probe: `/internal/do/probe`

The storage cleanup endpoints are private platform interfaces for native facet storage
cleanup and worker storage cleanup; they are not tenant-facing APIs. They are reserved
for future platform cleanup flows and are not yet exercised by the normal worker
lifecycle path.

DO protocol errors use `{ error, message, details? }`. Unlike the flat additive admin
HTTP error shape, DO protocol details are nested under `details` because the consumer is
the runtime/DO client protocol, not a generic admin JSON parser. Unknown internal
exceptions are still downgraded to safe `internal_error` / `Internal error` messages.
Storage delete-worker may return HTTP 207 with `{ ok:false, deleted, errors }` for a
partial batch result; that is a result envelope, not a generic JSON error envelope.
Tenant-originated DO fetch bodies are capped at 1 MiB in the runtime facade. The facade
rejects an oversized `Content-Length` before reading, and streamed bodies are read
incrementally so the cap is enforced before buffering the full body.

DO RPC method names use the JavaScript identifier grammar. The do-runtime protocol
reader caps them at 256 ASCII bytes. RPC arguments are structural JSON data capped at
1 MiB: finite numbers, strings, booleans, null, dense arrays, and plain objects are
accepted. Serialization does not invoke `toJSON()` hooks; sparse arrays, circular
structures, non-plain objects, and non-JSON values fail before dispatch.

do-runtime invokes tenant alarm and RPC methods through private fetch dispatches
intercepted by the generated wrapper. Those requests carry the outer request id so the
host facade can propagate it without adding platform metadata to the tenant argument
list. Persistent class instances use a small mutable diagnostic context, so concurrent
or re-entrant calls may observe another invocation's id. Nested DO fetch/connect
requests discard tenant-supplied `x-request-id` values and propagate the sanitized
context id when available. Request ids remain best-effort, untrusted diagnostic
metadata.

DO invoke envelopes identify persisted bundles by canonical namespace, worker, version,
and storage id. They do not accept inline worker source.

Tenant-facing DO object names and ids must be well-formed Unicode strings. Lone UTF-16
surrogates are rejected by `idFromName()` / `idFromString()` before hashing or dispatch;
do-runtime alarm ingress and Workflows revalidation enforce the same boundary.

DO host ids are capped at 512 UTF-8 bytes and use canonical `shardN` suffixes without
leading zeroes. DO binding class names use the ASCII JavaScript class-name grammar and
are capped at 468 bytes at deploy, so every shard suffix fits the aggregate host-id cap.

## Redis / Storage Contracts

Control assigns an opaque `doStorageId` per logical worker lifecycle and freezes it into
DO binding metadata. Native facet SQLite files live under do-runtime `localDisk`
storage, mounted on EFS in ECS.

Key families:

| Key | Type | Owner | Authority | Cleanup/delete semantics |
|---|---|---|---|---|
| `worker:do-storage:<ns>:<worker>` | String | Control | Authoritative pointer from logical worker to current `doStorageId`. | Whole-worker delete removes the pointer; redeploy without the pointer allocates a new storage id. |
| `do:objects:<doStorageId>` | Set | do-runtime | Best-effort registry/tombstone of objects observed under a storage id. | Preserved after whole-worker delete for future platform cleanup; object SQLite state remains in localDisk/EFS. |
| `do:owner:scope:<encoded scope>` | String EX | do-runtime | Authoritative owner lease for `doStorageId:className:shard<N>`. | Redis server `TIME` drives lease expiry; stale owners must not commit. |
| `do:owner:scope:<encoded scope>:generation` | String | do-runtime | Monotonic generation counter for the owner scope. | Never decremented; stale generations are rejected. |
| `wf:internal:do-alarm:{<jobId>}:state` and related `wf:internal:do-alarm:*` keys | Hash/ZSET/Set | workflows | Authoritative backend job state for one SQLite alarm row. | Successful delivery, retry exhaustion, explicit delete, and whole-worker cleanup remove the job. |

Ownership is shard-based:

- Each Worker DO class has 16 fixed host actor shards.
- Shard = `stableHash(objectName) % 16`.
- Owner lease scope is `doStorageId:className:shard<N>`.
- Redis owner state carries task identity and monotonic generation.

Alarm state lives in object SQLite. Workflows receives set/delete requests from
do-runtime and stores one internal job per pending row. Row tokens fence user-driven
delete against stale backend delivery; Workflows run tokens fence dispatch retry and
completion inside DB 2.

workerd 2026-07-01 rejects SQLite object names under the reserved `_cf_` namespace
case-insensitively. `ctx.storage.deleteAll()` skips those names case-insensitively as
well, so old storage created before the stricter check with variants such as `_CF_*`
does not make cleanup fail. Those legacy reserved-name objects remain inaccessible to
tenant SQL and should be treated as upgrade debris, not application tables.

`getAlarm()` performs alarm-scoped read repair: if SQLite has a pending alarm row but
the Workflows DB 2 due index is missing, it idempotently rewrites the backend due index
without adding Redis IO to ordinary DO fetches. Active and retained alarms keep their
scheduled worker version; after an old version is deleted, alarm dispatch retargets to
the current active version only when the `doStorageId` still matches. Alarms self-clean
when the logical worker is gone or now points at a different `doStorageId`.

## Ownership / Concurrency / Failure Semantics

- One task owns a class shard at a time.
- Generation fencing prevents stale owners from committing after ownership moves.
- `do-runtime/protocol.js` owns the DO ownership error vocabulary. The injected runtime
  transport keeps its retry and stale-hint subsets private and pins them through
  response-classification tests.
- Facet identity is `className:objectName` inside stable `doStorageId`, so worker
  promotion preserves object state.
- Existing native facets keep the constructed class version until host actor restart or
  facet deletion. Promotion changes future loads and routing metadata, not an already
  constructed facet in the current host actor.
- Whole-worker delete assigns a new `doStorageId` on redeploy; old native storage is
  tombstoned for cleanup rather than immediately purged.
- WebSocket upgrades must complete on the owner endpoint. Owner-hinted WebSocket direct
  retry cannot fall back to a router-established 101.
- WDL intentionally keeps the client-facing WebSocket at the gateway when possible,
  including backend reconnect after user-runtime or do-runtime restart. This is stronger
  connection continuity than Cloudflare's shutdown behavior, which may terminate
  WebSocket connections so a new Durable Object instance can take over. The current
  backend facet is still owner-scoped: after the initial `101`, WebSocket message and
  close events are not re-fenced against the Redis owner generation on every frame.
  Future tightening should preserve the client connection where possible while
  rebinding or rejecting stale backend owner facets; it should not rely on client
  disconnect as the primary safety mechanism.
  Client messages queued under an older backend reconnect epoch may be discarded
  without per-frame ack/nack when the gateway resets that epoch.
- Ordinary fetch/RPC can perform one router rediscovery after a trusted owner-hint or an
  explicit pre-dispatch stale-owner/owner-race response carrying do-runtime's private
  ownership-error control header, including for non-idempotent methods and RPC. Tenant
  response bodies cannot opt into replay. An unmarked direct owner transport failure, or
  a 502/503/504 without either trusted marker, evicts the cached hint. Safe `GET`/`HEAD`
  requests may replay through the router; non-idempotent methods and RPC return
  `owner_unavailable` because the owner may already have applied the request.
- The shared runtime transport owns owner-hint cache wiring, invoke race retry, and
  response-header stripping for both the host binding and injected facade. Its connect
  wrapper deliberately omits the invoke-only router fallback required to preserve
  owner-established WebSocket upgrades.
- `WEBSOCKET_RECONNECT_DELAYS_MS` and `WEBSOCKET_MAX_BUFFERED_MESSAGES` tune gateway
  backend reconnect budget and client-message buffering without a code rebuild.
- Alarm delivery is at-least-once. Scheduler wakes Workflows; Workflows promotes due
  internal alarm jobs to ready, claims one job under a DB 2 run token, and calls
  do-runtime `/internal/do/alarms/dispatch`. do-runtime constructs a native
  `DoInvoke{kind:"alarm"}` request and uses the normal owner router/fence path.
- Alarm mutation, retarget, dispatch, and whole-worker storage cleanup accept only the
  canonical positive JavaScript-safe-integer worker version grammar. Invalid internal or
  persisted versions fail before a job is stored or a worker invoke is attempted.
- Alarm due times are Unix millisecond timestamps supplied to `setAlarm()`. Workflows
  and do-runtime both evaluate those timestamps with their local wall clocks; if a
  backend ready hint reaches do-runtime before the SQLite alarm row is locally due,
  do-runtime ignores that dispatch without clearing the row so the backend due-index
  repair path can deliver it later. This is an alarm compatibility boundary, not part
  of the Redis-time owner lease fence.
- Failed alarms retry with exponential backoff and jitter from
  `WORKFLOWS_DO_ALARM_RETRY_DELAY_MS`, `WORKFLOWS_DO_ALARM_RETRY_MAX_DELAY_MS`, and
  `WORKFLOWS_DO_ALARM_RETRY_JITTER` up to `WORKFLOWS_DO_ALARM_RETRY_MAX_TRIES`
  (default `6`), then discard and increment
  `do_alarm_dispatches{outcome="discarded"}`.
- If the Workflows client times out after calling do-runtime, the backend keeps the
  running claim until `WORKFLOWS_DO_ALARM_CLAIM_LEASE_MS` expires instead of
  immediately scheduling a retry. The default is five minutes, and the configured value
  is clamped above `WORKFLOWS_DISPATCH_TIMEOUT_MS` so normal timeout handling avoids
  overlapping alarm bodies while do-runtime may still be executing the original
  dispatch. Operators should size the claim lease for the longest expected alarm handler
  body, not only for the HTTP dispatch timeout; alarm bodies remain at-least-once and
  may overlap after the claim lease expires.

Owner resolution is the single-writer protocol:

1. do-runtime derives an owner scope from `doStorageId`, class name, and shard.
2. Owner resolution WATCHes the owner record, generation key, worker delete lock, and
   active worker storage pointer. A `whole` delete lock rejects ownership; a `version`
   lock remains part of the watched snapshot but does not interrupt active storage. The
   WATCH prevents a claim from committing after whole-worker delete starts. Renewal
   takes a pipelined owner/storage snapshot, then uses a Lua CAS to atomically compare
   the exact owner bytes and active storage pointer before refreshing the TTL. Its
   generation fence is carried by the owner record rather than a second generation-key
   read.
3. If a live owner exists on another task, the router returns that owner or an
   owner-hint header; the runtime facade may retry directly, but the owner task still
   rechecks the fence.
4. If the owner is missing or expired, the claimant bumps the monotonic generation
   counter and writes a new owner record with TTL in one Redis transaction.
5. Local dispatch checks `taskId`, `generation`, lease expiry, active `doStorageId`,
   and remaining lease budget before using a native facet. A stale generation, expired
   lease, or changed storage pointer fails closed. Every owner-side assertion, including
   `/delete-storage`, reads the owner record, active storage pointer, and Redis time in
   one snapshot. If less than
   `DO_OWNER_LEASE_GUARD_MS` remains (default `1000`), the owner first tries a
   same-task, same-generation CAS renew; if renewal fails, it fails closed. This guard
   narrows the takeover window; it is not a per-SQL-call or SQLite commit-time fence.
6. Supervisor renews local owned scopes through `127.0.0.1:8788`; `/internal/do/probe`
   exposes task and owner state for diagnostics. Drain stops new ownership and waits up
   to `DO_DRAIN_IN_FLIGHT_TIMEOUT_MS` (default `8000`) for host-actor dispatches to
   finish before releasing matching generations. If drain succeeds, `do-supervisor`
   kills workerd directly instead of relying on workerd's post-SIGTERM graceful window,
   which otherwise leaves the listener half-dead and can create a takeover 504 window.
   If drain times out, it returns 503 and keeps leases intact so failover waits for
   normal lease expiry. In-flight handlers also have a lease-budget watchdog that
   rechecks ownership `DO_OWNER_LEASE_GUARD_MS` before expiry, forgets the affected owner
   scope, and aborts the affected facet if renewal stops or ownership moves; it does not
   put the whole task into draining state.

The generation key is not a cache. It is the fence that makes stale owners fail later
owner-side checks after an expired Redis owner record disappears and a different task
claims the same scope. This prevents stale owners from starting new protected dispatches
or passing lease-budget rechecks; it does not physically fence an already-running SQLite
commit.

Terraform sets an explicit memory hard limit on the do-runtime workerd container in
addition to the Fargate task memory limit, and reserves memory for the colocated
redis-proxy sidecar. That is a container failure boundary, not a per-storage-call memory
interrupt.

## Security Boundaries

- do-runtime internal endpoints are private-mesh only and require the shared
  `WDL_INTERNAL_AUTH_TOKEN` through `x-wdl-internal-auth`; health and metrics are the
  only unauthenticated endpoints.
- Tenant code reaches DOs only through runtime-generated facades and frozen metadata.
- Tenant-visible DO metadata and errors must not include owner task ids, backend
  endpoints, or raw transport error text.
- Owner hints are trusted only when returned by do-runtime headers and validated against
  endpoint grammar. Owner hints and invoke fences must carry a positive
  JavaScript-safe-integer generation.
- Task identities and persisted owner records are validated on write and read. A
  persisted record's `ownerKey`, `hostId`, storage id, class, and shard must reconstruct
  the Redis scope under which it was read. During owner resolution, its canonical
  namespace and worker must also match the invoking bundle before do-runtime reads that
  bundle's active storage pointer. Owner forwarding accepts only the DO service/headless
  DNS forms or private RFC1918/100.64 IPv4 addresses on port 8788; invalid records fail
  closed before internal auth is attached.
- Owner-hint and ownership-error defense is layered: tenant response bodies and
  tenant-supplied control headers are ignored, only do-runtime control headers are
  trusted, and endpoint grammar/acceptable-address checks must pass for hints.
- The injected DO transport and shared D1/DO endpoint validator evaluate before tenant
  modules and capture the intrinsics used for private-header stripping, request bounds,
  invoke serialization, replay classification, and endpoint validation. Tenant prototype
  mutation cannot rewrite a trusted target or replay policy after those checks.
- The injected alarm shim also evaluates before the tenant module and captures the
  request, response, numeric, proxy, and reflection operations that classify internal
  alarms, update their SQLite state, and install the storage facade. Tenant top-level
  mutation of those intrinsics cannot redirect an internal alarm to the tenant fetch
  handler or prevent that facade installation.
- do-runtime supervisor must call local `127.0.0.1:8788` drain/renew endpoints; Service
  Connect aliases may hit a different task.

## Observability

do-runtime emits structured logs around owner resolution, dispatch, alarm execution,
drain, renew, and WebSocket handling. Workflows emits backend alarm
delivery/retry/discard/in-flight-unknown outcomes through `do_alarm_dispatches`;
do-runtime metrics cover runtime operations. Gateway request logs do not measure the
lifetime of backend WebSocket recovery after the initial 101.

## Deployment / Rollout Notes

- do-runtime should roll with user/system runtime when DO binding transport shape
  changes.
- Drain should run before workerd process termination so owned shards release or fail
  over by lease expiry.
- EFS shared storage is safe only because owner lease + generation fence keep one writer
  per owner scope.
- A best-effort downgrade of a localDisk volume from workerd 2026-07-03 or later to
  2026-07-01 should apply the scheduler-metadata cleanup documented in the
  [infra rollout notes](infra.md#deployment--rollout-notes).
- That cleanup restores process startup only. Before downgrading to workerd
  2026-07-01, rewrite or delete any `Blob` values persisted through
  `ctx.storage.put()` because that runtime cannot deserialize them.
- Drain and renew must target the local `127.0.0.1:8788` service. A Service Connect or
  Kubernetes service alias may hit a different task and cannot express local-owner
  release semantics.

## Tests That Protect This Module

- `tests/integration/durable-objects-core.test.js`
- `tests/integration/durable-objects-storage.test.js`
- `tests/integration/durable-objects-ownership.test.js`
- `tests/integration/durable-objects-alarms.test.js`
- `tests/integration/durable-objects-websocket.test.js`
- `tests/unit/do-alarm-client.test.js`
- `tests/unit/do-alarm-shim.test.js`
- `tests/unit/do-owner-registry.test.js`
- `tests/unit/do-owner-client.test.js`
- `tests/unit/do-object-registry.test.js`
- `tests/unit/do-runtime-actor.test.js`
- `tests/unit/do-runtime-http.test.js`
- `tests/unit/do-runtime-load.test.js`
- `tests/unit/do-runtime-protocol.test.js`
- `tests/unit/do-state.test.js`
- `tests/unit/do-task-identity.test.js`
- `tests/unit/runtime-do-client.test.js`
- `rust/supervisor/src/drain.rs`
- `rust/supervisor/src/renew.rs`

## Known Constraints And Non-Goals

- Native facet SQLite storage is not physically purged on worker delete in the current
  lifecycle.
- Whole-worker delete removes the active `worker:do-storage:<ns>:<worker>` pointer and
  asks Workflows to remove internal DO alarm jobs after the delete commits. Late
  `setAlarm()` writes from an old facet are ignored once the pointer is gone. Cleanup is
  fenced to the deleted `doStorageId`, so a same-name redeploy with a new storage id is
  not swept by the old delete. If best-effort cleanup fails, a far-future residual alarm
  job can remain in DB 2 until it becomes due; it then self-discards because the storage
  pointer is gone. First owner claim watches the same per-worker delete lock and storage
  pointer; only the `whole` lock kind rejects ownership, so deleting an inactive version
  does not interrupt the active worker. A whole-worker delete therefore cannot miss
  owner/generation state created after its final owner scan. `do:objects:<doStorageId>`
  remains a tombstone for future platform cleanup.
- DO object registry writes are best-effort. Dispatch continues if the registry write
  fails, so the tombstone set may be incomplete; future cleanup must tolerate missing
  members and treat the active storage pointer plus owner/alarm state as the stronger
  lifecycle signals.
- Gateway-held WebSocket recovery is best-effort for client connection continuity.
  Backend DO facets are not re-fenced per message after the initial `101`; owner handoff
  safety relies on reconnect/rebind behavior and the owner-side dispatch fences that run
  before a backend facet is created. Client messages queued under an older backend
  reconnect epoch may be discarded without per-frame ack/nack when the gateway resets
  that epoch.
- Owner-hinted WebSocket direct retry failures do not fall back to the router, because
  the final 101 must come from the owner endpoint.
- Unmarked ordinary fetch/RPC direct failures only fall back to the router for safe
  `GET`/`HEAD` requests. Non-idempotent methods and RPC return `owner_unavailable` when
  the outcome may be unknown. A trusted owner-hint or explicit stale-owner/owner-race
  response remains retryable once for every method because it proves dispatch did not
  reach tenant code.
- Renamed/deleted migrations are deferred.
- Long handlers still need user-level care; lease-budget watchdogs protect platform
  ownership and narrow failover races, not every storage call or the final SQLite
  commit point.

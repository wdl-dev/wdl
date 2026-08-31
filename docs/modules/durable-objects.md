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

- `runtime/do-client.js`, `runtime/_wdl-do-scoped-request.js`
- `runtime/bindings/do.js`, `runtime/_wdl-do-transport.js`
- `do-runtime/index.js`, `do-runtime/actor.js`, `do-runtime/load.js`
- `do-runtime/owner-registry.js`, `do-runtime/owner-client.js`
- `do-runtime/alarm*.js`
- `supervisor` for drain/renew process supervision

workerd provides the native Durable Object execution model inside the host actor: class
construction, facet identity, SQLite-backed storage, synchronous `ctx.storage.sql`,
alarms as a storage-facing API surface, and the in-facet WebSocket hibernation APIs. WDL
supplies the parts that Cloudflare's platform would normally provide outside the
isolate: namespace binding materialization, owner lookup, routing to the owning task,
Redis-backed lease/fence state, gateway-managed public WebSocket forwarding, alarm
scheduling through Workflows, and lifecycle cleanup metadata.

The runtime shims `ctx.storage.setAlarm()`, `getAlarm()`, and `deleteAlarm()` because
stock workerd throws for native alarms on the SQLite-backed facets WDL uses. Alarm state
lives in object SQLite; Workflows owns the backend due/retry/discard job state in DB 2.
Alarm writes are supported inside async `ctx.storage.transaction()` callbacks, where the
shim can flush backend side effects after the transaction commits. `transactionSync()`
cannot await those side effects, so `setAlarm()` and `deleteAlarm()` throw when called
from a synchronous transaction callback. `deleteAll()` is supported only outside a
transaction through WDL's best-effort storage shim. Alarm APIs are also unsupported
inside a nested async transaction because releasing a child savepoint is not a backend
commit boundary.

## Interfaces

- Tenant binding: Durable Object namespace facade in loaded worker env.
- Native `ctx.storage.sql` supports SQLite R*Tree virtual tables (`rtree`, `rtree_i32`)
  and `rtreecheck()` under the same facet storage and ownership boundaries.
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
Tenant-originated DO fetch bodies are capped at 1 MiB in the runtime host adapter. The
adapter rejects an oversized `Content-Length` before reading, and streamed bodies are
read incrementally so the cap is enforced before buffering the full body.

DO RPC method names use the JavaScript identifier grammar. The do-runtime protocol
reader caps them at 256 ASCII bytes. RPC arguments are structural JSON data capped at
1 MiB: finite numbers, strings, booleans, null, dense arrays, and plain objects are
accepted. Serialization does not invoke `toJSON()` hooks; sparse arrays, circular
structures, non-plain objects, and non-JSON values fail before dispatch.
The host adapter also reads RPC response envelopes under a 1 MiB cap, rejecting an
oversized `Content-Length` before buffering and cancelling a streamed body as soon as
it crosses the cap. A body read, UTF-8 decode, JSON parse, or successful-envelope
validation failure throws `do_rpc_result_unknown`; the method may already have run, so
the caller must not blindly replay it.

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
do-runtime alarm ingress and Workflows revalidation enforce the same boundary. The
binding-scoped native Fetcher uses the same canonical reversible ASCII encoding for
both object-name header hops, so HTTP header normalization cannot trim whitespace or
reject Unicode before the original identity reaches do-runtime validation.

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
| `worker:session-policy:<ns>:<worker>` | String | Control | Active JSON projection `{version,mode,restartSequence}` committed with the route flip. | Whole-worker delete removes it; a missing projection means default `preserve`. |
| `worker:session-policy-seq:<ns>:<worker>` | String | Control | Permanent monotonic allocator for restart events. | Survives whole-worker delete so the next restart after recreation cannot reuse a sequence observed by an old Gateway session. |
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
case-insensitively. WDL's best-effort `ctx.storage.deleteAll()` skips those names as well,
so old variants such as `_CF_*` remain inaccessible upgrade debris until the private
do-runtime facet-deletion owner removes the complete database.

Pinned stock workerd's native SQLite reset path assumes a root actor while recursively
deleting child facets, but WDL tenant objects are facets. WDL therefore implements the
historical `deleteAll()` surface through public storage operations: list/delete KV, drop
tenant SQL objects, then update the Workflows alarm index. KV list/delete work runs in
pages of at most 128 keys so neither values nor a delete call become unbounded. This path
is deliberately not atomic across KV, SQLite, and DB 2. A rejection can leave a partial
result, and callers must not race `deleteAll()` with any other storage or alarm mutation.
The WDL-specific `deleteAlarm:false` option preserves the local/backend alarm; omitted or
`true` requests best-effort alarm cancellation. Other native `deleteAll()` options do not
strengthen this contract. Private facet deletion remains the authoritative platform
cleanup path.

`getAlarm()` performs best-effort alarm-scoped read repair: if SQLite has a pending alarm
row but the Workflows DB 2 due index is missing, it attempts an idempotent backend rewrite
without adding Redis IO to ordinary DO fetches. Repair failure is logged and swallowed;
the returned timestamp confirms only the local SQLite row, not the backend job. Inside an
async storage transaction, `getAlarm()` reads only that local transactional state and
does not attempt backend repair. Under `preserve`, active and retained alarms keep their
scheduled worker version. A `restart` promotion retargets a superseded alarm to the active
version even while the old version remains retained; deleting a retained version does the
same. Both transitions require the `doStorageId` to remain unchanged. Alarms self-clean
when the logical worker is gone or now points at a different `doStorageId`.
All Workflows alarm-index mutations for one object, including best-effort read repair,
share one process-local promise tail. Concurrent requests therefore reach the backend in
API order. Mutating API failures are reported to their caller; best-effort read-repair
failures are logged and swallowed. Neither case blocks later mutations. A top-level
transaction reserves its position on this tail at the first alarm mutation and fills
that position with the final coalesced effect only after native commit, so a later
non-transactional mutation cannot overtake it.

Alarm mutation crosses object SQLite and Workflows DB 2 and is intentionally not a
distributed transaction. A successfully completed `setAlarm()` enters the at-least-once
delivery contract. Input validation fails before either store is mutated and preserves
the current alarm. Once backend index mutation begins, a rejected `setAlarm()` has an
unknown final state; a replacement attempt may also leave the previous alarm unable to
fire. A caller that still requires an alarm must call `setAlarm()` again. `getAlarm()`
repair applies only while a SQLite alarm row remains and does not confirm a failed set
mutation. A rejected `deleteAlarm()` after backend mutation begins is also
outcome-unknown: token-fenced compensation may restore the SQLite row after the backend
job was already removed. A caller that still requires deletion must call `deleteAlarm()`
again. A caller that requires the alarm to remain must call `setAlarm(desiredTime)` again
and observe success; `getAlarm()` may expose a surviving local time and trigger
best-effort repair, but neither a timestamp nor `null` confirms backend state. Async
`ctx.storage.transaction()` commits its local writes before the post-commit backend alarm
flush. If a transactional `setAlarm()` or `deleteAlarm()` flush rejects, other tenant
storage writes from the callback remain committed while the alarm row undergoes its
operation-specific compensation and retains the unknown-state semantics above. Callers
must not blindly rerun the complete transaction callback. The transaction callback
captures its entry alarm row as the backend baseline. A final delete fences, sends, and
compensates only that baseline; when no baseline exists, `setAlarm()` followed by
`deleteAlarm()` stays local and sends no backend delete. Explicit `txn.rollback()` drops
all queued alarm side effects, and subsequent shim alarm operations on that transaction
throw. On SQLite-backed workerd, alarm calls through owning `ctx.storage` aliases share
the same native transaction as calls through the callback `txn` object; same-event
branches running before callback settlement therefore participate in that transaction and
must observe the outer transaction result. The shim keeps its active transaction fence
until native commit/rollback settles. A Promise reaction after callback settlement sees
the closed transaction and cannot enqueue a backend alarm before native rollback.
Transaction-local `getAlarm()` never performs backend repair. The constructor-visible
storage alias and later `this.ctx.storage` reuse one proxy/context. Rollback bookkeeping
changes only after native rollback succeeds; a failed rollback leaves queued effects
intact, while repeated successful rollback calls retain native no-op behavior.

Bundled workerd exposes `ctx.abort(reason, { retryAlarm: false })` for its native alarm
scheduler. WDL invokes tenant `alarm()` through an authenticated owner-fenced fetch and
uses Workflows DB 2 for retry state, so workerd does not classify that dispatch as a
native alarm event. The option still aborts the facet but does not suppress WDL alarm
retries; WDL does not silently claim the new native retry-control contract.

A pending delete row stores an internal fence token with `in_flight=1`. The in-flight bit
is a same-service rolling reader fence: an older do-runtime does not understand the token
prefix, but it skips `getAlarm()` repair and rejects delivery of the original backend
token as a mismatch, so it cannot execute the tombstone as a tenant alarm. During mixed
versions an old mutator may still send the fence token as an ineffective backend CAS;
current readers always unwrap it before deletion.
Creating the fence updates only `token` and `in_flight` under the current token; it does
not revalidate unrelated scheduled-time or retry fields, so corrupt/legacy rows remain
deletable.

## Ownership / Concurrency / Failure Semantics

- One task owns a class shard at a time.
- Generation fencing prevents stale owners from committing after ownership moves.
- `do-runtime/protocol/wire-grammar.js` owns the DO ownership error vocabulary consumed
  by the producer and binding-scoped host transport, plus the host's
  pre-dispatch-safe retry subset.
- Facet identity is `className:objectName` inside stable `doStorageId`, so both session
  policy modes preserve SQLite object state.
- The worker-level session policy contract — the `sessionPolicy` bundle metadata
  field, the atomic route/projection promote commit, permanent sequence allocation, the
  `session-policy:restart` publication, and public WebSocket reconciliation — is owned
  by `docs/modules/control-auth.md` and `docs/modules/gateway.md`. This module consumes
  the committed projection.
- Missing metadata and explicit `preserve` keep the existing facet behavior: a
  constructed native facet keeps its class version until host actor restart or facet
  deletion. `restart` instead retires stale facets lazily.
- The latest active projection wins. A later `preserve` promotion keeps the allocated
  sequence but supersedes a lazy restart that Workflows or a host actor has not yet
  observed. It cannot undo a facet abort that already occurred.
- do-runtime does not enumerate every owner or facet at promotion time. Owner resolution
  and owner-side dispatch read the active projection with the owner/storage snapshot.
  The restart sequence and current mode are owner-local state derived from those Redis
  reads, not invoke/connect wire fields. A current `restart` projection rejects
  superseded immutable versions. The first host-actor dispatch with a higher sequence
  aborts only that stale facet through `facets.abort()` before recreating it; it never
  calls `facets.delete()`, so SQLite storage remains. A delayed lower-sequence dispatch
  cannot replace a newer facet.
- Native facet containers are task-local even when host SQLite is shared. The host actor
  therefore stores each observed restart sequence under `(task_id, facet_name)` and
  reloads only its task's row after reconstruction. This keeps the lazy restart fence
  intact across eviction and owner movement. It neither scans all facets nor lets one
  task acknowledge another task's stale native facet, and it avoids repeatedly aborting
  a facet that already reached the current sequence. Storage cleanup removes every task
  row for the facet.
- ECS task identity is a per-task ARN, and workerd exposes no authoritative signal that a
  task's native facet container has been permanently retired. The ledger therefore has
  no TTL or LRU retirement: under either residency mode, a long-lived facet can retain
  one small row for every task that dispatched it until storage cleanup. This long-term
  storage cost is accepted to avoid pruning a fence that a still-live task needs on a
  later owner return.
- The actor-local facet-registration cache and successful object-registry memo are each
  bounded to 10,000 advisory entries. Facet metadata eviction reloads the authoritative
  SQLite row; object memo eviction repeats the idempotent Redis `SADD`. These bounds do not
  prune the persistent session-policy ledger, object registry, native facet, or loaded
  worker isolate. The `workerLoader` binding remains the sole worker-stub factory and owns
  native isolate residency; the host actor does not mirror worker ids. Worker/class
  resolution runs only in the native facet startup callback, not on warm-facet dispatch.
- Storage cleanup is scoped to stable `doStorageId`, not an immutable worker version. It
  may dispatch through an owner while its requested version is superseded, but still
  enforces whole-worker delete exclusion, the active storage pointer, owner generation,
  and lease fences before the actor's storage-delete branch runs.
- Existing facets and already-running calls are not synchronously enumerated or
  interrupted by the promotion; they converge when the facet is next dispatched.
- Whole-worker delete assigns a new `doStorageId` on redeploy; old native storage is
  tombstoned for cleanup rather than immediately purged.
- WebSocket upgrades must complete on the owner endpoint. A valid trusted owner hint from a
  cached endpoint clears the cache and permits one router rediscovery before connecting to
  the newly resolved owner. A response carrying the private hint marker but using the
  wrong status, marker value, metadata, or shard fails as sanitized `owner_unavailable`;
  a transport failure after router handoff does not fall back to a router-established 101.
- An uncached ordinary fetch/RPC enters one router. If another task owns the shard, that
  router forwards the bounded invoke once and returns the owner's final response with
  trusted owner headers; the host learns the hint without uploading the invoke again.
  Legacy invoke hint opt-in is ignored, while WebSocket connect retains the hint-only
  handoff above.
- A forwarded or cached-direct call carries its trusted route fence to the target task.
  When that fence matches the receiving task's process-local owner record and canonical
  owner shard, do-runtime skips only its outer owner-resolution snapshot and queues the
  call to the host actor. The actor still reads Redis time, exact owner/generation,
  the worker delete lock, active storage, and the complete session-policy projection
  before tenant dispatch. Actor dispatch and storage deletion also enter the shared
  in-flight admission before mutation, so task drain rejects new work and waits for
  admitted work before releasing ownership.
  Missing local lifecycle state or a mismatched task falls back to normal resolution; a
  stale fence produces the same trusted pre-dispatch ownership error and one host-side
  router rediscovery. Old senders omit the fence and old readers ignore it, so rolling
  deployment retains the full-resolution path.
- Ordinary fetch/RPC can perform one router rediscovery after an explicit pre-dispatch
  stale-owner/owner-race response carrying do-runtime's private ownership-error control
  header, including for non-idempotent methods and RPC. Current routers forward ordinary
  invokes, so the host does not follow a legacy owner-hint response or upload the invoke
  a second time. Tenant response bodies cannot opt into replay. An unmarked direct owner
  transport failure, or a 502/503/504 without a trusted marker, evicts the cached hint.
  Safe `GET`/`HEAD` requests may replay through the router; non-idempotent methods and RPC
  return `owner_unavailable` because the owner may already have applied the request.
  Broad trusted ownership errors such as `owner_unavailable` use the same split:
  `GET`/`HEAD` may rediscover, while non-idempotent methods and RPC terminate.
- The host adapter exclusively owns owner-hint cache wiring, invoke/connect framing,
  race retry, direct-owner forwarding, and response-header stripping. The injected
  facade only packages the public request, canonical object name, and diagnostic
  request id into a binding-scoped call. The connect transport permits rediscovery only
  after a valid trusted owner-hint control response or a narrow pre-dispatch ownership
  race; it does not replay broad ownership errors or unmarked transport failures through
  the router. Host adapters in one loader isolate share a process-local
  10,000-entry LRU keyed by `doStorageId`, class, and canonical owner shard. Different
  object names in one shard reuse only its routing hint; the authenticated invoke still
  carries the exact object name, and the owner actor remains authoritative. A trusted
  returned `ownerKey` must match the locally projected shard before it can be cached or
  followed, and an explicit stale-owner response clears that shard entry. Any malformed,
  wrong-status, or mismatched WebSocket hint control response is discarded and reduced to
  `owner_unavailable`, so task identity and private endpoints cannot cross the tenant
  response boundary. Eviction only removes a routing hint; the next request returns to the
  router, so high-cardinality traffic can increase misses for other tenants but cannot
  cross an object identity or ownership fence.
- Runtime materializes one host adapter per declared DO binding. Immutable adapter props
  fix namespace, worker version, storage identity, and class before internal auth is
  attached; loaded-worker env never contains a generic DO router or owner-network
  Fetcher. Module evaluation may observe this scoped transport, but it cannot select a
  different DO binding identity. DO fetches cross into that adapter through native
  `Fetcher.fetch()`, not custom Request RPC. Static host workers enable incoming request
  signals, so aborting an explicitly abortable caller `Request` cancels bounded body
  ingestion before owner dispatch. Structured DO RPC continues to cross the custom RPC
  boundary as bounded JSON data rather than a `Request`.
- `WEBSOCKET_RECONNECT_DELAYS_MS` and `WEBSOCKET_MAX_BUFFERED_MESSAGES` tune gateway
  backend reconnect budget and client-message buffering without a code rebuild.
- Alarm delivery is at-least-once. Scheduler wakes Workflows; Workflows promotes due
  internal alarm jobs to ready, claims one job under a DB 2 run token, and calls
  do-runtime `/internal/do/alarms/dispatch`. do-runtime constructs a native
  `DoInvoke{kind:"alarm"}` request and uses the normal owner router/fence path. Delivery
  correctness does not depend on HTTP caller-disconnect signals; dispatch timeout and the
  Workflows-owned claim lease bound unknown results.
- Alarm mutations use one 5-second fetch/body deadline. Alarm delivery keeps tenant
  execution under the Workflows dispatch-timeout and claim-lease contract, then applies
  an independent 5-second deadline to the returned body. Body reads reject independently
  of best-effort stream cancellation. Responses are capped at 16 KiB and use strict
  UTF-8 JSON variants. do-runtime accepts only the shim's exact `{ok:true}` or
  `{ok:true,ignored:true}` actor result, then preserves the established
  `{ok:true,ignored:boolean}` Workflows wire. Workflows validates that outer shape before
  finalizing a claimed job. Mutation success requires exact typed `ok`, `jobId`,
  `changed`, and `deleted` fields: set/delete require a non-empty job id, while
  whole-worker cleanup requires `jobId:null`. An unreadable or malformed 2xx response
  rejects into the existing operation-specific compensation and unknown-outcome
  contract.
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
- Only a connect failure before the Workflows request reaches do-runtime or a trusted,
  complete `do_alarm_dispatch_failed` response schedules an immediate alarm retry. A timeout,
  explicit `do_alarm_dispatch_result_unknown`, owner-forward transport failure, or
  response that cannot be read and classified keeps the running claim until
  `WORKFLOWS_DO_ALARM_CLAIM_LEASE_MS` expires. A running job is removed from ready and
  indexed in due at that lease expiry, so repeated ticks do not resample it while the
  result remains unknown. The default lease is five minutes, and the configured value is
  clamped above `WORKFLOWS_DISPATCH_TIMEOUT_MS` so unknown-result handling avoids
  overlapping alarm bodies while do-runtime may still be executing the original
  dispatch. Operators should size the claim lease for the longest expected alarm handler
  body, not only for the HTTP dispatch timeout; alarm bodies remain at-least-once and
  may overlap after the claim lease expires.

Owner resolution is the single-writer protocol:

1. do-runtime derives an owner scope from `doStorageId`, class name, and shard.
2. Owner resolution WATCHes the owner record, generation key, worker delete lock,
   active worker storage pointer, and active session policy projection. A `whole` delete
   lock rejects ownership; a `version` lock remains part of the watched snapshot but
   does not interrupt active storage. A `restart` projection rejects an older immutable
   version and supplies owner-local sequence state to target-version dispatch. The WATCH
   prevents a claim from committing after whole-worker delete or session policy state
   changes.
   Renewal takes a pipelined owner/storage snapshot, then uses a Lua CAS to atomically
   compare the exact owner bytes and active storage pointer before refreshing the TTL.
   Its generation fence is carried by the owner record rather than a second
   generation-key read.
3. If a live owner exists on another task, the router forwards ordinary fetch/RPC once
   and returns the final response with owner headers. The runtime host adapter caches
   those headers for later direct calls. Forwarded and cached-direct calls carry the
   route fence into the owner actor, which remains the authoritative admission layer;
   the target task may therefore omit its otherwise redundant outer snapshot. WebSocket
   connect instead receives an owner hint and establishes its `101` directly with that
   endpoint. In every path the owner actor rechecks the complete fence.
4. If the owner is missing or expired, the claimant bumps the monotonic generation
   counter and writes a new owner record with TTL in one Redis transaction.
5. Local dispatch checks `taskId`, `generation`, lease expiry, the worker delete lock,
   active `doStorageId`, and remaining lease budget before using a native facet. A
   `whole` lock, stale generation, expired lease, or changed storage pointer fails
   closed; a `version` lock does not interrupt active storage. Every owner-side
   assertion, including `/delete-storage`, reads the owner record, delete lock, active
   storage pointer, and Redis time in one snapshot. If less than
   `DO_OWNER_LEASE_GUARD_MS` remains (default `1000`), the owner first tries a
   same-task, same-generation CAS renew; if renewal fails, it fails closed. This guard
   narrows the takeover window; it is not a per-SQL-call or SQLite commit-time fence.
6. Supervisor renews local owned scopes through `127.0.0.1:8788`; `/internal/do/probe`
   exposes task and owner state for diagnostics. Supervisor allows the local drain HTTP
   call up to `DO_DRAIN_TIMEOUT_MS` (default `10000`). Within that request, do-runtime
   stops new ownership and waits up to `DO_DRAIN_IN_FLIGHT_TIMEOUT_MS` (default `8000`)
   for host-actor dispatches and storage deletions to finish before releasing matching
   generations. Drain and renew response bodies are streamed under a 256 KiB cap before
   JSON parsing or diagnostic truncation. If drain succeeds, `do-supervisor` kills
   workerd directly instead of relying on workerd's post-SIGTERM graceful window, which
   otherwise leaves the listener half-dead and can create a takeover 504 window. If
   drain times out, it returns 503 and keeps leases intact so failover waits for normal
   lease expiry. In-flight handlers also have a
   lease-budget watchdog that rechecks ownership `DO_OWNER_LEASE_GUARD_MS` before
   expiry, forgets the affected owner scope, and aborts the affected facet if renewal
   stops or ownership moves; it does not put the whole task into draining state.

The generation key is not a cache. It is the fence that makes stale owners fail later
owner-side checks after an expired Redis owner record disappears and a different task
claims the same scope. This prevents stale owners from starting new protected dispatches
or passing lease-budget rechecks; it does not physically fence an already-running SQLite
commit.

Terraform sets an explicit memory hard limit on the do-runtime workerd container in
addition to the Fargate task memory limit, and reserves memory for the colocated
redis-proxy sidecar. That is a container failure boundary, not a per-storage-call memory
interrupt.

## Actor Residency And Eviction

`DO_PREVENT_EVICTION` is a deployment-level do-runtime setting. An unset value or the
exact value `true` selects the resident config with `preventEviction = true`. The exact
value `false` selects the otherwise identical evictable config, which omits
`preventEviction` and lets stock workerd evict idle actors. Other values are
configuration errors and make `do-supervisor` exit before starting workerd. The
Terraform `do_prevent_eviction` variable, Compose, and the local Kubernetes overlay all
default to `true`.

Changing the setting requires a do-runtime rollout. It does not change the host actor
unique key, owner scope, object identity, or localDisk path, and it does not delete
SQLite. The supervisor records the selected mode in the
`do_actor_residency_configured` structured event.

With the setting at `false`, the current bundled workerd may shut down an actor after
about 10 seconds of inactivity and periodically cleans disconnected actor containers on
an approximately 70-second loop. These are upstream implementation timings, not WDL
latency or eviction SLAs. The WDL gate observes actor reconstruction after longer idle
periods but does not claim to observe the internal ActorContainer erase pass; workerd
owns that implementation-level test. Active requests and non-hibernating WebSockets may
keep an actor active. A quiescent socket accepted through `ctx.acceptWebSocket()` is
different: workerd can retain the native socket while discarding the JavaScript actor,
then reconstruct the actor when a message arrives.

Actor reconstruction has the following boundaries:

- SQLite storage, object identity, and a quiescent hibernatable WebSocket's attachment,
  tags, and native socket survive. Current workerd's legacy hibernation manager has known
  actor-eviction races in which an in-flight application send or close can be silently
  lost. Workerd 2026-08-25 preserves in-flight auto-response state across revival and
  protects that state from GC, removing the prior blocked auto-response caveat. The
  resident default avoids exposing existing deployments to the remaining races. Set
  `DO_PREVENT_EVICTION=false` only for a workload that does not require this in-flight
  boundary and has passed the focused eviction gate.
- JavaScript instance fields, actor-held worker/class references, in-memory caches, and
  non-durable timers do not survive. Applications must not treat them as persistent
  state; an underlying workerLoader code cache may have a separate lifetime.
- Owner lease and generation renewal are independent of JavaScript actor residency.
  Renewal neither pins the actor heap nor replaces the owner-side dispatch fence after
  reconstruction.
- The first dispatch after eviction is a cold dispatch; an immediate following dispatch
  is warm again. Explicit `false` trades that first-dispatch latency for lower
  inactive-actor residency.

The qualifying two-task ECS comparison ran three interleaved resident/evictable pairs. In
that bounded fixture, the evictable mode reduced median 75-second do-runtime memory by
29%. This establishes the direction of the residency tradeoff; it is not a capacity
guarantee for other workloads.

Setting the variable to `false` is not a process-memory ceiling. V8, SQLite, allocator
arenas, loaded code, active requests, and non-hibernating sockets can still retain
memory, and released heap does not have to return to the operating system immediately.
Container memory limits, owner transfer, and workload-specific capacity tests remain
required.

## Security Boundaries

- do-runtime internal endpoints are private-mesh only and require the shared
  `WDL_INTERNAL_AUTH_TOKEN` through `x-wdl-internal-auth`; health and metrics are the
  only unauthenticated endpoints.
- Tenant code reaches DOs only through runtime-generated facades and frozen metadata.
- Module-scope raw env contains only binding-scoped DO host adapters and do-runtime's
  fixed worker/storage alarm-index adapter. DO adapters rebuild internal invoke/connect
  metadata from fixed props and strip tenant-supplied control headers before dispatch;
  the alarm adapter exposes no generic Fetcher, and delivery remains fenced by the
  matching SQLite row token.
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
  trusted, and endpoint grammar/acceptable-address checks must pass for hints. A final
  trusted ownership-control error preserves its code only for an allowlisted `503`;
  every other code/status combination becomes `503 owner_unavailable`. The host always
  replaces its message and drops private details before returning it to tenant code.
- The binding-scoped host adapter owns the DO transport and shared D1/DO endpoint
  validation outside the tenant realm. It captures the intrinsics used for
  private-header stripping, request bounds, invoke serialization, replay classification,
  and endpoint validation. The injected tenant facade contains only public namespace/id/
  stub behavior and the scoped-request codec; tenant prototype mutation cannot rewrite a
  trusted target or replay policy.
- The injected alarm shim also evaluates before the tenant module and captures the
  request, response, numeric, proxy, and reflection operations that classify internal
  alarms, update their SQLite state, and install the storage facade. Tenant top-level
  mutation of those intrinsics cannot redirect an internal alarm to the tenant fetch
  handler or prevent that facade installation.
- do-runtime supervisor must call local `127.0.0.1:8788` drain/renew endpoints; Service
  Connect aliases may hit a different task.

## Observability

do-runtime emits structured logs around actor residency selection, owner resolution,
session-policy fences, lazy facet restart, dispatch, alarm execution, drain, renew, and
WebSocket handling. Through `do_alarm_dispatches`, Workflows emits backend alarm
delivery, retry, discard, and in-flight-unknown outcomes; do-runtime metrics cover
runtime operations. Dispatch admission updates only the process-local in-flight
counter; `/_metrics` publishes its gauge immediately before rendering. Gateway request
logs do not measure the lifetime of backend WebSocket recovery after the initial 101.

## Deployment / Rollout Notes

- Cross-tier DO protocol changes follow the reader-before-writer procedure in the
  [infra rollout notes](infra.md#deployment--rollout-notes).
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
- `DO_OWNER_TTL_SECONDS` is a canonical positive decimal string no greater than
  `9007199254740`, so conversion from seconds to milliseconds remains a safe integer.
  Non-canonical or out-of-range values fall back to 120 seconds in both workerd and its
  supervisor so claim and renew schedules cannot diverge.
- Supervisor-side `DO_DRAIN_TIMEOUT_MS` is a canonical positive decimal string no
  greater than `9007199254740991`; invalid values fall back to 10000 milliseconds. It
  bounds the supervisor's local HTTP call and is distinct from do-runtime's
  `DO_DRAIN_IN_FLIGHT_TIMEOUT_MS` host-actor wait.
- Changing `DO_PREVENT_EVICTION` requires a do-runtime rollout. Keep the default `true`
  for workloads that use hibernatable WebSockets or require resident actor state. Use
  `false` only after accepting the documented cold-dispatch and in-flight WebSocket
  boundaries and passing the focused eviction gate against the target workerd image.

## Tests That Protect This Module

- `tests/integration/durable-objects-core.test.js`
- `tests/integration/durable-objects-storage.test.js`
- `tests/integration/durable-objects-ownership.test.js`
- `tests/integration/durable-objects-alarms.test.js`
- `tests/integration/durable-objects-eviction.test.js`
- `tests/integration/durable-objects-websocket.test.js`
- `tests/unit/do-alarm-client.test.js`
- `tests/unit/do-alarm-shim.test.js`
- `tests/unit/do-owner-registry.test.js`
- `tests/unit/do-owner-client.test.js`
- `tests/unit/do-object-registry.test.js`
- `tests/unit/do-runtime-actor.test.js`
- `tests/unit/do-runtime-http.test.js`
- `tests/unit/do-runtime-index.test.js`
- `tests/unit/do-runtime-load.test.js`
- `tests/unit/do-runtime-protocol.test.js`
- `tests/unit/do-state.test.js`
- `tests/unit/do-task-identity.test.js`
- `tests/unit/runtime-do-client.test.js`
- `tests/unit/runtime-do-transport.test.js`
- `rust/supervisor/src/drain.rs`
- `rust/supervisor/src/config.rs`
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
  pointer is gone. Owner claim watches the same per-worker delete lock and storage
  pointer, while every actor-side dispatch assertion reads that lock with the owner and
  storage snapshot. Only the `whole` lock kind rejects active storage, so deleting an
  inactive version does not interrupt the active worker. A whole-worker delete therefore
  cannot miss owner/generation or object-registry state created after its final scan.
  `do:objects:<doStorageId>` remains a tombstone for future platform cleanup.
- DO object registry writes are best-effort. Dispatch continues if the registry write
  fails, so the tombstone set may be incomplete; future cleanup must tolerate missing
  members and treat the active storage pointer plus owner/alarm state as the stronger
  lifecycle signals.
- Gateway-proxied WebSocket recovery is best-effort for client connection continuity in
  `preserve` mode while the pinned immutable version remains active. A detached inactive
  version is not reloaded. Exact restart and whole-worker-delete hints request an
  authoritative check for that worker; a current `restart` projection or inactive worker
  closes old public Gateway sessions with `1012`, and restart lazily aborts each stale DO
  facet when that facet is next dispatched. If same-name recreation completes before the
  delete hint obtains an inactive snapshot, the latest projection wins; the non-durable
  hint has no incarnation fence with which to identify pre-delete sessions.
  Backend DO facets are not re-fenced per message after the initial `101`; owner handoff
  safety relies on reconnect/rebind behavior and the owner-side dispatch fences that run
  before a backend facet is created. Client messages queued under an older backend
  reconnect epoch may be discarded without per-frame ack/nack when the gateway resets
  that epoch.
- A cached WebSocket owner may trigger one router rediscovery only through a valid trusted
  same-shard owner hint or a narrow pre-dispatch ownership race. Broad ownership errors
  and unmarked transport failures do not fall back because the final 101 must come from
  the owner endpoint.
- Unmarked ordinary fetch/RPC direct failures only fall back to the router for safe
  `GET`/`HEAD` requests. Non-idempotent methods and RPC return `owner_unavailable` when
  the outcome may be unknown. Only an explicit trusted stale-owner/owner-race response
  remains retryable once for every method because it proves dispatch did not reach tenant
  code. Ordinary invoke owner hints are sanitized rather than followed.
- Renamed/deleted migrations are deferred.
- Long handlers still need user-level care; lease-budget watchdogs protect platform
  ownership and narrow failover races, not every storage call or the final SQLite
  commit point.

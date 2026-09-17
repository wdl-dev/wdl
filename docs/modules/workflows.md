# Workflows

## Purpose

Workflows provide a Cloudflare-shaped workflow API backed by a WDL-owned Rust engine and
Valkey DB 2. They support same-worker workflow definitions, durable instance state, step
replay, events, pause/resume/restart/terminate, and scheduler-driven execution.

## Current Implementation

The workflow engine is `workflows`, an independent axum service on `:9120`. Runtime
exposes workflow bindings through `runtime/workflows-client.js` and dispatch helpers
under `runtime/dispatch/workflow-*.js`. Control parses workflow metadata and owns
deploy-time workflow definition keys. This module doc is the current workflows design
reference.

New deployments initialize schema 3 in dedicated DB 2. Existing schema-2 state requires
the offline [migration procedure](#deployment--rollout-notes).

workerd provides the user-code execution environment, `WorkflowEntrypoint` class shape,
module loading, and the ability for runtime to invoke a workflow class in a frozen
worker version. It does not provide a reusable local workflow engine for WDL. WDL
supplies the engine externally in workflows: DB 2 persistence, leases, ready/due
scheduling, step replay, sleeps, waits, event buffering, lifecycle transitions,
retention, and dispatch back into runtime.

## Interfaces

User-facing:

- Wrangler `[[workflows]]`
- Runtime `Workflow` binding: create, createBatch, get
- `WorkflowInstance`: status, pause, resume, terminate, restart, sendEvent
- `cloudflare:workflows` import/export specifiers rewritten to the local shim. The shim
  exposes `WorkflowEntrypoint` and `NonRetryableError`; only real module specifiers are
  rewritten, while user strings, comments, templates, regex literals, member
  `.import()` calls, `import.meta.resolve`, and private `#import` members are left
  untouched.

Control / CLI:

- `GET /ns/<ns>/workflows` lists workflow definitions and uses `workflow.list`.
  It accepts `limit` (default 100, maximum 1000) and an opaque `cursor`. A page
  processes at most 16 workers, at most 16 MiB of source snapshots plus one bounded
  lookahead, and returns at most 8 MiB of JSON. Results are
  sorted within the page, not globally across the namespace scan. Follow the
  cursor until it is `null`, including after empty or short pages. If a route
  segment or partially returned definition set changes, restart without a cursor
  after `workflow_metadata_contention`; pagination is not a durable snapshot.
  Concurrent namespace changes can repeat scan entries across pages; consumers
  collecting a complete view should deduplicate by worker/name or restart.
  Cursor input is limited to 2048 ASCII bytes. Route discovery treats
  `HSCAN COUNT 32` as a hint and rejects pages above 512 field/value pairs or
  128 KiB of combined field/value bytes before transfer. Compact hash
  encodings may return the whole hash regardless of COUNT. Increasing thresholds
  such as `hash-max-listpack-entries` can therefore make listing fail closed;
  WDL does not enforce a Valkey configuration value for those thresholds.
- `GET /ns/<ns>/workflows/<worker>/<workflow>/instances` lists instances and uses
  `workflow.read`. `limit` defaults to 100 and accepts 1-1000; each page is also bounded
  to 8 MiB of serialized JSON, including output/error payloads and the cursor envelope.
  A byte-limited page can contain fewer than `limit` instances: continue with the returned
  `cursor` until it is `null`, not until a short page. The rank cursor counts consumed
  index entries, including missing/invisible instances, and resumes at the first
  unreturned instance when the byte budget fills. It does not provide snapshot isolation
  from concurrent index changes. Payload-ref errors for an instance being read fail
  closed rather than silently omitting that instance.
- `GET /ns/<ns>/workflows/<worker>/<workflow>/instances/<id>` returns instance status
  and uses `workflow.read`. Optional query parameters use camelCase only:
  `includeSteps=true|false` includes step records, and `stepLimit=<n>` limits returned
  steps.
- `POST
  /ns/<ns>/workflows/<worker>/<workflow>/instances/<id>/{pause,resume,restart,terminate}`
  uses `workflow.write`.
- CLI `wdl workflows list|instances|status|pause|resume|restart|terminate` is a thin
  control API wrapper.

Internal:

- Runtime `Workflow` facade -> workflows endpoints: `/internal/workflows/create`,
  `/internal/workflows/create-batch`, `/internal/workflows/get`,
  `/internal/workflows/status`, `/internal/workflows/pause`,
  `/internal/workflows/resume`, `/internal/workflows/terminate`,
  `/internal/workflows/restart`, `/internal/workflows/send-event`.
- Scheduler -> workflows `/internal/workflows/tick`
- do-runtime -> workflows alarm mutation endpoints: `/internal/workflows/do-alarms/set`
  and `/internal/workflows/do-alarms/delete`
- Control -> workflows alarm cleanup endpoint:
  `/internal/workflows/do-alarms/cleanup-worker`
- workflows -> runtime `POST /internal/workflows/run` on `:8088`
- workflows -> runtime `POST /internal/workflows/notify` for progress callbacks
- workflows -> do-runtime `POST /internal/do/alarms/dispatch` for Workflows-owned
  internal Durable Object alarm delivery.
- Runtime step facade -> workflows endpoints: `/internal/workflows/claim-step`,
  `/internal/workflows/replay-steps`, `/internal/workflows/commit-step-success`,
  `/internal/workflows/commit-step-error`, `/internal/workflows/register-sleep`,
  `/internal/workflows/register-wait`.
- The authoritative internal endpoint set lives in `rust/workflows/src/server.rs`;
  update this section when that surface changes.
- Control -> workflows endpoints for `/internal/workflows/instances`, instance
  status/lifecycle proxy, and `/internal/workflows/lifecycle/check-delete` before
  worker/version delete.

## Redis / Storage Contracts

Workflows exclusively owns Valkey DB 2 for instance execution state. A custom
`WORKFLOWS_REDIS_URL` selects the Redis endpoint and may omit its database or explicitly
select DB 2; an explicit non-DB2 selection is rejected. Control owns
`wf:defs:<ns>:<worker>` in DB 0 for deploy-time workflow key allocation and stable
identity. The hash retains retired names until whole-worker delete. Definition listing
enumerates that retired history for currently active workers; deploy and single-workflow
status/lifecycle paths read only the names they need.

Each worker may retain at most 1024 definition names and 1 MiB of hash field/value
bytes. The active declaration array also has a 1 MiB JSON cap, including materialized
workflow keys. Deploy checks aggregate quota under the existing WATCH before committing;
only declared definition values are returned to the writer, while bounded size accounting
includes retired names. Quota rejection uses `workflow_definitions_too_large`.
Definition-list snapshots check cardinality and byte lengths inside Valkey before
returning values. The same snapshot atomically checks the worker's expected active
route and returns the byte total for page accounting; Control does not issue a separate
per-worker route read or re-encode the returned strings to count them. The final route
page fingerprint is still rechecked before returning the page.
Their per-version bundle metadata read budget is 8 MiB; exceeding it
returns `workflow_listing_metadata_too_large`, not an unbounded read. This is a listing
budget, not a new size limit on unrelated Worker code. Oversized retained definition
state fails closed rather than being truncated or silently adopted.

At normal startup and before a schema-migration command connects, Workflows compares the
parsed database identities of its active DB 2 and reserved archive DB 15 with
`CONTROL_REDIS_URL` and with the effective Rust data-plane URL
(`DATA_REDIS_URL ?? REDIS_URL`). Any overlap fails before connecting. Deployments with a
separate data plane must expose its canonical `DATA_REDIS_URL` to the Workflows service
so this ownership check covers it; URL credentials and unrelated query settings do not
change database identity.

Key concepts:

- `workflowKey` is the physical workflow identity.
- `(ns, worker, workflowName)` keeps a stable workflowKey across redeploys.
- Instance state, step records, payload refs, events, ready/due indexes, run leases,
  retention indexes, and callbacks live in DB 2.
- Workflow payloads are JSON data under explicit byte caps. Large application data
  should live in R2/S3/D1/KV and be referenced from workflow payloads.
- Persisted result refs are checked against the 1 MiB result limit before JSON parsing
  through the shared reader used by list, get, and status. Oversized stored results are
  invalid state, not caller input errors. Dispatch and restart params use the shared
  JSON parser with their own 1 MiB read cap. Restart validates before creating its
  pending marker or changing instance state and preserves the original JSON bytes.
  Errors for missing, oversized, or malformed JSON instance-list payloads
  include bounded instance-id and payload-ref context in the existing
  server-side `request_complete.error_message`. Payload contents are not logged;
  public 5xx responses keep the generic message.
- Instance listing reads payload refs in batches of at most 32 and releases each parsed
  instance after serializing it into the bounded page. It does not retain all selected
  payload JSON trees or build a second response tree. Control reads the page under the
  same 8 MiB cap and a five-second backend deadline, then forwards successful JSON bytes
  without re-encoding them. Oversized, unreadable, or timed-out backend pages fail as
  `503 workflow_internal_dispatch_failed`; valid backend error codes retain their
  existing sanitized mapping. This deadline does not change mutation retry semantics.
- DB 2 keys for one instance share the `{ns:workflowKey:instanceId}` hash tag, but
  workflow state also uses global ready/due/retention keys. Current deployments therefore
  require a single non-cluster Valkey shard (`num_node_groups = 1`) rather than Redis
  Cluster; a primary/replica pair for HA is fine because replication does not shard the
  keyspace, but multiple shards would split the un-hash-tagged global keys and fail with
  CROSSSLOT.
- Internal Durable Object alarm jobs also live in DB 2 under
  `wf:internal:do-alarm:*`. They are Workflows-owned backend jobs, not tenant workflow
  instances, and are reachable only through internal do-runtime/workflows endpoints.

Key families:

| Key | Type | Owner | Authority | Cleanup/delete semantics |
|---|---|---|---|---|
| `wf:defs:<ns>:<worker>` | Hash | Control | Authoritative workflow definition/key allocation for deploy metadata. | Worker delete removes definitions after lifecycle checks pass. |
| `wf:schema_version` | String | workflows | DB 2 workflow-state schema marker. | Current value is `3`; greenfield deployments start on schema 3. |
| `wf:schema3-migration` | String | workflows operator/service | Migration ownership and completion on the configured Workflows endpoint DB 0. | `in_progress:<token>` is CAS-owned by one operator task and blocks startup; `complete` permits normal service and remains after optional archive deletion. |
| `wf:instance:{<ns>:<workflowKey>:<instanceId>}:state` | Hash | workflows | Authoritative instance state. | Terminal retention and lifecycle cleanup remove expired state. |
| `wf:instance:{...}:payloads` | Hash | workflows | Payload ref storage under aggregate caps. | Deleted with the instance state family. |
| `wf:instance:{...}:steps`, `step-summaries`, `step-summary-index` | Hash/ZSET | workflows | Authoritative step replay/history state. | Deleted with the instance; history reads are bounded and reject mismatched summary/index counts or missing summaries in the requested page. |
| `wf:instance:{...}:events`, `events-by-type` | Hash/ZSET | workflows | Buffered event records and type index. | Consumed/stale events are removed during wait matching or cleanup. |
| `wf:ready:<shard>`, `wf:ready:active`, `wf:ready:cursor` | Set/String | workflows | Ready-token hints, active shard set, and fair-dispatch cursor. | Tokens are deduplicated hints; instance state remains authority; the cursor rotates shard start order across ticks. |
| `wf:due:<shard>` | ZSET | workflows | Sleep/retry/event-timeout due index. | Tick promotion moves eligible entries back to ready. |
| `wf:by-worker:<ns>:<worker>` | Set | workflows | Instance discovery by worker. | Used by list/delete checks; entries are removed by retention/delete cleanup. |
| `wf:by-workflow:<ns>:<worker>:<workflowKey>` | ZSET | workflows | Per-workflow instance list index ordered for bounded pagination. | Retention/delete cleanup removes the sorted-set member. |
| `wf:by-version:<ns>:<worker>:<version>` | Set | workflows | Frozen-version referrer index. | Blocks version delete while live instances reference the version. |
| `wf:pending-version:<ns>:<worker>:<version>` | ZSET | workflows | Short-lived restart target-version blockers, scored by expiry time. | Version-delete checks active members; restart atomically validates its marker before creating the durable `wf:by-version` referrer. Members expire after 30 seconds, and the ZSET has a 60-second key TTL for physical cleanup. |
| `wf:retention` | ZSET | workflows | Terminal retention due index. | Retention tick deletes expired terminal instances. |
| `wf:internal:do-alarm:{<jobId>}:state` | Hash | workflows | Authoritative backend job state for one Durable Object SQLite alarm row. | Successful delivery, retry exhaustion, explicit delete, and worker cleanup remove the job. |
| `wf:internal:do-alarm:due:<shard>` | ZSET | workflows | DO alarm scheduling and claim-lease index. Score is the next schedulable timestamp in milliseconds: alarm/retry due time or running claim lease expiry. | Tick promotion moves eligible jobs to ready. |
| `wf:internal:do-alarm:ready:<shard>`, `ready:active`, `ready:cursor` | Set/String | workflows | DO alarm ready hints, active shard set, and fair-dispatch cursor. | Dispatch removes ready hints or reschedules on retry; the cursor rotates shard start order across ticks. |
| `wf:internal:do-alarm:by-worker:<ns>:<worker>` | Set | workflows | Worker cleanup index for internal DO alarm jobs. | Whole-worker delete asks Workflows to remove indexed jobs after the delete commits; residual jobs self-discard on their next dispatch. |
| `wf:internal:do-alarm:by-worker:<ns>:<worker>:cleanup-snapshot:<random>` | Set | workflows | Temporary cleanup-worker snapshot of one by-worker DO alarm index. | Internal only; TTL is 60 seconds and is refreshed while cleanup drains the snapshot. |

## Ownership / Concurrency / Failure Semantics

- Workflow bindings target definitions in the same worker.
- Instances freeze the worker version/class identity they were created with.
- Control fails closed on malformed active workflow entries and malformed `wf:defs`
  records encountered by an operation; management paths return `corrupt_meta`, while
  deploy returns `workflow_definition_corrupt` when reusing a damaged historical
  definition. Damaged authoritative metadata is not exposed as a normal missing or
  retired workflow. Normal deploy and single-workflow paths do not scan unrelated
  historical definitions.
- Workflows lifecycle checks reject malformed referrer members instead of treating
  them as absent.
- Lifecycle preflight processes one bounded SSCAN page per backend call: COUNT 128
  is a hint, and pages above 512 members or 128 KiB of member bytes fail closed before
  transfer. At most 20 blockers are returned; dry-run treats expired pending creates
  as blockers without accumulating the whole set. Cleanup immediately applies the
  existing create-token fence and prunes missing-state referrers with an atomic
  absence check. Within each 20-member chunk, mutations share one pipeline; only
  failed mutation slots are reread in a second batch and remain blocking while state
  exists. Pipelines are not replayed after ambiguous failures.
  A continuation page returns `allowed:false` with an internal cursor; it never
  authorizes deletion. Control starts at zero, follows only backend cursors, renews
  the held delete lock before each page, and limits a check to 16 pages / ten seconds
  with five-second per-call and 64 KiB response bounds. Budget exhaustion returns
  `workflow_lifecycle_check_incomplete`; retrying deletion preserves completed cleanup.
  The absolute budget also bounds an in-flight page: its expiry returns the same
  incomplete code and is a warning in the transport log. A per-call timeout
  before the total budget expires, or a genuine backend/read failure, retains
  `workflow_internal_dispatch_failed` and error-level transport diagnostics.
  The limiting deadline is fixed at request setup; delayed rejection does not
  turn an earlier per-call timeout into total-budget exhaustion. Equal deadlines
  use the total-budget classification.
  Dry-run does not prune referrers, so repeated dry-runs need not converge when
  stale entries exceed the scan budget; actual deletion performs fenced cleanup.
  Lock-renewal waits use the same remaining ten-second budget; this is not a deadline
  for the entire public delete request or for unrelated Redis operations.
  Public request cursors are not accepted as deletion authorization.
  Cleanup requires the referrer set to be empty before granting permission, so a
  cursor traversal interrupted by Redis failover cannot silently skip a live referrer.
  If that final check still finds members without identified blockers, the response
  has `allowed:false`, an empty blocker list, and no cursor. Control returns
  `workflow_lifecycle_check_incomplete` and requires a fresh deletion request, rather
  than reporting an active-instance conflict or restarting the scan automatically.
- Scheduler only wakes workflows; workflows owns admission, fairness, shard ticks,
  ready/due movement, and runtime dispatch. Scheduler reads the tick response under a
  64 KiB cap and requires a valid JSON object root; individual missing or unknown fields
  remain forward-compatible and default to no reported progress.
- Scheduler also wakes Workflows-owned internal DO alarm jobs through the same
  `/internal/workflows/tick` endpoint; scheduler never reads or writes DO alarm state
  directly.
- Workflows rejects non-canonical DO alarm identity before persisting jobs and
  revalidates persisted alarm identity before dispatch. One Control-DB Lua snapshot
  reads the current storage pointer, active route, retained-version score, and the
  active session policy projection. A current `restart` projection retargets a
  superseded alarm to the active version even while its scheduled version remains
  retained; a later `preserve`
  projection supersedes an unobserved restart and keeps a retained alarm on its scheduled
  version. Malformed session policy projections, route/projection disagreement, and
  malformed active versions needed for retargeting fail closed. Namespace, worker,
  and version checks reuse `wdl-rust-common`; do-runtime protocol grammar and identity
  helpers own the canonical alarm-specific fields and aggregate 512-byte DO host-id
  contract. Workflows mirrors and revalidates that contract before persistence and
  dispatch.
  Runtime run dispatch and progress callbacks share one system-vs-user runtime endpoint
  selector inside the workflows crate.
- 32 scheduling shards partition ready/due work. A tick interleaves candidates from
  active shards instead of draining shards serially, acquires a dispatch permit before
  claim, and hands each claimed activation to a Workflows-owned background task. The
  tick waits for maintenance, claim, and admission only; it does not wait for tenant
  execution or DO alarm delivery to finish. `WORKFLOWS_READY_DISPATCH_CONCURRENCY`
  bounds workflow execution across overlapping ticks on one Workflows replica, defaults
  to `128`, and is bounded to the 1–128 ready batch. The independent DO alarm pool is
  controlled by `WORKFLOWS_DO_ALARM_DISPATCH_CONCURRENCY`, defaults to `32`, and is
  bounded to 1–100 jobs. Admitted tasks retain their pool permit and shutdown in-flight
  guard through runtime dispatch and fenced commit. Workflow execution process loss
  recovers through the run lease and ready hint. A running DO alarm is removed from
  ready and parked in its due shard at the claim lease expiry; due promotion restores
  it to ready after expiry. Scheduler applies an independent client deadline to
  maintenance and admission with `WORKFLOWS_TICK_TIMEOUT_MS`, which defaults to 60
  seconds; it is not a workflow execution deadline and is independent of the Workflows
  runtime dispatch timeout. Scheduler uses
  `WORKFLOWS_TICK_ACTIVE_INTERVAL_MS` (default 100 ms) while a tick reports maintenance,
  admission, or pending work blocked on either dispatch pool; otherwise it uses
  `WORKFLOWS_TICK_INTERVAL_MS` (default 1 second).
- Ready tokens are deduplicated hints; instance hash state is authority.
- Runtime terminal responses are tagged variants: `completed` requires an `output` field
  and `failed` requires an `error` field, while an explicit JSON `null` remains a valid
  payload. A `suspended` response clears its run claim only after the authoritative step
  backend has already moved the same generation and run token to `waiting`; an otherwise
  well-formed but out-of-order response is a fenced no-op.
- Execution commits are fenced by `generation`, `runToken`, active instance status, and
  an unexpired run lease. Step commits/registers accept the same-run `running` or
  `waiting` state so parallel siblings can finish after another sibling schedules
  retry/wait. Completed runtime terminals require `running`; failed runtime terminals
  may also close a same-run `waiting` state created by an invalid unawaited suspending
  step while the run lease is still valid. If that lease already expired, workflows only
  restores the ready hint so the next claim can replay under a fresh lease. Initial run
  admission, expired-run requeue, lifecycle commits, `sendEvent`, and retention cleanup
  also compare the persisted `createdAtMs` with `generation` so a stale snapshot cannot
  mutate a later instance recreated under the same id with a different creation
  timestamp. Lifecycle paths rotate `generation` in the same Lua commit when they
  invalidate in-flight execution.
- Runtime replay cache is advisory. DB 2 step state is authoritative. A runtime isolate
  may reuse terminal step records across run claims for the same instance incarnation;
  successful outputs are retained as serialized snapshots and decoded for each replay
  so tenant mutation cannot alter later claims. The module-level cross-request cache
  retains at most 16 MiB of serialized data per runtime isolate; oversized records are
  not cached and older caches are evicted under pressure. The
  `workflow_replay_cache_bytes` gauge reports that cross-request retained size and is
  published from the authoritative counters when `/_metrics` renders. Global eviction
  stops cross-request retention; active controllers retain their detached state until
  release. Retained and detached serialized bytes plus in-flight replay read reservations
  share a 64 MiB budget per Runtime isolate. Identity response lengths reserve their
  declared bytes; unknown/compressed responses reserve the 32 MiB reader ceiling, then
  shrink to actual bytes. The reservation remains until page validation/cache admission
  finishes. Unused retained caches are reclaimed first; saturation returns retryable
  `workflow_backend_unavailable`, never an advisory miss or fresh claim. The last
  controller releases detached state. Controller closure also cancels its pending
  replay read and synchronously releases that reservation; cleanup does not depend
  on an async finally running after workerd has ended the request. These are accounted
  bytes, not an RSS limit.
  A new claim
  reopens bounded paging so records committed by another isolate remain discoverable.
  Backend replay records are projected to the fields Runtime actually consumes before
  byte accounting and retention, so unconsumed response metadata cannot bypass the
  cache budget.
- Runtime may issue multiple `step.do` calls concurrently, commonly via `Promise.all`;
  each call receives a deterministic ordinal in user-code call order, records DAG
  dependencies from the current completed-step frontier, and commits independently under
  the run fence. Step config is JSON data passed by value across the workerLoader JSRPC
  boundary; callable hooks are not part of that contract. Workflows owns canonical
  config encoding and its exact 64 KiB limit. A `step.do` callback must not start another
  workflow step, including after an `await`; create parallel sibling promises from the
  run body before callback code is in flight. A run that returns before all started
  steps settle fails as invalid, so user code must await the concurrent step promises.
  Suspending operations (`step.sleep`, `step.sleepUntil`, `step.waitForEvent`) remain
  exclusive and must not overlap another in-flight step because they suspend the whole
  workflow run.
- Completed instances use success retention; failed and terminated instances use error
  retention. Both retention classes default to 8 hours for newly created instances and
  may be overridden with
  `create({ retention: { successRetention, errorRetention } })`.
- The bundled Workers types expose a best-effort `locationHint` create option. WDL has
  no regional Workflow placement plane, so `create()` and `createBatch()` reject a
  supplied `locationHint` field, including inherited fields, before backend I/O and
  without evaluating getter values.
- `Workflow.createBatch()` accepts at most 100 entries per call. Runtime prevalidation
  and Rust admission share this pinned limit. Rust reads the deduplicated instance-state
  snapshot in one bounded pipeline and shares the mutation preflight across entries;
  each new instance still keeps its own create token, post-create control-plane
  revalidation, cleanup, and finalize fence.
- A single workflow result is capped at 1 MiB and a runtime-to-workflows backend JSON
  request at 2 MiB. Runtime prevalidation and the Rust backend share the pinned
  `workflow_payload_too_large` contract. The per-instance aggregate payload cap is
  16 MiB. Runtime dispatch bounds serialized result and workflows-backend request bytes
  before forwarding; those documents allow at most 127 object/array levels including the
  platform envelope and reject lone UTF-16 surrogates in keys or values, matching the
  locked Rust JSON parser. Workflows owns backend JSON parsing, canonical step config,
  and persisted aggregate accounting. Step/event over-cap writes fail the request;
  over-cap runtime terminal results transition the instance to failed in the same
  transaction.
- Workflows semantic request caps use `request_too_large`; this is distinct from
  HTTP-body parser `request_body_too_large` in control/runtime protocols. Workflow
  errors otherwise use the platform `{ error, message }` envelope on HTTP boundaries.
  Workflows service 5xx responses retain their stable error code but use a fixed public
  message; the raw diagnostic remains available only to service-side request logs.

Workflow execution uses two channels:

1. The generated `Workflow` facade calls a binding-scoped host adapter. The adapter
   accepts only public Workflow operations, replaces namespace, worker, version,
   workflow key, and class with immutable binding props, attaches mesh authentication,
   and forwards to workflows. Tenant request fields cannot select another workflow.
   The scoped adapter uses native `Fetcher.fetch()`, and static host workers enable
   incoming request signals. Aborting an explicitly abortable scoped-transport
   `Request` therefore cancels bounded body ingestion before the workflows request. The
   public `Workflow` facade remains a structured operation API and does not add a
   `Request` or `AbortSignal` option.
2. workflows dispatches claimed runs back to runtime `/internal/workflows/run` on
   `:8088`. Runtime loads the frozen worker version and invokes `className.run(event,
   stepFacade)`.

Get, status, and list reads derive the payload hash from the requested namespace,
workflow key, and instance id. Persisted `ns`, `workflowKey`, `instanceId`, and
`payloadsKey` must match that canonical identity before any result/error payload is
read; divergence fails closed as invalid state.

Create and restart pin versions differently from replay. A new `create()` or `restart()`
canonicalizes against the current active route before writing DB 2, so new durable
business processes start on the active version. Existing instances replay against their
stored `frozenVersion`; promotion does not change their code. Worker-version delete is
blocked by `wf:by-version` while non-expired instances still reference the version.
Before restart revalidates the active export, it publishes a short-lived target-version
blocker. Its final DB 2 transition atomically creates the durable referrer and removes
that blocker, so version delete cannot pass between active-version resolution and the
restart commit.
Runtime validates every dispatched `frozenVersion` with the same positive
JavaScript-safe-integer version parser used by bundle keys; malformed persisted tags
fail before worker loading.

Scheduling is hint-based but state-authoritative:

1. `create`, `resume`, `restart`, and event delivery add an immediate token to
   `wf:ready:<shard>`.
2. Sleep, retry, and wait timeout write/update a due token in `wf:due:<shard>`.
3. scheduler calls `/internal/workflows/tick`; workflows promotes due tokens, samples
   ready tokens, acquires per-replica dispatch permits shared across ticks and shards,
   and claims eligible instances. The tick returns after admission rather than waiting
   for those runs to finish.
4. Claim validates status, generation, and lease state from the instance hash. Duplicate
   or stale ready/due tokens self-clean and do not execute user code.
5. A tracked Workflows task owns each admitted runtime dispatch plus its fenced result
   commit. The dispatch is bounded by `WORKFLOWS_DISPATCH_TIMEOUT_MS`. On authoritative
   step backend operations, transport failures, a missing backend binding,
   `internal_auth_failed`, `502`/`503`/`504`, and explicit `internal_error` or
   `redis_error` responses make Runtime return a fixed 503 instead of a terminal
   workflow result. Workflows then releases the ordinary run claim so a later tick can
   replay. If that same run already committed authoritative `waiting` state before the
   response failed, release preserves `waiting` and uses the suspended-claim cleanup
   path instead of restoring the pre-run status. Replay-page prefetch is advisory: a
   prefetch failure falls back to the authoritative step operation, and only a failure
   on that path applies this 503 contract.
   When parallel steps are in flight, Runtime closes new step admission and waits within
   the remaining Workflows-owned dispatch timeout, reserving one second to return the
   response. A KV read infrastructure failure reported by an escaping step callback
   during that bounded wait takes precedence over an ordinary terminal step error.
   Exhausting the authoritative dispatch budget is itself result-unknown and returns
   the same fixed 503 so Workflows releases the claim and replay can reconcile any late
   durable step commit.
   Workflows computes and sends the absolute deadline before request serialization, so
   transport, queueing, body parsing, tenant execution, and sibling settlement consume
   the same budget. Runtime rejects an already-expired budget before acquiring the
   tenant entrypoint, races the root run Promise against the deadline, and rejects a
   completed, failed, or suspended outcome that settles or finishes response construction
   after the deadline. It applies the remaining budget and a 32 MiB cap while reading
   every authoritative Workflows backend response;
   canonical identity-encoded `Content-Length` responses fill one exact buffer. A direct
   host KV read infrastructure Error that escapes `run()` or a step callback with the
   same identity reports a retryable dispatch failure only when its recorded source
   capability belongs to the current Workflow env. A step callback report is observed
   before its error can be committed; catching the rejected `step.do()` in outer `run()`
   cannot convert it back to success. Errors caught inside the relevant boundary and
   converted to a fallback are ordinary Workflow results.
   Deterministic step, fence, payload, and persisted-state errors remain terminal. A
   missing or unknown Runtime outcome, or a terminal variant missing its required
   payload field, is also a protocol error rather than an implicit failure result.
   Generation/run-token fences prevent double durable commits, but
   external side effects in user code may repeat; workflow code and step callbacks
   should be idempotent. `WORKFLOWS_RUN_LEASE_MS` is clamped above the dispatch timeout
   and acts as a stale-claim backstop, not the normal long-run timeout knob.

The step facade implements durable replay:

- `step.do(name, [config], callback)` uses the backend-owned operation kind, ordinal,
  name, same-name count, DAG dependencies, and canonical config hash as the replay
  identity. A completed matching step returns the stored result. A shape mismatch fails
  closed with `workflow_step_mismatch`.
- A single step can record at most 1000 dependency edges. If more than 1000 unjoined
  sibling steps feed one later `step.do`, workflows rejects that step request as
  `request_too_large`; add intermediate joins to keep fan-in bounded.
- A single runtime dispatch turn can have at most 1000 in-flight workflow steps and can
  start at most 1000 fresh backend steps. This caps root/sibling fan-out before those
  steps create backend claim/commit load; completed/failed replay cache hits do not
  count against the fresh-start limit. Waiting replay records recheck the workflows
  backend and count against that limit so due and wait indexes can be repaired before
  the run suspends again. Parallel `step.do` siblings must be created in the same
  synchronous fan-out batch before awaiting any of them. After user code awaits one
  sibling, it must await the whole batch before starting the next durable step, so
  replay computes the same dependency frontier.
- `step.sleep()` and `step.sleepUntil()` record waiting state and due time, then suspend
  the current run through a reserved internal sentinel.
- `step.waitForEvent()` first checks buffered events, then records a wait and optional
  timeout. `sendEvent` stores event payload and type index before the wait exists, so
  event-before-wait is supported.
- Runtime replays user code from the start. It fetches replay pages lazily and may cache
  them in-process, but DB 2 step state is authoritative.
- Workflows step responses are tagged variants. A `claim-step` or `register-wait`
  `complete` response and a replay `completed` record require their own `output` field;
  failed variants require their own `error` field. Explicit JSON `null` is a valid
  payload. A malformed advisory replay record falls back to authoritative `claim-step`;
  a malformed authoritative response is result-unknown and makes Runtime retry the run
  instead of fabricating null.
  Each new step record also carries the backend-owned operation kind (`do`, `sleep`,
  `sleepUntil`, or `waitForEvent`). A missing or mismatched replay kind is a cache miss
  and falls back to the corresponding authoritative endpoint.
- Replay step records and their referenced payloads share the full generation,
  run-token, creation-time, lease, and active-status fence. A referenced payload is
  resolved only while that fence remains valid, so restart cannot mix payloads from
  another execution generation into the page.
- The workflow engine records a durable DAG for `step.do`. Runtime assigns ordinals
  synchronously in call order, treats completed steps as the current dependency
  frontier, and stores that frontier on each later step.
  `Promise.all([step.do(...), step.do(...)])` produces
  sibling nodes with the same parents; a later `step.do` after the join depends on both
  siblings. Dependency scheduling, joins, and cancellation remain expressed by normal
  user-code `await` / `Promise` structure; workflows persists the resulting graph
  instead of running a separate graph planner.

Fence model:

- Execution commits (`claim-step`, step success/error, sleep/wait registration, runtime
  terminal) are fenced by `generation`, `runToken`, active instance status, and an
  unexpired run lease. Step commits/registers accept same-run `running` or `waiting`;
  completed runtime terminals require `running`; failed runtime terminals may also close
  a same-run `waiting` state created by an invalid unawaited suspending step while the
  run lease is still valid. If that lease already expired, workflows only restores the
  ready hint so the next claim can replay under a fresh lease.
- A Runtime `suspended` result only releases its run claim when the same authoritative
  state is already `waiting`; Runtime cannot create a waiting state by assertion alone.
- Initial run claims and expired-run requeues, lifecycle commits (`pause`, `resume`,
  `restart`, `terminate`), `sendEvent`, and retention cleanup compare both `generation`
  and the persisted `createdAtMs` incarnation field. Lifecycle paths rotate `generation`
  where they invalidate in-flight execution. If a concurrent restart or a same-id
  incarnation with a different creation timestamp wins, the stale mutation is rejected.
- Payload bytes, payload refs, counters, state changes, and ready/due updates must be
  committed in DB 2 together; workflows must fail closed on missing payload refs.

## Progress Callbacks

Progress callbacks are best-effort same-worker Durable Object pushes. A create request
may store a callback descriptor `{ kind: "do", binding, idFromName, path? }` in instance
state. workflows posts progress to runtime `POST /internal/workflows/notify`; runtime
invokes the reserved `__WdlWorkflowNotify__` entrypoint, which calls the same-worker DO
binding. Lookup and delivery use separate bounded semaphores:
`WORKFLOWS_PROGRESS_CALLBACK_LOOKUP_CONCURRENCY` defaults to `128`, and
`WORKFLOWS_PROGRESS_CALLBACK_CONCURRENCY` defaults to `32`. Saturation drops the
best-effort callback and records a dropped outcome; delivery is not transactional, so DB
2 status remains authoritative.

## Security Boundaries

- workflows private API is not public-routed.
- Positional env receives the runtime `Workflow` facade. Module evaluation can observe
  only its binding-scoped host adapter, never the generic authenticated workflows
  service Fetcher; the adapter fixes identity and exposes only public operations.
- Observer roles receive `workflow.list` only. Instance list/status are payload-bearing
  and require `workflow.read`.
- Workflow read endpoints must be treated as payload-bearing unless explicitly designed
  as metadata-only.
- Control delete fails closed if workflows lifecycle checks fail.

## Cloudflare Compatibility Notes

- WDL follows Cloudflare's durable-step model for named `step.do`, retries,
  `step.sleep`, `step.sleepUntil`, and `step.waitForEvent`, but is not a byte-for-byte
  implementation of Cloudflare's internal engine.
- WDL persists DAG edges from runtime execution. Cloudflare's dashboard visualizer
  derives a richer graph from AST parsing, including conditionals, loops, nested
  functions, and promise entry/exit ordering. WDL does not run an AST planner; it
  records the graph created by actual `step.do` calls.
- `Promise.all([step.do(...), step.do(...)])` is supported and recorded as parallel
  sibling nodes. A later `step.do` after the join records dependencies on those
  siblings.
- WDL is intentionally stricter around unresolved durable steps before suspension. If
  user code starts `step.do` promises, observes only a `Promise.race()` winner, and then
  calls `step.sleep` / `step.sleepUntil` / `step.waitForEvent` while another started
  step is still in flight, WDL fails the run as `workflow_invalid_step`. Wrap
  nondeterministic races inside a single `step.do` or await all started durable steps
  before suspending.
- WDL also treats a permanently failed `step.do` as terminal for the whole run, even if
  user code catches the thrown error. Put primary/fallback logic inside one `step.do`
  callback when the fallback should remain part of the same durable step.

## Observability

Runtime publishes `wdl_workflow_replay_active_bytes`,
`wdl_workflow_replay_detached_bytes`, `wdl_workflow_replay_read_in_flight_bytes`,
`wdl_workflow_replay_working_set_bytes`, and
`wdl_workflow_replay_working_set_high_water_bytes`. Working set equals retained
cache bytes plus detached bytes plus read reservations; active bytes overlap the
retained/detached views and must not be added again. High-water lasts for the
process lifetime and is not reset by scraping. Saturation increments
`wdl_workflow_replay_cache_total{outcome="saturated"}`.

workflows follows the Rust service observability shape: JSON logs, `/_healthz`,
`/_metrics`, request in-flight tracking, shutdown drain, and bounded labels. Runtime
emits workflow dispatch, replay cache, payload-limit, and callback outcomes. Workflows
emits bounded `workflow_dispatches` completion outcomes, including fenced no-op commits,
and internal DO alarm delivery/retry/discard/in-flight-unknown outcomes through
`do_alarm_dispatches`. Scheduler tick logs report admission and dispatch-pool capacity
pressure, and log workflow tick failures separately from queue/cron dispatch.

## Deployment / Rollout Notes

- Update clients that consume Workflow definition lists to follow cursors before
  deploying the paginated Control endpoint. Using the companion CLI requires `1.9.0`
  or later; upgrading WDL services does not upgrade installed CLI clients.
  During a mixed Control/Workflows rollout,
  an older Control may conservatively reject deletion on an unfinished lifecycle
  page; quiesce deletion until both are updated if uninterrupted deletion is needed.
- The instance-list byte ceiling is a writer-first exception: update Workflows
  before the Control worker in system-runtime. Existing readers accept shorter
  pages and the unchanged cursor shape; the bounded reader must not precede the
  writer that guarantees the response ceiling. This change requires no
  persisted-state migration.
- Cross-tier Workflow protocol changes follow the reader-before-writer procedure in the
  [infra rollout notes](infra.md#deployment--rollout-notes). The release changelog names
  the affected services.
- The required runtime dispatch deadline is a sender-first exception. The schema-3
  maintenance sequence below satisfies it by draining every old Workflows sender before
  rolling user-runtime and system-runtime. The old Runtime accepts the additive field,
  while the new Runtime rejects an old sender that omits it.
- DB 2 is the workflow instance state boundary; do not add direct DB 2 writes from
  control/runtime/scheduler.
- Workflows persists `wf:schema_version` in DB 2. Schema `3` stores DAG dependency edges
  and backend-owned operation kinds on step records. Missing kinds fail closed; there is
  no ambiguous in-place adoption for schema-2 records.
- Schema 3 uses an offline, preservation-based migration. Quiesce Workflow creation,
  mutation and execution, Worker/version deletion, and Durable Object alarm mutation.
  Stop Scheduler and let admitted alarm deliveries settle or their claim leases expire
  while old Workflows and do-runtime remain available. Then stop and drain every old
  Workflows dispatch, including Runtime-hosted root invocations, and prevent the old
  release from restarting. Stopping the sender alone does not cancel remote work. Keep
  affected surfaces paused until migration and participant upgrades finish. A
  full-platform stop is optional.
- Run `/workflows schema3-migrate check` from the final image as a one-off process with
  the Workflows Redis environment and access to both Workflows and Control endpoints.
  Initial migration requires dedicated schema-2 DB 2, empty DB 15, no unexpired or invalid
  running alarm lease, and no Redis key TTL. Wait for the two 60-second transient families
  (`wf:pending-version:*` and internal alarm cleanup snapshots) to drain. Retention,
  run/pending-create leases and sleep/retry/alarm deadlines are fields or scores, not TTLs.
  Preflight validates each key family's Redis type, instance identities, payload
  accounting/references, step history and pinned Worker exports/definitions.
  Payload validation and step conversion release parsed JSON between items;
  cross-step checks retain dependency metadata, not output/error trees.
  Invalid or ambiguous records fail before the initial
  swap; the tool neither guesses kinds nor runs tenant code. Converted instances must fit
  the existing 16 MiB aggregate payload limit.
- Run `/workflows schema3-migrate apply` with affected writers stopped. It acquires
  `wf:schema3-migration` in DB 0 of the Workflows endpoint, archives DB 2 with
  `SWAPDB 2 15`, copies state back, converts steps, verifies the destination, and publishes
  marker `3` last. It then changes coordination from `in_progress:<token>` to `complete`.
  Workflows refuses startup during incomplete migration. Concurrent `apply` fails. After
  confirming a failed process has exited, `/workflows schema3-migrate resume` takes over
  by exact-value CAS and repeats unfinished work from the immutable archive. If marker
  `3` is already published, it only finishes coordination without recopying. Completed
  `apply` is a no-op; completed `resume` is rejected.
- Conversion preserves IDs, generations, pinned versions, params, terminal output/error,
  payloads, dependency edges, events, summaries and lifecycle/retention indexes. It derives
  step kinds from legacy record shape, host payload references and event consumption, not
  tenant config names alone. Existing step field values retain their original JSON text,
  including floating-point output/error and config strings; conversion only adds `kind`.
  Completed steps replay without rerunning callbacks;
  unfinished root work retains at-least-once semantics. Drained running instances become
  queued with old run leases removed. Waiting instances keep absolute deadlines and
  buffered events, paused instances stay paused, terminal instances do not rerun, and
  pending-create records are not promoted to committed instances. DO alarm projection
  and SQLite rows retain their tokens; SQLite is not migrated.
- Configure final Workflows, user-runtime, system-runtime and do-runtime revisions and
  drain all old participants. Persist final immutable images in the deployment system's
  desired state before restoring capacity. After migration reports `complete`, start final
  Workflows before Runtime processes that resolve its backend address at startup, then
  start/update the Runtime participants, start Scheduler, and reopen paused surfaces without
  old/new overlap. Ordinary Workflow APIs and deletion checks are available immediately;
  restored DB 2 identities/referrers enforce their normal rules. Retained DB 15 does not
  gate service. Exact stop/update/start commands depend on the deployment system.
- DB 15 stays immutable and inactive by default, including after successful migration.
  Only explicit `apply --delete-archive` or `resume --delete-archive` removes it, and only
  after verified migration completes. The flag may accompany initial migration or a later
  no-op `apply`; it never recopies archive data over live DB 2. `check` rejects the flag.
  Deletion uses `FLUSHDB ASYNC`: keys disappear immediately while Valkey frees memory
  in the background. The post-command memory snapshot may still include pending freeing.
  Keep the DB 0 completion record. An archive is a point-in-time source, not a rollback of
  subsequent Workflow/KV/D1/DO side effects; an external snapshot does not authorize deletion.
- JSON reports include instance/step counts, COPY memory estimates, memory before and
  after the command, `maxMemoryPolicy`, and advisory warnings. Missing estimates, eviction
  policy and low headroom do not impose capacity restrictions; the operator decides.
  DB 15 shares memory and eviction with active databases, so monitor its entire retention
  period. COPY remains O(N) per value even with one command at a time. On a shared Valkey
  endpoint, low-traffic maintenance accepts possible Control/KV/Queue stalls; pause the
  platform if these are unacceptable. A separate Workflows endpoint provides isolation.
  Control DB 0 definitions/bundles are read, not migrated.
- Independently confirmed disposable DB 2 state may instead be cleared for greenfield
  startup. This replaces only `check|apply|resume`, not quiescence, participant drain or
  startup ordering. Clearing DB 2 does not delete SQLite alarm rows; later `getAlarm()`
  may repair their projection. The tool never infers that retained data is disposable.
- A legacy deployment that used any non-DB2 Workflows database must treat this as a
  configuration migration; the tool only accepts schema-2 DB 2. Remove the non-DB2
  `WORKFLOWS_REDIS_DB` override first. Never clear the old database if it is shared. Use
  DB 2 on the existing endpoint only when it is empty and dedicated to Workflows;
  otherwise point `WORKFLOWS_REDIS_URL` at a new endpoint whose DB 2 is empty. Omit the
  URL database or select `2`; explicit non-DB2 URL state is rejected rather than silently
  abandoned.
- A dedicated DB 2 containing schema-2 Workflows runtime state is a supported migration
  source. A DB 2 containing keys owned by another subsystem is an unsupported
  configuration: do not use prefix cleanup or clear it; move Workflows to a new endpoint
  with an empty DB 2. A non-empty DB 2 without `wf:schema_version` fails startup and may
  be cleared only after the operator confirms it is dedicated and disposable. WDL
  workflow definitions live in DB 0 under `wf:defs:*` and are not part of DB 2 cleanup.

## Tests That Protect This Module

- `tests/unit/runtime-dispatch-workflows.test.js`
- `tests/unit/workflow-replay-cache.test.js`
- `tests/unit/runtime-load.test.js`
- `tests/unit/runtime-workflows-client.test.js`
- `tests/unit/control-handlers-workflows.test.js`
- `tests/unit/control-lib.test.js`
- `tests/unit/auth-lib.test.js`
- `rust/workflows/src/tests.rs`
- `tests/integration/workflows-service.test.js`
- Workflow integration file group: `tests/integration/workflows-runtime-core.test.js`,
  `tests/integration/workflows-runtime-scheduler.test.js`,
  `tests/integration/workflows-runtime-pausing.test.js`,
  `tests/integration/workflows-runtime-retention.test.js`
- `tests/integration/workflows-schema-migration.test.js`
- `tests/integration/workflows-metadata.test.js`
- `tests/integration/workflows-durable-objects.test.js`
- `tests/unit/style-contracts.test.js`

## Known Constraints And Non-Goals

- No `locationHint` placement, cross-worker or `script_name` workflows.
- WDL's custom binding facade does not expose native workerd
  `WorkflowInstance.delete()` or `Workflow.deleteBatch()`; instance lifecycle remains
  owned by the documented WDL APIs and retention engine.
- Instance event subscriptions (`subscribe()`), rollback APIs, and restart from a
  selected step (`restart({ from: ... })`) are not supported. Own or inherited
  `rollback` fields in terminate options and `from` fields in restart options are
  rejected by presence before backend calls, without evaluating getters, even when
  their values are `false` or `undefined`. Omitted lifecycle options or options
  without those fields retain the ordinary operation. Both `step.do`
  overloads reject supplied rollback options before replay reads, claims, or callback
  execution. The wrapper sends only a presence marker to the host, not tenant rollback
  objects or functions; serialization hooks cannot bypass the host's terminal
  `workflow_invalid_step` rejection, even when tenant code catches the error.
- No platform-managed large payload spill to object storage.
- No tenant Durable Object storage as workflow backend.
- Runtime replay does not skip directly to continuations; user JS replays through
  deterministic step ordinals, including ordinals allocated to concurrent `step.do`
  calls.

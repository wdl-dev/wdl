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

The V2 name distinguishes the current DAG-capable engine from the earlier test-only V1
engine. New environments should be treated as greenfield schema-2 workflows state.

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
- `GET /ns/<ns>/workflows/<worker>/<workflow>/instances` lists instances and uses
  `workflow.read`.
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

Workflows exclusively owns Valkey DB 2 for instance execution state. Control owns
`wf:defs:<ns>:<worker>` in DB 0 for deploy-time workflow key allocation and stable
identity. The hash retains retired names until whole-worker delete. Definition listing
enumerates that retired history for currently active workers; deploy and single-workflow
status/lifecycle paths read only the names they need.

Key concepts:

- `workflowKey` is the physical workflow identity.
- `(ns, worker, workflowName)` keeps a stable workflowKey across redeploys.
- Instance state, step records, payload refs, events, ready/due indexes, run leases,
  retention indexes, and callbacks live in DB 2.
- Workflow payloads are JSON data under explicit byte caps. Large application data
  should live in R2/S3/D1/KV and be referenced from workflow payloads.
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
| `wf:schema_version` | String | workflows | DB 2 workflow-state schema marker. | Current value is `2`; greenfield deployments start on schema 2. |
| `wf:instance:{<ns>:<workflowKey>:<instanceId>}:state` | Hash | workflows | Authoritative instance state. | Terminal retention and lifecycle cleanup remove expired state. |
| `wf:instance:{...}:payloads` | Hash | workflows | Payload ref storage under aggregate caps. | Deleted with the instance state family. |
| `wf:instance:{...}:steps`, `step-summaries`, `step-summary-index` | Hash/ZSET | workflows | Authoritative step replay/history state. | Deleted with the instance; summaries may be truncated on read. |
| `wf:instance:{...}:events`, `events-by-type` | Hash/ZSET | workflows | Buffered event records and type index. | Consumed/stale events are removed during wait matching or cleanup. |
| `wf:ready:<shard>`, `wf:ready:active`, `wf:ready:cursor` | Set/String | workflows | Ready-token hints, active shard set, and fair-dispatch cursor. | Tokens are deduplicated hints; instance state remains authority; the cursor rotates shard start order across ticks. |
| `wf:due:<shard>` | ZSET | workflows | Sleep/retry/event-timeout due index. | Tick promotion moves eligible entries back to ready. |
| `wf:by-worker:<ns>:<worker>` | Set | workflows | Instance discovery by worker. | Used by list/delete checks; entries are removed by retention/delete cleanup. |
| `wf:by-workflow:<ns>:<worker>:<workflowKey>` | ZSET | workflows | Per-workflow instance list index ordered for bounded pagination. | Retention/delete cleanup removes the sorted-set member. |
| `wf:by-version:<ns>:<worker>:<version>` | Set | workflows | Frozen-version referrer index. | Blocks version delete while live instances reference the version. |
| `wf:pending-version:<ns>:<worker>:<version>` | ZSET | workflows | Short-lived restart target-version blockers, scored by expiry time. | Version-delete checks active members; restart atomically validates its marker before creating the durable `wf:by-version` referrer. Members expire after 30 seconds, and the ZSET has a 60-second key TTL for physical cleanup. |
| `wf:retention` | ZSET | workflows | Terminal retention due index. | Retention tick deletes expired terminal instances. |
| `wf:internal:do-alarm:{<jobId>}:state` | Hash | workflows | Authoritative backend job state for one Durable Object SQLite alarm row. | Successful delivery, retry exhaustion, explicit delete, and worker cleanup remove the job. |
| `wf:internal:do-alarm:due:<shard>` | ZSET | workflows | DO alarm due index. Score is due timestamp in milliseconds. | Tick promotion moves eligible jobs to ready. |
| `wf:internal:do-alarm:ready:<shard>`, `ready:active`, `ready:cursor` | Set/String | workflows | DO alarm ready hints, active shard set, and fair-dispatch cursor. | Dispatch removes ready hints or reschedules on retry; the cursor rotates shard start order across ticks. |
| `wf:internal:do-alarm:by-worker:<ns>:<worker>` | Set | workflows | Worker cleanup index for internal DO alarm jobs. | Whole-worker delete asks Workflows to remove indexed jobs after the delete commits; residual jobs self-discard on their next dispatch. |
| `wf:internal:do-alarm:by-worker:<ns>:<worker>:cleanup-snapshot:<random>` | Set | workflows | Temporary cleanup-worker snapshot of one by-worker DO alarm index. | Internal only; TTL is 60 seconds and is refreshed while cleanup drains the snapshot. |

## Ownership / Concurrency / Failure Semantics

- Workflows are same-worker only in V2.
- Instances freeze the worker version/class identity they were created with.
- Control fails closed on malformed active workflow entries and malformed `wf:defs`
  records encountered by an operation; management paths return `corrupt_meta`, while
  deploy returns `workflow_definition_corrupt` when reusing a damaged historical
  definition. Damaged authoritative metadata is not exposed as a normal missing or
  retired workflow. Normal deploy and single-workflow paths do not scan unrelated
  historical definitions.
- Scheduler only wakes workflows; workflows owns admission, fairness, shard ticks,
  ready/due movement, and runtime dispatch.
- Scheduler also wakes Workflows-owned internal DO alarm jobs through the same
  `/internal/workflows/tick` endpoint; scheduler never reads or writes DO alarm state
  directly.
- Workflows rejects non-canonical DO alarm identity before persisting jobs, revalidates
  persisted alarm identity before dispatch, and validates an active route
  version before using it as a retarget. Namespace, worker, and version checks reuse
  `wdl-rust-common`; do-runtime protocol grammar and identity helpers own the canonical
  alarm-specific fields and aggregate 512-byte DO host-id contract. Workflows mirrors
  and revalidates that contract before persistence and dispatch.
  Runtime run dispatch and progress callbacks share one system-vs-user runtime endpoint
  selector inside the workflows crate.
- 32 scheduling shards partition ready/due work.
- Ready tokens are deduplicated hints; instance hash state is authority.
- Execution commits are fenced by `generation`, `runToken`, active instance status, and
  an unexpired run lease. Step commits/registers accept the same-run `running` or
  `waiting` state so parallel siblings can finish after another sibling schedules
  retry/wait. Completed runtime terminals require `running`; failed runtime terminals
  may also close a same-run `waiting` state created by an invalid unawaited suspending
  step while the run lease is still valid. If that lease already expired, workflows only
  restores the ready hint so the next claim can replay under a fresh lease. Lifecycle
  commits use a generation fence and rotate `generation` in the same Lua commit.
- Runtime replay cache is advisory. DB 2 step state is authoritative.
- Runtime may issue multiple `step.do` calls concurrently, commonly via `Promise.all`;
  each call receives a deterministic ordinal in user-code call order, records DAG
  dependencies from the current completed-step frontier, and commits independently under
  the run fence. A `step.do` callback must not start another workflow step, including
  after an `await`; create parallel sibling promises from the run body before callback
  code is in flight. A run that returns before all started steps settle fails as
  invalid, so user code must await the concurrent step promises. Suspending operations
  (`step.sleep`, `step.sleepUntil`, `step.waitForEvent`) remain exclusive and must not
  overlap another in-flight step because they suspend the whole workflow run.
- Termination is an explicit non-success terminal outcome and uses error retention.
- `Workflow.createBatch()` accepts at most 100 entries per call. Runtime prevalidation
  and Rust admission share this pinned limit.
- A single workflow result is capped at 1 MiB and a runtime-to-workflows backend JSON
  request at 2 MiB. Runtime prevalidation and the Rust backend share the pinned
  `workflow_payload_too_large` contract. The per-instance aggregate payload cap is
  16 MiB. Step/event over-cap writes fail the request; over-cap runtime terminal
  results transition the instance to failed in the same transaction.
- Workflows semantic request caps use `request_too_large`; this is distinct from
  HTTP-body parser `request_body_too_large` in control/runtime protocols. Workflow
  errors otherwise use the platform `{ error, message }` envelope on HTTP boundaries.
  Client-facing proxies should treat workflows 5xx as backend/platform failure and not
  rely on raw backend diagnostic messages in the response body.

Workflow execution uses two channels:

1. Loaded workers call workflows through the reserved `__WDL_WORKFLOWS_BACKEND__`
   Fetcher binding. Runtime adds identity from bundle metadata; workflows does not trust
   tenant body fields for namespace, worker, version, workflow key, class, or instance
   identity.
2. workflows dispatches claimed runs back to runtime `/internal/workflows/run` on
   `:8088`. Runtime loads the frozen worker version and invokes `className.run(event,
   stepFacade)`.

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
   ready tokens, and claims eligible instances.
4. Claim validates status, generation, and lease state from the instance hash. Duplicate
   or stale ready/due tokens self-clean and do not execute user code.
5. A runtime dispatch is bounded by `WORKFLOWS_DISPATCH_TIMEOUT_MS`. On runtime
   dispatch error or timeout, workflows releases the ordinary run claim so a later tick
   can retry. Generation/run-token fences prevent double durable commits, but external
   side effects in user code may repeat; workflow code and step callbacks should be
   idempotent. `WORKFLOWS_RUN_LEASE_MS` is clamped above the dispatch timeout and acts
   as a stale-claim backstop, not the normal long-run timeout knob.

The step facade implements durable replay:

- `step.do(name, [config], callback)` uses ordinal, name, same-name count, DAG
  dependencies, and canonical config hash as the replay identity. A completed matching
  step returns the stored result. A shape mismatch fails closed with
  `workflow_step_mismatch`.
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
- V2 records a durable DAG for `step.do`. The runtime assigns ordinals synchronously in
  call order, treats completed steps as the current dependency frontier, and stores the
  frontier on each later step. `Promise.all([step.do(...), step.do(...)])` produces
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
- Lifecycle commits (`pause`, `resume`, `restart`, `terminate`, retention cleanup) use
  generation fencing and rotate `generation` where they invalidate in-flight execution.
- `sendEvent` targets the current generation of the instance. If a concurrent restart
  wins, send-event returns a conflict rather than mutating stale state.
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
- Tenant code only receives the runtime `Workflow` facade, not the raw backend Fetcher.
- Reserved `__WDL_WORKFLOWS_BACKEND__` binding is stripped before user env exposure.
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

workflows follows the Rust service observability shape: JSON logs, `/_healthz`,
`/_metrics`, request in-flight tracking, shutdown drain, and bounded labels. Runtime
emits workflow dispatch, replay cache, payload-limit, and callback outcomes. Workflows
emits internal DO alarm delivery/retry/discard outcomes and the bounded
`do_alarm_dispatches` metric. Scheduler logs workflow tick failures separately from
queue/cron dispatch.

## Deployment / Rollout Notes

- Workflows rollout spans control, runtime, do-runtime, scheduler, and workflows.
- Runtime must support workflow internal dispatch paths before workflows dispatches runs
  to it.
- do-runtime may roll only after workflows when it calls a new workflows API shape,
  including the internal Durable Object alarm mutation endpoints.
- Scheduler may roll after workflows is deployed because it only calls the tick
  endpoint.
- DB 2 is the workflow instance state boundary; do not add direct DB 2 writes from
  control/runtime/scheduler.
- Workflows persists `wf:schema_version` in DB 2. Schema `2` stores DAG dependency edges
  on step records and summaries. Current deployments are greenfield for this schema; do
  not add in-place migration paths for in-flight legacy workflow instances without a new
  design.
- If a development or maintenance environment starts with unversioned `wf:*` runtime
  keys in workflows DB 2, stop workflows and clear that DB 2 runtime state before
  restarting. WDL workflow definitions live in DB 0 under `wf:defs:*` and are not part
  of this DB 2 runtime-state cleanup.

## Tests That Protect This Module

- `tests/unit/runtime-dispatch-workflows.test.js`
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
- `tests/integration/workflows-metadata.test.js`
- `tests/integration/workflows-durable-objects.test.js`
- `tests/unit/style-contracts.test.js`

## Known Constraints And Non-Goals

- V2 is not full Cloudflare Workflows compatibility.
- No cross-worker or `script_name` workflows.
- No platform-managed large payload spill to object storage.
- No tenant Durable Object storage as workflow backend.
- Runtime replay does not skip directly to continuations; user JS replays through
  deterministic step ordinals, including ordinals allocated to concurrent `step.do`
  calls.

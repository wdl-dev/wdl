# Queues And Cron

## Purpose

Queues and cron are the scheduler-facing work-dispatch features. Queues provide durable,
at-least-once background delivery for Worker `queue()` handlers. Cron provides
Cloudflare-compatible minute-aligned `scheduled()` events with best-effort delivery and
no missed-fire replay.

Both features are configured by worker deploy metadata, materialized by control into
Redis projections, and dispatched by `scheduler` to runtime's private internal socket on
`:8088`.

## Current Implementation

Control validates deploy payloads and freezes queue/cron metadata into the immutable
bundle metadata. Promotion is the boundary where that metadata becomes active:

- `control/handlers/deploy.js` parses `crons` and `queueConsumers`.
- `control/routing.js` promotes cron projections, queue consumer projections, route
  state, and lifecycle indexes inside the active-version WATCH/MULTI path.
- `control/lifecycle-indexes.js` owns the JS-side Redis key helpers for cron worker
  hashes, cron slot refs, cron discovery index updates, and queue consumer projections.

Runtime has two separate roles:

- Queue producer bindings are materialized from `runtime/bindings/queue.js`.
  `env.MY_QUEUE.send()` and `sendBatch()` build bounded queue envelopes and send them
  through the Rust `redis-proxy` sidecar to the data-plane Redis database.
- Queue and cron consumer dispatch enters runtime through `runtime/internal.js` on
  `:8088`, then `runtime/dispatch.js` invokes workerd's native `scheduled()` and
  `queue()` entrypoints.

Scheduler owns runtime delivery:

- Cron code lives under `rust/scheduler/src/cron/`.
- Queue registry, consume, delayed delivery, DLQ, and orphan handling live under
  `rust/scheduler/src/queue/`.
- Before constructing a cron runtime worker id, scheduler revalidates the projection's
  route namespace, worker name, and version with the canonical `wdl-rust-common`
  grammar. Invalid cron refs are removed before slot advance or runtime dispatch, and
  cron sweep does not recreate refs from malformed worker metadata. Queue consumer
  dispatch identities are also revalidated before use. A noncanonical identity makes
  the projection unusable and follows absent-consumer handling, so queued backlog may
  move to the orphan stream.
- Scheduler defaults to one replica in deployment, and current dispatch paths are
  multi-replica safe. Extra replicas improve runtime concurrency but do not imply
  zero-gap deployment: production rollout may still use stop-before-start semantics and
  briefly pause scheduling.

## Interfaces

User-facing queue interfaces:

- Wrangler producers: `[[queues.producers]]`
- Wrangler consumers: `[[queues.consumers]]`
- Runtime producer API: `env.<BINDING>.send(body, opts?)`
- Runtime producer API: `env.<BINDING>.sendBatch(messages, opts?)`
- Producer limits are 128,000 bytes per message, 100 messages per batch, and 256,000
  bytes total per batch.
- Runtime consumer handler: `export default { async queue(batch, env, ctx) {} }`
- Producer `delivery_delay` is supported as the default delay for sends on that binding.
- Consumer `retry_delay` is supported as the default delay for retries without an
  explicit `delaySeconds`.
- Consumer `max_concurrency` is currently rejected.

User-facing cron interfaces:

- Wrangler simple form: `[triggers] crons = [...]`
- Runtime handler: `export default { async scheduled(event, env, ctx) {} }`

Internal runtime dispatch interfaces:

- `POST /_scheduled` on runtime internal socket `:8088`
- `POST /_queued` on runtime internal socket `:8088`

These internal paths are socket-private architecture, not gateway path reservations. A
tenant worker may still define public paths named `/_scheduled` or `/_queued`; gateway
traffic goes to the normal loader socket, not to `:8088`.

Cron triggers and queue consumers are dispatch features, so deploys may declare them
only for routeable namespaces: ordinary tenant namespaces and the narrow reserved
route namespace `__system__`. Platform-tier workers are cold-load targets selected by
`[[platform_bindings]]`; they are not public/runtime dispatch targets and may not
declare cron triggers or queue consumers.

## Scheduler Dispatch Model

Scheduler is not a general job runner. It turns Redis projections into runtime calls and
lets Redis state decide whether a piece of work is still current.

Cron uses wall-clock minute slots:

1. `wait_ms_until_next_slot()` sleeps until the next UTC minute boundary. The tick loop
   then scans the current `cron-slot:<slot_ms>` bucket and the previous bucket. The
   previous-bucket scan covers refs inserted near a minute rollover.
2. A separate sweep/reconcile path reads active cron hashes from `cron:index:workers`,
   computes each entry's next fire time with croner and the configured timezone, rounds
   it to the minute slot, and writes refs into slot buckets. This is repair logic, not
   the authority. Control uses JavaScript `croner` only for the initial promote-time
   slot placement; scheduler uses Rust `croner` for repair and advancement. Worker
   hashes without canonical identity or metadata are skipped instead of reseeding
   invalid refs, and emit a bounded `invalid_identity` or `invalid_meta` reason.
3. Cron refs carry the entry generation. At fire time scheduler reads
   `crons:<ns>:<worker>` and compares `gen`; missing metadata, corrupt JSON, or
   generation mismatch makes the ref stale and removes it from the slot.
4. The atomic claim compares the exact metadata and entry snapshot again before taking
   the lease or changing either slot. A concurrent configuration change causes one
   bounded re-read and recalculation. If it changes again, scheduler leaves the source
   ref untouched for a later tick and records `config_changed_deferred`.
5. Scheduler atomically leases the ref, removes it from the current slot, and adds it to
   the next slot before calling runtime. This ordering gives single-fire-per-slot
   behavior and prevents a runtime/network failure from turning into an automatic cron
   retry.
6. If a ref is stranded in a slot older than the current wall-clock slot, scheduler
   advances it to the next future slot without firing. Outage or long scheduler downtime
   therefore skips missed cron events rather than replaying them.
7. The `scheduledTime` sent to runtime is the slot timestamp, not the POST time.
   `cron_queue_lag_ms` measures how late scheduler was relative to that slot.

Cron therefore follows Cloudflare-style best-effort scheduled events: minute aligned, no
catch-up replay, overlap allowed, and user-handler failure reported as an outcome rather
than retried by scheduler.

Queue dispatch is stream-driven rather than wall-clock driven:

1. Producers write message envelopes into DB 1 streams, or into delayed ZSETs when
   `delivery_delay` / retry delay is non-zero.
2. Scheduler repairs `queue:index:*` discovery sets from authoritative hashes, streams,
   and delayed ZSETs at startup and every `SCHEDULER_SWEEP_MS` (five minutes by
   default). The 1.5-second reconcile loop reads those indexes, removes stale members,
   creates the fixed `wdl-scheduler` consumer group for live streams, and keeps an
   in-memory set of known delayed queues. Writers remain responsible for updating the
   indexes on normal writes; periodic repair bounds recovery from an interrupted
   data-plus-index write without putting a keyspace scan on the reconcile hot path.
   Missing optional consumer fields default to
   `max_batch_size=10`, `max_batch_timeout_ms=5000`, `max_retries=3`,
   `retry_delay_secs=0`, and no `dead_letter_queue`. Present malformed/out-of-range
   fields or an invalid dead-letter queue make that consumer projection unusable. Group
   creation isolates errors per stream,
   continues later bounded chunks, refreshes healthy in-memory consumers, and reports
   any aggregate reconcile failure after that healthy work is retained.
3. The consume loop uses `XREADGROUP` to read main streams and dispatches batches up to
   `max_batch_size`, which must be in `[1, 100]`; out-of-range projections are rejected.
   Each read caps `COUNT` to the current consumer batch-size snapshot for the active
   stream set so one poll does not place more entries into the PEL than a current
   consumer can dispatch in one batch.
   PEL reap uses the same per-consumer cap when the consumer still exists; missing-
   consumer orphan movement may still page up to the hard cap. Consume and PEL reap can
   dispatch streams in parallel under the queue semaphore. Before each dispatch path
   sends messages to runtime, scheduler re-reads the authoritative `queue-consumer`
   hash for that stream and updates the in-memory registry, so a promoted consumer
   version does not wait for the next reconcile tick once messages are selected.
   `max_batch_timeout_ms` is not a batching wait window in the current model.
4. Runtime returns a queue outcome envelope. Explicit `ack`, explicit `retry`, batch
   retry, and implicit ack are resolved in scheduler. Retry and DLQ transitions execute
   as independent atomic scripts inside one transport pipeline: all target writes
   complete before the source entry is acknowledged and deleted. A target-side error
   therefore retains that source entry and does not prevent healthy transitions later
   in the same batch from completing. Platform failures that cannot become valid by
   retrying the same bytes, such as queue-message decode failures or invalid queue
   dispatch bodies, go directly to DLQ without consuming the retry budget. Aggregate
   request-body-too-large responses are split and retried with smaller batches first.
5. The delayed loop wakes from `queue-delayed-wake` and from wall-clock sleeps until the
   next due delayed member. Each due member first takes a `queue-delayed-claim:*` lease
   sized to `SCHEDULER_FIRE_TIMEOUT_MS + 5000ms`; the winner moves it back to the main
   stream, or to the orphan stream if the consumer vanished.
6. Orphan/Pending-Entry cleanup is diagnostic and protective. It prevents consumer
   deletion or scheduler crash paths from silently losing messages, but the main queue
   stream remains the durable backlog and is intentionally not trimmed. Delayed ZSET and
   orphan stream-tail migrations are paged by `QUEUE_SWEEP_BATCH_SIZE` (default `100`).

## Redis / Storage Contracts

Valkey DB split:

- DB 0: control-plane metadata, cron config/projections, queue consumer config.
- DB 1: queue data-plane streams, delayed queues, orphan streams, log-tail streams.

Cron keys:

```text
crons:<ns>:<worker>               Hash, authoritative live cron config
cron:seq:<ns>:<worker>            String, permanent generation high-water mark
cron:index:workers                Set, non-authoritative discovery index of crons hashes
cron:index:workers:backfilled     String, one-time legacy backfill marker
cron-slot:<slot_ms>               Set, minute-bucket consumption index, expiring around slot+10min
cron-lease:<slot_ms>:<ref>        String EX, per-ref single-fire lease
```

Queue keys:

```text
queue-consumer:<ns>:<queue>       Hash, authoritative active consumer projection
queue:index:consumers             Set, discovery index of queue-consumer hashes
queue:index:streams               Set in DB 1, discovery index of main queue streams
queue:index:delayed               Set in DB 1, discovery index of delayed ZSETs
queue:<ns>:<queue>:s              Stream, main at-least-once message stream
queue-delayed:<ns>:<queue>        ZSET, delayed visibility queue
queue-delayed-claim:<hash>        String PX, per-member delayed promotion lease
queue:<ns>:<queue>:dlq            Stream, dead-letter diagnostic stream
queue-orphaned:<ns>:<queue>       Stream, messages whose consumer disappeared
queue-delayed-wake                Stream, wake signal for delayed ZSET dispatcher
```

`wdl-rust-common::queue_keys` owns the wake stream and its `delayed_key` / `visible_at`
entry fields so the redis-proxy producer and scheduler consumer cannot drift.

Indexes are not authority. Writers add index entries, and scheduler reconcile owns stale
cleanup after proving the referenced key is absent.

## Ownership / Concurrency / Failure Semantics

Cron:

- `crons:<ns>:<worker>` is authority.
- `cron:seq:<ns>:<worker>` is the sole permanent generation allocator. It survives empty
  projections and whole-worker deletion, and new allocations start at `1024`.
- Cron projection metadata stores only the active worker version used for dispatch.
- `cron:index:workers` is only discovery. `cron:index:workers:backfilled` marks that the
  scheduler crossed pre-index legacy state; after that, an empty index means no cron
  workers.
- `cron-slot:<slot_ms>` is a rebuildable consumption index. The current bucket plus the
  previous bucket are checked so near-rollover writes are not delayed by a full minute.
- Cron refs are shaped as `<ns>:<worker>:<cron_id>:<gen>`.
- `gen` fences stale bucket refs. Removing and re-adding the same cron, including after
  whole-worker deletion, gets a fresh generation.
- Scheduler leases and advances a ref before firing. If the runtime call fails, that
  slot is still consumed. This preserves Cloudflare-style best-effort cron semantics
  rather than retrying scheduled events.
- Stranded old slots advance without firing. Missed cron events are skipped after
  outage.
- Handler failures are returned as `outcome:"error"` with HTTP 200 from runtime so
  scheduler does not treat user-code failure as transport retry.

Queues:

- Main streams are durable and at-least-once. They are not server-side trimmed.
- DLQ and orphan streams are diagnostic channels and may use bounded approximate trim.
  Defaults are 10k entries each, controlled by `SCHEDULER_MAX_DLQ_LEN` and
  `SCHEDULER_MAX_ORPHANED_LEN`.
- The consumer group is fixed to `wdl-scheduler`.
- One queue has one active consumer worker. Promotion replaces the full queue-consumer
  projection so removed optional fields disappear.
- `max_batch_size` is enforced before runtime dispatch and also bounds ordinary
  `XREADGROUP` / PEL reclaim reads for live consumers. `max_batch_timeout_ms` is
  currently configuration metadata, not a Cloudflare-style aggregation window.
- Internal retry count starts at 0; Worker-facing `Message.attempts` starts at 1.
- `maxRetries = N` means the handler can observe at most `N + 1` attempts before DLQ.
- Platform-detected permanent dispatch failures (`queue_message_decode_failed` or
  invalid queue dispatch body) are terminal for the affected batch and move messages
  to DLQ immediately. If runtime rejects an aggregate queue request as too large,
  scheduler splits the batch and retries smaller requests; only a single-message
  request that still exceeds the runtime body cap becomes terminal. Auth failures
  and unknown application `4xx` responses keep the existing retry behavior unless
  they are explicitly mapped.
- Explicit `delaySeconds` on retry overrides consumer `retry_delay_secs`, including `0`
  for immediate retry.
- Delayed promotion claims use the same timeout horizon as runtime dispatch plus a 5s
  margin, so a loaded scheduler does not lose its delayed-member claim before the
  move/drop Lua owner check runs.
- Delayed retry wakeups are best-effort hints. A scheduler that misses the wake stream
  will still discover due delayed messages on the next delayed-loop reconcile/sleep
  interval; this is a bounded latency tradeoff, not a correctness fence.
- If a consumer disappears, existing stream messages move to the orphan stream rather
  than being dropped.

## Security Boundaries

- Runtime dispatch paths for cron and queues are reachable only on the private runtime
  internal socket `:8088`.
- Gateway must not path-filter tenant `/_scheduled` or `/_queued`; the socket split is
  the security boundary.
- Queue producer bindings go through the runtime `redis-proxy` sidecar and enforce message/batch
  caps before writing.
- Queue names use the shared queue name grammar. `:` is forbidden because queue keys are
  colon-delimited.
- Scheduler posts normal worker ids to user-runtime and literal `__system__:` worker ids
  to system-runtime.

## Observability

Scheduler emits structured logs and Prometheus metrics for cron and queue outcomes.

Important cron signals:

- `cron_fires{outcome=...}`
- `cron_fire_duration_ms{outcome=...}`
- `cron_queue_lag_ms{outcome=...}`
- `cron_bucket_size`
- `cron_stale_refs_cleaned`
- `cron_sweep_entries_skipped`
- `cron_sweep_workers_skipped`
- Logs: `cron_fired`, `cron_lease_lost`, `cron_config_changed_deferred`,
  `cron_ref_stale`, `cron_ref_stale_advanced`, `cron_sweep_entry_skipped`,
  `cron_sweep_worker_skipped`, `cron_reconcile`

Important queue signals:

- `queue_messages{outcome=...}`
- `queue_dispatch_failures{kind=...}`
- `queue_batch_duration_ms{outcome=...}`
- `queue_delayed_wake_read_errors`
- Logs around reconcile, `XREADGROUP`, delayed sweep, PEL reap, DLQ, and orphan
  movement, including `queue_batch_dispatched` and
  `queue_consumer_projection_invalid`.

Runtime tail logs also emit `worker_scheduled` and `worker_queue` start/finish events
for loaded worker execution.

## Deployment / Rollout Notes

- Deploy control/runtime/scheduler together when changing queue or cron wire shape.
- Runtime `:8088` must be deployed before scheduler uses internal dispatch paths that do
  not exist on the older runtime.
- Cron discovery index has a one-time backfill path. After
  `cron:index:workers:backfilled` exists, control writers own the discovery projection.
- Queue indexes are non-authoritative and repairable. New writers that create queue
  streams, delayed queues, or consumer projections must add the matching index member.
- Scheduler has separate cron and queue dispatch semaphores:
  `SCHEDULER_CRON_MAX_CONCURRENCY` and `SCHEDULER_QUEUE_MAX_CONCURRENCY`, both
  defaulting from `SCHEDULER_MAX_CONCURRENCY`.

## Tests That Protect This Module

Representative test anchors:

- `tests/unit/control-lib.test.js`: cron and queue manifest parsing.
- `tests/unit/control-routing.test.js`: promotion projections for cron and queue
  consumers.
- `tests/unit/control-lifecycle-indexes.test.js`: JS cron/queue key helpers and
  projection staging.
- `tests/fixtures/queue-key-parse.json`: shared JS/Rust queue discovery-key parser
  contract.
- `tests/unit/runtime-lib.test.js`: internal dispatch body normalization.
- `tests/unit/runtime-dispatch-handlers.test.js`: scheduled and queue dispatch behavior, tail
  event envelope behavior.
- `tests/unit/style-contracts.test.js`: cross-tier Redis key/layout drift checks.
- `rust/scheduler/src/cron/` unit tests.
- `rust/scheduler/src/queue/*` unit tests.
- `tests/integration/cron-triggers.test.js`
- Queue integration file group: `tests/integration/queues-delivery.test.js`,
  `tests/integration/queues-retry-and-delay.test.js`,
  `tests/integration/queues-orphan-and-control.test.js`,
  `tests/integration/queues-batch-and-isolation.test.js`
- `tests/integration/queue-native-dispatch.test.js`

## Known Constraints And Non-Goals

- Cron is minute-aligned and best-effort. There is no missed-fire replay.
- Cron overlap is allowed.
- Queue `max_batch_timeout_ms` is not yet a true aggregation window.
- Queue `contentType = "v8"` is rejected.
- Queue consumer `max_concurrency` is rejected.
- Main queue streams are intentionally untrimmed; backlog is an operational signal.
- Scheduler multi-replica safety is covered by integration tests for cron due-ref
  claiming, cron sweep recovery, queue reconcile plus consumer-group delivery, delayed
  queue promotion, PEL reap, and workflow ticks. The Workflows service drives Durable
  Object alarms; scheduler does not. Any new scheduler dispatch path must be audited for
  its own Redis lease/fence semantics before assuming replica safety.

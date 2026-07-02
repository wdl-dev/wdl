# Log Tail And Observability

## Purpose

Observability provides bounded metrics, structured logs, request-id propagation, and
live tenant log tailing without turning Redis Streams into an audit log.

## Current Implementation

Shared JS primitives live in `shared/observability.js`. Runtime tailing uses:

- `runtime/tail-worker.js` for console/exception capture.
- `runtime/tail-forwarder.js` for active-set checks and append POSTs.
- `redis-proxy` logs endpoints for active tail checks and stream appends.
- `control/handlers/logs-tail.js` for SSE sessions and heartbeat activation.
- CLI `wdl tail` for user consumption.

Rust services (`scheduler`, `redis-proxy`, `workflows`, `supervisor`) use structured
JSON logs and service-specific Prometheus metrics.

## Interfaces

- Runtime loaded-worker tail capture via workerd tails.
- Control SSE endpoint for `wdl tail`.
- `redis-proxy` active-set and append APIs.

## Tail Delivery Model

Live tail is an activation-gated pipe, not a durable logging system:

- workerd tails deliver console, exception, fetch, scheduled, and queue events to the
  runtime tail worker. Runtime always keeps structured stdout as the durable platform
  log path.
- `runtime/tail-forwarder.js` checks redis-proxy `/logs/tail/active` before append.
  Positive and negative active-set results are cached briefly so inactive workers do not
  pay a Redis write per event.
- Control authorizes each SSE tail session, writes/refreshes the worker gate in
  `logs:tail:active`, reads `logs:<ns>:<worker>:s`, and emits SSE frames. Reconnects
  re-enter normal auth.
- redis-proxy appends bounded stream entries with `MAXLEN ~ 500` and refreshes TTL. The
  stream exists to bridge live consumers, not to preserve history.
- Single-worker `wdl tail` can use `Last-Event-ID` to resume from the stream window.
  Multi-worker tail is a fan-in session; reconnect starts fresh because the client
  cannot express one cursor per worker in a single SSE cursor.
- Events racing session activation, runtime rolling, redis-proxy failure, or slow SSE
  readers can be dropped. This is acceptable because stdout/log aggregation remains the
  durable observability path.

Service probes:

| Service | Health | Metrics |
|---|---|---|
| Gateway | `/healthz` | `/_metrics` |
| user-runtime / system-runtime internal | `/_healthz` | `/_metrics` |
| d1-runtime | `/healthz` | `/_metrics` |
| do-runtime | `/healthz` | `/_metrics` |
| scheduler | `/_healthz` | `/_metrics` |
| workflows | `/_healthz` | `/_metrics` |
| redis-proxy sidecars | `/_healthz` | `/_metrics` |
| control / auth | none | none |

Gateway probes are on the public listener. `/healthz` is intentionally public for load
balancer readiness. `/_metrics` must remain low-cardinality and free of tenant identity
labels because it can share that listener; deployments that treat gateway traffic volume
or cache state as sensitive should protect or block only `/_metrics` at ingress.

## Redis / Storage Contracts

Live tail uses DB 1:

```text
logs:tail:active       Hash/HFE, active tail worker gates
logs:<ns>:<worker>:s   Stream, transient live-tail events
```

Tail streams use bounded `MAXLEN ~ 500` and TTL refresh on writes. They are a transient
pipe, not durable log storage.

## Ownership / Concurrency / Failure Semantics

- Structured stdout is the source of truth for durable platform logging.
- No active tailer means runtime still logs stdout but skips per-event stream append
  work after local active-set miss caching.
- Active tail sessions are time-bounded authorization leases and must reconnect through
  normal auth. `LOG_TAIL_MAX_SESSION_MS` sets the control-side maximum; invalid or
  empty values fall back to 15 minutes.
- Current stock workerd behavior, tracked upstream as
  [#6832](https://github.com/cloudflare/workerd/issues/6832), does not reliably call
  async response-body `ReadableStream.cancel()` on client disconnect. WDL treats this
  as a permanent compatibility boundary: Control has independent watchdogs, not a
  temporary workaround waiting on upstream. The max-session watchdog bounds
  reauthorization, and the idle-pull watchdog closes a session when the SSE body has not
  been pulled for three keepalive intervals. Active clients naturally pull at the
  keepalive cadence because each heartbeat frees queue space; abandoned clients stop
  pulling and are cleaned up without waiting for the full session lifetime. A TCP
  connection that stays open but whose application stops reading for that window may be
  closed and should reconnect.
- Tail events racing activation can be dropped.
- High QPS or slow SSE readers can miss middle events due to stream caps.

## Security Boundaries

- Tail authorization happens in control for each SSE session.
- Tail streams are namespace/worker scoped.
- Metrics labels must stay bounded. Namespace, worker, version, token id, raw key, path,
  and error text belong in logs, not metric labels.
- Request ids are sanitized and bounded before propagation.

## Observability Strategy

WDL uses one observability strategy across JS workerd tiers and Rust services: logs
answer what happened for a specific request, worker, object, or control-plane action;
metrics answer how often, how slow, and which bounded class changed. Logs therefore
carry correlation and bounded debug identity, while metrics carry only bounded enum
labels suitable for Prometheus aggregation.

Central owners:

- JS services emit platform logs through `shared/observability.js` (`createLogger`,
  `createHttpRequestScope`, and `recordRequestComplete`). Direct `console.*` in
  production JS is limited to that primitive and to embedded source strings that cannot
  import modules.
- Rust services emit logs through `wdl-rust-common::log::emit_log_line` or a thin
  wrapper over it, and expose metrics through `wdl-rust-common::metrics::MetricStore`.
- HTTP request completion is recorded once through the request-scope helper or service
  middleware so request counters, duration summaries, probe suppression, request-id
  fields, and `request_complete` logs do not drift per tier.
- Service-specific metrics should prefer one metric family with bounded labels such as
  `outcome`, `reason`, `kind`, `mode`, `stage`, `status`, `scope`, `operation`, or
  bounded machine `code` over parallel hit/miss/error metric names, unless a separate
  family adds genuinely different signal.

## Observability Contracts

Common rules:

- `x-request-id` propagates across gateway, control/runtime, loaded workers, and D1
  where possible. Missing inbound ids are minted at ingress; dirty ids such as
  multi-valued, control-character, or overly long ids are treated as absent. JS
  entrypoints using `shared/request-scope.js` echo the sanitized id on responses and
  log it as `request_id` on `request_complete`; Rust sidecars sanitize inbound ids and
  log them where request middleware owns completion. Control's Redis `PUBLISH` path logs
  the id locally but does not put it in the pub/sub payload.
- Logs use snake_case fields. Only `level=error` is emitted on stderr; debug/info/warn
  JSON log lines go to stdout so log routing stays identical across JS, Rust, and
  embedded workerd shims.
- Internal operational logs, including system-worker cleanup logs and defensive
  Redis callback warnings, use the same single-line JSON envelope:
  `ts`, `service`, `level`, `event`, plus snake_case fields. JS services use
  `shared/observability.js`; Rust services use
  `wdl-rust-common::log::emit_log_line`. Error text is emitted as `error_message`;
  secret values, raw credentials, token material, raw Redis keys, and unbounded payloads
  are not emitted.
- Product API response bodies should use camelCase unless an endpoint explicitly
  documents a different wire contract.
- Metrics should use bounded enumerated labels only.
- Rust HTTP sidecars that expose request metrics use the same `requests`,
  `request_duration_ms`, and `request_errors` metric families and the same bounded
  `service`/`route`/`status` labels; per-route error context stays on
  `request_complete` logs as `error_code` / `error_message`.
- JS and Rust observability implementations intentionally share metric prefix `wdl`,
  request metric families `requests` / `request_duration_ms` / `request_errors`,
  cardinality warning threshold `100`, and Prometheus content type
  `text/plain; version=0.0.4; charset=utf-8`. The shared fixture
  `tests/fixtures/observability-contract.json` pins those values without introducing
  a runtime metrics owner.
- redis-proxy records KV payload sizes in the `kv_value_bytes` summary with only
  bounded `service`/`operation`/`kind` labels. It records value, metadata, and raw
  batch byte counts so operators can decide whether large-value offload is needed;
  namespace, key, and object identity never enter metric labels.
- Rust `request_complete` logs report integer `duration_ms` values so log fields stay
  stable across services; Prometheus duration summaries keep their floating point
  values.
- JS `MetricsRegistry` emits one structured `metric_cardinality_warning` log once per
  metric name at 100 series, then drops brand-new series for that metric while still
  updating existing series. Rust `MetricStore` currently keeps the same warning-only
  tripwire. The warning carries the metric name, observed series count, and configured
  limit; offending tenant-specific detail should already be absent from labels. Because
  this warning is emitted by the metric registry, it is not suppressed by `LOG_LEVEL`.
- `*_max` is a separate gauge family, not an extra sample under a Prometheus summary
  family. Summary output may only contain `_count`, `_sum`, and quantile samples.
- Successful probe routes (`healthz`, `metrics`, `/_healthz`, `/_metrics`) suppress
  `request_complete` logs while still incrementing counters; errors still log.
- `LOG_LEVEL` gates log output only. Metrics bypass it, so high-QPS deployments may set
  `LOG_LEVEL=warn` without losing Prometheus signal.
- Service-binding trace propagation is caller-explicit. `ServiceBinding#fetch` forces
  `x-worker-id` to the target but only preserves `x-request-id` when the caller forwards
  it on the Request; JSRPC does not carry Node async context across isolates.

Tail event families:

- `worker_console`
- `worker_exception`
- `worker_fetch`
- `worker_scheduled`
- `worker_queue`
- `tail_warning`

Tail identity rules:

- `worker_console` identity for fetch requests comes from forwarded request headers
  because workerd reports `scriptName=none` for `workerLoader`-loaded workers.
- `scheduled()` and `queue()` console events are JSRPC events without a request shape, so
  their console tail events omit `worker_id` and `request_id` instead of inventing
  `"unknown"` sentinels.
- Runtime emits explicit `worker_fetch`, `worker_scheduled`, and `worker_queue`
  start/finish events around invocation boundaries. `worker_fetch` includes method,
  worker-visible pathname only, status/outcome, and duration.
- Control-generated `tail_warning` SSE events have no Redis stream id, so they do not
  corrupt single-worker resume cursors.

## Deployment / Rollout Notes

- Runtime, redis-proxy, and control must agree on tail active/append protocol.
- Tail is best-effort; rolling runtime/control can drop live tail events.
- Metrics label changes can break dashboards and should be treated as observability
  contract changes.

## Tests That Protect This Module

- `tests/unit/runtime-tail-worker.test.js`
- `tests/unit/control-logs-tail.test.js`
- `tests/unit/observability.test.js`
- `tests/integration/log-tail.test.js`
- `tests/integration/observability.test.js`
- `tests/unit/style-contracts.test.js`

## Known Constraints And Non-Goals

- `wdl tail` is not audit storage.
- There is no historical log query API.
- Server-side filtering/search is not part of live tail.
- Console events from scheduled/queue handlers may not carry the same identity fields as
  fetch events.

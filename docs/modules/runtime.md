# Runtime Loader And Bindings

## Purpose

Runtime loads immutable worker versions from Redis, builds tenant-facing `env` bindings,
dispatches fetch/scheduled/queue/workflow events, and keeps user-runtime and
system-runtime trust boundaries separate.

## Current Implementation

The shared runtime source serves both user-runtime and system-runtime. The entrypoints
are:

- `runtime/index.js`: tenant fetch loader socket `:8081`.
- `runtime/internal.js`: internal dispatch socket `:8088`.
- `runtime/load.js` and `runtime/load/*`: bundle assembly, module rewriting, env
  construction, wrapper generation.
- `runtime/runtime.js`: workerLoader cache bookkeeping and sibling eviction.
- `runtime/dispatch.js` and `runtime/dispatch/*`: fetch/scheduled/queue/workflow
  dispatch helpers.

Workers are loaded by immutable id `<ns>:<worker>:<version>`. Promotion creates a new
id, so active-version changes naturally cold-load a fresh isolate.
`runtime/load.js` also owns the shared `workerLoader.get()` wrapper: every cache miss is
recorded for later eviction, while only active-version dispatch paths request sibling
eviction. Service bindings load their pinned version without evicting siblings.

The two runtime pools use the same image and source but different capnp configs:

- `runtime/config-user.capnp` pins loaded-worker outbound to public-only networking.
- `runtime/config-system.capnp` hosts loader, control, auth, and tail worker on a
  private+public network service so loaded `__system__` workers can reach platform mesh
  services.

That privilege asymmetry lives in capnp, not in Terraform or Kubernetes egress policy.
`env.SERVICE_NAME` is a capnp literal (`user-runtime` or `system-runtime`) and is used
for logs/metrics so the two pools do not collapse into one observability stream.

workerd config wiring has a few non-obvious constraints:

- `workerd serve` takes config and service binding arguments as separate argv tokens;
  do not collapse them into colon-separated strings.
- Cap'n Proto `external = ... http` is for HTTP/TLS-capable peers. Plain TCP peers such
  as Redis belong behind a `network` service and `connect()`.
- HTTPS fetches through a `network` service require
  `tlsOptions = (trustBrowserCas = true)`, and runtime images must include
  `ca-certificates`.
- workerd embedded module names cannot contain `..`; shared embedded modules therefore
  use flat names such as `shared-redis`.
- Some support sources are used under two names: a flat capnp module name for normal
  workerd modules, and a `.js` module name for generated loaded-worker modules. For
  example D1 embeds the same `shared/d1-data-field.js` source as `shared-d1-data-field`
  for workerd modules and `_wdl-d1-data-field.js` for tenant WorkerCode modules.
- user-runtime keeps both `internal-network` for platform plumbing and
  `public-network` for tenant outbound. Loaded user workers get only
  `public-network`; platform wrappers keep the private reach they need.

## Interfaces

- Loader socket `:8081`: gateway-routed tenant fetch traffic.
- Internal socket `:8088`: `GET /_healthz`, `GET /_metrics`, workflow run/notify,
  scheduled dispatch, and queue dispatch only.
- `redis-proxy` sidecar: cold-load, tenant secret envelope decrypt, KV, queue producer,
  AI provider resolution/model discovery, and log-tail active checks and appends.
- Host-side stateful binding transport: generic D1, DO, and workflows backend Fetchers
  remain in the loader realm, while loaded-worker env receives declaration-scoped host
  adapters. The DO host adapter performs owner direct forwarding through runtime's
  internal-network outbound; there is no loaded-worker owner-network Fetcher.
- Env-backed bindings: queues and KV call `redis-proxy`; R2 signs S3-compatible requests;
  AI resolves encrypted namespace credentials through redis-proxy and uses a
  public-only network service; ASSETS uses deploy-time metadata to generate tokenized
  CDN URLs rather than hidden Fetchers.

Tenant-visible bindings include KV, R2, D1, Durable Objects, Queues, AI, ASSETS,
service bindings, platform bindings, and workflows.

## Binding Implementation Model

workerd supplies the isolate, module evaluation, named entrypoint, and JSRPC machinery.
WDL supplies the platform bindings that Cloudflare normally backs with external
services. Runtime therefore treats bindings as adapters:

- Pure data bindings such as KV and queue producers call the colocated redis-proxy
  sidecar. The loaded worker sees a Cloudflare-shaped object, but method calls cross
  back to runtime through workerd JSRPC and then to redis-proxy over HTTP.
- Secret values also cross redis-proxy on cold-load. Runtime receives plaintext
  `ns_secrets` and `worker_secrets` in the internal load envelope after redis-proxy
  decrypts `WDL-ENC:` values; tenant-facing `env` shape stays unchanged. Env
  materialization merges in fixed precedence: bundle vars, then namespace secrets, then
  worker secrets. A worker-level secret with the same name wins over a namespace-level
  secret, which wins over a var. Control enforces a headroomed estimate of workerd's
  `workerLoader` serialized env budget during deploys and secret mutations. That
  estimate calls the same `buildWorkerEnv()` materializer as cold-load with shape-only
  factories, then measures user vars/secrets plus runtime-injected binding env values,
  including required caller secret copies in platform/service binding props, so an
  over-large env fails in the control plane instead of during runtime cold-load.
- Stateful bindings such as D1, Durable Objects, and Workflows call dedicated backend
  services through binding-scoped host adapters. Immutable props restrict each adapter
  to the declared database, object namespace, or workflow; loaded workers never receive
  a generic authenticated DO or Workflows Fetcher.
- R2 is an S3-compatible object-storage adapter: runtime signs requests with platform
  credentials and sends them to the configured endpoint.
- AI is a host binding plus generated tenant-realm facade. Runtime asks redis-proxy for
  an atomic provider/credential snapshot, revalidates the exact official destination,
  and uses the public-only `AI_NETWORK` service. Provider credentials never enter the
  loaded Worker env. See [`ai.md`](ai.md) for the protocol and lifecycle contract.
- ASSETS is a deploy-artifact URL helper: control uploads assets to S3-compatible
  storage during deploy, while runtime reads `__meta__.assets` plus `ASSETS_CDN_BASE`
  and only exposes `env.ASSETS.url(path)` for tokenized CDN URLs.
- Service and platform bindings use workerd JSRPC/fetch machinery, but control metadata
  decides which worker, namespace, version, and entrypoint are allowed.

Root module names beginning `_wdl-` are reserved for WDL-generated modules. Control
rejects new bundles that use the prefix, and runtime/do-runtime fail closed when
cold-loading a persisted version that contains one. Existing versions must rename the
module and redeploy; WDL does not provide a compatibility alias. Generated module
exports are implementation details and not a tenant import surface. The reservation is
root-only: a path such as `src/_wdl-helper.js` remains a tenant module name.

KV is the simplest example. The runtime exports `KV` as a named entrypoint and
instantiates one object per binding with `{ ns, id }` props. `get`, `put`, `delete`,
`list`, batch `get`, and metadata calls all go to redis-proxy DB 1. redis-proxy stores
each namespace/id in 32 hash buckets named `kvh:<ns>:<id>:b:<bucket>`; value fields are
`v:<key>` and metadata fields are `m:<key>`. Put uses `HSET`/`HSETEX` plus hash-field
expiration, delete removes both value and metadata fields, list scans bucket fields with
an opaque cursor, and batch/list metadata paths enforce aggregate raw value/metadata byte
budgets before base64 response encoding.

## Binding Surface Contracts

KV supports the common `KVNamespace` calls: `get`, batch `get`, `getWithMetadata`, batch
`getWithMetadata`, `put`, `delete`, and `list`. `get` supports text, JSON, arrayBuffer,
and stream result shapes; batch reads support text and JSON. Values are capped at 25 MiB
in the runtime shim before proxying, and stream values are read with the same cap. Keys
are capped at 512 UTF-8 bytes at the redis-proxy boundary for all KV operations,
including list prefixes and batch reads. `put()` expiration and expiration-TTL values
must be positive JavaScript safe integers; the proxy applies an expiring value-only
write and stale-metadata removal atomically.
`list()` is backed by Redis `HSCAN`, not a Cloudflare ordered B-tree: keys are not
sorted, cursors are opaque WDL cursors, and concurrent writes may appear out of order or
be re-seen. A page with `list_complete: false` may contain fewer than `limit` keys,
including zero; callers must continue with the returned cursor until `list_complete` is
true. `limit` is capped at 1000. `cacheTtl` is accepted only as API shape; there is no
Cloudflare edge read cache or global eventual-consistency window.

R2 bindings map `bucket_name` to a namespace-scoped virtual bucket under the platform
S3-compatible bucket: `r2/<ns>/<bucket_name>/<object-key>`. Workers in the same
namespace using the same `bucket_name` intentionally share data; different namespaces
are isolated by prefix. Runtime supports the common `head`, `get`, `put`, `delete`, and
`list` paths. `get()` returns a streaming body, and convenience readers enforce the
25 MiB cap. `bodyUsed` reflects disturbance of the exposed stream; convenience readers
reject after tenant code reads from that stream even if its reader later releases the
lock. `put(stream, ...)` currently buffers and sends one S3 PUT with the same cap;
multipart upload, SSE-C, and checksum selection are not supported. Conditional requests
and range GETs implement the common R2 behavior. `list({ include: [...] })` performs
extra HEAD requests for metadata fields and applies a concurrency cap. Tenant-supplied
`Headers` metadata must carry a canonical IMF-fixdate `Expires` value when that header
is present; malformed write metadata is rejected before the host binding call.
R2 validates BufferSource internal slots before writing. Blob PUT bodies are checked
against the object limit before their bytes are materialized. Buffered stream readers
snapshot each delivered chunk before retaining it across the next read. Raw GET streams
retain zero-copy normalization for chunks backed by fixed `ArrayBuffer`s and snapshot
resizable or shared backing before exposure.
`ReadableStreamDefaultController.enqueue()` queues object references, so producer
mutations after enqueue can change bytes before WDL receives them. Direct `Uint8Array`
PUT bodies backed by a fixed `ArrayBuffer` retain zero-copy normalization. Detached or
out-of-bounds fixed views are rejected before the host binding call.
Tenant-facing R2 errors expose operation/status plus virtual object keys where useful,
but not raw S3 response bodies or physical `r2/<ns>/<bucket>/...` keys. Control-plane
R2 admin errors may retain backend detail for operators.

AI accepts at most one `{ type: "ai" }` binding. Generated wrapper code exposes
`fetch()`, `run()`, and `models()` through positional handler/entrypoint env. When
importable env is enabled, invocation-time reads from the imported env proxy see the
same facade. The host entrypoint itself exposes only `fetch()`. The virtual raw origin
is `https://ai.wdl`; provider aliases, official destinations, Redis shapes, byte/time
bounds, WebSocket rules, and non-goals are owned by [`ai.md`](ai.md).

Generated host-facade wrappers preserve the Worker's importable-env compatibility
contract. They use workerd `withEnv()` only when `disallow_importable_env` is absent.
With importable env enabled, tenant modules may retain the imported env proxy at module
scope and read bindings from that proxy during an invocation. Capturing an individual
binding during module evaluation is not a facade contract: evaluation precedes wrapper
invocation and can observe only the raw binding-scoped host adapter. With
`disallow_importable_env`, imported env remains empty at module scope and during
invocations, while positional env still receives generated facades.

ASSETS is a deploy-artifact helper, not a full Cloudflare Pages asset pipeline. Control
uploads files to `assets/<ns>/<worker>/<token>/<path>`, injects an `ASSETS` binding, and
runtime exposes `env.ASSETS.url(path)`. Tenant code awaits the JSRPC result; the
host-side URL construction is IO-free and returns a CDN-facing URL using
`ASSETS_CDN_BASE`. Path segments are split on `/`, empty
segments plus `.` and `..` are rejected, and each segment is percent-encoded. Version is
bound at load time, so rollback flips asset URLs. If workers need auth or rewrite logic
for static bytes, they should keep files in the bundle instead of using declared
`assets`.

R2 and ASSETS deliberately have different lifecycle semantics. ASSETS are deploy
artifacts and version/worker deletion stages `worker-delete-s3-cleanup` work. R2 is
tenant runtime data and worker deletion never deletes R2 objects.

Service bindings are frozen at caller deploy time. Control resolves target namespace,
worker, version, and entrypoint, stores them in caller metadata, and runtime loads that
exact target version on first use. Promoting the target later does not move existing
callers; caller redeploy is the refresh boundary. This version pinning makes rollback
and version delete referrer checks deterministic. Runtime revalidates the persisted
pinned version with the canonical positive JavaScript-safe-integer grammar before
materializing the binding.

Cross-namespace service bindings require a target `[[exports]]` entry for the bound
entrypoint, including `entrypoint = "default"` for the default export. The entry's
`allowed_callers` controls cross-namespace access; `["*"]` opens the entrypoint to any
namespace, and an empty list closes it to cross-namespace callers. Targets without
`[[exports]]` expose only their default entrypoint to same-namespace callers.
Same-namespace callers bypass ACLs but still obey strict entrypoint visibility once the
target declares `[[exports]]`. ACL changes are deploy-time, not call-time; existing
callers remain pinned until redeploy.

Platform bindings are WDL-specific service bindings into platform-tier namespaces such
as `__platform__`. Callers declare `[[platform_bindings]]`; control resolves the
`SCREAMING_SNAKE_CASE` symbol against active `[[exports]] as = "..."` entries, freezes
the target, and forwards only target-declared `required_caller_secrets` from the
caller. Raw `[[services]] ns = <platform-tier-ns>` is rejected, and gateway rejects
public traffic for platform-tier namespaces before Redis lookup.

## Redis / Storage Contracts

Runtime reads immutable bundle and metadata keys from DB 0 through `redis-proxy`.
Data-plane bindings use their own storage:

- Secret hash values in DB 0 are envelope ciphertext. redis-proxy decrypts them during
  runtime-load and fails closed when provider configuration or envelope validation
  fails.
- Runtime decodes bundle entries as views over the bounded cold-load envelope. Text and
  JSON modules are decoded from those views; wasm and data modules receive standalone
  byte copies so tenant-visible buffers cannot expose adjacent modules or secrets.
- Loaded host-binding wrappers cache only the stripped template for a stable workerd env
  object. Default object/function handlers receive a fresh top-level env copy and fresh
  facade instances per event. Persistent class entrypoints receive one wrapped env and
  facade set at construction and reuse it; only their diagnostic context is refreshed
  per invocation as described below.
- KV and queue producers use DB 1 through `redis-proxy`.
- Workflow bindings call `workflows`; runtime does not read DB 2 directly. Frozen
  Workflow identity exists once in binding-scoped host props. The generated facade
  carries only public operation fields, and Node-compatible `process.env` cannot expose
  the props.
- D1 and DO bindings call their dedicated runtime services.
- R2/ASSETS use S3-compatible object storage.

Runtime must treat Redis bundle metadata as control-authored, but still revalidates
reserved runtime entrypoint and binding names when materializing older stored metadata.
It also fails closed if older metadata has a `compatibilityDate` before `2026-04-01`,
contains Python module entries or upstream experimental compatibility flags, disables
WDL's required enhanced error serialization, or violates another WDL-owned metadata
contract. The date floor and enhanced-serialization requirement are WDL forward-only
policy. Workerd remains responsible for rejecting redundant or otherwise incompatible
upstream flags during cold load.

## Ownership / Concurrency / Failure Semantics

- workerLoader cache has no LRU. Runtime injects `__WdlAbort__` into every loaded worker
  and evicts sibling historical versions on active-version cold loads.
- Service-binding cold loads record loaded versions but do not evict siblings, because a
  service binding may intentionally target a frozen historical version.
- Internal active-version scheduled/queue dispatches opt into sibling eviction; frozen
  workflow dispatches do not.
- Wrapper generation hides raw env from unwrapped entrypoints whenever privileged
  internal Fetchers are injected. Its host-wrapper runtime evaluates before tenant
  modules and captures the intrinsics used to decide handler or env wrapping, so tenant
  top-level prototype mutation cannot bypass the env boundary.
- Generated wrappers pass request ids directly to per-request facade objects. Persistent
  class instances keep a small mutable diagnostic context that is refreshed when a
  wrapped handler starts; concurrent or deliberately re-entrant calls may therefore
  observe another invocation's id. Propagation is best-effort observability, not a
  security or correctness boundary, and Runtime does not rewrite tenant compatibility
  flags to support it. Request ids must never authorize, fence, or deduplicate work.
  Request-id syntax remains owned by the injected canonical request-id module.
- Request context wrappers swap facade objects into env and propagate request id where
  that event class can carry it. do-runtime alarm and RPC calls enter through private
  fetch dispatches carrying the outer request id, without adding platform metadata to
  tenant method arguments.
- An uncaught tenant `fetch()` exception maps to a platform `502 runtime_error`
  response with the request id. Exception details are emitted to structured logs/live
  tail and are not copied into the client body. Tail formatting cannot replace the
  original throwable when its string conversion fails.
- Internal scheduled, queue, and workflow dispatch routes use result envelopes for
  handler outcomes. A tenant handler error is represented as outcome state for the
  scheduler/workflow protocol, not as a generic platform transport error.
- No route-cache invalidation protocol exists at runtime. `workerLoader` cache keys are
  immutable worker ids, so a promoted version is a new key and naturally cold-loads.
- `runtime/tail-worker.js` is attached to every dynamic load through `workerCode.tails`.
  It always emits structured stdout; forwarding into `wdl tail` only happens after the
  shared tail forwarder sees an active subscription.

## Security Boundaries

- user-runtime loaded workers receive public-only outbound. Runtime itself keeps
  internal outbound for Redis and S3-compatible storage work.
- AI provider traffic uses an explicit public-only `AI_NETWORK` service. The host
  binding may use internal outbound only for its authenticated colocated redis-proxy
  resolve call; tenant code never receives either hidden capability or the credential.
- system-runtime loaded `__system__` workers intentionally receive private+public
  outbound.
- Privileged runtime endpoints must be added to `runtime/internal.js` on `:8088`, not to
  the gateway-facing loader socket.
- Reserved bindings matching `__WDL_*__` and reserved entrypoints matching `__Wdl*__`
  are platform-owned.
- Raw env values observable during tenant module evaluation are restricted to their
  declared binding identity. DO and Workflow adapters attach internal auth only after
  replacing caller-supplied identity; generic authenticated backend Fetchers are never
  placed in loaded-worker env.
- Owner hints for D1/DO are trusted only when authored by runtime services, not from
  tenant response bodies.

## Observability

Runtime emits request logs and metrics for loading, binding operations, AI pool state,
`redis-proxy` calls, workflow replay cache, loader evictions, and dispatch envelopes. Tail worker
emits structured stdout for console/exception capture and forwards to `wdl tail` only
when a matching active tail session exists.

Cold-load duration metrics use workerd's request clock. The
`bundle_load_stage_duration_ms` series therefore covers only the awaited
`redis_proxy_load` stage. `bundle_load_duration_ms` remains useful for end-to-end
request-visible load latency, but neither metric profiles uninterrupted bundle decode,
env construction, or wrapper generation CPU time; workerd does not advance the clock
during those synchronous stages.

## Deployment / Rollout Notes

- Runtime/Control contract changes follow the reader-before-writer procedure in the
  [infra rollout notes](infra.md#deployment--rollout-notes); do not rely on an
  uncoordinated simultaneous roll.
- Runtime does not enable workerd's broad `experimental` flag for loaded workers.
  Historical-version eviction injects `__WdlAbort__`, but `abortIsolate()` is
  available without that flag in the bundled workerd baseline. The current upstream
  implementation removes the loader cache identity while allowing outstanding calls to
  drain; workerd upgrades must reverify that behavior.
- Bundled workerd returns `0` from `Date.now()`, zero-argument `new Date()`, and
  `performance.now()` outside an active request, including dynamic module evaluation.
  Generated wrappers pass through the request-owned
  `ExecutionContext`, so `ctx.abort(reason?)`, `ctx.tracing.startSpan()`, and span
  attribute setters are available without a WDL shim. `ctx.abort()` terminates only the
  current stateless invocation; it is unrelated to host-only `abortIsolate()` eviction.
- At compatibility date `2026-08-04` or later, workerd enables both `nodejs_compat` and
  `nodejs_compat_v2` by default. A tenant that needs neither surface must specify both
  `no_nodejs_compat` and `no_nodejs_compat_v2`.
- WDL preserves explicit positive compatibility flags even when the selected date already
  enables them. Workerd 2026-08-25 accepts that redundant spelling because it produces the
  same compiled flag set; WDL does not duplicate upstream's date-to-flag table.
- Control rejects upstream `$experimental` compatibility enable flags and WDL's explicit
  `allow_irrevocable_stub_storage`, `new_module_registry`, and
  `streams_disable_constructors` deny policy at deploy; runtime rejects retained metadata
  containing either class. The new module registry graduated upstream, but WDL keeps it
  disabled because every dynamic Worker runs through `_wdl-wrapper.js`, which would make
  tenant `import.meta.main` false. Static host workers also omit the irrevocable-stub
  flag. Disable-style flags such as `no_*` are not part of the experimental mirror unless
  WDL explicitly rejects them or upstream marks the enable flag itself experimental.
- Python Workers modules are not supported. Upstream's `python_workers_20260610` flag is
  no longer experimental and is no longer implied by date; `python_workers_20260817` is
  experimental. Control still rejects new `py` module manifests and runtime/do-runtime
  reject retained metadata that contains them instead of letting workerd bootstrap
  Pyodide during cold load.
- Since WDL's 2026-07-01 workerd pin, runtime processes have used process-level
  `--experimental` because upstream gates `workerLoader` bindings on that switch.
  Do not add the `experimental` compatibility flag or `allowExperimental` to loaded
  WorkerCode unless another upstream API explicitly requires it.
- Since WDL's 2026-07-01 workerd pin, Control has enforced upstream's 64 MiB dynamic
  worker code cap and 1 MiB serialized dynamic env cap. It estimates final WorkerCode
  once before version allocation, including runtime/do-runtime-injected wrapper/client
  modules and workflow import rewrites. Materialized D1 ids, DO storage ids, Workflow
  keys, and version identity live in host props rather than generated source, so WATCH
  retries do not repeat that code estimate. Vars, namespace/worker secrets, and
  runtime-injected binding env values are checked against a headroomed `workerLoader`
  env budget in watched commit and secret-mutation paths. Workflow identity is part of
  runtime-injected binding env and is covered by the env budget. Commit and
  secret-mutation paths check the actual version they will load. Deploy and
  namespace-secret mutations use the version they can load; worker secret mutations
  also recheck the forced bump inside the WATCH/COPY transaction so the allocated bump
  version is covered before routing flips. The estimate starts from JSON bytes and adds
  V8 two-byte string overhead for non-Latin-1 strings, so mixed ASCII plus CJK or emoji
  secrets do not slip past control and fail later at cold-load.
- In current stock workerd, a client disconnect during an async `ReadableStream`
  response body may not call the stream source's `cancel()` callback. Tenant streaming
  and SSE workers should use their own heartbeat, timeout, or application close path
  instead of relying on disconnect-driven `cancel()` as the only resource cleanup hook.
- workerd upgrades can still change default or compatibility-flagged runtime
  surfaces; review the exposed surface, not only the loader/abort path.

## Tests That Protect This Module

- `tests/unit/runtime-load.test.js`
- `tests/unit/runtime-dispatch-handlers.test.js`
- `tests/unit/runtime-dispatch-workflows.test.js`
- `tests/unit/runtime-service-binding.test.js`
- `tests/unit/runtime-queue-producer.test.js`
- `tests/unit/runtime-d1-client.test.js`
- `tests/unit/runtime-do-client.test.js`
- `tests/unit/runtime-bindings-do.test.js`
- `tests/unit/runtime-r2-client.test.js`
- `tests/unit/runtime-r2-host.test.js`
- `tests/unit/runtime-workflows-client.test.js`
- `tests/integration/service-bindings.test.js`
- `tests/integration/service-bindings-rpc.test.js`
- `tests/integration/platform-bindings.test.js`
- Queue integration file group: `tests/integration/queues-delivery.test.js`,
  `tests/integration/queues-retry-and-delay.test.js`,
  `tests/integration/queues-orphan-and-control.test.js`,
  `tests/integration/queues-batch-and-isolation.test.js`
- `tests/integration/cron-triggers.test.js`
- Workflow integration file group: `tests/integration/workflows-runtime-core.test.js`,
  `tests/integration/workflows-runtime-scheduler.test.js`,
  `tests/integration/workflows-runtime-pausing.test.js`,
  `tests/integration/workflows-runtime-retention.test.js`
- `tests/integration/d1-binding.test.js`
- `tests/integration/durable-objects-core.test.js`

## Known Constraints And Non-Goals

- Runtime does not query Redis on every hot request to decide whether a version is
  active.
- Active-version cold-load eviction is asynchronous and is not coordinated with Gateway
  cache invalidation. In the bundled workerd, `abortIsolate()` removes the historical
  loader cache entry but does not abort outstanding calls. A request or WebSocket already
  admitted to the previous immutable version can therefore drain and retain that isolate
  until it finishes or the container recycles.
- During promotion's brief stale-route window, an ordinary request can still be admitted
  to the previous immutable version and complete there. Gateway WebSocket lifecycle
  snapshots reject a stale initial upgrade and do not reload an inactive pinned version
  after backend loss; they do not add an active-version lookup to ordinary Runtime calls.
- Workflow replay cache is advisory only.
- Runtime is not the control-plane authorization boundary.
- Service-binding cold loads for pinned historical versions may recur after promote
  because those versions are deliberately not evicted. Container recycle remains the
  backstop for non-route-churn isolate leaks.

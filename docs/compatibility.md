# Workers Compatibility Matrix

This document answers a different question from the module docs: not "where is the
implementation?" but "how close is WDL to Cloudflare Workers for a given surface, and
what does WDL implement itself?"

Current code and tests remain authoritative. Treat this matrix as the current
compatibility contract for WDL's Workers platform shape.

Each row separates four compatibility claims:

- **What workerd provides**: the upstream runtime surface WDL reuses.
- **Stronger / added in WDL**: platform behavior or guardrails WDL implements
  beyond the raw workerd surface.
- **Different from Cloudflare**: intentional model differences that are not
  necessarily stronger or weaker.
- **Not implemented / gaps**: absent Cloudflare behavior or unsupported shapes.

## Status Legend

- **Supported**: expected to work for ordinary WDL tenant applications and covered by
  unit or integration tests.
- **Partial**: implemented for the documented WDL model, but missing Cloudflare
  behavior, global edge semantics, or less-common configuration shapes.
- **Not supported**: rejected at deploy/config time or intentionally absent.
- **Internal**: platform-facing surface, not tenant API compatibility.

## Runtime And Language Surface

| Surface | Status | What workerd provides | Stronger / added in WDL | Different from Cloudflare | Not implemented / gaps |
|---|---|---|---|---|---|
| ES module Workers and `fetch()` | Supported | Module evaluation, request dispatch, `Response`/`Request`, service binding JSRPC machinery. | Dynamic `workerLoader`, immutable version ids, wrapper-generated `env`, gateway routing, request logs, and public/private outbound separation. | Root module names beginning `_wdl-` are platform-reserved; new deploys are rejected and retained versions fail closed on cold load. An uncaught tenant `fetch()` exception maps to platform `502 runtime_error`; exception detail goes to structured logs/live tail, not the client body. | WDL does not emulate every compatibility-date behavior change; workerd is configured with the platform's enabled flags. |
| WebSocket upgrade | Supported | WebSocket API and 101 response handling inside workerd. | Gateway terminates a local public WebSocket pair and directly forwards to runtime/do-runtime with bounded backend reconnect and client-frame buffering. | Cloudflare disconnects every WebSocket when new code is deployed. Unless a worker opts into `sessionPolicy: "restart"` (which closes its sessions at promotion), WDL keeps gateway-proxied sockets across a promotion while the pinned version's backend stays healthy; only after that backend is lost does Gateway revalidate the active route and, if the pinned version is no longer active, close with `1012` so the client upgrades to the active version. | Gateway rolling still drops physical client sockets; application-level resume is not implemented. Reconnect is application-owned: neither the WebSocket protocol nor the bare API defines a close code that triggers reconnection. |
| `compatibility_date` / `compatibility_flags` | Partial | workerd feature flags and compatibility behavior where configured in capnp. | CLI/control stores bundle metadata. Dynamic Workers reject explicit `compatibility_date` values earlier than `2026-04-01`; Control also validates that the value is a real `YYYY-MM-DD` date that is not later than the current UTC date or the maximum date supported by the bundled workerd, rejects upstream `$experimental` enable flags such as `experimental` / `unsafe_module`, and rejects `legacy_error_serialization`, `allow_irrevocable_stub_storage`, and `streams_disable_constructors`. Workerd owns validation of redundant or otherwise incompatible upstream flags at cold load. | WDL treats compatibility metadata as deploy-time platform metadata rather than a complete per-worker historical emulation layer. | No per-worker emulation of every Cloudflare historical behavior; tenant workers cannot opt into upstream experimental-only flags, disable WDL-required enhanced error serialization or standard stream constructors, or persist irrevocable capability stubs. |
| `nodejs_compat` | Partial | workerd provides Node.js compatibility modules and globals. Starting at compatibility date `2026-08-04`, `nodejs_compat` and `nodejs_compat_v2` are both enabled by default. | CLI carries compatibility flags into metadata. A worker that needs the pre-`2026-08-04` surface must set both `no_nodejs_compat` and `no_nodejs_compat_v2`; disabling only the first leaves the independently defaulted V2 surface enabled. | WDL exposes the workerd-enabled compatibility surface rather than a separate Node.js runtime. | This is not a full Node.js platform contract beyond workerd's enabled surface. |
| Execution context and module time | Partial | workerd exposes `ctx.abort(reason?)`, `ctx.tracing.startSpan()`, chained span attribute setters, and production-like request time. Outside an active request, including top-level module evaluation, `Date.now()`, zero-argument `new Date()`, and `performance.now()` return `0`. | WDL passes the native execution context through generated wrappers and does not emulate wall time during module evaluation. | `ctx.abort()` ends the current stateless invocation; it is distinct from WDL's host-only historical-isolate eviction. | WDL rejects the experimental `tail_worker_user_spans` flag, so tenant-created span export through WDL live tail is not a supported contract. |
| Python Workers modules | Not supported | workerd has a release-gated Python Workers path; `python_workers_20260610` is no longer experimental and is implied only after compatibility date `2026-08-25`. | Control rejects `py` module manifests with `python_workers_unsupported`; runtime and do-runtime also fail closed for retained metadata containing `py` modules. | WDL keeps tenant bundles JavaScript/WebAssembly/data only and does not permit cold-load-time Pyodide bootstrap. | Python Workers and mixed JS/Python bundles are unsupported. |

WDL does not generally guarantee workerd downgrades. As best-effort guidance, a target
binary can cold-load a retained Dynamic Worker version only when it supports that
version's `compatibility_date`; see the
[infra rollout notes](modules/infra.md#deployment--rollout-notes).

Node.js TLS behavior follows the bundled workerd binary. Starting with WDL's
2026-07-01 workerd pin, workers whose compatibility date is at least 2026-06-16 get
`throw_on_not_implemented_tls_options`: unsupported `node:tls` options such as
`checkServerIdentity` now throw `ERR_OPTION_NOT_IMPLEMENTED` instead of being silently
ignored. Separately, workerd's `servername` / expected-certificate-hostname behavior
changed outside any compatibility flag, so certificate hostname validation follows the
bundled workerd behavior for all dates.

Starting with WDL's 2026-08-11 workerd pin, byte-stream BYOB reads use live
`ArrayBufferView` bounds instead of cached offsets and lengths, including when buffers
are resized or transferred. The same pin adopts ada-url 4: an ASCII hostname label
that starts with `xn--` but does not decode to a valid UTS #46 label is preserved and
lowercased instead of being rejected. WDL accepts such labels as literal route labels
when they also satisfy its ASCII DNS grammar in `shared/ns-pattern.js`; numeric-host
canonicalization remains in `control/topology.js`, and Unicode route names remain
unsupported. For compatibility dates on or after `2026-08-11`, workerd also enables
`workflows_enable_fast_engine_creation`, changing how its native Workflow engine
allocates instance IDs. WDL does not use that engine; its custom facade and DB 2-backed
workflows service retain WDL-owned instance identity and lifecycle semantics.

Bundled workerd permits `Fetcher` and Durable Object class stubs to cross JSRPC as
opaque arguments without an experimental flag. WDL treats possession of such a stub
as capability delegation: the receiver can call the delegated target with the
host-authored caller properties embedded in that stub, but cannot rewrite those
properties or recover hidden platform backend capabilities. This delegation may live
in memory, but WDL rejects `allow_irrevocable_stub_storage` at deploy and retained-state
load, and static host workers do not enable it, so long-term stub persistence is not a
supported WDL surface.

## Bindings And Storage

| Surface | Status | What workerd provides | Stronger / added in WDL | Different from Cloudflare | Not implemented / gaps |
|---|---|---|---|---|---|
| KV namespace | Supported | A binding object can be exposed to user code through workerd entrypoint/JSRPC mechanics. | Runtime `KV` facade calls redis-proxy; redis-proxy stores values and metadata in DB 1 hash buckets `kvh:<ns>:<id>:b:<bucket>`, with `v:<key>` and `m:<key>` fields, 512-byte key/list-prefix cap, 25 MiB value cap, batch raw-byte budget, TTL/EXAT, and prefix list cursors. | KV storage is Redis-backed and namespace-scoped in a WDL deployment. `cacheTtl` is not a storage freshness contract. | No Cloudflare global edge replication or eventual consistency model. |
| R2 bucket | Supported | Fetch/stream primitives in workerd. | Runtime R2 facade signs S3-compatible requests with platform credentials; CLI parses `[[r2_buckets]]`. | Bucket lifecycle and placement are the S3-compatible backend's responsibility. `Headers`-form `httpMetadata` accepts `Expires` only as a canonical IMF-fixdate and rejects malformed values before the host call. | `preview_bucket_name` and `jurisdiction` are not supported. |
| Workers AI binding | Partial | workerd supplies WorkerEntrypoint/JSRPC, Fetcher, streams, and WebSockets. | WDL adds namespace-scoped encrypted BYO credentials, official OpenAI/xAI/DeepSeek adapters, `fetch()`/`run()`/`models()`, OpenAI-compatible JSON/SSE, Responses and Realtime WebSockets, public-only egress, and bounded host pools. | This is a namespace-owned provider registry and credential boundary, not Cloudflare Workers AI infrastructure or a WDL-managed catalog. Model ids use `<provider>/<alias>` and the raw surface uses `https://ai.wdl/v1`. | No arbitrary endpoint, managed catalog, platform credential, usage/quota/billing, `toMarkdown()`, async batch, background Responses, WebRTC, or automatic tool execution. DeepSeek is HTTP/SSE only. |
| ASSETS | Partial | Worker can receive a platform-provided binding object. | CLI uploads assets to S3-compatible storage and runtime exposes the WDL `env.ASSETS.url(path)` helper for tokenized CDN URLs. | WDL assets are an S3/CDN helper model, not Cloudflare Pages asset hosting. | WDL does not provide a full Cloudflare Pages asset pipeline or a fetch-style assets binding contract. |
| D1 database | Partial | workerd can host a D1-like binding facade and localDisk-backed SQLite actor code; the bundled SQLite includes R*Tree virtual tables and `rtreecheck()`. | WDL implements control-plane metadata, d1-runtime, owner lease/generation fencing, migrations, SQL execution, drain/renew, deploy-time alias freezing, and caps on query bodies, decoded statement payloads, rows, and result bytes before SQLite work or response emission. | D1 storage is WDL-owned SQLite with owner routing, not Cloudflare global D1. Physical SQLite files are not deleted on metadata delete. | No Cloudflare global replication/bookmark semantics. SQLite names under the reserved `_cf_` namespace are rejected by workerd case-insensitively. |
| Durable Objects | Partial | Native Durable Object class execution, facet identity, SQLite-backed `ctx.storage.sql` including R*Tree virtual tables, and in-facet WebSocket hibernation API. | WDL implements runtime facade, do-runtime owner routing, shard leases, Redis generation fencing, alarm shim with Workflows-backed due/retry jobs, gateway-managed public WebSocket forwarding, storage ids, cleanup tombstones, and bundle-owned `preserve`/`restart` session policy. `preserve` is the default; `restart` closes superseded public WebSockets with `1012` and lazily aborts stale facets without deleting SQLite. Deployment-level `DO_PREVENT_EVICTION` defaults to `true`, keeping host actors resident; explicit `false` permits idle actor reconstruction while preserving SQLite and quiescent hibernatable socket state. | WDL DO identity, owner routing, session policy, cleanup, and residency policy are WDL-managed rather than Cloudflare migration-compatible. `restart` matches Cloudflare's default, where deploying new code restarts every Durable Object and disconnects its WebSockets; `preserve` is the WDL default and has no Cloudflare equivalent. A later `preserve` promotion can supersede restart work not yet observed. | Same-worker classes only. `script_name`, rename/delete migrations, and platform-level WebSocket session recovery are not implemented. Actor-local JavaScript state does not survive eviction. Tenant `ctx.storage.deleteAll()` is a WDL best-effort KV/SQL/Workflows sequence rather than Cloudflare's atomic storage operation; rejection may leave a partial result, and concurrent mutation is unsupported. Alarm mutation spans object SQLite and Workflows DB 2; after backend mutation starts, rejected `setAlarm()` or `deleteAlarm()` has unknown final state and requires a successful operation-specific retry. `getAlarm()` repair is best effort and does not confirm backend state. Current workerd can silently lose an in-flight hibernatable WebSocket send or close and can strand an in-flight auto-response across actor eviction, so eviction remains explicit opt-in. SQLite names under the reserved `_cf_` namespace are rejected by workerd case-insensitively. |
| Queues producer/consumer | Partial | Worker queue handler API surface in loaded workers. | Runtime producer facade writes through redis-proxy DB 1; scheduler owns consumer dispatch, retry, DLQ, orphan, delayed queues, and batch splitting for oversized dispatch bodies. | `max_batch_timeout_ms` is configuration metadata, not a true aggregation window; dispatch concurrency is scheduler-owned. | `max_concurrency` is rejected. |
| Cron triggers | Supported | `scheduled()` handler surface in workers. | Control stores cron config; scheduler owns indexed discovery, cron-slot buckets, due dispatch, and repairable projections. | Control and scheduler use JS/Rust `croner` engines and keep their behavior documented through tests and module docs. | New scheduler dispatch paths still require their own Redis lease/fence audit. |
| Workflows | Partial | User workflow class code runs inside loaded workers when dispatched by runtime. | workflows owns DB 2 instance state, step/event/sleep commits, runtime-observed `step.do` DAG edges including `Promise.all` siblings, generation/run-token fencing, lifecycle APIs, progress callbacks, and scheduler tick. | WDL Workflows V2 has WDL-specific payload semantics and terminal failure rules; permanent `step.do` failure is terminal for the run even if caught, and one step may record at most 1000 dependency edges. | WDL Workflows V2 is not Cloudflare Workflows parity. Native workerd `WorkflowInstance.delete()` and `Workflow.deleteBatch()` are not exposed by WDL's custom facade; `script_name`, cross-worker workflows, and Cloudflare's source-AST visualizer are also unsupported. |
| Service bindings | Supported | workerd service binding JSRPC and fetch dispatch. | WDL resolves target worker metadata, cold-loads immutable versions, propagates request ids where available, and enforces namespace/action ACL through control metadata. | Frozen-version service targets do not evict active siblings by design. | None currently documented. |
| Platform bindings | Supported | workerd named entrypoint/JSRPC mechanics. | WDL expands ACL-checked platform bindings from control metadata. | Platform bindings are WDL-specific. | Platform bindings are not a Cloudflare tenant portability feature. |
| Vars and secrets | Supported | Env values can be materialized into worker `env`. | Control stores worker vars/secrets metadata, encrypts secret values as `WDL-ENC:` envelopes at rest, redis-proxy decrypts them during cold-load, enforces a headroomed workerd 1 MiB `workerLoader` serialized env budget for user vars/secrets plus runtime-injected binding env values and binding-scoped Workflow identity props during deploy/secret mutation, accounts for V8 two-byte string storage for non-Latin-1 strings, and immutable versions are promoted when worker secrets change. | Secrets are platform-managed by WDL; the at-rest envelope provider is a deployment concern. | WDL secrets are not Cloudflare account secrets. |
| Worker code size | Supported | Dynamic worker module bodies are accepted by `workerLoader` up to workerd's 64 MiB limit. | Control estimates final WorkerCode, including runtime/do-runtime injected wrapper/client modules and workflow import rewrites, before writing a deployed version. | WDL's deploy JSON body limit is lower for ordinary inline deployments. | Large server-side bundle assembly paths must keep this guard. |

## Control Plane And Developer Tooling

| Surface | Status | What workerd provides | Stronger / added in WDL | Different from Cloudflare | Not implemented / gaps |
|---|---|---|---|---|---|
| Wrangler project parsing | Partial | None. | WDL CLI parses the supported subset of `wrangler.toml` / JSONC: KV, D1, R2, AI, services, DO, workflows, queues, vars, assets, routes, and explicit `workers_dev` platform-domain opt-out. | Unsupported fields are rejected rather than silently emulated. Cloudflare uses `workers_dev` for the Worker's `*.workers.dev` route; versioned preview URLs are controlled separately by `preview_urls`, which defaults to `workers_dev`. WDL maps `workers_dev` to `<ns>.<platform-domain>/<worker>/`, the namespace's ordinary serving path, and does not support `preview_urls`. A project that disables its Cloudflare `*.workers.dev` route therefore disables a production entry point here, so WDL requires at least one route pattern before accepting the opt-out. | Per-binding rows above call out the major rejected shapes. |
| Worker deploy/promote/delete | Supported | workerd loads what the platform supplies. | Control/auth own bundle commit, route promotion, WATCH/MULTI fences, lifecycle indexes, retained versions, secrets, and async S3 cleanup intents. | WDL API/CLI is the management surface. | Cloudflare API parity is not the goal. |
| Log tail | Supported | Worker console output and positional `Error` metadata are available to the runtime tail worker. | Runtime emits bounded structured logs and preserves workerd `errorInfo` as `error_info`; control authorizes tail sessions and redis-proxy stores bounded streams. | Tail activation is time-bounded. | Messages racing activation can be dropped; experimental user-span export is not enabled. |
| Metrics/health | Supported | Service code can expose HTTP endpoints. | Gateway, runtime, d1-runtime, do-runtime, scheduler, workflows, and redis-proxy expose their service-specific probes/metrics as documented in module docs. | Metrics sockets are per-service. | Control/auth have no standalone public metrics socket. |

## Unsupported Or Not Yet Modeled Cloudflare Surfaces

These entries are intentionally explicit so compatibility gaps do not hide behind module
docs:

| Surface | Status | Current WDL position |
|---|---|---|
| Cache API / Cloudflare edge cache semantics | Not supported | `caches.default` is not part of the stock workerd surface WDL exposes, and WDL does not implement Cloudflare's edge cache tier. Tenant code should not depend on this binding or use it as a persistence/CDN contract. |
| Vectorize, Analytics Engine, Browser Rendering, Hyperdrive, Email Workers | Not supported | No binding facade, control-plane metadata, or backing service exists in WDL. |
| R2 multipart upload, customer-provided encryption keys, and Cloudflare-specific checksum behavior | Not supported | The current R2 facade targets S3-compatible object operations needed by WDL workers/assets. Advanced Cloudflare R2 behaviors need explicit design before being documented as compatible. |
| Queue `contentType = "v8"` and per-consumer `max_concurrency` | Not supported | Queue messages support the documented `json`, `text`, and `bytes` content types; only `v8` is rejected. Dispatch concurrency remains scheduler-owned, and `max_concurrency` is rejected instead of silently ignored. |
| Incoming TCP `connect()` handlers and Socket RPC transfer | Not supported | The bundled workerd has an incoming `connect()` entrypoint and an autogated Socket RPC transfer path, but WDL configures no tenant raw-TCP ingress and does not enable that autogate. This does not affect the documented outbound `cloudflare:sockets` surface. |
| Upstream experimental and WDL-denied compatibility flags | Not supported | Tenant `compatibility_flags` entries whose upstream workerd flag is marked `$experimental`, plus WDL's explicit deny policies for `allow_irrevocable_stub_storage` and `streams_disable_constructors`, are rejected at deploy and runtime decode. This excludes New Module Registry-only surfaces such as `import.meta.dirname` and `import.meta.filename` and prevents tenant workers from disabling the standard stream constructors required by WDL runtime facades. |
| Python Workers | Not supported | WDL rejects Python module manifests instead of letting workerd fail at cold-load. |
| Durable Object cross-script bindings and migration rename/delete semantics | Not supported | WDL DO classes are same-worker only. Storage identity, owner routing, and delete cleanup are WDL-managed rather than Cloudflare migration-compatible. |
| Cloudflare account API parity | Not supported | WDL exposes its own CLI/control API. Cloudflare API compatibility is not a stated goal. |

## Design Rule

When workerd already provides an in-isolate programming model, WDL tries to keep that
programming model visible to tenant code. When Cloudflare's production platform supplies
an external service, WDL must implement the missing external piece: control metadata,
Redis/S3 storage adapters, owner routing, scheduler dispatch, or lifecycle cleanup.
Compatibility work should therefore state both halves: the workerd surface being reused
and the WDL service that supplies the platform behavior around it.

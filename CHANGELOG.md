# Changelog

## Unreleased

- Added an opt-in namespace-scoped AI binding with encrypted BYO credentials, official OpenAI/xAI/DeepSeek adapters, OpenAI-compatible HTTP/SSE and WebSocket protocols, agent `run()` helpers, bounded model discovery, public-only egress, process-local lifetime/resource guards, and terminal runtime-loss handling that never replaces a provider session behind an existing client connection. Roll out redis-proxy and Gateway before user-runtime/do-runtime; pause Control mutations while system-runtime delivers its reader and Control writer, then publish the CLI. Public release requires the official-provider live matrix from the Tokyo ECS runtime and do-runtime egress.
- Kept Workflow identity exclusively in binding-scoped host props; generated tenant facades carry only public operation fields, avoiding duplicate identity in wrapper code and request bodies.
- Propagated application-terminal backend WebSocket Close frames through Gateway instead of treating protocol, policy, resource, and application close codes as backend-loss signals eligible for transparent reconnect.

## wdl.20260811.1 - 2026-08-11

- Updated the bundled workerd and Workers types pins to `1.20260811.1` and `5.20260811.1`, raising the maximum tenant compatibility date to `2026-08-18`; BYOB reads now honor live buffer bounds after resize or transfer, and Control accepts ada-url 4-preserved literal `xn--` host labels while retaining potential-IPv4 rejection. New Module Registry-only APIs remain unavailable.

## wdl.20260810.1 - 2026-08-10

- Added strict `DO_PREVENT_EVICTION` configuration: actors remain resident by default, while explicit `false` enables stock workerd eviction for workloads that accept current in-flight hibernating WebSocket limitations. Task-scoped session-policy fences prevent stale native facet reuse across host reconstruction and owner movement while preserving SQLite and quiescent hibernating WebSocket continuity.
- Updated the bundled workerd and Workers types pins to `1.20260810.1` and `5.20260810.1`, raising the maximum tenant compatibility date to `2026-08-17`.

## wdl.20260809.1 - 2026-08-10

- Updated the bundled workerd and Workers types pins to `1.20260809.1` and `5.20260809.1`, raised the maximum tenant compatibility date to `2026-08-16`, moved isolates to V8 15.1, and adopted upstream reliability fixes, zero module clocks, stateless `ctx.abort(reason?)`, expanded tracing spans, SQLite R*Tree support, and bounded console `Error` metadata.
- Reflected workerd's `2026-08-04` default enablement of `nodejs_compat` and `nodejs_compat_v2` (full opt-out requires both `no_nodejs_compat` and `no_nodejs_compat_v2`), split workerd and Node type-check programs, and continued rejecting Python modules and unsupported experimental flags.
- Kept Workflow identity out of `process.env`, reserved wrapper budgets across immutable version bumps, and documented native Workflow instance and batch deletion as unsupported by WDL's custom facade. Roll out user-runtime and do-runtime before system-runtime/Control, pausing Control mutations while the writer tier rolls.
- Pinned Quick Start and CLI integration to `@wdl-dev/cli@1.7.1`, including session-policy deploy support and fail-fast rejection of unsupported event subscriptions.

## wdl.20260804.2 - 2026-08-05

- Renamed the opt-in Durable Object restart rollout to the worker session policy: deploy and promote use `sessionPolicy`, Redis state moved to `worker:session-policy:*`, `worker:session-policy-seq:*`, and the `session-policy:restart` channel, and bundle metadata persisted under the retired `durableObjectRollout` field is dual-read.
- Allowed `sessionPolicy: "restart"` without a Durable Object binding. The policy governs established sessions only: open WebSockets close with `1012` at promotion and stale DO facets abort on their next dispatch, so a pure WebSocket worker can opt into Cloudflare-style deploy disconnects.
- For this upgrade, deploy Gateway, Workflows, and do-runtime before system-runtime/Control and keep Control mutations paused for the entire roll. A worker last promoted with `restart` behaves as `preserve` until its next promotion; leftover `worker:do-rollout*` keys are inert repair-only cleanup.

## wdl.20260804.1 - 2026-08-04

- Added opt-in Durable Object restart rollouts through the Control API's `durableObjectRollout: "restart"`, defaulting to `preserve`. Promotion commits the active projection and permanent restart sequence atomically, and do-runtime aborts superseded facets on their next dispatch without deleting SQLite state.
- Made Redis projection state the durable rollout authority, with Pub/Sub only requesting prompt Gateway reconciliation. Deploy Gateway, Workflows, and do-runtime readers before system-runtime/Control writers, and pause Control mutations while that writer tier rolls.
- Closed Gateway WebSocket sessions with `1012` when their version is superseded or their worker is deleted, and revalidated the active route before admitting a replacement backend so an already-inactive version is never reloaded.
- Removed platform code with no production callers and validation a stronger gate already guarantees, and inlined single-caller abstractions; no observable contract changed.
- Updated the bundled workerd and Workers types pins to `1.20260804.1` and `5.20260804.1`; upstream changes only advance release and maximum compatibility dates.

## wdl.20260801.1 - 2026-08-02

- Closed both sides of completed or failed Gateway WebSocket sessions, preserved status-free closes, normalized unsendable abnormal close codes, enabled bidirectional binary-frame forwarding, and restored public and backend close handshakes so abandoned sockets do not remain pinned until an external idle timeout.
- Removed the per-session `GatewayWsHolder` Durable Object; Gateway now terminates the public WebSocket pair directly and reconnects through the resolved runtime binding with the same bounded retry and client-frame buffer contracts.
- Updated the bundled workerd and Workers types pins to `1.20260801.1` and `5.20260801.1`; upstream changes only advance release and maximum compatibility dates.

## wdl.20260730.1 - 2026-07-30

- Further reduced Valkey round trips, command bytes, and request-path allocation across platform control and data paths through a shared typed command surface, bounded mixed pipelines, cached Lua scripts, and reusable RESP buffers.
- Kept Queue streams with different batch caps progressing alongside idle or slow streams through a shared readiness probe and bounded non-blocking top-ups, limited delayed-message materialization per chunk, and revalidated missing consumers immediately before destructive orphan transitions.
- Preserved Workflow replay outputs as exact persisted JSON snapshots across run claims, capped cross-request replay retention at 16 MiB per runtime isolate while bounding active-controller working sets, batched `createBatch()` preflight reads, and aligned Runtime JSON depth and lone-surrogate rejection with Rust.
- Scoped Gateway projection retries to the affected namespace or host after membership gates are warm, avoiding invalidation from unrelated deployments. Reduced serialization, copying, and allocation in D1/DO transport, R2, module loading, live-tail forwarding, and JS/Rust observability.
- Bounded sparse KV list scans to one unfinished Valkey `HSCAN` window per request; incomplete pages may contain fewer than the requested limit, including zero, and callers must follow the cursor until `list_complete`.
- Consolidated duplicated runtime, ownership, Redis/RESP, and Rust support helpers under canonical owners, refreshed compatible Rust dependencies, and pinned Quick Start and CLI integration to `@wdl-dev/cli@1.6.1`.
- Updated the bundled workerd and Workers types pins to `1.20260730.1` and `5.20260730.1`; upstream workerd changes only advance release and maximum compatibility dates.

## wdl.20260727.1 - 2026-07-27

- Hardened custom route and host declarations by normalizing them to canonical ASCII DNS hostnames and rejecting URL-authority delimiters or non-canonical IPv4 shorthand before routing metadata is written.
- Pinned Quick Start and CLI integration to `@wdl-dev/cli@1.6.0` and added end-to-end coverage for Wrangler `workers_dev` opt-out and route-only URL output.
- Updated the bundled workerd and Workers types pins to `1.20260727.1` and `5.20260727.1`.

## wdl.20260724.1 - 2026-07-25

- Added Control API `workersDev=false` support for workers with custom route patterns, allowing pattern routes to remain active while that Worker's platform-domain path returns 404; promote responses now report the active platform and pattern-route URL hints. Cloudflare uses the corresponding Wrangler `workers_dev` field for the Worker's `*.workers.dev` route, while versioned preview URLs are controlled separately by `preview_urls`, which defaults to `workers_dev`. WDL maps the setting to an ordinary platform-domain serving route, so the opt-out requires at least one route pattern.
- Updated the bundled workerd and Workers types pins to `1.20260724.1` and `5.20260724.1`.

## wdl.20260723.1 - 2026-07-23

- Reduced Valkey round trips and request bytes across Durable Object ownership, KV, queues, Workflows, Auth, Gateway routing, and log-tail paths with bounded typed pipelines, cached direct Rust scripts, atomic snapshots, and reusable RESP buffers.
- Shortened Control WATCH windows and owner lease renew/release paths with batched reads and exact-value compare-and-set operations while preserving generation, storage-pointer, and persisted-state fences.
- Made Queue retry and dead-letter transitions atomic per message: target writes complete before the source entry is acknowledged, one failed transition no longer blocks the rest of its batch, and stale discovery-index members are removed by key type with periodic repair off the reconcile hot path.
- Fenced Workflow mutations against same-id instance recreation through the persisted creation timestamp, bounded Gateway routing lookups under invalidation churn behind a new `gateway_routing_unavailable` 503, and restricted KV expiration inputs to positive JavaScript safe integers.
- Made Cron generations permanent across projection and worker deletion, and revalidated exact Cron configuration atomically before claim and advance. For this upgrade, pause Control mutations while system-runtime rolls, wait for it to stabilize, then resume mutations and deploy Scheduler and the remaining services.
- Updated the bundled workerd and Workers types pins to `1.20260723.1` and `5.20260723.1`.

## wdl.20260719.2 - 2026-07-20

- Decoupled Scheduler ticks from tenant Workflow and DO alarm completion: ticks now perform bounded maintenance and admission while admitted work continues in tracked per-replica tasks. Workflow permits remain held through runtime dispatch and fenced commit, and Scheduler uses an independent 60-second tick deadline.
- Replaced tick completion counters with admission and capacity-pressure signals, added bounded metrics for fenced Workflow commits and unknown in-flight alarm outcomes, and made Scheduler response-body read failures explicit.

## wdl.20260719.1 - 2026-07-19

- Removed shard-serial Workflows queueing by interleaving at most 128 ready candidates through a global dispatch pool, running workflow and DO alarm pools concurrently with defaults of 128 and 32, and aligning Scheduler's 130-second tick deadline with Terraform's 120-second dispatch timeout.
- Updated the bundled workerd and Workers types pins to `1.20260719.1` and `5.20260719.1`, and pinned CLI integration and Quick Start to `@wdl-dev/cli@1.5.0`.

## wdl.20260718.1 - 2026-07-19

- Upgraded the bundled runtime to workerd `1.20260718.1` and Workers types `5.20260718.1`; this date-only upstream release extends the maximum compatibility date to `2026-07-25` without runtime or schema changes.
- Made Kubernetes overlays pull the published `latest` WDL images directly instead of requiring node-local `:dev` image imports.
- Replaced the local ingress-nginx overlay with Gateway API resources for NGINX Gateway Fabric 2.6.7, preserving wildcard tenant routing, long-lived gateway requests, and the read-only ASSETS facade.
- Reduced the default retention for newly created completed, failed, and terminated Workflow instances from 7 days to 8 hours so terminal instances release worker-version deletion blockers sooner.

## wdl.20260717.1 - 2026-07-18

- Upgraded the bundled runtime to workerd `1.20260717.1` and Workers types `5.20260717.1`; tenant JSRPC can serialize `Blob` values and delegate opaque service and Durable Object class stubs, while WDL continues to reject irrevocable long-term stub storage.
- Upgraded the Rust toolchain baseline from `1.96.0` to `1.97.1` and the CLI integration pin from `1.4.0` to `1.4.1`.
- Established WDL's default forward-only, greenfield-oriented upgrade policy: downgrades are not generally guaranteed, and the documented retained compatibility-date, Durable Object `Blob`, and D1/DO localDisk metadata steps are best-effort operator guidance.

## wdl.20260701.2 - 2026-07-17

- Updated maintenance dependencies and image baselines: workerd images now use pinned distroless `base-debian13`, local and Kubernetes deployments use `valkey/valkey:9.1-alpine`, and `@wdl-dev/aws-sigv4` 3.0.1 adds stable request snapshots, redirect rejection, lowercase region validation, and non-blocking response cleanup.
- Consolidated duplicated Control, Auth, runtime, and Rust request, retry, Redis-key, projection, and observability contracts into canonical shared owners.
- Hardened gateway/runtime isolation by stripping private headers in both directions, validating scoped D1/DO owners and private forwarding endpoints, and limiting non-idempotent DO rediscovery to authenticated pre-dispatch ownership failures; request ids remain best-effort diagnostics.
- Tightened forward-only management inputs: explicit dynamic-worker compatibility dates must be at least `2026-04-01`; unsupported compatibility flags, ambiguous internal-auth/request-id values, reserved secret keys, and runtime-reserved module names are rejected.
- Tightened tenant data contracts: DO RPC accepts at most 1 MiB of structural JSON without `toJSON()` hooks, DO identities require well-formed Unicode and bounded host ids, and `Headers`-form R2 expiry requires canonical IMF-fixdate syntax.
- Made required bundle/workflow metadata, queue base64 bodies, persisted secret envelopes, and D1/DO ownership and alarm state fail closed; Control D1 lifecycle 5xx responses no longer expose raw backend diagnostics.
- Closed delegated-token, worker-delete, DO-owner, and workflow restart/version-delete races, and made whole-worker deletion clean up orphan workflow definitions without interrupting active DO traffic during inactive-version deletion.
- Normalized `PLATFORM_DOMAIN`, made the published-image Compose overlay pull-only, restricted Kubernetes user-runtime loader ingress to gateway, aligned scheduler/Workflows drain and stop windows across deployment targets, and removed the unsupported DO inline worker-code hook.

## wdl.20260701.1 - 2026-07-08

- Adapted the platform to stock workerd `1.20260701.1`, splitting process-level and loaded-worker experimental usage so `--experimental` stays only on the workerLoader-owning runtimes; see `docs/compatibility.md` for the tenant-visible runtime behavior the bump carries (notably `node:tls` unsupported-option and certificate-hostname-validation changes).
- Enforced the workerd dynamic-worker limits in the control plane before deploy and secret mutations: the estimated `workerLoader` env is held under a headroomed 1 MiB budget (`worker_env_too_large`) and total module bytes under 64 MiB (`worker_code_too_large` / `worker_code_invalid`), so oversized workers fail fast instead of at cold-load.
- Rejected tenant-declared experimental compatibility flags at deploy (`experimental_compat_flag_unsupported`) and on retained runtime metadata load, pinned against a mirror of the workerd `1.20260701.1` experimental flag set, and removed the blanket loaded-worker `experimental` flag.
- Rejected Python Worker modules at deploy and on load (`python_workers_unsupported`); WDL bundles stay JavaScript/WebAssembly/data only.
- Made worker-secret mutations commit the secret write, bundle copy, and route flip in one WATCH/MULTI transaction with an exact-version env budget recheck (removing the earlier write-then-rollback path), and moved namespace-secret mutations to an optimistic WATCH/MULTI transaction that re-estimates every affected worker/version env budget and backpressures under contention (`namespace_secret_mutation_contention`).
- Made the Durable Object `deleteAll` shim skip workerd's `_cf_`-reserved SQLite names case-insensitively, matching workerd's reserved-name enforcement.
- Bounded log-tail sessions against the workerd 2026-06-19+ behavior where client disconnects no longer reliably cancel response streams, using independent max-session and idle-pull watchdogs.
- Fixed S3 asset cleanup to checkpoint retry pagination one List/Delete page per run with a cumulative deleted count, so large-prefix cleanups drain across cron ticks without burning failure attempts, and to build the ListObjectsV2 query with S3 canonical percent-encoding so prefixes containing spaces sign and list correctly.
- Moved the Terraform stack to Fargate with explicit D1/DO runtime container memory ceilings, validated task sizing, ECS capacity-provider dependency ordering, and enhanced Container Insights.
- Upgraded the vendored `@wdl-dev/aws-sigv4` signer to 2.0.0 and the Rust `redis-proxy` `aes-gcm` (0.11) and `redis` (1.3) dependencies, preserving secret-envelope decryption behavior.
- Upgraded the local/dev stack images: S3Mock to 5.1.0 (with its renamed initial-buckets environment variable), Envoy pinned to v1.38.3, and Valkey to 9.1.
- Adopted `HGETEX` to refresh tail-activation TTLs in one round-trip, and converged secret Redis key construction and runtime injection-source ownership into single shared owners.

## wdl.20260617.2 - 2026-06-27

- Added `VERSION` as the release tag source of truth and relaxed release tag validation so multiple WDL patch releases can ship on the same locked workerd date.
- Documented HA/public ingress behavior and added configurable additional public ALB hosts for future public surfaces.
- Added the CLI integration delegated token template.
- Replaced the vendored `aws4fetch` signer with `@wdl-dev/aws-sigv4` across S3/R2 paths while preserving transient retry behavior.
- Fixed S3 list query encoding for prefixes containing spaces and expanded signer coverage for retry and signed-header behavior.
- Redacted D1 and Durable Object owner task IDs from tenant-visible metadata and error paths.
- Bounded Durable Object fetch body reads before buffering.

## wdl.20260617.1 - 2026-06-17

Initial WDL open source release

- Opens WDL as a self-hosted multi-tenant Workers platform built on stock Cloudflare workerd.
- Supports dynamic worker loading, namespace routing, control/auth, service and platform bindings, live tailing, and Prometheus metrics.
- Provides the core runtime surfaces: KV, R2, D1, Durable Objects, queues, cron triggers, Workflows, and ASSETS.
- Ships public Docker images, Docker Compose local development, Terraform greenfield AWS deployment, and Kubernetes manifests.
- Published under the Apache License, Version 2.0.

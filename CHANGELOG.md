# Changelog

## Unreleased

- Reduced Valkey round trips and request bytes across Durable Object ownership, KV, queues, Workflows, Auth, Gateway routing, and log-tail paths with bounded typed pipelines, cached direct Rust scripts, atomic snapshots, and reusable RESP buffers.
- Shortened Control WATCH windows and owner lease renew/release paths with batched reads and exact-value compare-and-set operations while preserving generation, storage-pointer, and persisted-state fences.
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

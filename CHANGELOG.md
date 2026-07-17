# Changelog

## Unreleased

- Updated maintenance dependencies and image baselines: workerd runtime images now use distroless `base-debian13`, local and Kubernetes Valkey use `valkey/valkey:9.1-alpine`, and root development tooling moved to current patch/minor releases.
- Consolidated duplicated Control request/error/retry paths, Redis locks and key grammars, queue and cron projections, S3 retry policy, observability helpers, and Rust sidecar contracts into canonical owners with shared behavioral and cross-language fixture coverage.
- Hardened the tenant/runtime boundary: gateway strips private `x-wdl-*` headers, generated wrappers and D1/R2/DO facades resist tenant prototype mutation at capability boundaries, owner records and forwarding targets require service-specific private endpoints, generated wrappers propagate diagnostic request ids on a best-effort basis, and DO ownership retries rely on private do-runtime markers without replaying unknown-outcome non-idempotent requests.
- Tightened forward-only input contracts: dynamic Workers reject explicit `compatibility_date` values earlier than `2026-04-01`, upstream experimental flags, and flags that disable WDL's required enhanced error serialization; internal auth tokens must be visible ASCII without whitespace or commas, request ids are visible ASCII, secret names reject reserved `Object.prototype` keys, runtime-reserved module-name collisions require redeployment under a non-reserved name, DO class names are capped so every shard fits the host-id limit, and `Headers`-form R2 metadata requires a canonical IMF-fixdate `Expires` value.
- Made required bundle metadata, queue base64 bodies, and persisted secret envelopes fail closed under their owning contracts; JavaScript and Rust now share the secret-envelope, queue-key, request-id, internal-auth, worker-version, scheduler-projection, and workflow-limit fixtures.
- Normalized `PLATFORM_DOMAIN` across routing tiers, made the published-image Compose overlay pull-only, and limited Kubernetes user-runtime loader ingress to gateway.
- Removed the unreachable DO inline worker-code test hook and its Terraform switch; do-runtime invoke paths now accept only canonical persisted bundle identities and reject non-canonical or oversized host ids.
- Aligned scheduler and Workflows Compose, Kubernetes, and ECS stop windows with their 25-second application drain so rollout and scale-in do not terminate in-flight work before the configured drain completes.
- Upgraded `@wdl-dev/aws-sigv4` to 3.0.1; WDL's S3-only URL-input paths preserve their existing signatures while adopting stable request snapshots, fail-closed redirects, lowercase region validation, and non-blocking response cleanup.
- Closed workflow restart/version-delete races, made malformed workflow and pattern projections fail closed, and ensured whole-worker delete removes orphan workflow definitions.

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

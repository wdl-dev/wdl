# Changelog

## Unreleased

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
- Upgraded the local/dev stack images: S3Mock to 5.1.0 (with its renamed initial-buckets environment variable) and Envoy pinned to v1.38.3.
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

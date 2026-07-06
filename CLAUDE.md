# CLAUDE.md

Primary project guidance for agents working in this repository.

Detailed, current design contracts live under `docs/`. This file is intentionally a
short invariant checklist plus pointers; do not let it grow back into a second README.

## Start Here

- Architecture overview: `docs/architecture.md`
- Security model: `docs/security.md`
- Workers compatibility: `docs/compatibility.md`
- Redis key layout: `docs/redis-key-layout.md`
- Source ownership map: `docs/source-map.md`
- Module docs: `docs/modules/README.md`
- Protocol contracts: `docs/protocol-contracts.md`
- Testing contract: `docs/testing.md`
- Contributor reading path: `docs/contributing.md`
- Project-wide standards: `docs/project-standards.md`
- Workerd JS standards: `docs/workerd-js-standards.md`
- Rust sidecar standards: `docs/rust-sidecar-standards.md`

When changing behavior, update the owning module doc in the same change. When changing
cross-module Redis ownership, update `docs/redis-key-layout.md`. When changing source
ownership or permanent directories, update `docs/source-map.md`.

## Project Shape

WDL is a self-hosted multi-tenant Workers platform built on stock Cloudflare workerd.
It dynamically loads immutable worker versions from Valkey/Redis through workerd's
`workerLoader` API and implements the missing platform services around it: control,
auth, KV, R2, D1, Durable Objects, queues, cron, Workflows, ASSETS, service/platform
bindings, observability, and lifecycle cleanup.

Core service families:

- `gateway`: public and admin-host ingress.
- `user-runtime` / `system-runtime`: workerd runtime pools with local redis-proxy
  sidecars.
- `d1-runtime` / `do-runtime`: stateful workerd runtimes supervised by Rust PID 1
  processes.
- `scheduler`: cron, queue, and workflow tick dispatch.
- `workflows`: database 2 workflow state machine.
- `control` / `auth`: static workers hosted in system-runtime.

Rust crates live under `rust/`; package and binary names match service names except the
shared crate `wdl-rust-common`.

## Core Invariants

- **No workerd fork.** Treat upstream workerd behavior as the base contract; WDL builds
  around it.
- **Namespace is the account boundary.** Tenant namespace names follow the current
  grammar in `shared/ns-pattern.js`; reserved namespaces are exact literals, not a
  broad `__*` convention.
- **Worker identity is immutable version id.** Runtime loader keys are
  `<ns>:<worker>:<version>`. Do not route hot requests by mutable active state inside
  runtime.
- **Gateway owns route resolution.** Runtime takes `x-worker-id`; it does not keep a
  route table or subscribe to route invalidations.
- **Control is the control-plane writer.** Do not add alternate Redis writers for
  bundle metadata, routes, lifecycle indexes, secrets, or auth state without a module
  doc update and tests.
- **Direct Redis/Valkey writes are repair-only.** They are not a supported consistency
  path; readers should fail closed on malformed persisted state instead of adding
  WATCH/MULTI protocols for arbitrary manual writes.
- **Control/auth authorization is action-based.** `parseControlRoute()` assigns the
  action; auth evaluates that action against `shared/auth-roles.js`. Do not infer
  permission from URL prefix in handlers.
- **Admin/control naming split is intentional.** Client-facing literals use "admin"
  (`X-Admin-Token`, `ADMIN_TOKEN`, admin host). Service-side code uses "control".
- **Secrets are encrypted at rest.** Secret hash values are `WDL-ENC:` envelopes.
  Runtime receives plaintext only in the internal load envelope after redis-proxy
  decrypts during `/runtime/load`. Env materializes in fixed precedence — vars, then
  namespace secrets, then worker secrets — so a worker secret shadows a namespace secret
  shadows a var on the same key. Control must keep the estimated full workerLoader env
  — user vars/secrets plus runtime-injected binding/workflow env values such as
  required caller secret copies — within WDL's headroomed workerd serialized env budget
  before deploy/secret mutation, not let that fail later during cold-load.
- **DB split is intentional.** DB 0 is control metadata, DB 1 is data-plane KV/queue/log
  streams, DB 2 is Workflows. See `docs/redis-key-layout.md`.
- **D1/DO correctness comes from owner lease + generation fence.** Service DNS only
  reaches a router task; it does not prove ownership.
- **Workflows owns DB 2.** Control owns only DB 0 workflow definitions (`wf:defs:*`).
  Runtime/control talk to the workflows service instead of writing DB 2 directly.
- **Queue main streams are not trimmed.** At-least-once delivery beats storage caps.
  Diagnostic streams may be bounded.
- **Metrics labels must be bounded.** Namespace, worker, version, token id, raw key,
  path, and error text belong in logs, not metric labels.

## Security Boundaries

- user-runtime loaded workers get public-only outbound from workerd config.
- system-runtime loaded `__system__` workers are privileged by design.
- Runtime internal `:8088`, D1 `:8787`, DO `:8788`, workflows `:9120`, and Redis are
  private mesh services.
- Private mesh service calls require `x-wdl-internal-auth` with the shared
  `WDL_INTERNAL_AUTH_TOKEN`; health and metrics endpoints are the only unauthenticated
  service endpoints.
- Privileged runtime endpoints belong on `runtime/internal.js` and `:8088`, not on the
  gateway-facing loader socket.
- Hidden backend Fetchers and internal auth tokens for D1/DO/workflows are platform
  plumbing and must be stripped before tenant code observes `env`, request headers, or
  tenant-realm facade state.
- Tenant-running Fargate task roles must stay least-privilege; tenant code must not
  receive broad cloud credentials through task metadata.

## Refactor Discipline

- Pick one deployable boundary per change when possible.
- Keep behavior and docs in the same staged unit.
- Prefer current local patterns over new abstractions. Consolidating a duplicated
  cross-cutting primitive into its single `shared/` or `wdl-rust-common` owner is
  convergence, not a new abstraction.
- Do not add fake old/new protocol fallback for in-tree tiers that ship together unless
  there is a real external rollout requirement.
- If you change a route/body protocol, roll the side that accepts the new shape before
  the side that sends it.
- If you change Redis ownership, update writers, readers, docs, and style-contract
  tests in the same deployable boundary.
- Cross-language contracts (JS↔Rust grammars, wire literals, metric shapes) are
  pinned by `tests/fixtures/*.json` read on both sides plus
  `tests/unit/style-contracts.test.js`; change both sides and the fixture together.
- Use the explicit language baselines by default: JavaScript targets ES2025 on Node 24,
  and Rust targets Edition 2024 on Rust 1.96. Do not reintroduce older spellings when
  the repository standard already uses a modern API or syntax. Cross-language rules
  live in `docs/project-standards.md`; language-specific rules live in
  `docs/workerd-js-standards.md` and `docs/rust-sidecar-standards.md`.
- For docs: active English prose is hard-wrapped; active Chinese prose is not
  hard-wrapped.

## Testing And Checks

Use the smallest check that protects the touched behavior, then widen when the blast
radius crosses module boundaries.

Common commands:

```bash
npm run lint
npm run lint:unused
npm run typecheck
npm run typecheck:strict
npm run compile:workerd
npm run test:unit
npm run test:integration
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets -- -D warnings
cargo test --manifest-path rust/Cargo.toml --workspace
cargo deny --manifest-path rust/Cargo.toml check --config rust/deny.toml
```

Test helper and fixture selection, integration runner behavior, sharding, artifacts,
and flags are documented in `docs/testing.md`. When adding tests, prefer the existing
helper matrix and local harnesses over ad hoc response parsing, temp directories,
output capture, or fixture loading.

## Common Gotchas

- Never use `Number(x) || default` or `parseInt(x, 10) || default` when `0` is a valid
  user value.
- Node's undici-backed `fetch()` strips the `Host` header; integration helpers use the
  plain `http` module when gateway host routing matters.
- `bundleKey(ns, name, version)` stores by integer version:
  `worker:<ns>:<name>:v:1`, not `:v:v1`.
- `ctx.props` is the cross-JSRPC caller-identity channel. Service bindings must read
  caller identity from host-side props, not caller-controlled arguments.
- Host binding `WorkerEntrypoint` prototype methods are exposed over JSRPC; keep
  host-only helpers in module functions or WeakMaps.
- `workerd serve` emits raw console stdout in addition to structured tail events. The
  structured JSON log is the platform source of truth.
- On current workerd, mid-response disconnects do not reliably trip `request.signal`
  or async response-body `ReadableStream.cancel`; do not use either as the only
  cleanup signal. Bound streaming responses with an independent timeout or an
  explicit app-level heartbeat/close path.
- Forwarding WebSocket `101` responses requires preserving `response.webSocket`; use the
  shared response helper instead of wrapping with `new Response(body, init)`.
- workerd/capnp wiring traps apply to every tier's `*.capnp`, not just runtime: `workerd
  serve` takes space-separated args; `external` is HTTP/TLS-only so plain TCP (Redis) goes
  through a `network` service plus `connect()`; `network` HTTPS fetch needs `tlsOptions =
  (trustBrowserCas = true)` plus `ca-certificates`; embedded module names cannot contain
  `..` (hence flat names like `shared-redis`). Details live in `docs/modules/runtime.md`.

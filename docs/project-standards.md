# Project Standards

This document owns cross-language conventions for WDL. It sits above the
JavaScript/workerd and Rust standards: language-specific syntax and module layout stay
in those documents, while this file records rules that must stay consistent across JS,
Rust, tests, docs, and deployment code.

## Language Baselines

The explicit baselines are part of the project contract:

- JavaScript targets ES2025 on Node 24.
- Rust targets Edition 2024 on Rust 1.96.

Do not reintroduce older spellings when the repository already has a modern standard
for the same operation. If a dependency, runtime, or build image blocks the modern
form, keep the local exception narrow and document the reason in the owning module or
review notes.

## Contract Ownership

Every cross-tier contract needs one owner:

- Protocol-domain shape, schema, payload, binding registry, and state-machine testing
  rules live in `docs/protocol-contracts.md`. This file records the cross-language
  policy; the protocol document records how to make those policies explicit per
  surface.
- Redis keys and logical DB ownership belong in `docs/redis-key-layout.md` and in the
  source helper that constructs the key.
- Product API errors own machine code, human message, and HTTP status together.
- Request ids are correlation data, not authority. Do not use client-provided request
  ids as lock values, task ids, or idempotency owners.

When a key shape, wire shape, or error code is produced in one language and consumed in
another, keep a source-scan or behavior test that would catch drift. Prefer one shared
helper per language plus a cross-language contract test over duplicated literals.
Request-id sanitization is pinned by `tests/fixtures/request-id-sanitizer.json`; JS and
Rust tests both read this file.

Schema-like normalizers should be introduced at the protocol owner, not at downstream
call sites. If a new binding, Redis payload, or control API shape needs validation in
multiple places, put the normalizer behind the owner and have other tiers consume the
normalized value.

## Error And Return Contracts

WDL has several protocol domains, but each domain must name its envelope explicitly:

- Client-facing/admin HTTP errors use `{ "error": "<machine-code>", "message":
  "<safe human summary>" }`. Extra fields are additive, and clients branch on `error`
  rather than parsing `message`.
- Details must not override the top-level `error`, `message`, or legacy `reason`
  fields. The reserved-field rule protects the response top level; nested records may
  keep domain field names only when the owning module documents that protocol shape.
- New public APIs should not add a separate top-level `reason`. Auth rejection reasons
  surface as the machine `error`; logs may carry `reason` as diagnostic context.
- Client-facing 5xx messages are safe summaries. Raw backend error text, Redis
  diagnostics, exception messages, SQL text, and storage/provider messages belong in
  structured logs unless the owning module explicitly documents a diagnostic API.
- Internal platform HTTP defaults to the same `{ error, message }` envelope. A module
  may own a different protocol only when it documents the shape and the consumers that
  understand it, such as D1 query payloads, DO protocol errors, or batch result
  envelopes.
- A result envelope is not an error envelope. HTTP 200/207 bodies such as
  `{ outcome: "error" }` or `{ ok: false, ... }` are valid when the transport succeeded
  and the protocol represents job, batch, scheduler, or queue outcome state.
- Empty responses, streaming bodies, WebSocket upgrades, and `HEAD` responses may be
  explicit exceptions. Document the exception at the owning route instead of forcing a
  JSON body through a protocol where it does not belong.

Error-code vocabulary is protocol-owned:

- Platform/admin HTTP codes use `snake_case`.
- D1 query/facade compatibility codes use the D1 vocabulary, including `hyphen-case`
  codes such as `limit-exceeded` and `sql-error`.
- HTTP body parser caps use `request_body_too_large`. Workflow semantic payload and
  fan-in caps use `request_too_large`. D1 statement/result caps map to D1
  `limit-exceeded`.

If a route/body protocol changes, update the owning module doc, the behavior tests, and
any source-scan contract in the same change.

## Security Boundaries

Security boundaries are cross-language contracts, not implementation details:

- Tenant worker code is untrusted even when it enters through a typed wrapper or a Rust
  service endpoint.
- Internal mesh endpoints are private platform protocols. Do not expose runtime
  internal `:8088`, D1, DO, workflows, redis-proxy, Redis, or stateful service sockets
  through public ingress unless a new authentication and authorization design is added.
- Gateway route resolution is not authorization. Control/auth action checks own
  control-plane authorization.
- Hidden platform Fetchers, storage credentials, secret material, and private owner
  network bindings must not become tenant-visible `env` fields.
- Secret plaintext may exist only during validation/encryption or runtime env
  materialization. At rest, secret values are `WDL-ENC:` envelopes.
- A tenant-runtime escape must not imply cloud credential access. Tenant-running tasks
  use least-privilege task roles and must not receive broad infrastructure credentials.

If a change moves data across a trust boundary, update `docs/security.md` and add a
test or style-contract guard that protects the boundary.

## Logging And Observability

Logs, metrics, and request ids are shared platform APIs:

- Product success payloads use camelCase. Logs use snake_case. Redis fields may keep
  their storage grammar.
- Logs may carry bounded tenant identity for debugging, such as namespace, worker,
  version, request id, owner id, or error code. They must not carry plaintext tokens,
  token hashes, secret values, raw platform credentials, or unbounded tenant payloads.
- Platform log lines use one single-line JSON envelope: `ts`, `service`, `level`,
  `event`, then snake_case fields. JS tiers use `shared/observability.js`; Rust
  services use `wdl-rust-common::log::emit_log_line` or a thin wrapper. `ts` must
  use the UTC JavaScript `Date.toISOString()` shape
  (`YYYY-MM-DDTHH:mm:ss.SSSZ`). Only `level=error` goes to stderr; debug/info/warn
  log lines go to stdout.
- Metrics labels must stay bounded and low-cardinality. Bounded machine codes are
  acceptable; namespace, worker, version, token id, raw Redis key, path, raw error text,
  and payload data belong in logs, not labels.
- Metric cardinality warnings are structured `metric_cardinality_warning` log events
  emitted by the metric registry once per metric name at 100 series. The JS metrics
  registry then drops brand-new series for that metric while continuing to update
  existing series; Rust currently keeps the warning-only tripwire. These guards are not
  a substitute for fixing unbounded labels.
- Request ids are sanitized and bounded before propagation. They are correlation data
  only; never treat them as trusted identity or lock ownership.
- `LOG_LEVEL` gates log output only. Metrics must remain available even when logs are
  reduced.
- Probe routes may suppress successful request-complete logs, but they must still keep
  health/metrics behavior and error logs intact.

Metrics label changes, log field renames, and request-id propagation changes are
observability contract changes. Update `docs/modules/log-tail-observability.md` and the
tests or dashboards that depend on the changed shape.

## Shared Primitive Rules

Repeated primitives should converge to the smallest neutral owner:

- JS primitives shared across workerd tiers belong in `shared/` unless isolate
  embedding or trust boundaries require a local copy.
- Rust primitives shared across services belong in `wdl-rust-common` only when they are
  small, semantic, and cross-crate by contract.
- Test stubs that mirror production helpers must import a shared stub or remain
  production-faithful. A data URL fixture is not permission to fork behavior.

Do not create a shared helper only to remove a few local lines. Create it when it
removes duplicated policy, duplicated key grammar, duplicated error mapping, or a drift
pattern that reviewers would otherwise need to remember.

## Fail-Closed Validation

Server-side validation is canonical. CLI and tests may keep cheap fail-fast checks, but
they must not become a second normalizer that accepts or rejects a different language
than the server.

For malformed persisted state, prefer fail-closed behavior over lossy normalization.
For indexes and projections, document the authoritative record and stale cleanup path.
For lifecycle, lease, generation, and run-token fences, keep the revalidation inside
the owner state machine and test the stale claimant path.

## Unsafe And Unchecked Code

Unchecked code needs a reason at the call site:

- Production JS must not use explicit `any` JSDoc or `Function` typedefs. Use
  `unknown` plus local narrowing or a minimal callable/object shape.
- Rust service logic should not use `unsafe`. Platform boundary calls that require it,
  such as process signals, need a `SAFETY:` comment that names the invariant.
- Rust tests may use unsafe environment mutation only behind a process-wide lock and a
  `SAFETY:` comment.

## Review And Validation

Refactors should reduce real complexity: duplicated policy, duplicated contract
grammar, fake compatibility paths, stale stubs, or review burden. A large commit is
acceptable when it is one coherent direction and its tests cover the touched contract.

Use the smallest check that protects the changed behavior, then widen when the change
crosses a module or language boundary. Cross-language changes usually need:

- JS type/unit/style checks for workerd tiers.
- Rust fmt/check/test/clippy for touched crates.
- A source-scan or behavior test for shared Redis, wire, error, or metric contracts.
- Targeted integration when runtime behavior, deployment shape, or a state machine
  changes.

The protocol-specific integration matrix is in `docs/protocol-contracts.md`; use it
when a change affects metadata, Redis payloads, internal envelopes, binding
materialization, or state-machine fences.

# Workerd JavaScript Standards

This document defines the default standards for JavaScript code that runs in or around
workerd tiers. It complements `project-standards.md` and
`rust-sidecar-standards.md`; it does not replace module-specific docs.

## Scope

These standards cover:

- `gateway/`
- `runtime/`
- `control/`
- `auth/`
- `d1-runtime/`
- `do-runtime/`
- `shared/`
- JS unit and integration tests

The repository currently uses JavaScript for these tiers, not TypeScript. If TS is
introduced later, it should keep the same ownership, contract, and test rules.

TypeScript is still used as a JavaScript checker. `tsconfig.json` is the broad `allowJs`
/ `checkJs` baseline for workerd product code, and `tsconfig.strict.json` is its strict
JSDoc gate. `tsconfig.node.json` separately checks Node scripts and tests, following
their real imports into production source where applicable. The downstream CLI split
keeps its own JavaScript standard and compatibility surface.

Workerd and `@cloudflare/workers-types` advance together. Current Workers declarations
intentionally expose broad `Buffer`, `process`, and `global` compatibility globals;
they must not share a TypeScript program with `@types/node`, because declaration merging
can erase or invalidate Node global checks. Product code must not consume those ambient
globals directly. Node-compatible workerd code uses the narrow structural contracts in
`types/workerd-node-compat.d.ts`, while `types/workerd-node-modules.d.ts` declares only
the imported Node module surface that product code consumes. ESLint prevents new bare
`Buffer`, `process`, or `global` references. `types/node-typecheck-contracts.ts` makes
the Node-only checker fail if Workers ambient declarations are accidentally added back.

## Language Baseline

The repository targets Node `>=24` for scripts and tests. Runtime code targets workerd
releases that support the same JavaScript baseline. Keep
`tsconfig.json`, `tsconfig.strict.json`, `tsconfig.node.json`, `eslint.config.js`,
`package.json#engines`, and vendor build targets aligned when the baseline changes.

Use the modern standard library where it reduces local helper code or mutation risk:

- `Object.hasOwn(...)` instead of `Object.prototype.hasOwnProperty.call(...)`.
- `toSorted()` / `toReversed()` for non-mutating array ordering or reversal.
- `RegExp.escape(...)` when interpolating dynamic literal text into regexes.
- `Promise.withResolvers()` only for real deferred-promise state.
- `Map.groupBy(...)`, `Object.groupBy(...)`, and `Set` algebra only when the code is
  naturally grouping or comparing sets.

Do not introduce newer APIs only to look modern. Leave callback-wrapping `new
Promise(...)`, parser stacks, queue mutation, and performance-sensitive local algorithms
in their clearer form. Treat `||` defaulting as a correctness review, not a mechanical
syntax cleanup: replace it with `??` only after confirming that `0`, `false`, or `""`
are valid values that must be preserved.

`npm run typecheck:strict` and `npm run typecheck:node` are contract gates, not just
formatters. Public boundary
typedefs should describe the smallest real shape being accessed. Prefer `unknown` plus
local narrowing over `any`; avoid `@typedef {any}` aliases that only rename an unchecked
value. Functions that always throw should say `@returns {never}` so strict checking can
narrow the caller.

The no-`any` implementation bar applies to production JS under `auth/`, `control/`,
`gateway/`, `runtime/`, `d1-runtime/`, `do-runtime/`, `shared/`, and `system-workers/`
excluding generated or vendored bundles. Tests may still use narrow `any` casts for
dynamic fixtures, globals, and thrown-error probes, but that exception must not migrate
back into implementation code.

Use module-level `TextEncoder` / `TextDecoder` singletons on repeated binary/string
paths. Creating them inline is acceptable in one-off tests, but production decode paths,
Redis payload parsing, and binding adapters should reuse the module singleton unless
stateful decoder options are required.

When a path only needs an exact UTF-8 byte count, use `shared/utf8.js#utf8ByteLength`
instead of open-coding an encoder. The helper keeps short strings on its allocation-free
arithmetic path and delegates long strings to the native encoder, which may allocate a
transient result. Intrinsic-hardened injected modules may keep a local captured
`encodeInto()` scratch path when importing a shared module would weaken their capture
boundary. Node-compatible host modules that already depend on `Buffer` may use
`Buffer.byteLength(value, "utf8")`; it is also exact and avoids allocating the encoded
bytes.

## Ownership

Entrypoints should stay thin. They should dispatch, authenticate or route, wire
observability, and call named helpers. Put pure parsing, key construction,
normalization, and policy decisions in files that can be unit-tested without workerd.

Preferred ownership boundaries:

- route parsing and request-shape normalization
- binding materialization and wrapper generation
- Redis key families and projection staging
- protocol clients and server handlers
- lifecycle state machines and cleanup queues
- observability helpers and bounded metric label policy
- test stubs and hermetic harnesses

Do not split only because a file is long. Split when a reviewer can name the behavior
and verify its contract independently.

Shared helpers should own repeated primitives. Use existing helpers for error message
formatting, random hex/prefixed ids, env knob parsing, base64 byte conversion,
request-id resolution, and platform JSON response shaping. If a helper is mirrored into
a test data URL, keep that stub production-faithful or import the shared test stub
instead of rewriting the behavior locally.

Control handler state must flow through `control/shared.js` accessors. Direct
`state.foo` reads and destructuring `const { foo } = state` belong only in
`control/shared.js` and the dispatcher that initializes it.

## Workerd Boundaries

Public tenant fetch, control/admin routing, and privileged runtime dispatch must stay
separate:

- Gateway public traffic goes to the runtime loader socket.
- Scheduler and workflows dispatch use runtime internal `:8088`.
- Control/auth run through system-runtime and own authorization.
- D1 and DO runtimes expose private internal service APIs only.

Do not protect privileged operations by reserving tenant-visible paths at gateway. Use
the socket/service boundary.

Hidden platform Fetcher bindings must not leak to user code. Runtime wrappers that
inject internal Fetchers must strip them from user-visible `env` and avoid raw `export
*` paths that would expose unwrapped entrypoints.

## Tenant-Realm Context And Provenance

Tenant-executed JavaScript cannot carry trusted host context. The owning security rules
are in [`security.md`](security.md#tenant-realm-provenance-boundaries); implementation
and review must follow these defaults:

- Do not use imported env, `withEnv()`, `AsyncLocalStorage`, `snapshot()` / `bind()`, a
  private Symbol, or another ambient object graph as an authorization, fence,
  deduplication, or invocation-attribution owner. A tenant can replace ambient env,
  restore a captured frame, and use a `Proxy` to observe unknown property keys.
- Use private `WeakMap` / `WeakSet` state only for exact object identity. Do not infer
  invocation identity from that object, recursively trust `cause` / `AggregateError`,
  or classify by message, name, or a tenant-forgeable code alone.
- Treat JSRPC wildcard method resolution as tenant-controlled property lookup. Bind the
  callable before the tenant can access its receiver, and expose a new explicit facade
  whose target does not retain the raw RPC object. Do not rely on a `get`-only Proxy when
  descriptors, prototype properties, or `dup()` can recover or replace the raw method.
- Treat custom host thenables the same way. If correctness depends on the exact rejection
  identity, capture the native settlement method before tenant evaluation and normalize
  the host result without consulting tenant-modifiable `.then`, `constructor`, or
  `Symbol.species` properties.
- If an Error contract depends on provenance, associate only the Error rejected directly
  by the reviewed host call with that call's bound capability. Act only when the same
  object escapes the named Promise boundary and the capability belongs to the current
  environment. Document where catch is allowed. Catching and returning a fallback or
  detaching the Promise prevents the current boundary from reporting but does not erase
  the private association. A replacement, wrapper, `cause`, `AggregateError`, or other
  object does not inherit provenance. Rethrowing the exact Error may report under the
  same capability-membership check, including in a later invocation using the same
  binding identity. Making an Error affect a boundary it does not re-escape requires an
  explicit host-owned protocol instead of more ambient bookkeeping.
- Keep correctness state in the host isolate or authoritative service. A private
  capability may carry only a low-cardinality operation into that owner. Mutable state
  stays host-side; an opaque id may cross only through a hidden prop consumed before
  tenant construction or a generated boundary Error's standard name/message fields,
  must remain scoped to one active operation, and is removed from the host registry on
  every terminal path.
- Do not assume a local `RpcTarget` or a stub returned by one RPC can be serialized into
  Dynamic Worker env/props. Evaluate native compatibility flags before building a WDL
  substitute: platform-owned static workers should adopt a suitable flag proactively
  when it simplifies the system, while tenant Dynamic Worker flags require a narrower
  capability and compatibility review. `allow_irrevocable_stub_storage` is not suitable
  for the current report-only path because it would expose broader persistent-stub
  behavior to tenant workers. Prefer a `ctx.exports` loopback/service stub with immutable
  props for this boundary.

Before accepting a new tenant-to-host context design, identify which workerd-owned
behaviors can reach that design and prove those paths in real workerd. Candidate cases
include nested `withEnv()` with plain or Proxy env values, captured async-frame restore,
module-cached facades across entrypoint instances, reverse-JSRPC callbacks, and
hidden-prop consumption before tenant construction. Do not retain an old test matrix for
mechanisms the final design no longer reads. Unit mocks alone cannot establish a claimed
serialization or async-context property. Host-local owner registries still require
concurrent-isolation and terminal cleanup tests, but those may remain unit tests when the
capability transport itself has a real-workerd gate and no stale capability crosses a
product boundary.

## API Contracts

Platform JSON errors use:

```json
{ "error": "machine_code", "message": "human readable" }
```

Control, gateway, runtime, and ordinary D1/DO route errors should use the shared JSON
response helpers unless the module owns and documents a different protocol envelope.
Details are additive and must not override top-level `error`, `message`, or legacy
`reason`. New APIs should not reintroduce `reason`; keep `error` and `message` as the
client-facing contract. If a route legitimately returns a result envelope, streaming
body, `HEAD` response, or WebSocket upgrade instead of JSON, document that exception in
the owning module.

Do not hand-write literal `{ error, message }` response bodies in handlers when a
shared helper can own the reserved-field and content-type rules. Protocol-specific
helpers such as D1 or DO error mappers must carry their own tests and module docs.

Product success payloads use camelCase. Logs use snake_case. Redis fields may use their
own storage grammar, but new public API fields should not inherit Redis/log naming.

Request ids must be sanitized and bounded before propagation. Never use raw error
strings, token ids, namespace/worker/version, paths, or Redis keys as metric labels.

## Redis And State

Use shared key helpers when one exists. When a new key family crosses modules, add a
style-contract or source-scan guard that checks producer and consumer literals together.

Before adding a Redis index, state whether it is authoritative or repairable. If it is
repairable, document the authoritative record and stale cleanup path.

WATCH/MULTI behavior belongs to one owner. Do not split preflight reads from commit-time
revalidation without tests that prove the watched key set.

Workerd I/O objects are tied to their `IoContext`. Keep the shared `RedisClient`
socket-per-call model, and batch related commands inside one typed operation instead of
retaining a socket or request-created promise across invocations. Use `RedisSession`
only when one invocation or one long-lived owning task intentionally holds the
connection for a WATCH/transaction or subscription lifecycle. `RedisClient`'s
`commandTimeoutMs` applies only to its socket-per-call operations; `session()` rejects a
client configured with that option rather than silently dropping the deadline.

A process-local registry may retain a resolver and settle its request-created Promise
across invocations only under a strict signaling boundary. The non-owning invocation
may settle the resolver with an I/O-free value, but must not call request-owned handlers
or touch request-owned I/O. The Promise and every continuation that touches I/O must be
created by the owning request; workerd's cross-request settlement must resume those
reactions in that request's `IoContext`. The owning module must document and test this
boundary.

Transport-independent command construction and reply decoding belong to the shared
typed command surface. `RedisClient` and `RedisSession` own their distinct connection
lifetimes and expose additional operations only where those lifetimes require them.

Do not add a generic pipeline escape hatch to application code. Add the smallest typed,
bounded helper that can validate reply count, reply order, and domain decoding at the
Redis owner.

Shared RESP pipelines and transactions write through the common bounded command writer.
It preserves complete command framing and reply order while grouping ordinary commands
into bounded buffers; one command larger than the target occupies its own write group.

## Tests

Tests should protect real contracts:

- route grammar
- error shape
- binding exposure
- Redis key layout
- lifecycle blockers
- hidden Fetcher stripping
- internal socket ownership
- deployment/IaC drift where runtime testing is too expensive

Use style-contract tests for known drift patterns and keep the regex narrow enough to
fail loudly. Add a short comment when a source scan is intentionally strict or when a
behavioral test would require heavier infrastructure.

Keep test stubs shared or imported when they mirror production helpers. Do not copy
production behavior into many independent stubs.

When a source-scan guard needs an exception, make the exception as narrow as the
contract requires: prefer `file:literal` or `file:function` allowances over whole-file
allow-lists.

## Validation

Baseline checks for JS/workerd changes:

```bash
npm test
npm run typecheck:strict
node --test tests/unit/style-contracts.test.js
```

Run targeted integration when executable behavior changes:

- gateway routing, admin-host, WebSocket behavior: gateway integration
- runtime loader, wrappers, bindings, or internal socket: affected runtime and binding
  integration
- control/auth route or ACL changes: control/auth and CLI integration
- D1/DO facade or owner protocol changes: D1/DO integration
- queue/cron dispatch shape changes: scheduler/runtime integration
- workflows facade or dispatch protocol changes: workflows integration

Docs-only changes can use `git diff --check`, link/path checks, and style-contract tests
when doc navigation or source-scan rules change.

## Refactor Discipline

JS refactors follow the same staged-review discipline as Rust service and sidecar
changes:

- define one deployable boundary
- keep unrelated cleanup out of the active boundary
- stage the complete candidate for review
- keep feedback fixes unstaged until reviewed
- commit only after checks covering the touched contract pass

If a structural change also changes behavior, say so explicitly and run the integration
that covers that behavior.

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
/ `checkJs` baseline for the whole JS tree. `tsconfig.strict.json` is the stricter JSDoc
gate for workerd and server-side tiers (`auth`, `control`, `gateway`, `runtime`,
`d1-runtime`, `do-runtime`, `shared`, selected scripts, tests, and system worker code).
The downstream CLI split keeps its own JavaScript standard and compatibility surface.

## Language Baseline

The repository targets Node `>=24` for scripts and tests. Runtime code targets workerd
releases that support the same JavaScript baseline. Keep
`tsconfig.json`, `tsconfig.strict.json`, `eslint.config.js`, `package.json#engines`, and
vendor build targets aligned when the baseline changes.

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

`npm run typecheck:strict` is a contract gate, not just a formatter. Public boundary
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
connection for a WATCH/transaction or subscription lifecycle.

Do not add a generic pipeline escape hatch to application code. Add the smallest typed,
bounded helper that can validate reply count, reply order, and domain decoding at the
Redis owner.

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

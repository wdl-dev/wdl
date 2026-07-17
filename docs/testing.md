# Testing

This document records the current testing contract for WDL. The source code and test
suite remain authoritative; this page explains which test layers exist, which artifacts
they exercise, and which runner flags are supported.

## Test Layers

| Layer | Command | Scope |
| --- | --- | --- |
| Lint | `npm run lint` | ESLint flat-config checks for JavaScript source and tests. |
| Typecheck | `npm run typecheck` | `tsc --noEmit` with `allowJs` / `checkJs` over the broad JavaScript surface. |
| Strict typecheck | `npm run typecheck:strict` | Stricter JSDoc gate for workerd and server-side JavaScript tiers that have been brought under strict coverage. |
| Unit tests | `npm run test:unit` | Pure Node tests for helpers, protocol contracts, style contracts, and local harnesses. No Docker stack is required. |
| Integration tests | `npm run test:integration` | Docker Compose end-to-end tests against compiled workerd configs and prebuilt local images. |
| CLI integration subset | `npm run test:integration:cli` | Tests marked with `// @wdl-cli-integration`, run through the same pool runner on a separate port range. |
| Full local gate | `npm run test:all` | Fast checks plus integration tests. |

`npm run test` is the fast pre-push gate: lint, typecheck, strict typecheck, and unit
tests. It intentionally does not run Docker integration tests.

Strict typecheck covers the runtime/control JavaScript tiers, the JavaScript-like
test sources under `tests/` (`.js`, `.cjs`, and `.mjs`), and maintenance scripts
under `scripts/` (`.js` and `.mjs`). Non-JS fixtures such as JSON payloads,
Markdown, and Cap'n Proto fixture files stay outside TypeScript.

## Artifact Model

All workerd paths boot from compiled `dist/workerd-configs/*.bin` artifacts:

- Local Docker Compose and integration tests use `*-local.bin`, which routes internal
  service hops through the local Envoy mesh.
- Production-style configs use the unsuffixed `.bin` files for Service Connect style
  routing.
- Source bind mounts are not part of the runtime contract. Workerd-side JavaScript and
  Cap'n Proto changes must be compiled into `.bin` artifacts before the stack observes
  them.

The integration runner prepares those artifacts once before starting shards:

1. `node scripts/compile-workerd-configs.js --local`
2. `docker compose build gateway workflows`

Pool-managed shards then start compose services with `--no-build` so concurrent shards
do not race on Docker builds. Direct single-file integration runs use the same prepare
step before the first stack startup.

## PR Integration Gate

The Docker Compose integration job needs Docker Hub and Build Cloud credentials, so
CI does not run it on pull requests. It runs only on trusted push events.
Maintainers should run targeted or full integration locally before treating
protocol, runtime, Redis shape, or state-machine changes as merge-ready.

Targeted local integration is still useful before pushing a PR:

| Change area | Recommended targeted integration before full gate |
| --- | --- |
| Gateway/admin routing or auth | `tests/integration/gateway.test.js`, `tests/integration/auth-worker.test.js`, or the integration file that tests the touched route |
| Deploy, promote, delete, lifecycle, or S3 cleanup | deploy/delete/control lifecycle integration files that cover the changed path |
| Runtime binding metadata or facade behavior | the binding's focused integration file plus runtime/load unit tests |
| D1 or DO owner/state behavior | the focused D1/DO integration file, including multi-runtime profile tests when ownership changes |
| Scheduler, queues, cron, or workflows | the focused queue/cron/workflows integration file plus the touched Rust crate tests |
| Redis key or payload shape | every integration file that exercises a writer/reader pair for that key family |

`docs/protocol-contracts.md` owns the broader protocol-review matrix. This testing
document owns the commands and CI runner behavior.

## Integration Runner

`scripts/run-integration-tests.js` is the default integration entrypoint. It builds a
FIFO queue of selected files and runs them across independent slots.

Each slot gets its own:

- Docker Compose project: `wdl-it-<slot>`
- Gateway host port: `18080 + slot`
- s3mock host port: `29500 + slot`
- Valkey container and data volume

Passing slots are torn down immediately. Failed slots stay up for debugging unless
cleanup-on-failure is explicitly enabled.

The first file in a slot pays the dependency-ordered stack startup/restart cost. Later
files in the same slot use the `WDL_INTEGRATION_SLOT_PREPPED=1` fast path; the caller
promises that the slot has already picked up the compiled configs and prebuilt images.

## Running Integration Tests

Full integration:

```bash
npm run test:integration
```

Single integration file:

```bash
node --test tests/integration/<file>.test.js
```

The direct single-file path prepares local artifacts automatically unless
`WDL_INTEGRATION_SKIP_PREPARE=1` is set.

CLI subset:

```bash
npm run test:integration:cli
```

The CLI subset is included by `npm run test:integration`; run it separately only when
focusing on CLI paths.

CLI integration tests use the `wdl` executable on `PATH` by default. Local runs
should install the published CLI version pinned by the top-level
`WDL_CLI_PACKAGE` value in `.github/workflows/ci.yml`:

```bash
npm install -g @wdl-dev/cli@1.4.0
```

For unpublished CLI changes, link or wrap the checkout so `wdl` is on `PATH`.
`WDL_CLI_BIN` remains available only for focused runs that need an explicit
executable override.

If the selected CLI binary is missing, the integration runner fails during preflight
with a `WDL_CLI_BIN` hint instead of skipping CLI coverage.

## Supported Flags

### User-Facing

| Flag | Default | Meaning |
| --- | --- | --- |
| `WDL_INTEGRATION_SHARDS` | `4` | Parallel slots for the full integration runner. Use `1` when debugging one stack. |
| `WDL_INTEGRATION_CLI_SHARDS` | `2` | Parallel slots for the CLI integration subset. |
| `WDL_KEEP_INTEGRATION_STACK=1` | unset | Keep every slot stack after the run. |
| `WDL_TEARDOWN_INTEGRATION_STACK_ON_FAILURE=1` | unset | Tear down failed slot stacks instead of leaving them for debugging. |

### Advanced

| Flag | Default | Meaning |
| --- | --- | --- |
| `WDL_INTEGRATION_DURATIONS_FILE` | `.integration-test-durations.json` | Historical duration input used to order files for better shard balance. |
| `WDL_INTEGRATION_SKIP_PREPARE=1` | unset | Skip compile/build preflight. Use only when compiled configs and images are already current. |
| `WDL_CLI_BIN` | `wdl` on `PATH` | Optional executable CLI override used by focused integration runs that need to bypass `PATH` resolution. |

### Runner-Internal

These are implementation details between the pool runner and integration helpers. Tests
may read them, but they should not be regular user knobs.

| Flag | Meaning |
| --- | --- |
| `WDL_INTEGRATION_NO_BUILD=1` | Instructs compose helper paths to add `--no-build` after the shared preflight build. |
| `WDL_INTEGRATION_SLOT_PREPPED=1` | Marks that a slot already paid the full startup/restart preparation. |
| `WDL_GATEWAY_HOST_PORT` | Per-slot gateway host port injected by the runner. |
| `WDL_S3MOCK_HOST_PORT` | Per-slot s3mock host port injected by the runner. |
| `WDL_WORKERD_CONFIG_VARIANT=local` | Selects the local compiled workerd config variant for compose. |

## Helpers And Fixtures

Test code is split into three trees with distinct ownership:

### Helper Selection Quick Reference

Prefer the narrow helper that matches the response or fixture source:

| Test need | Use | Notes |
| --- | --- | --- |
| Unit test reading a real or fake JSON `Response` | `readJsonResponse(...)` / `assertJsonResponse(...)` from `tests/helpers/response-json.js` | Checks status and gives labelled JSON parse/status diagnostics. |
| Integration HTTP JSON status + body | `readIntegrationJson(...)` / `assertIntegrationJson(...)` from `tests/integration/helpers/http-response.js` | Use for Fetch `Response` objects or collected `{ status, body }` responses when a test needs both status and JSON body diagnostics. |
| Integration HTTP helper response JSON | `responseJson(...)` / `responseJsonOrNull(...)` from `tests/integration/helpers/http-response.js` | For already-collected `{ status, body }` responses after status has been asserted separately, and wrappers that install `.json()` / `.jsonOrNull()`. |
| Integration HTTP status assertion | `assertStatus(...)`, `assertStatusIn(...)`, or `assertNotStatus(...)` from `tests/integration/helpers/assertions.js` | Use when the response carries structured diagnostics or when a status failure should print a stable body. |
| Integration Redis state | Typed helpers from `tests/integration/helpers/redis.js` | Use `redisHGet(...)`, `redisXAdd(...)`, `redisPublish(...)`, `redisFlushAll(...)`, and the `db` option instead of direct `redis-cli` strings. |
| Integration stack lifecycle | `setupIntegrationSuite(...)` from `tests/integration/helpers/stack.js` | Use `afterStackUp`, `beforeEachReset`, or `reset: false` for file-specific setup instead of hand-written `before(ensureStackUp)` / `beforeEach(resetStack)`. |
| Integration queue scenarios | Worker sources and helpers from `tests/integration/helpers/queue-scenarios.js` | Use for queue producer/consumer source, queue-specific stack setup, and repeated send/read helpers while keeping protocol-specific assertions in the test file. |
| Integration workflow scenarios | Worker source and DB2 helpers from `tests/integration/helpers/workflows-scenarios.js` | Use for workflow demo source, workflow state keys, ready-shard helpers, and direct runtime replay helpers while keeping workflow assertions in the test file. |
| Integration fetch worker source wrapper | `workerFetchCallerSource(...)` from `tests/integration/helpers/worker-source.js` | Use when several tests need the same `export default { async fetch(req, env) { try { ... } } }` caller shell while keeping the business body inline and readable. |
| Unit control handler module graph | `createControlHandlerState(...)` / `importControlHandler(...)` from `tests/helpers/control-handler-harness.js` | Use for `control/handlers/*` tests that need `control-shared` state, logs, env, metrics, Redis, or backend service stubs. |
| Unit Control shared module graph | `compileControlSharedGraph(...)` / `compileControlSharedDependencies(...)` from `tests/helpers/load-control-shared.js` | Use for tests of `control/shared.js` and synthetic shared stubs instead of rebuilding its production-backed dependency graph. |
| Unit runtime R2 binding module graph | `makeR2Bucket(...)` and fetch installers from `tests/helpers/load-runtime-r2-binding.js` | Use for `runtime/bindings/r2.js` host-surface tests instead of rebuilding the R2 module replacement graph in each file. |
| Unit D1/DO owner-client module graph | `loadD1OwnerClient(...)` / `loadDoOwnerClient(...)` from `tests/helpers/load-d1-owner-client.js` and `tests/helpers/load-do-owner-client.js` | Use for owner forwarding client tests instead of rebuilding the state, protocol, internal-auth, and owner-forwarder replacement graph in each file. |
| Unit auth entrypoint harness state | `authMockState(...)`, `authLogs(...)`, and `lastAuthLog(...)` from `tests/helpers/load-auth-index.js` | Tests should not read or write `globalThis.__authMockState` directly; the global is private storage for the harness' inline module mocks. |
| Unit/integration Redis command parity | `redisConformanceCases` from `tests/helpers/redis-conformance-cases.js` | Add a shared case when fake Redis and the real integration Redis wrapper must agree on command semantics. Run `tests/unit/fake-redis.test.js` and the focused Redis conformance integration file. |
| Mocked `fetch` call recording | `makeRecordingFetch(...)` / `withRecordingFetch(...)` from `tests/helpers/mock-fetch.js` | Use `capture` when the test needs a custom call record shape. |
| Temporary global or property replacement | `withMockedGlobal(...)`, `withMockedProperty(...)`, and `withMockedPropertyDescriptor(...)` from `tests/helpers/mock-global.js` | Use install-style helpers only when the file owns before/after cleanup. |
| Console or stream output capture | `withCapturedConsole(...)`, `installConsoleMethodCapture(...)`, or `installStreamWriteCapture(...)` from `tests/helpers/output-capture.js` | Keep direct `console.*` and `process.stderr/stdout.write` replacement out of test files. |
| Simple sleeps or polling | `delay(...)` / `waitUntil(...)` from `tests/helpers/timing.js` or the integration `stack.js` re-export | Do not replace sleeps inside tenant worker source strings; those are fixture code under test. |
| Temporary directories | `withTempDir(...)` from `tests/helpers/temp-dir.js` | Prefer scoped cleanup over hand-written `mkdtemp` / `rm` `finally` blocks. |
| Repository JSON fixture files | `readRepositoryJson(...)` from `tests/helpers/load-shared-module.js` | Keeps fixture reads labelled and repository-relative. |

- Cross-language JSON fixtures that are consumed by both JavaScript and Rust
  tests live under `tests/fixtures/`. JavaScript tests read them with
  `readRepositoryJson(...)`; Rust tests read the same files with
  `include_str!(...)`. These fixtures pin test contracts and drift guards only;
  they do not create runtime shared owners.
- `tests/helpers/` is the unit-test helper home. It holds the `load-*.js` ESM
  data-URL loader family (one loader per repo module under test, e.g.
  `load-auth-lib.js`, `load-control-lib.js`, `load-runtime-dispatch.js`),
  shared fixtures (`runtime-dispatch-fixtures.js`, `control-shared-stub.js`),
  static-analysis utilities (`source-scan.js`), and `mocks/` for mocked
  Cloudflare runtime surfaces. `load-shared-module.js` owns repository module
  source loading, data-URL construction, and import-specifier rewrite helpers;
  load import-free contract owners such as `shared/worker-contract.js` and
  `shared/ns-pattern.js` directly with `repositoryFileUrl(...)` instead of
  reimplementing their grammar in a stub.
  Use it instead of ad hoc `readFileSync(...).replace(...)` chains when a test
  rewrites repo module imports. Use `readRepositoryFile(...)` plus
  `applyModuleReplacements(...)` for local source rewrites,
  `readRepositoryModuleSource(...)` when reading repository module source and
  applying replacements in one step,
  `repositoryModuleDataUrl(...)` for reusable repo modules, and
  `importSpecifierReplacements(...)` for import-map shaped stubs. This is a
  tested convention: `tests/unit/test-helper-style-contracts.test.js` rejects new
  source-producer `.replace(...)` module rewrite chains outside the shared
  loader helper, including direct chains, variable-indirect forms, and second
  rewrites of `applyModuleReplacements(...)` output.
  `control-handler-harness.js` owns the common `control-shared` harness for
  `control/handlers/*` unit tests. Use it for state/env/log/metrics/backend
  injection instead of declaring another file-local `control-shared` data-URL
  stub when the handler under test follows the shared control entrypoint shape.
  `load-control-shared.js` owns the production-backed dependency graph used by
  both the real `control/shared.js` test and that synthetic harness stub. Keep
  only state-bound wiring and test instrumentation local to the stub.
  `load-runtime-r2-binding.js` owns the `runtime/bindings/r2.js` host binding
  module graph, including SigV4Client/fetch recording hooks. Extend that loader
  for additional R2 host tests instead of copying the replacement graph back
  into a test file. `load-auth-index.js` owns auth entrypoint mock state
  accessors; tests use `authMockState(...)`, `authLogs(...)`, and
  `lastAuthLog(...)` rather than touching `globalThis.__authMockState`
  directly.
  `mocks/fake-redis.js` owns the reusable in-memory Redis session/multi subset
  for unit tests, including command tracing for reads, writes, batch helpers,
  and multi ops; extend it when multiple tests need the same Redis command
  shape instead of adding another file-local mock. Use
  `sharedRedisStubUrl(...)` for data-URL `shared-redis` replacements so tests
  share the fake `WatchError` constructor and production `decodeBulk`
  semantics; append only graph-specific state or client exports.
  `request-body.js` owns typed JSON request-body parsing for mock backends;
  use it when tests capture `RequestInit.body` instead of open-coding
  `JSON.parse(...)`. `do-envelope.js` owns test decoding for Durable Object
  binary invoke envelopes; use it instead of local `decodeDoEnvelope()`
  copies. The style-contract suite rejects new direct mock request-body
  `JSON.parse(...)` sites and file-local DO envelope decoder copies.
  `mock-global.js` owns temporary global and global-property replacement; use
  `withMockedGlobal(...)` / `withMockedProperty(...)` for one lexical async
  scope and install-style helpers only when a file needs before/after hook
  ownership. `mock-fetch.js` is the typed fetch wrapper for that helper. The
  style-contract suite scans authored `.js` tests and rejects new direct
  non-`__` `globalThis.<name> = ...` assignments and direct
  `console.log/warn/error/info = ...` assignments outside those helpers. It
  also rejects direct assignments to common built-in/global property hooks used
  by tests (`process.stderr.*`, `AbortSignal.*`, `Object.*`, `JSON.*`,
  `Headers.prototype.*`, `Array.prototype.*`, and `Function.prototype.*`).
  Built-in prototype descriptor mocks use
  `withMockedPropertyDescriptor(...)` instead of direct
  `Object.defineProperty(...)` save/restore blocks.
  Unit tests do not boot Docker; they import through these loaders.
- `tests/integration/helpers/` is the integration-test helper home. Each
  concern lives in its own sub-module — `admin-http.js`, `cli.js`,
  `compose.js`, `env.js`, `gateway-http.js`, `internal-http.js`,
  `http-response.js`, `websocket.js`, `stack.js`, `runtimes.js`, `redis.js`,
  `prometheus.js`, `misc.js`, `worker-source.js`, plus `d1-runtime.js` and
  `durable-objects.js` for tier-specific fixtures. `index.js` is a barrel
  re-export for consumer ergonomics; helpers
  themselves import from the concrete sub-modules they need, not from the
  barrel. The barrel deliberately carries only the general helpers; the
  tier-specific fixture modules (`d1-runtime.js`, `durable-objects.js`) stay
  deep-import because they run import-time top-level `await` to compile their
  protocol graphs, and only D1/DO tests should pay that cost. The focused
  `redis.js` and `prometheus.js` concerns are deep-import for the same reason —
  a test pulls them in only when it asserts against Redis or metrics.
  `redis.js` is the shared Redis CLI wrapper for integration tests; pass its
  `db` option for DB 1 / DB 2 assertions instead of adding tier-local
  `redis-cli -n ...` wrappers. It owns common commands such as
  `redisFlushAll(...)`, `redisPublish(...)`, `redisXAdd(...)`,
  `redisHSet(...)`, and `redisSetEx(...)`; direct `redis-cli` and
  `composeExec("redis", ...)` calls belong only inside that helper. `stack.js`
  owns standard Docker-backed lifecycle setup: use `setupIntegrationSuite()` at
  module scope, with `afterStackUp`, `beforeEachReset`, or `reset: false` when
  a file needs one-time setup, extra per-test cleanup, or no default reset.
  `http-response.js` owns cached integration response JSON accessors:
  `readIntegrationJson(...)` and
  `assertIntegrationJson(...)` combine status assertion with labelled JSON
  parsing for either Fetch `Response` objects or collected `{ status, body }`
  responses. Use `responseJson(...)` when status was already asserted and a
  JSON body is required, and `responseJsonOrNull(...)` when empty body is an
  explicit `null`. Integration tests should not `await response.json()`
  directly; use the shared helpers so failures include the status/body context.
  HTTP helpers attach `.json()` / `.jsonOrNull()` where appropriate.
  Integration tests should not open-code JSON parsing for HTTP
  response body/text values or local wrapper `.json()` accessors; use
  `readIntegrationJson(...)`, `assertIntegrationJson(...)`,
  `responseJson(...)`, `responseJsonOrNull(...)`, or
  `withResponseJsonAccessors(...)`. The style-contract suite rejects new
  direct response JSON parses.
  `json-payload.js` owns labelled parsing for other structured integration
  payloads, including command/stdout JSON and base64 JSON bodies.
  `websocket.js` owns WebSocket text-frame JSON helpers (`frameJson(...)` and
  `readJsonServerFrame(...)`), while Redis-stored schema JSON should use the
  typed Redis helpers (`redisHGetJson(...)`, `redisHashJsonField(...)`,
  `redisJsonMember(s)(...)`) or domain helpers such as `readMeta(...)`. Keep
  domain-specific stream event parsing local when the event shape is not a
  shared protocol, as in log-tail SSE tests. The style-contract suite rejects
  new direct parses for these structured payload sources too. `worker-source.js`
  owns small integration worker source shells such as
  `workerFetchCallerSource(...)`; it removes repeated wrapper boilerplate
  while keeping the tested fetch body inline in the test file. Standard
  Docker-backed tests should call
  `setupIntegrationSuite()` once at module scope instead of repeating
  `before(ensureStackUp)` plus `beforeEach(resetStack)`. Keep explicit hooks
  only when the file adds one-time setup, skips per-test reset, or restarts
  services.
- `test-workers/` contains fixture workers that integration tests deploy or
  read directly. Two layouts coexist on purpose: a full Wrangler workspace
  (`package.json + wrangler.toml + src/`) for tests that go through the
  CLI deploy path, and `src/`-only for fixtures the test inlines via
  `readFileSync(new URL("../../test-workers/<name>/src/index.js", ...))`.
  Pick by usage; see `test-workers/README.md`.
- `examples/` contains manual demos and reference projects.
  Integration tests should not silently depend on them unless the fixture
  has intentionally moved under `test-workers/`.
- `tests/integration/manual/` holds `*.manual.mjs` reproduction scripts that
  are intentionally not discovered by the runner.
- CLI integration tests must include a top-level `// @wdl-cli-integration`
  marker so the CLI subset runner can discover them.
- Rust crate-local test helpers live inside crate `#[cfg(test)]` modules (for
  example `rust/redis-proxy/src/lib.rs::test_support`). Use those for repeated
  parser/protocol assertions within one crate instead of duplicating helpers in
  sibling Rust modules.

### What stays inline

The helper directories are deliberately not catch-alls. Keep code inline in
the test file when:

- it is a single-caller utility specific to that file's domain;
- it is a Redis write whose payload is a literal shell-escaped string;
- it is a one-off Redis mock command that is not shared by another test file;
- it is a stream-protocol command on DB 1 / DB 2 (e.g. `XADD`, `XPENDING`,
  `XREADGROUP`) that the typed Redis helper does not expose yet;
- two callers exist but their implementations diverge semantically (e.g.
  different headers or different request shape).

Promote to a helper once a non-trivial helper is duplicated byte-for-byte
across two or more files, or once a fixture's inline source exceeds about
35 lines and is loaded purely as a string.

### Tripwires

`tests/helpers/style-contract-scanner.js` owns the shared source-scanning and
literal-extraction helpers used by style-contract tests. The production-facing
tripwires stay in `tests/unit/style-contracts.test.js`; they assert on
cross-tier and source-contract strings such as workerd config ownership,
service anchors, `composeNoBuildFlag`, grammar mirrors, Redis key conventions,
and active-doc parity. Test-helper tripwires live in
`tests/unit/test-helper-style-contracts.test.js`; that file walks both helper trees
recursively and guards test-helper conventions such as
`load-shared-module.js` data-URL construction, module rewrites, response JSON
helpers, Redis CLI wrappers, and the module-loader convention above. Repository module source
rewrites should remain centralized in `load-shared-module.js` instead of
drifting back into file-local source-reader or source-producer `.replace(...)`
chains. The shared source scanners skip generated dependency and worker build
directories (`node_modules`, `.deploy-dist`, `.wrangler`) so these tripwires
cover authored repository sources rather than local install churn. Renaming or
moving a helper without updating the tripwire trips the unit suite.

## Continuous Integration

WDL uses separate validation and release workflows:

- GitHub Actions is the validation gate for pull requests and `main`: JavaScript,
  Rust, and hygiene checks run on PRs and pushes.
- Docker Compose integration runs only on trusted push events because it needs
  Docker Hub and Build Cloud credentials.
- GitHub release workflows build and publish release images from `wdl.*` tag
  pushes; release tags must match `VERSION` and have matching `CHANGELOG.md`
  notes, and manual runs can validate or publish the same build path.

`.github/workflows/ci.yml` runs the integration suite only on trusted push events.
The `integration` job `needs` the `node`, `rust`, `rust-supply-chain`, and
`ci-hygiene` jobs, so a failing lint, typecheck, unit, Rust, dependency, or
hygiene check never spends Build Cloud minutes or boots a stack. The npm audit
gate runs against this repository's locked dependency tree; the published CLI is
installed globally only in the integration job and does not enter this package
lock.

The job runs fully serial (`WDL_INTEGRATION_SHARDS=1`): one Docker Compose stack
at a time, so a roughly twelve-container stack never doubles up and exhausts a
two-core hosted runner. Wall time is therefore close to the sum of all file
durations rather than the sharded pool's wall time.

Image builds are offloaded to Docker Build Cloud. `docker/setup-buildx-action`
creates a `driver: cloud` builder, and the workflow points
`docker compose build gateway workflows` at it, so `wdl-workerd:dev` and
`wdl-rust:dev` build remotely with a shared cargo/layer cache and load into the
runner; the hosted runner never compiles Rust locally. Docker Hub credentials live in a
temporary `DOCKER_CONFIG` and are removed after the image prep step. The integration
runner then sets `WDL_INTEGRATION_SKIP_PREPARE=1`, so CLI tests run after the Docker
credentials have been dropped.

CI installs the `@wdl-dev/cli` package pinned by the top-level `WDL_CLI_PACKAGE`
value in `.github/workflows/ci.yml`, with npm lifecycle scripts disabled before
running integration. CLI integration tests exec the resulting `wdl` command by
default. Local unpublished CLI validation should also make `wdl` resolve on `PATH`;
`WDL_CLI_BIN` is only a focused executable override. Slow-first ordering is seeded from
the previous run's `.integration-test-durations.json` via `actions/cache`; cache
misses fall back to the runner's built-in slow-first list. The freshly produced file
is both re-cached and uploaded as an artifact.

CI configuration required in the repository settings:

| Kind | Name | Purpose |
| --- | --- | --- |
| variable | `DOCKER_USER` | Docker Hub username for the Build Cloud login. |
| secret | `DOCKER_PAT` | Docker Hub access token for the Build Cloud login. |
| environment | `release` | Tag-restricted deployment record for Docker image publication. |

The Build Cloud builder endpoint (`getwdl/builder`) is set inline in the
workflow. The release environment limits deployments to `wdl.*` tags.

## Operational Notes

- Node's `fetch()` strips the `Host` header. Integration helpers use the plain `http`
  module when they need gateway subdomain routing, such as `Host: admin.test`.
- D1 and DO multi-runtime tests use compose profiles (`d1-multi`, `do-multi`) and must
  restore the single-runtime baseline before returning.
- D1 test-hook endpoints are default-off and must only be enabled for disposable
  integration runs.
- If a run is interrupted, clean the affected compose projects with `docker compose down
  -v` under the corresponding `COMPOSE_PROJECT_NAME`.

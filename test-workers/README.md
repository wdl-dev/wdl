# Test Worker Fixtures

`test-workers/` contains worker fixtures owned by `tests/integration/`. Tests may
rely on exact file names, manifest shapes, binding names, and response payloads in
this tree, so keep fixture changes in the same commit as the tests that consume
them. Three fixture structures coexist on purpose.

## Full Workspace (`package.json` + `wrangler.toml`/`wrangler.jsonc` + `src/`)

Use a full workspace when a test deploys through the CLI:

```js
runWdlCli(["deploy", "test-workers/<name>", "--ns", ns]);
```

The CLI reads the manifest and package metadata like any external project, so this
shape exercises the full deploy path. Pick it when the test point is the
deploy/lifecycle flow itself, or when you want the fixture to remain runnable with
Wrangler while iterating.

A full-workspace fixture may also be read as source when one test needs CLI
coverage and another needs direct source input; keep that dual use explicit in the
owning tests.

## Source-Only (`src/index.js` only)

Use a source-only fixture when an integration helper reads the worker source as a
string and inlines it into a programmatic deploy payload:

```js
const SOURCE = readFileSync(
  new URL("../../../test-workers/<name>/src/index.js", import.meta.url),
  "utf8"
);
```

Pick this when the test does not go through the CLI, usually because the helper
builds the deploy body directly to keep the fixture in-tree but out of the test
file. No Wrangler config is needed; do not add one.

## Embedded Workerd Service (`config.capnp` + `src/`)

Use an embedded service when a local workerd configuration needs a hermetic
protocol peer that is not tenant code. The local configuration imports the
fixture's `config.capnp`; production configurations continue to use their real
network or service binding. `ai-provider` uses this shape for the AI provider
protocol exercised by local integration tests.

## Adding Or Moving Fixtures

- Going through `runWdlCli(["deploy", ...])` means full workspace.
- Loaded inline by a helper via `readFileSync` means source-only.
- Imported by a local workerd configuration as a protocol peer means embedded
  service.
- Do not mix layouts within one fixture without an owning test reason.
- Do not use `examples/` as hidden test dependencies. If a demo becomes a
  test contract, move or copy the minimal fixture into `test-workers/`.
- Local `node_modules/`, `.wrangler/`, and `.deploy-dist/` directories are
  install/build output and must not become fixture contracts.

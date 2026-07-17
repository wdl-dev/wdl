# CLI And Wrangler Input

## Purpose

The CLI implementation lives downstream. This document records the platform-side
contract that downstream `wdl` CLIs must satisfy when deploying Wrangler projects into
WDL, managing namespace-scoped resources, and driving admin APIs without talking
directly to Redis, S3, or runtime services.

## Current Implementation

The downstream CLI is published as the `@wdl-dev/cli` package and may also be developed
from a standalone checkout. This repository keeps only the control-plane and runtime
contracts that the CLI talks to. Integration tests use the `wdl` executable on `PATH`
by default, with CI installing the `@wdl-dev/cli` version pinned in
the top-level `WDL_CLI_PACKAGE` value in `.github/workflows/ci.yml` before the
integration job. Local unpublished CLI validation should link or wrap the checkout so
`wdl` is on `PATH`; `WDL_CLI_BIN` is only a focused integration override when a run
must bypass `PATH` resolution. Command syntax, command grouping, and user-facing
wording are downstream concerns. This document only records the platform behavior
that CLI calls must preserve.

## Control Context Resolution

Ordinary CLI commands resolve a control URL, admin token, and namespace before making an
HTTP request to control.

The CLI reads shell/CI environment variables and an optional project `.env` file. The
`.env` file supports base `KEY=value` entries plus per-namespace INI sections:

```ini
CONTROL_URL=http://admin.test:8080
WDL_NS=demo

[demo]
ADMIN_TOKEN=local-dev-token

[prod]
CONTROL_URL=https://ctl.prod.example
ADMIN_TOKEN=<prod-token>
```

Precedence is:

1. CLI flag
2. shell/CI environment
3. selected `[namespace]` section
4. base `.env`
5. code default

Canonical spelling is `--control-url` / `CONTROL_URL`. Platform integration tests
only provide `CONTROL_URL`.

Namespace selection is `--ns`, then `WDL_NS` from shell/base `.env`, then a command
fallback if that command has one. If no namespace resolves, only base `.env` values are
loaded. If `--ns foo` has no `[foo]` section, base values are used silently. Section
names use the CLI-local `isAdminAcceptableNs()` rule: ordinary tenant namespaces and
delimiter-safe `__...__` reserved-looking section names are accepted. This is not the
server's exact reserved namespace literal set. `WDL_NS` inside a selected section is
ignored with a warning so a section cannot redirect itself.

Bare production control hosts default to `https://`. Bare local-development hosts such
as `admin.test:8080` and `localhost:8080` default to `http://`; any bare `:8080`
control URL is treated as local HTTP. Include an explicit scheme when this heuristic is
not desired.

`CONTROL_CONNECT_HOST` is a debug/transport override for direct connection while keeping
the logical control URL host intact. It is not part of the ordinary tenant contract.

## Diagnostic Discovery

Downstream CLI diagnostics may call `GET /whoami` through the configured control URL to
confirm which token and endpoint are active. The response is intentionally a self-view:
it includes `principal`, `tokenId`, `requestId`, `platformVersion`, `minCliVersion`,
and `urls`, but never token plaintext, token hashes, other token records, or the raw
workerd version.

CLI output may display:

- `platformVersion`: the WDL platform version reported by control. The canonical
  derivation is documented in `control-auth.md` under `/whoami`; the CLI should display
  the value without trying to reconstruct it from package metadata.
- `minCliVersion`: the minimum downstream CLI version supported by this platform build.
- `urls.control`: the control origin that the request actually reached.
- `urls.namespace`: the tenant namespace origin, returned only for namespace tokens when
  the platform explicitly configures `PLATFORM_DOMAIN`; Control does not validate that
  the configured hostname is publicly reachable.
- `urls.assets`: the configured public assets base URL, returned only when the control
  plane has a safe absolute `http`/`https` `ASSETS_CDN_BASE`; query and fragment are
  stripped before returning the hint.

The CLI must treat these fields as diagnostics and defaults for user-facing guidance,
not as a replacement for explicit user configuration. If `minCliVersion` is greater than
the running CLI version, the CLI should warn or fail before attempting mutating
commands. Missing optional URL hints should be displayed as unavailable rather than
guessed.

## Deploy Pipeline

`wdl deploy <project>` is the supported worker bundling path. It shells out to the
project-local `wrangler` binary, or to `WDL_WRANGLER_BIN` when that env var is set, and
uses Wrangler's dry-run output as the bundle source. The CLI sets a dummy
`CLOUDFLARE_API_TOKEN` for dry-run bundling so ordinary projects do not need real
Cloudflare credentials to build locally.

WDL worker names follow the platform grammar, not Wrangler's narrower deployment-name
grammar: `[A-Za-z0-9][A-Za-z0-9_-]{0,254}`. Uppercase letters, digits, underscores, and
hyphens are valid. If Wrangler dry-run validation would reject the real platform worker
name, the downstream CLI may pass a dummy Wrangler name for bundling, but the control
payload and deployed WDL worker name must remain the user-requested platform worker name.

Successful Wrangler dry-run output is hidden by default and WDL progress is shown
instead. `--verbose` streams Wrangler's raw output for debugging.

After bundling, the CLI walks the entire Wrangler output directory and sends every
emitted artifact to control:

- JavaScript chunks
- Wasm modules
- imported text, JSON, CSS, and other data assets
- all files except source maps and Wrangler's generated output `README.md`

Binary files use base64 in the control JSON payload and are decoded exactly once before
control stores raw bytes. Runtime never sees base64 bundle bytes.

The CLI package owns Wrangler dry-run bundling and bundle-artifact collection. The
platform repository should not duplicate that packaging path.

## Wrangler Config Contract

The CLI reads `wrangler.toml`, `wrangler.jsonc`, or `wrangler.json`; all three use the
same snake_case field shape. Named environments are selected with `--env <name>` or
`CLOUDFLARE_ENV`.

If named environments exist, selecting one is required. WDL does not silently deploy a
top-level default when `[env.<name>]` tables are present. `env.<name>.name` is rejected:
the deployed worker name is always the top-level `name`. Staging/production side by
side should use separate namespaces.

WDL follows Wrangler inheritance for selected environments:

- Non-inheritable keys must be redeclared per env: `vars`, `kv_namespaces`,
  `r2_buckets`, `d1_databases`, `services`, `queues`, `workflows`, durable object
  bindings, and similar binding tables.
- Inheritable keys such as `assets` follow Wrangler's selected-env behavior and may be
  overridden explicitly.
- Top-level-only keys such as `name` and `migrations` are rejected inside env tables.

Supported config surfaces:

| Field | WDL behavior |
|---|---|
| `name`, `main`, `compatibility_date`, `compatibility_flags` | Stored in immutable bundle metadata. Control rejects supplied `compatibility_date` values earlier than `2026-04-01`, as well as malformed, future, or bundled-workerd-unsupported values, before commit; final WorkerCode, including runtime/do-runtime-injected modules and generated workflow keys, must fit workerd's 64 MiB `workerLoader` code limit. |
| `[vars]` | String, number, and boolean values are accepted and stringified into `env`; vars, namespace/worker secrets, and runtime-injected binding/workflow env values must fit WDL's headroomed workerd 1 MiB `workerLoader` env budget. |
| `[[kv_namespaces]]` | `id` is a platform-local KV namespace id, not a Cloudflare UUID. |
| `[[r2_buckets]]` | `binding` plus `bucket_name` become a namespace-scoped virtual R2 bucket under the platform S3 bucket. |
| `[assets]` | `directory` contents upload to S3-compatible assets storage and auto-inject `ASSETS`. |
| `[[d1_databases]]` | Binding resolves by `database_id` first, then namespace-local `database_name`; migrations use matching config. |
| `[[durable_objects.bindings]]` | Same-worker classes from `[[migrations]].new_classes` or `new_sqlite_classes`; `script_name` and rename/delete migrations are unsupported. |
| `[[services]]` | Freezes target namespace, worker, version, and entrypoint at caller deploy time. Cross-namespace `ns` is a WDL extension and requires target opt-in. |
| `[[platform_bindings]]` | Resolves a `SCREAMING_SNAKE_CASE` symbolic platform export from platform-tier namespaces and freezes it into the caller. |
| `route` / `routes` | Sent raw to control; control owns pattern grammar and platform-domain rejection. |
| `[triggers] crons` and `[[triggers.schedules]]` | UTC Cloudflare-compatible crons plus WDL timezone extension. |
| `[[queues.producers]]` and `[[queues.consumers]]` | Producer and consumer metadata. `max_concurrency` is rejected. |
| `[[workflows]]` | Same-worker Workflows V2 bindings. |

`[[analytics_engine_datasets]]` is rejected at deploy at both top level and selected-env
level. Unsupported fields should fail loudly rather than be silently dropped when they
would imply platform behavior WDL does not implement.

## Platform Resource Contract

The downstream CLI may expose these surfaces with its own command shape, but the
platform-side behavior is fixed:

- Worker listing reads active versions, retained versions, secret-only entries, and
  workflow-definitions-only entries from control.
- Worker deletion hard-deletes routes, retained versions, worker secrets, workflow
  definitions, queue consumers, and crons, then stages asset cleanup after the Redis
  commit.
- Version deletion hard-deletes one retained non-active version.
- Secret mutation requires an explicit worker scope or namespace scope. Namespace-wide
  writes must not be accidental. A submitted empty string is a set secret, not unset.
- D1 commands manage namespace D1 databases and forward-only migration files. The
  migration filename is the migration id; already-applied files should not be renamed or
  edited.
- R2 commands operate under the namespace prefix `r2/<ns>/`. Empty declared virtual
  buckets are not visible from prefix-derived listings until their first object is
  written.
- Workflows commands talk to the workflows service; the CLI must not write DB2 directly.
- Tail commands open live SSE sessions through control.

Destructive commands ask for confirmation by default. Automation should pass `--yes`
only after it has already checked the target. Commands that support `--json` return the
raw control response for automation instead of the human summary.

## Tail Contract

Tail streams live fetch invocation events, `console.*` output, uncaught fetch-handler
exceptions, and scheduled/queue invocation events. Multiple worker names create one
explicit fan-in terminal.

The downstream CLI may expose raw output, bounded stream resume, and reconnect knobs,
but control remains the only tail session owner.

Tail is a live debug path, not audit storage. Details of the tail protocol live in
[Log Tail And Observability](log-tail-observability.md).

## Ownership / Failure Semantics

- The CLI never writes Redis directly for ordinary operations.
- Control remains the authority for auth, validation, Redis commit, routing, lifecycle,
  and cleanup intent.
- CLI parsing can warn about missing caller secrets, but deploy may still succeed when
  pre-deploy secret flow is valid.
- If a bundle artifact fails to round-trip from Wrangler output to control/runtime, it
  is a WDL bug, not an intentional silent drop.

## Tests That Protect This Contract

The platform repository exercises the published `@wdl-dev/cli` command through
integration files marked `// @wdl-cli-integration`:

- `tests/integration/auth-platform.test.js`
- `tests/integration/cli-multi-env.test.js`
- `tests/integration/cli-smoke.test.js`
- `tests/integration/log-tail.test.js`
- `tests/integration/pages-assets-demo.test.js`
- `tests/integration/r2-cli-binding.test.js`
- `tests/integration/route-demo.test.js`
- `tests/integration/s3-cleanup.test.js`

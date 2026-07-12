# Rust Service And Sidecar Standards

This document defines the default standards for Rust code in WDL services and sidecars.
It complements `project-standards.md`. It applies both when writing new Rust code and
when refactoring existing Rust code. It is not a line-count rule and it is not a
chronological refactor log.

## Scope

These standards cover:

- `scheduler`
- `workflows`
- `redis-proxy`
- `supervisor`
- `wdl-rust-common` (`rust/common/`)

The Rust crates live under `rust/` in one Cargo workspace. `rust/Cargo.lock` is the
shared dependency lock; member crates must not add their own lockfiles. Source
directories are named by service (`rust/scheduler/`, `rust/workflows/`,
`rust/redis-proxy/`, `rust/supervisor/`, `rust/common/`), and service package/binary
names match those directory names. The shared helper crate keeps the explicit
`wdl-rust-common` name.

Each Rust crate can keep local structure when its runtime, shutdown, logging, or
protocol semantics differ from the others. Consistency means matching ownership and
validation discipline, not forcing every crate to have the same file layout.

## Language And Toolchain

All Rust crates use `edition = "2024"`. The toolchain is pinned in
`rust/rust-toolchain.toml`; the current baseline is Rust 1.96.0. Prefer modern syntax and
standard-library APIs available on that baseline; do not keep older spellings only out of
habit. A newer syntax or API becomes acceptable when CI/build images can compile it and
the touched runtime dependencies support it. `rust-toolchain.toml` is the single source
of truth: local and the CI rust job resolve it directly, and both Dockerfiles copy it into
the build so the `rust:1-alpine` base's `rustup` installs the same pinned toolchain rather
than whatever the rolling image ships. A Rust upgrade is therefore one intentional edit to
`rust-toolchain.toml`.

Allowed current idioms:

- Edition 2024 syntax and module paths.
- `let ... else` for early-return parsing and validation.
- `OnceLock`, atomics, mutexes, and task-owned state instead of `static mut`.
- `is_some_and` / `is_none_or` when they make validation predicates clearer.
- `cast_signed()` / `cast_unsigned()` for domain-checked signedness conversion when it
  is clearer than `as`.
- Explicit `TryFrom`/`try_from` for potentially narrowing numeric conversion.

Modern-by-default does not mean compatibility-blind. Syntax and APIs that need review
attention:

- No nightly features, `#![feature(...)]`, unstable cargo flags, or edition preview
  syntax.
- Edition 2024 reserves `gen`; use `r#gen` when the wire/storage field is literally
  named `gen`, and do not rename the external field to avoid the raw identifier.
- Be explicit around Rust 2024 lifetime-capture behavior for `impl Trait`. Prefer
  concrete return types, named generics, or local helper structs when a public
  crate-internal helper would otherwise depend on subtle inferred captures.
- Do not introduce `unsafe` code, `unsafe` attributes, raw pointers, or `static mut` for
  normal service logic. If a platform boundary genuinely needs unsafe code, isolate it
  behind a tiny module with a safety comment and tests.
- Avoid clever pattern ergonomics in complex matches. If edition changes make a binding
  mode non-obvious, write the reference or ownership pattern explicitly.
- Use newly stabilized standard-library APIs once CI and Docker build images support
  them. If a crate dependency cannot work with the newer style, keep the older form
  locally and document the reason in the owning module or review notes. Redis access is
  the common example: prefer modern Rust syntax around it, but do not force an API shape
  that the `redis` crate version cannot express cleanly.

## Ownership

Prefer medium-sized ownership files over many tiny helper files. A module should own a
behavior that a reviewer can name and validate.

Good split boundaries are:

- protocol handlers with distinct request or response contracts
- state machines with independent concurrency or fencing rules
- Redis key families and cursor envelopes
- background task orchestration
- policy surfaces such as limits, stable error mappings, or route names
- module-specific tests that protect local invariants

Tiny modules are acceptable only when they name a stable owner, such as a key family,
limit surface, cursor envelope, error mapping, or directory-level `mod` glue. Do not
create a file whose only purpose is to move a few lines out of a longer file. Conversely,
large files are acceptable when they keep one state machine or protocol path readable
end-to-end.

Apply the same standard across services:

- `scheduler` may split cron, queue, remote tick, registry, orphan, and delivery logic
  because those are separate background loops or queue state machines.
- `workflows` may split create, execution, lifecycle, tick, replay/history, payload,
  identity, routing, and schema logic because each owns a distinct API surface,
  Lua/fence path, or Redis key family.
- `redis-proxy` may keep KV, queue, logs, runtime-load, and secrets as medium-sized
  handlers when splitting further would scatter one HTTP protocol path.
- `supervisor` may stay compact because config, process, renew, drain, and logging are
  already clear owners.
- `common` should remain many small primitive owners; those modules are shared contracts,
  not service state machines.

Do not split only because a file is long. If splitting a file makes a state transition,
retry rule, or protocol response harder to read end-to-end, keep the owner together.

## Imports And Re-Exports

Leaf production modules should import the names they use explicitly. Avoid `use
super::*`, `use crate::*`, and broad local glob imports in service logic because they
hide the true dependency surface and make drift easy when a parent module changes.

Crate roots and directory-level `mod` glue may keep an explicit local prelude when that
is the established crate shape, for example `pub(crate) use config::*` or `pub(crate)
use state::*`. This is a crate wiring convention, not a license for leaf modules to
depend on large implicit parent scopes. If a module only needs a few types from a
sibling owner, import those types directly.

Test modules may use `use super::*` when they intentionally exercise private module
items. Do not rewrite colocated tests only to remove this idiom unless it reduces a
real review or drift risk.

## Tests

Colocate tests when they protect one module's behavior. Keep central tests only for
crate-wide style contracts or cross-module invariants.

Examples:

- Scheduler cron, queue, runtime-client, workflow-tick, state, observability, and time
  behavior belong beside their owning modules.
- `redis-proxy` KV, queue, logs, runtime-load, and app error response behavior belong
  beside their owning modules.
- Supervisor config, log, drain, and shared helper tests belong beside those small
  owners; its production files do not need to be split for symmetry.
- Workflows may keep crate-wide tests when they enforce cross-module contracts, while
  local execution/history/payload invariants should live near the owning module when
  practical.

Moving tests is only useful when it lowers future review and drift risk. Do not add
tests solely to increase count.

## Shared Code

`wdl-rust-common` (`rust/common/`) is the only shared Rust helper crate in this batch.
It owns small cross-crate primitives whose behavior must stay identical, such as
environment number parsing, log-level parsing, HTTP health probes, shutdown/in-flight
tracking, common JSON log-line emission, wall-clock millisecond helpers, short
non-cryptographic random hex suffixes, stable non-cryptographic hashes, queue Redis key
builders, worker version / bundle-key parsing, Prometheus metric storage/formatting,
Prometheus text responses, structured error field merging, internal-auth token/header
matching, Redis command construction helpers, a neutral Redis connection execution
wrapper, and UTF-8-safe string truncation. It must not become a dumping ground for
service behavior. Axum-facing helpers are feature-gated so non-HTTP users such as
the D1/DO supervisor binaries do not pay for the HTTP stack.

The `test-support` feature exposes the single process-environment override helper used
by Rust service tests. It serializes overrides across modules in one test process and
restores all values during unwinding; production dependency builds leave it disabled.

Local explicit code is still preferable when sidecars have different behavior around:

- service-specific shutdown/drain timing and shutdown log events
- logging call sites, event names, and request completion semantics
- Redis access patterns
- runtime dispatch and retry semantics
- protocol-specific error mapping

When more shared code becomes justified, define one owner and one contract first. Do not
add a shared helper just to remove a few local lines.

Rules for shared primitives:

- If two crates must produce or parse the same Redis key shape, that shape belongs in
  `wdl-rust-common` or one clearly named owner. Do not duplicate FNV hash constants,
  queue key builders, worker bundle key parsing, or version-tag grammar across crates.
- Keep shared helpers semantically neutral. For example, a 64-bit random hex suffix
  helper should not be named after scheduler or workflows when it is also used for
  pending-create tokens.
- Do not add service-specific lifecycle, retry, Redis transaction, or protocol-response
  behavior to `wdl-rust-common`; keep those in the service crate that owns the state
  machine.
- Redis helpers in `wdl-rust-common` may only build commands from explicit keys and
  args or run caller-provided closures against an explicit `ConnectionManager`. Service
  crates still own script bodies, which connection is selected, retry/timeout behavior,
  error mapping, and state ownership.
- HTTP framework helpers in `wdl-rust-common` must stay behind the crate's `axum`
  feature. Sidecars that only need non-HTTP primitives should opt out of default
  features.

## Redis, Errors, And Schema Contracts

Redis-facing code should make key ownership, error classification, and schema behavior
explicit.

- Classify Redis server errors through stable error codes (`err.code()`) rather than
  string-substring matching on formatted errors. Tests may search Lua source strings
  when they are protecting script contents, but runtime logic should not.
- Service error types own their machine code, human message, and HTTP status. Do not
  reconstruct HTTP status from string codes in a separate server layer.
- Redis schema markers should fail closed when the persisted shape does not match the
  service contract. Do not keep fake compatibility, markerless adoption, or destructive
  reset commands after a maintenance-window migration has already completed. If a
  future migration is required, design it as a new explicit migration path.
- If a service writes a runtime claim, token, lease, or generation fence, keep the fence
  fields owned by that service's state machine and covered by local tests. Avoid
  copying the same fence key derivation in multiple modules.

## Validation

Before a Rust change, identify the behavior that should catch a regression. If coverage
is weak, add or reshape the relevant test inside the same change.

The workspace manifest owns the baseline Clippy lint groups used for every sidecar
crate. Service crates should opt in with `[lints] workspace = true`; CI still promotes
Clippy warnings to errors so the manifest remains the source of lint scope and the gate
remains strict.

Baseline checks for every touched Rust crate, run from `rust/`:

```bash
cargo fmt --package <package> --check
cargo check --locked -p <package>
cargo test --locked -p <package>
cargo clippy --locked --all-targets -p <package> -- -D warnings
```

For full Rust sweeps, run the same commands with `--all` or `--workspace` from `rust/`,
depending on the cargo subcommand.

Run targeted integration when executable behavior changes:

- `workflows` dispatch, lifecycle, execution, or API contract changes need workflows
  integration.
- `scheduler` cron, queue, workflow tick, or shutdown/drain changes need matching cron,
  queue, workflows, or shutdown-drain integration.
- `redis-proxy` KV, log, queue, or routing protocol changes need integration that
  exercises the affected binding/runtime path.
- `supervisor` drain, renew, or process behavior changes need targeted D1/DO rolling or
  drain coverage where available; otherwise document why unit coverage is the strongest
  local gate.

Pure test colocation and Rust module reshaping require the relevant crate gates and `git
diff --check`; they do not require integration unless they change executable behavior.
Documentation-only changes require `git diff --check`, link/path checks where relevant,
and style-contract tests when navigation or source-scan rules change.

## Examples

These examples show how to apply the standard. They are not an exhaustive list of
allowed or forbidden files.

- Split scheduler background task orchestration out of server bootstrap because it is a
  named owner with lifecycle behavior.
- Keep scheduler queue delivery, retry planning, and runtime response handling together
  when they form one continuous delivery state machine.
- Split a KV cursor envelope from a `redis-proxy` handler when cursor parsing and
  serialization are a stable protocol surface.
- Keep `redis-proxy` KV and queue protocol handlers together when further splitting would
  scatter request handling without creating a new owner.
- Keep short workflow limit or route-name modules when they define stable policy
  surfaces.
- Avoid splitting supervisor production code purely for symmetry when its current files
  already map cleanly to config, logging, drain, process, and entrypoint ownership.

## Refactor Discipline

Rust service and sidecar refactors in this repository follow the same staged-review
discipline as other WDL refactors:

- define one deployable boundary before editing
- keep unrelated cleanup out of the active boundary
- stage the complete boundary for review
- keep feedback fixes unstaged until reviewed
- commit only after the relevant crate gates and targeted integration pass

When a refactor also fixes a behavior bug, say so explicitly in the change and run the
integration that covers that behavior. Do not describe a behavior fix as pure structure
work.

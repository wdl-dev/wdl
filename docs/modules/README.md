# Module Documentation Map

Module docs are the entrypoint for current module design. Each module doc should
describe the current implementation and the constraints future changes must preserve.
Use the listed source files, current design docs, and tests as refresh/review inputs,
then re-check every claim against current code before updating a module doc.

Cross-cutting protocol changes should also consult
[`../protocol-contracts.md`](../protocol-contracts.md). Contributor-oriented reading
paths live in [`../contributing.md`](../contributing.md).

## Current Modules

| Module | Target docs | Current primary sources |
|---|---|---|
| Gateway routing | `gateway.md`, `gateway.zh.md` | `gateway/`, gateway integration tests |
| Runtime loader and bindings | `runtime.md`, `runtime.zh.md` | `runtime/`, `shared/`, runtime unit/integration tests |
| AI binding | `ai.md`, `ai.zh.md` | `runtime/bindings/ai.js`, `runtime/ai-client.js`, `control/handlers/ai.js`, `rust/redis-proxy/src/ai.rs`, AI tests |
| CLI and Wrangler input | `cli.md`, `cli.zh.md` | Downstream standalone CLI, CLI integration tests, README Quick Start / Deploy A Worker |
| Control and auth | `control-auth.md`, `control-auth.zh.md` | `control/`, `auth/`, `shared/auth-*` |
| Durable Objects | `durable-objects.md`, `durable-objects.zh.md` | `do-runtime/`, `runtime/do-client.js`, DO tests |
| D1 | `d1.md`, `d1.zh.md` | `d1-runtime/`, `runtime/bindings/d1.js`, D1 tests |
| Queues and cron | `queues-cron.md`, `queues-cron.zh.md` | `rust/scheduler/`, runtime queue bindings, control routing |
| Workflows | `workflows.md`, `workflows.zh.md` | `rust/workflows/`, `runtime/dispatch/workflow-*.js` |
| Log tail and observability | `log-tail-observability.md`, `log-tail-observability.zh.md` | `runtime/tail-worker.js`, `control/handlers/logs-tail.js`, `shared/observability.js` |
| Infra and deployment | `infra.md`, `infra.zh.md` | `terraform/`, `deploy/kubernetes/`, `.github/workflows/` |

## Module Doc Contract

Each module doc should cover:

- Purpose and scope
- Current implementation
- Public and internal interfaces
- Redis or storage contracts
- Ownership, concurrency, and failure semantics
- Security boundaries
- Observability
- Deployment and rollout notes
- Tests that protect the module
- Known constraints and non-goals

Keep the English and Chinese files for the same module in the same commit unless a
temporary stub is explicitly marked.

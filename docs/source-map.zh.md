# Source Map

本文记录核心服务源码树的 ownership map。它比架构概览更具体，但不替代各模块文档中的语义合同。Repository meta files、CI definitions、下游 tenant-facing docs 和 docs sources 不在本 map 范围内，除非它们拥有 runtime 或 deployable service 行为。

## Runtime 和 Control Workerd Tiers

| Path | 责任 |
|---|---|
| `docker-compose.yml` | 本地开发栈：Valkey、s3mock、gateway、user-runtime、system-runtime、d1-runtime、do-runtime、scheduler、workflows、redis-proxy sidecars、supervisors，以及可选 `d1-multi` / `do-multi` profiles。 |
| `docker-compose.images.yml` | 本地 stack override，拉取已发布的 `docker.io/getwdl/wdl-workerd` 和 `docker.io/getwdl/wdl-rust` images，而不是依赖本地构建的 image tags。 |
| `Dockerfile.workerd` | workerd-side image。构建 supervisors，编译 workerd configs，从 npm package 提取 `workerd`，并携带编译后的 `dist/workerd-configs/*.bin`，不带 `node_modules`。 |
| `Dockerfile.rust` | Rust 统一 image，包含 redis-proxy、scheduler 和 workflows binaries；container command 选择具体服务。 |
| `envoy/envoy.yaml` | 本地 Compose private mesh proxy，供集成测试和本地开发使用。 |
| `gateway/config.capnp` | Gateway workerd config：public `:8080`，`RUNTIME_USER`、`RUNTIME_SYSTEM` 和 `CONTROL` externals。 |
| `gateway/config-local.capnp` | 为 Docker Compose 编译的本地 Gateway workerd config，使用 Envoy-backed private service routes。 |
| `gateway/index.js` | Gateway worker dispatch 分支：admin-host short-circuit、subdomain routing 和 pattern routing。 |
| `gateway/dispatch.js` | 纯 gateway dispatch decision tree 和 route target selection。 |
| `gateway/holder.js`、`gateway/websocket.js` | WebSocket holder、reconnect forwarding 和 `101` upgrade preservation。 |
| `gateway/runtime.js` | Gateway route/pattern caches、Redis subscriber invalidation、logging、metrics 和 health/metrics snapshots。 |
| `gateway/lib.js` | workerd 和 Node tests 共用的纯 routing helpers。 |
| `runtime/config-user.capnp` | User runtime config：loader `:8081`、internal `:8088`、loaded-worker public-only outbound。 |
| `runtime/config-system.capnp` | System runtime config：loader `:8081`、internal `:8088`、control `:8082`、auth worker、private+public outbound。 |
| `runtime/config-user-local.capnp`、`runtime/config-system-local.capnp` | 为 Docker Compose 编译的本地 runtime workerd configs，使用 Envoy-backed private service routes。 |
| `runtime/index.js` | Runtime loader socket entrypoint。 |
| `runtime/internal.js` | 私有 `:8088` runtime dispatch surface，承载 scheduled、queue、workflow run/notify 和其它 platform-only event。 |
| `runtime/runtime.js` | Service-name binding、loaded-worker registry、sibling eviction、logger、metrics 和 request-scope setup。 |
| `runtime/metrics.js` | Runtime Prometheus snapshot helpers 和 bounded metric aggregation。 |
| `runtime/dispatch.js` 和 `runtime/dispatch/*` | Fetch、scheduled、queue、workflow dispatch、workflow step facade、replay cache 和 deterministic workflow JSON helpers。 |
| `runtime/load.js` 和 `runtime/load/*` | Bundle decode、module rewrite、env construction、wrapper generation、runtime 注入源码 ownership 和 hidden binding stripping。 |
| `runtime/bindings/` | KV、D1、R2、Durable Objects、ASSETS、service 和 queue 的 host-side binding adapters。 |
| `runtime/workflows-client.js`、`runtime/dispatch/workflow-*.js`、`runtime/load/env-build.js` | Workflow binding materialization、backend client、dispatch facade、replay cache 和 step semantics。 |
| `runtime/tail-worker.js` / `runtime/tail-forwarder.js` | Workerd tail capture 和 `wdl tail` 的 activation-gated append path。 |
| `runtime/lib.js` | 纯 runtime helpers，例如 bundle-to-worker-code、byte normalization 和 dispatch body normalization。 |
| `control/index.js` | system-runtime `:8082` 上的薄 HTTP dispatcher；auth 后交给 handlers。 |
| `control/handlers/` | deploy、promote、versions、workers、delete、secrets、hosts、reload、auth tokens、D1、R2、workflows 和 log tail endpoint handlers。 |
| `control/shared.js` | Control singletons、auth wrapper、Redis publish helpers、state-bound workflow transport wiring 和共享 lifecycle/delete helpers。Direct `state.*` access 只应在这里或 dispatcher。 |
| `control/errors.js`、`control/json-body.js`、`control/optimistic.js` | 由 `control/shared.js` re-export 的纯 Control error-response、bounded JSON request-body contract，以及 shared optimistic retry loop 上的 strict `WatchError`/Redis-session adapter。 |
| `control/workflows-client.js` | timeout 由 caller 显式选择的 Control-to-Workflows internal POST transport；endpoint-specific response interpretation 仍由 caller 持有。 |
| `control/lib.js` | 纯 Control data-shaping：route-to-action classification、key helpers、canonical bundle `__meta__` parsing 和 referrer redaction。 |
| `control/bundle.js` | Bundle/module normalization、compatibility metadata、vars 和 emitted module manifest construction。 |
| `control/bindings.js` | Service/platform binding parsers、ACL evaluation 和 linker helpers。 |
| `control/topology.js` | Deploy metadata 中 routes、patterns、cron、queue consumer 和 workflow declaration parsing。 |
| `control/routing.js`、`control/routing/route-plan.js` | Promote、secret bump/promote、host reconcile WATCH/MULTI loops，以及纯 route/pattern planning helpers。 |
| `control/lifecycle-indexes.js` | Worker lifecycle、cron、queue consumer 和 referrer indexes 的 Redis mutation helpers。 |
| `control/env-budget.js` | Deploy 和 secret mutation guard 使用的 workerd `workerLoader` env size 控制面估算。 |
| `control/worker-code-budget.js` | Deploy guard 使用的最终 WorkerCode size 控制面估算，复用 runtime 与 do-runtime wrapper/module injection 规则。 |
| `control/d1-*` | D1 control metadata、store、lifecycle、migration 和 d1-runtime client modules。 |
| `control/r2.js` | 面向配置的 S3-compatible store 的 control-plane R2 bucket/object API client。 |
| `control/s3.js` | S3-compatible ASSETS upload helper。 |
| `control/cron-index.js` | Promote logic 使用的 cron identity 和 diff helpers。 |
| `auth/index.js`、`auth/lib.js`、`auth/runtime.js` | 静态 socket-less auth worker、纯 auth helpers、bootstrap token upsert、role evaluation 和 Redis-backed token store。 |

## Shared JavaScript

| Path | 责任 |
|---|---|
| `shared/redis.js`、`shared/redis-*.js` | 公共 Redis import surface，以及拆分后的 RESP codec、per-call client、WATCH/MULTI session 和 subscriber loop modules。Runtime hot path 优先使用 Rust redis-proxy sidecar。 |
| `shared/redis-lock.js` | Control 和 Auth 共用的 token-fenced Redis lock creation、acquire、renewal 和 best-effort token-scoped release。 |
| `shared/optimistic-retry.js` | Control、Auth 和 D1/DO owner-lease adapter 共用的通用有界 optimistic retry loop。 |
| `shared/owner-endpoint.js`、`shared/owner-lease.js`、`shared/owner-protocol.js`、`shared/owner-forwarder.js` | Control 与 D1/DO runtimes 共用的 owner endpoint grammar、owner lease parsing、generation counters、key derivation、fence matching、staged Redis owner writes 和 authenticated forwarding mechanics。 |
| `shared/auth-roles.js` | Role table、principal validation、reserved namespace policy 和 auth action capabilities。 |
| `shared/auth-token.js` | Control 和 auth 共用的 `x-admin-token` sanitizer。 |
| `shared/internal-auth.js` | JS caller 和 receiver 共用的 internal mesh auth header / token helpers。 |
| `shared/secret-envelope.js`、`shared/secret-keys.js` | Secret envelope encryption/decryption、canonical base64/JSON handling、AAD binding helpers 和 secret Redis key construction。 |
| `shared/base64.js` | Workerd tiers 共用的无依赖 byte/text base64 codec；在 `nodejs_compat` 下使用 `Buffer` fast path。 |
| `shared/hex.js`、`shared/random-id.js`、`shared/errors.js` | byte-to-hex rendering、random hex ids 和 string-only error message extraction 的无依赖小 primitive。 |
| `shared/observability.js` | JS tiers 的 structured logger、metrics registry、request-id helpers 和 log-level handling。 |
| `shared/respond.js` | 共享 HTTP response、JSON error、Prometheus text、best-effort response body discard 和 `x-request-id` echo helpers。 |
| `shared/bounded-body.js` | 共享 bounded byte-stream 和 request-body readers；各 tier 自己把 limit error 映射为对应 contract。 |
| `shared/ns-pattern.js` | Platform-domain normalization，以及 namespace、worker、binding、queue、KV/D1/R2 id、module path、reserved object-key 和 reserved namespace grammars。 |
| `shared/worker-contract.js` | Worker version grammar，以及 worker、route-plane、lifecycle、DO owner-scope key 与 route invalidation channel helpers。 |
| `shared/workerd-compat-flags.js` | 上游 workerd experimental enable flags 的 pinned mirror，以及 WDL-owned dynamic-worker 日期和 error-serialization policy。 |
| `shared/queue-keys.js` | JavaScript queue key helpers，供 tests 和 cross-tier key-shape checks 使用。 |
| `shared/route-projection.js` | Control writer、delete check 和 gateway reader 共用的紧凑 pattern-route projection encoding。 |
| `shared/d1-*.js`、`shared/sql-splitter.js` | Runtime、d1-runtime、control 和 tests 共用的 D1 parameter、data-field、transport、timeout、query-wire 和 SQL splitting utilities。 |
| `shared/fnv1a32.js` | Runtime-side shard 和 slot hashing 共用的 JavaScript FNV-1a helpers。 |
| `shared/s3-query.js` | s3-cleanup system worker 使用的 S3 query encoder；runtime R2 在 `runtime/r2-utils.js` 保留同一套 standalone helper，因为该文件会作为 worker source 注入。 |
| `shared/s3-retry.js` | runtime R2 与 s3-cleanup worker 共用的 idempotent S3 POST 有界瞬态重试策略。 |
| `shared/s3-xml.js` | Control R2、runtime R2 和 system cleanup 路径共用的 S3 XML parsing helpers。 |
| `shared/worker-id.js` | Gateway、runtime、DO runtime 和 tests 共用的 `x-worker-id` formatting、parsing 和 runtime-load identity grammar。 |
| `shared/cron-time.js` | Control 侧 cron parsing 和 slot-alignment helpers；scheduler advancement 使用 Rust `croner`。 |
| `shared/vendor/` | `npm run build:vendor` 重新生成的预打包第三方依赖。 |
| `types/workerd-embedded.d.ts` | workerd-embedded module specifier 的 ambient TypeScript declarations，例如 embedded runtime bundle 使用的 `*-source` aliases。 |

## Stateful Workerd Tiers

| Path | 责任 |
|---|---|
| `d1-runtime/` | D1 workerd service。Supervisor 是 PID 1，spawns workerd、renews leases，并在 SIGTERM 时 drain。Router/actor/owner modules 实现 per-database ownership、forwarding、read cache 和 SQLite localDisk execution。 |
| `do-runtime/` | Durable Object workerd service。Supervisor 是 PID 1，spawns workerd、renews owned shards、SIGTERM 时 drain，并在 drain 成功后 SIGKILL workerd 以避开 half-dead 504 window。Owner/actor/load/alarm modules 实现 owner scopes、native facet execution、SQLite storage、Workflows alarm client/shim/dispatch endpoint 和 WebSocket connect。 |
| `do-runtime/config-local.capnp` | 为 Docker Compose 编译的本地 Durable Objects runtime workerd config，使用 Envoy-backed private service routes。 |

## Rust Workspace

| Path | 责任 |
|---|---|
| `rust/redis-proxy/` | Runtime sidecar，提供 cold-load、secret decrypt、KV、queue producer 和 log-tail sidecar APIs。 |
| `rust/scheduler/` | Cron、queue、delayed queue、orphan migration 和 workflow tick scheduler。 |
| `rust/workflows/` | Workflows service、DB 2 state machine 和 internal DO alarm backend jobs。 |
| `rust/supervisor/` | D1/DO supervisor binaries。 |
| `rust/common/` | worker-contract grammar 与 keys、time、logging、internal-auth matching、Redis connection primitives 和 metrics primitives 等共享 Rust utilities。 |

## System Workers、Fixtures 和 Examples

| Path | 责任 |
|---|---|
| `system-workers/s3-cleanup/` | post-delete ASSETS cleanup 的 permanent `__system__` worker。它消费 `worker-delete-s3-cleanup`，在 D1 中持久化 task state，并用 cron replay。 |
| `test-workers/` | Integration-owned worker fixtures。测试可以依赖它们的精确形状。 |
| `examples/` | 手工 demo 和 reference projects。测试不应悄悄依赖它们，除非 fixture 明确迁入 `test-workers/`。 |
| `scripts/run-integration-tests.js` | Integration worker-pool runner。 |
| `scripts/compile-workerd-configs.js` | 把 workerd Cap'n Proto configs 编译成 `dist/workerd-configs/*.bin`。 |
| `scripts/extract-workerd-experimental-compat-flags.mjs` | pin bump experimental flag 提取脚本。 |

## Infrastructure

| Path | 责任 |
|---|---|
| `terraform/` | AWS ECS-shaped environment：ECS、Valkey、EFS、S3/R2 和 ALB rules。 |
| `deploy/kubernetes/` | 基于 Kustomize 的本地和 portable Kubernetes manifests。 |

# 协议合同

本文负责 WDL 跨 tier 协议合同的整体口径。单个 endpoint 和 Redis key family 仍由模块文档拥有；本文说明 metadata、Redis payload、control API、binding materialization 和 state-machine protocol 应如何从隐含 object shape / 分散 switch 收敛成显式合同。

## 范围

协议合同包括：

- control/admin request 和 response body；
- bundle metadata、binding metadata、route projection、lifecycle index 等 Redis DB 0 record；
- KV metadata、queue entry、delayed message、log-tail event 等 data-plane Redis payload；
- runtime-load、D1 query、D1 actor query、DO invoke、D1 query response 等二进制 internal envelope；
- scheduler、workflows、D1、DO 的 Rust service state-machine record；
- 当消费方依赖稳定 event name、field、metric family 或有界 label 时，logs 和 metrics 也属于协议。

当前代码和测试仍是事实来源。本文定义新工作和触碰既有协议面重构时的方向。

## 所有权规则

每个协议 shape 都需要一个 owning module 和一个当前书面来源：

| Surface | Owner |
|---|---|
| Control/admin HTTP body 和 error code | `docs/modules/control-auth.zh.md` 加 `control/` helpers |
| Bundle metadata 和 binding metadata | `docs/modules/runtime.zh.md`、`docs/modules/control-auth.zh.md`、`control/`、`runtime/load/` |
| Redis key 和逻辑 DB split | `docs/redis-key-layout.zh.md` 加共享 key helper |
| Route 和 pattern projection | `shared/route-projection.js`、`control/routing.js`、`gateway/` |
| D1 query/facade protocol | `docs/modules/d1.zh.md`、`shared/d1-*`、`d1-runtime/`、runtime D1 binding |
| AI provider record 和 resolver snapshot | `docs/modules/ai.zh.md`、`shared/ai-contract.js`、`control/handlers/ai.js`、`rust/redis-proxy/src/ai.rs`、`tests/fixtures/ai-contract.json` |
| AI tenant/provider HTTP、SSE、WebSocket 和 facade protocol | `docs/modules/ai.zh.md`、`runtime/bindings/ai*.js`、`runtime/ai-client.js` |
| 内部 WebSocket backend reconnect policy | `docs/modules/gateway.zh.md`、`shared/worker-contract.js`、`gateway/websocket.js` |
| Durable Object invoke/connect protocol | `docs/modules/durable-objects.zh.md`、`runtime/_wdl-do-scoped-request.js`、`runtime/_wdl-do-transport.js`、`do-runtime/protocol.js` |
| Session policy projection 和 lifecycle notification | `docs/modules/control-auth.zh.md`、`docs/modules/gateway.zh.md`、`shared/worker-contract.js`、`control/routing.js`、`control/handlers/delete-plan.js`、`gateway/runtime.js`、`gateway/websocket-lifecycle.js`、`do-runtime/owner-registry.js`、`rust/workflows/src/api/do_alarms/dispatch.rs` |
| Queue、cron、delayed queue record | `docs/modules/queues-cron.zh.md`、`shared/queue-keys.js`、scheduler/proxy Rust modules |
| Workflow definition 和 instance state | `docs/modules/workflows.zh.md`、`rust/workflows/`、runtime workflow dispatch |
| Observability event 和 metric shape | `docs/modules/log-tail-observability.zh.md`、`shared/observability.js`、`wdl-rust-common` |

如果某个 shape 由一个 tier 写、另一个 tier 读，同一改动必须同时更新 writer、reader、owning doc 和能抓漂移的测试。除非存在外部 rollout 要求，不要增加第二套 parser 或 fallback reader。

## Schema 方向

WDL 应把高风险 object shape 推向显式 schema 或 schema-like normalizer。这里的 “schema” 指一个 canonical 定义，它应：

- 命名 required field、optional field、default、cap 和 enum value；
- 对畸形 persisted state fail closed，而不是有损归一化；
- 输出一个小而明确的 normalized value；
- 被同一协议域的所有 writer 和 reader 引用；
- 带 success、malformed、legacy-rejected 和 boundary-size fixture 或行为测试。

第一批候选是：

- deploy request body 和 emitted bundle metadata；
- KV、R2、AI、D1、DO、workflows、service binding、platform binding、queue、assets、vars、secrets 的 binding metadata；
- active route、pattern route、cron/queue/workflow lifecycle index 和 workflow definition 的 Redis projection；
- runtime-load、DO/D1 binary envelope metadata；
- workflow instance state 和 scheduler discovery index。

不要先引入大型通用框架来做 schema 化。优先使用小的本地 validator，后续再按 registry 分组。

## Binding Registry 方向

Binding 工作应收敛到 registry-shaped pipeline。每个 binding kind 应有一个 entry 负责：

- deploy-time input validation 和 normalization；
- Redis/bundle metadata materialization；
- runtime `env` materialization；
- hidden backend binding 需求；
- host-wrapper 和 raw-export hiding 需求；
- tenant-visible facade 行为；
- docs 和 test fixture 名称。

大型 switch 只能作为临时的 registry entry dispatch。新增 binding 时，不应把 deploy validation、runtime env construction、host wrapper generation 和 docs 分散成互不相干的本地编辑。Review 单元应能一眼看出哪个 registry entry 拥有这个 kind。

## State-Machine 协议测试

State-machine 正确性应由 model-ish tests 和 failure injection 保护，而不只靠 happy-path integration。好的 state-machine 测试应命名：

- authoritative record 和 derived projection；
- 防止 stale writer 的 generation、lease、token 或 WATCH fence；
- 注入的失败或 interleaving；
- 期望的 repair、retry 或 fail-closed 行为。

优先级最高的 state machine：

- deploy、promote、rollback、version delete、whole-worker delete 和 S3 cleanup intent；
- D1/DO owner claim、forward、renew、drain、release 和 stale owner hint；
- workflow run-token、step dependency、ready/due、event、callback、retention 和 lifecycle delete blocker；
- scheduler queue consumer discovery、delayed due index、retry/DLQ 和 orphan migration；
- log-tail activation lease 和 bounded stream 行为。

Failure-injection 测试能确定性覆盖协议时，优先用 pure unit 或 service-local test。Integration test 应覆盖能证明分布式边界仍成立的最小 end-to-end path。

## Known Constraint Runbook

已知约束必须是显式合同，不应只存在 reviewer 记忆里：

- Gateway route invalidation 是非持久 hint。Gateway 在 subscriber connect/disconnect 时清 cache，下一次 lookup 从 Redis 重读修复。
- Runtime cold-load 容忍 immutable bundle metadata 和当前 namespace/worker secrets 之间的 torn read。Per-version secret snapshot 会是新协议。
- Admin host 必须在 `PLATFORM_DOMAIN` 外面；gateway 的 `ADMIN_HOST` 分支是 control-plane ingress shortcut，不是 tenant routing。
- Delete lock 覆盖 Redis lifecycle critical section。已提交 delete 后的 cleanup 由 durable cleanup intent 表达，而不是通过持锁覆盖 S3 工作。
- Streaming response、WebSocket upgrade、empty response、`HEAD` 和 result envelope 是明确的 error-contract 例外。
- Queue main stream 是 durable 且有意不 trim；辅助诊断或 activation stream 可以有界。

Review 遇到这些领域的疑似问题时，应先判断当前行为是否违反以上合同。如果行为是接受的，应更新 owning active doc 或增加 style-contract guard，而不是落一个只改变表象的“修复”。

## PR Review Gate

触碰协议的 PR 需要同时有本地验证和 integration 验证：

- control/admin protocol 变化：request/response shape 单测；route、auth、deploy 或 lifecycle 行为变化时跑定向 control/auth/gateway integration；
- runtime binding metadata 变化：runtime/load 单测、binding facade 测试，以及受影响 binding 的定向 integration；
- Redis key 或 payload 变化：source-scan drift guard 加每对 writer/reader 的定向 integration；
- Rust service state-machine 变化：crate tests、Rust check/clippy 和对应 service boundary 的定向 integration；
- 跨 tier wire 变化：除非外部 rollout 要求 staged plan，否则 accepting side、sending side、docs 和 tests 应在一个可部署边界内更新。

当协议行为、runtime config、Redis shape 或 state-machine logic 变化时，在称一个 code/runtime/config 边界 commit-ready 之前，仍必须跑完整 integration。

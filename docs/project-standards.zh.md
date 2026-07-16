# 项目全局标准

本文档负责 WDL 的跨语言约定。它位于 JavaScript/workerd 标准和 Rust 标准之上：语言特定的语法、模块布局和检查命令仍放在对应文档里；本文件只记录 JS、Rust、测试、文档和部署代码之间必须保持一致的规则。

## 语言基线

显式语言基线是项目合同的一部分：

- JavaScript 以 Node 24 上的 ES2025 为目标。
- Rust 以 Rust 1.96 上的 Edition 2024 为目标。

当仓库已经对同一操作形成现代写法时，不要重新引入旧写法。如果 dependency、runtime 或 build image 阻止使用现代形式，应把例外限制在局部，并在 owning module 或 review notes 里说明原因。

## 合同所有权

每个跨 tier 合同都需要一个 owner：

- 协议域 shape、schema、payload、binding registry 和 state-machine 测试规则归 `docs/protocol-contracts.zh.md`。本文件记录跨语言 policy；协议文档记录如何把这些 policy 在每个 surface 上显式化。
- Redis key 和逻辑 DB ownership 属于 `docs/redis-key-layout.zh.md` 以及实际构造该 key 的源码 helper。
- 产品 API error 的 machine code、human message 和 HTTP status 应由同一个 owner 维护。
- Request id 只是关联字段，不是权威身份。不要把客户端提供的 request id 用作 lock value、task id 或 idempotency owner。

当某个 key shape、wire shape 或 error code 由一种语言生产、另一种语言消费时，应有 source-scan 或行为测试能抓住漂移。优先使用每种语言一个共享 helper，再用跨语言合同测试钉住，而不是复制 literal。Request-id sanitization 由 `tests/fixtures/request-id-sanitizer.json` 固定；JS 和 Rust 测试都会读取这个文件。

Schema-like normalizer 应放在协议 owner 处，而不是放在下游调用点。如果新的 binding、Redis payload 或 control API shape 需要多处校验，应在 owner 后面提供 normalizer，让其它 tier 消费 normalized value。

## 错误和返回合同

WDL 有多个协议域，但每个协议域都必须明确自己的 envelope：

- Client-facing/admin HTTP 错误使用 `{ "error": "<machine-code>", "message": "<safe human summary>" }`。额外字段只能 additive；客户端按 `error` 分支，不解析 `message`。
- Details 不能覆盖顶层 `error`、`message` 或 legacy `reason` 字段。reserved-field 规则保护的是 response 顶层；只有 owning module 明确记录该协议形状时，nested record 才可以保留自己的领域字段名。
- 新公开 API 不应新增独立顶层 `reason`。Auth reject reason 作为 machine `error` 暴露；日志里可以把 `reason` 作为诊断上下文。
- Client-facing 5xx message 必须是安全摘要。Raw backend error text、Redis diagnostic、exception message、SQL text 和 storage/provider message 应进入结构化日志；除非 owning module 明确记录某个诊断 API，否则不进客户端 body。
- Internal platform HTTP 默认也使用 `{ error, message }` envelope。只有在模块文档记录了 shape 和消费者时，模块才能拥有不同协议，例如 D1 query payload、DO protocol error 或 batch result envelope。
- Result envelope 不是 error envelope。当 transport 成功，而协议表达 job、batch、scheduler 或 queue outcome state 时，HTTP 200/207 body 可以是 `{ outcome: "error" }` 或 `{ ok: false, ... }`。
- 空响应、streaming body、WebSocket upgrade 和 `HEAD` response 可以是显式例外。应在 owning route 记录例外，而不是强行把 JSON body 塞进不适合的协议。

Error-code vocabulary 由协议拥有：

- Platform/admin HTTP code 使用 `snake_case`。
- D1 query/facade compatibility code 使用 D1 vocabulary，包括 `limit-exceeded`、`sql-error` 这类 `hyphen-case` code。
- HTTP body parser cap 使用 `request_body_too_large`。Workflow 语义 payload / fan-in cap 使用 `request_too_large`。D1 statement/result cap 映射到 D1 `limit-exceeded`。

如果 route/body protocol 变化，应在同一改动里更新 owning module doc、行为测试和相关 source-scan contract。

## 安全边界

安全边界是跨语言合同，不是某个实现细节：

- Tenant worker code 永远不可信，即使它通过 typed wrapper 或 Rust service endpoint 进入平台。
- Internal mesh endpoint 是私有平台协议。不要把 runtime internal `:8088`、D1、DO、workflows、redis-proxy、Redis 或 stateful service socket 暴露到 public ingress；除非同时新增认证和授权设计。
- Gateway route resolution 不是授权。Control-plane 授权由 control/auth 的 action check 拥有。
- Hidden platform Fetcher、storage credential、secret material 和 private owner-network binding 不能变成 tenant-visible `env` 字段。
- Secret plaintext 只能出现在校验/加密期间，或 runtime env materialization 期间。At rest 的 secret value 必须是 `WDL-ENC:` envelope。
- Tenant-runtime escape 不应自动获得云凭证。Tenant-running task 使用 least-privilege task role，不能拿到宽权限基础设施 credential。

如果改动把数据移过 trust boundary，应同步更新 `docs/security.zh.md`，并增加保护该边界的测试或 style-contract guard。

## 日志和可观测性

日志、metrics 和 request id 是共享的平台 API：

- 产品成功响应使用 camelCase。日志使用 snake_case。Redis field 可以保留自己的存储语法。
- 日志可以携带有界 tenant identity 方便调试，例如 namespace、worker、version、request id、owner id 或 error code。日志不能携带 plaintext token、token hash、secret value、raw platform credential 或无界 tenant payload。
- 平台日志统一使用单行 JSON envelope：`ts`、`service`、`level`、`event`，再加 snake_case 字段。JS tier 使用 `shared/observability.js`；Rust service 使用 `wdl-rust-common::log::emit_log_line` 或其薄 wrapper。`ts` 必须使用 UTC JavaScript `Date.toISOString()` 形态（`YYYY-MM-DDTHH:mm:ss.SSSZ`）。只有 `level=error` 写 stderr；debug/info/warn 日志写 stdout。
- Metrics label 必须保持有界、低基数。有限的 machine code 可以作为 label；namespace、worker、version、token id、raw Redis key、path、raw error text 和 payload data 应进入日志，而不是 label。
- Metric cardinality warning 是 metric registry 在单个 metric name 达到 100 个 series 时输出一次的结构化 `metric_cardinality_warning` 日志。JS metrics registry 之后会丢弃该 metric 的全新 series，但继续更新已有 series；Rust 当前仍保持 warning-only tripwire。这些 guard 不是修复无界 label 的替代品。
- Request id 在传播前必须 sanitize 并限制长度。它只是关联字段；永远不要把它当成可信身份或 lock ownership。
- `LOG_LEVEL` 只控制日志输出。即使降低日志量，metrics 也必须继续可用。
- Probe route 可以抑制成功的 request-complete 日志，但必须保持 health/metrics 行为和错误日志。

Metrics label 变化、log field 重命名和 request-id propagation 变化都是 observability contract 变更。应同步更新 `docs/modules/log-tail-observability.zh.md`，以及依赖该 shape 的测试或 dashboard。

## 共享原语规则

重复原语应收敛到最小的中立 owner：

- workerd tier 间共享的 JS 原语应放在 `shared/`，除非 isolate embedding 或 trust boundary 要求本地副本。
- Rust service 间共享的原语只有在它很小、语义明确、且确实是跨 crate 合同时才进入 `wdl-rust-common`。
- 镜像生产 helper 的测试 stub 必须 import 共享 stub，或保持 production-faithful。data URL fixture 不代表可以 fork 行为。

不要只为了删几行本地代码而创建 shared helper。只有当它移除了重复 policy、重复 key grammar、重复 error mapping，或移除了 reviewer 必须记住的漂移模式时，才值得共享。

## Fail-Closed 校验

服务端校验是 canonical。CLI 和测试可以保留廉价 fail-fast 检查，但不能变成第二套接受/拒绝规则不同的 normalizer。

对畸形持久化状态，优先 fail closed，而不是有损归一化。对 index 和 projection，应记录权威记录和 stale cleanup path。对 lifecycle、lease、generation 和 run-token fence，应在 owner state machine 内重校，并测试 stale claimant 路径。

## Unsafe 和未检查代码

未检查代码必须在调用点说明原因：

- 生产 JS 不应使用显式 `any` JSDoc 或 `Function` typedef。使用 `unknown` 加本地 narrow，或写出最小 callable/object shape。
- Rust service 逻辑不应使用 `unsafe`。确实需要的 platform boundary 调用（例如 process signal）必须有 `SAFETY:` 注释说明 invariant。
- Rust 测试只有在持有进程级锁并写明 `SAFETY:` 注释时，才可以用 unsafe 修改环境变量。

## Review 和验证

重构应减少真实复杂度：重复 policy、重复 contract grammar、伪兼容路径、陈旧 stub 或 review 负担。只要方向内聚且测试覆盖被触碰的合同，大提交也可以接受。

仓库中的 active 文档只描述最终可部署状态。分支过程、已放弃方案和 review 决策应留在 PR 或 changelog，不应写入 normative module doc。测试应断言可观察行为、正确性负例，以及持久 wire、storage、security 或 deployment 合同；除非某种结构本身就是受支持合同，否则不要固定 helper 选择、源码布局或其他重构手段。

先运行能保护改动行为的最小检查；当改动跨模块或跨语言时再扩大。跨语言改动通常需要：

- workerd tier 的 JS type/unit/style 检查。
- 被触碰 Rust crate 的 fmt/check/test/clippy。
- 针对共享 Redis、wire、error 或 metric 合同的 source-scan 或行为测试。
- 当 runtime 行为、部署形态或 state machine 变化时，跑定向集成。

协议相关的 integration matrix 见 `docs/protocol-contracts.zh.md`；metadata、Redis payload、internal envelope、binding materialization 或 state-machine fence 变化时按该矩阵选择检查。

# 架构概览

本文是 WDL 的高层地图，说明部署单元、信任边界、状态所有权和主要请求路径。模块文档会展开每个部分的细节。当前代码和测试仍然是事实来源；修改行为前先用本文定位应阅读的模块文档。

面向 coding agent 的仓库级核心不变量见 [CLAUDE.md](../CLAUDE.md)。

## 系统形态

WDL 是基于 stock Cloudflare workerd 的自托管多租户 Workers 平台，不 patch workerd。用户 bundle 存在 Valkey/Redis 中，runtime 通过 workerd `workerLoader` API 动态加载。每个 tenant request 都绑定到 namespace；namespace 也是 routing、binding、secret 和 lifecycle index 使用的 account 边界。

平台分为几类服务：

- Gateway：公开入口和 admin-host 入口。
- Runtime pools：user-runtime 和 system-runtime workerd 服务。
- Stateful runtimes：d1-runtime 和 do-runtime workerd 服务，使用 EFS 上的 localDisk。
- Rust services：scheduler、workflows、redis-proxy sidecar 和 supervisor。
- Control/auth：通过 system-runtime 承载的静态 workerd worker。
- Data stores：Valkey logical DB、D1/DO 的 EFS localDisk、assets/R2/cleanup 使用的 S3-compatible object storage。

可部署的 app service 清单：

- `gateway`：公开入口上的 workerd container，负责 tenant/admin-host routing，但不负责 control-plane authorization。
- `user-runtime`：tenant worker 的 workerd runtime pool，带本地 `redis-proxy` sidecar，暴露 loader socket 和私有 internal dispatch socket。
- `system-runtime`：control/auth/static system worker 以及特权 `__system__` loaded worker 的 workerd runtime pool，同样带本地 `redis-proxy` sidecar。
- `d1-runtime`：D1 SQLite 执行的 workerd service。`supervisor` 作为 PID 1，负责本地 drain/renew 编排，并把 workerd 作为 child process 启动。
- `do-runtime`：Durable Object facet 执行的 workerd service。`supervisor` 作为 PID 1，负责本地 drain/renew 编排，并把 workerd 作为 child process 启动。
- `scheduler`：Rust service，负责 cron、queue 和 workflow tick dispatch。
- `workflows`：Rust service，拥有 Valkey DB 2 中的 workflow instance state。

本地 compose 中这些服务与 Valkey/Redis 和 `s3mock` 一起运行。Production-shaped 环境会把这些依赖替换成 managed 或 provisioned 等价物。当 owner module 的并发合同允许时，每个 service 都可以多副本运行；D1/DO 超过 1 个 task 时需要 per-replica storage identity。

Production HA 模型是单区域、基于 replica 的模型。WDL 不提供 global edge control plane 或跨区域复制，但在一个 operator-owned region 内提供明确 recovery contract：无状态 service family 可以放在 service discovery 后多副本运行，stateful owner 由 Redis lease 和 generation fence 保护，scheduler projection 可修复，workflows 进度由 DB 2 lease 和 run token 保护。因此 task 或 pod replacement 应是可恢复事件，而不是 metadata mutation。

关键 socket 和端口：

- Gateway public/admin ingress：`:8080`。
- Runtime loader socket：`:8081`。
- Scheduler/workflows dispatch 使用的 runtime internal socket：`:8088`。
- System-runtime 上的 control worker：`:8082`。
- D1 runtime：`:8787`。
- DO runtime：`:8788`。
- Workflows service：`:9120`。

源码布局跟 service boundary 对齐：

- JavaScript/workerd tiers：`gateway/`、`runtime/`、`d1-runtime/`、`do-runtime/`、`control/`、`auth/`、`shared/`、`system-workers/`、`test-workers/` 和 `examples/`。
- Rust workspace：`rust/redis-proxy/`、`rust/scheduler/`、`rust/supervisor/`、`rust/workflows/` 和 `rust/common/`。

Workerd tier 使用 `index.js` 作为 entrypoint，`config*.capnp` 作为 workerd config，并在适合单测时把纯 helper 放到 `lib.js`。Tier-local `runtime.js` 负责 Redis、cache、subscriber 或 logging mechanics，让 entry file 聚焦 dispatch。`shared/` 通过 `embed "../shared/*"` 嵌入 workerd config；预打包 npm dependency 放在 `shared/vendor/` 下，因此 workerd 不解析 `node_modules`。

各模块的当前细节在这里：

- [Gateway](modules/gateway.zh.md)
- [Runtime loader 和 bindings](modules/runtime.zh.md)
- [Control 和 auth](modules/control-auth.zh.md)
- [Durable Objects](modules/durable-objects.zh.md)
- [D1](modules/d1.zh.md)
- [Queues 和 cron](modules/queues-cron.zh.md)
- [Workflows](modules/workflows.zh.md)
- [Log tail 和 observability](modules/log-tail-observability.zh.md)
- [Infra 和 deployment](modules/infra.zh.md)

按 feature 查看 Cloudflare Workers 兼容度时，阅读 [Workers 兼容矩阵](compatibility.zh.md)。跨模块 trust zone 和 internal mesh 假设见 [安全模型](security.zh.md)。架构概览说明 service shape；这些文档分别记录兼容性和安全口径。

## 主要请求路径

Tenant HTTP/WebSocket 流量：

1. Client 进入 gateway public socket。
2. Gateway 从 Redis 解析 subdomain route 或 pattern route。
3. Gateway 带 `x-worker-id` 转发到 runtime loader socket。
4. Runtime 通过 `workerLoader` 加载 immutable worker bundle。
5. Runtime materialize bindings 并调用 worker。

WebSocket upgrade 先落到 gateway 的 WebSocket holder Durable Object，再转发给 backend，避免长生命周期 `101` response 留在普通 gateway request IoContext 上。

Admin/control 流量：

1. Client 进入 control URL。
2. Gateway 的 `ADMIN_HOST` 分支转发到 system-runtime 中的 control。
3. Control 调 auth 校验 token 和 action。
4. Control handler 修改 Redis metadata、object storage 或 service-specific control API。

Cron 和 queue dispatch：

1. Control 在 promote 时把 cron 和 queue consumer projection 写入 Redis。
2. Scheduler 发现 due work。
3. Scheduler 调 runtime 私有 internal socket `:8088`。
4. Runtime 调 loaded worker 的 `scheduled()` 或 `queue()`。

Stateful binding 调用：

- D1 facade 调 d1-runtime。D1 ownership 按 physical database 划分，并用 owner generation fence。
- Durable Object facade 调 do-runtime。DO ownership 按 owner scope 划分，native facet storage 由 owner lease + generation 保护。
- Workflow facade 调 workflows。workflows 拥有 DB 2 instance state，并通过 `:8088` 把 run dispatch 回 runtime。
- Queue producer 经 redis-proxy 写 DB 1，并先经过 runtime-side cap。
- R2 在 runtime 使用平台 S3-compatible credential 访问可变 tenant object data。
- ASSETS 是 control 在 deploy 时上传的 immutable deploy artifact；runtime 只根据 bundle metadata 和 `ASSETS_CDN_BASE` 构造 tokenized CDN URL。
- KV 使用 redis-proxy DB 1 key family；service 和 platform binding 是 isolate 内 JSRPC surface，其 ACL 和 target metadata 由 control/runtime 解析。

## 信任边界

Gateway 负责路由，不负责授权。Control/auth 负责控制面授权。Runtime wrapper 负责 tenant env shaping 和 hidden binding stripping。

特权 runtime entrypoint 通过 socket 隔离，而不是通过公开 path 保留。Tenant 流量走 runtime loader socket；scheduler 和 workflows dispatch 走私有 runtime internal socket `:8088`。

DO、D1、workflows backend 等 hidden Fetcher binding 是平台 plumbing，不能暴露给用户代码。Runtime wrapper 必须在用户代码观察 `env` 前删除这些 binding。

Tenant-running Fargate task role 必须保持 least-privilege；tenant code 不能通过 task metadata 拿到宽权限云凭证。

## 状态所有权

Valkey logical DB 按 authority 切分：

- DB 0：control-plane metadata、route state、lifecycle index、secrets、referrer index、D1/DO metadata、cron config、queue consumer projection。
- DB 1：data-plane KV、queue streams、delayed queues、log-tail streams、cleanup queues。
- DB 2：workflows workflow instance state。

Index 通常是可重建 projection。Authority 在每个模块文档列出的 owning hash、stream 或 lifecycle record 中。新增 fallback SCAN 或 secondary writer 前，必须说明 index 是权威还是可修复。

## 失败模型

WDL 优先使用显式 fence，而不是依赖隐式顺序：

- Control route 和 lifecycle 写入使用 WATCH/MULTI 边界。
- D1/DO owner record 包含 task identity 和 monotonic generation。
- DO alarm 区分 SQLite row token 和 Workflows DB2 run token。
- Workflow execution commit 使用 generation/run-token fence，lifecycle commit 只用并轮换 generation。
- Queue 和 cron scheduler index 是非权威、可修复 projection。

Replica failover 也遵循这些 fence。Gateway/runtime replacement 除了本地 cache 和已加载 isolate 之外是无状态的。D1 failover 按 physical database 发生；DO failover 按 owner scope 发生。新 owner 只有在旧 lease 消失或被 drain 释放后才 claim，并推进 generation，让 stale owner 在之后的 owner-side check fail closed。Scheduler replica 可以同时观察 due work，但 dispatch 路径使用 Redis claim 或可修复 projection，而不是把本地进程内存当作 authority。

Transport failure 和 user-code failure 要区分。除非协议明确要求，runtime handler error 不应变成 scheduler transport retry。

## Rollout 顺序

谁新增 endpoint 或先兼容新 body shape，谁先部署；调用方后部署。常见情况：

- Runtime internal `:8088` route 变化：先滚 runtime，再让 scheduler 或 workflows 调新 route。
- Runtime/control 调 workflows 的 API shape 变化：先滚 workflows，再滚调用方。
- Binding facade protocol 变化：runtime 与对应 stateful runtime 或 Rust service 一起滚。
- Redis key ownership 变化：writer、reader 和 style-contract test 在同一个 deployable boundary 中更新。

环境级 rollout 规则见 [Infra 和 deployment](modules/infra.zh.md)。

## 开发标准

- [项目全局标准](project-standards.zh.md) 覆盖跨语言合同、安全边界、可观测性、JS、Rust、测试、文档和部署代码。
- [Workerd JavaScript 标准](workerd-js-standards.zh.md) 覆盖 gateway、runtime、control、auth、d1-runtime、do-runtime、shared JS 和 JS 测试。
- [Rust service 和 sidecar 标准](rust-sidecar-standards.zh.md) 覆盖 `rust/` Cargo workspace 中的 scheduler、workflows、redis-proxy、supervisor 和共享的 `rust/common/` primitive。

两套标准遵循同一条 refactor 纪律：先定义一个可独立部署的边界，让测试保护真实合同，stage 后 review，运行能覆盖该行为的最小检查。

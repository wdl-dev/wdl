# Gateway

## 目的

Gateway 是公开数据面入口，也是 admin-host 入口 shim。它把 tenant HTTP/WebSocket 流量路由到正确 runtime pool，并把 admin-host 流量转发给 control；control 不需要知道 gateway 副本拓扑。

## 当前实现

workerd 入口是 `gateway/index.js`。纯 route 解析在 `gateway/dispatch.js` 和 `gateway/lib.js`；Redis/cache/subscriber 逻辑在 `gateway/runtime.js`；WebSocket 生命周期在 `gateway/holder.js` 和 `gateway/websocket.js`。

Gateway 有三条 dispatch 分支：

- 归一化并转小写后的 host 等于 `env.ADMIN_HOST`：短路到 `env.CONTROL.fetch()`。这个分支不查 namespace 或 route Redis state，因此 admin-host 请求在 route cache 漂移、route lookup outage 或 DB 0 `FLUSHALL` 恢复工作中仍可到达 control。Auth 和大多数 control 操作仍依赖 Redis；相关 Redis state 不可用时会 fail closed。
- `<ns>.<PLATFORM_DOMAIN>/<worker>/<path>`：从 `routes:<ns>` 做 subdomain route lookup。
- Pattern host：先从 `declared-hosts` 做 declared-host gate，再从 `patterns:<host>` 做 longest-prefix slot matching。

解析出的 `{ ns, worker, version }` 会转成 runtime 请求头 `x-worker-id: <ns>:<worker>:<version>` 和 `x-worker-prefix`。字面量 `__system__` route 进入 `RUNTIME_SYSTEM`；普通 tenant namespace 进入 `RUNTIME_USER`。

转发前，gateway 会清除客户端提供的 `x-worker-id`、`x-worker-prefix` 和所有 `x-wdl-*` header。同一套内部 header 策略也会过滤所有转发响应，包括失败和成功的 WebSocket upgrade，再将其返回公开 socket。

`ADMIN_HOST` 分支是 infrastructure traffic，不是 loaded-worker request。它不设置 `x-worker-id` 或 `x-worker-prefix`。`PLATFORM_DOMAIN` 和 `ADMIN_HOST` 可通过环境变量配置。各 routing tier 会把 `PLATFORM_DOMAIN` normalize 成最多 126 bytes、final label 仅包含字母的 ALB-compatible ASCII DNS hostname；单个尾点会被移除，结果转成小写，并默认使用 `workers.local`。Control 的 `/whoami` 只使用显式配置的值，绝不广告 `workers.local` 默认值。Admin-host short-circuit 默认仍未设置。

## 接口

- 公开 HTTP socket：`:8080`。
- Health 和 metrics：公开 listener 上的根 `/healthz` 和 `/_metrics` 是 gateway 保留路径。
- Admin-host forwarding：`ADMIN_HOST` 转发到 control。
- 数据面 forwarding：进入 runtime loader socket，而不是 runtime internal dispatch socket。
- WebSocket upgrade：移入 `GatewayWsHolder` Durable Object，避免长生命周期 101 响应挂在普通 gateway request IoContext 上。

## Routing 和 Cache 模型

Gateway 没有控制面权威。它只是把 Redis route state 投影成一个小的本地 routing cache：

- 每个请求先归一化并转小写 URL host。`ADMIN_HOST` 分支绕过 route Redis state，通过 `env.CONTROL.fetch()` 转发到 control/auth。
- Subdomain routing 先拒绝 reserved namespace，再检查 `namespaces` 和 `routes:<ns>`。转发到 runtime 前会去掉最前面的 worker segment，因此 tenant code 看到的是 worker name 后面的 path。
- Pattern routing 先检查 `declared-hosts`，再读取 `patterns:<host>` 并选择最长匹配的 path slot。这个 gate 只回答“这个 host 是否被任意 namespace 声明过”，不分配 host owner。Ownership 和 conflict check 仍由 active `patterns:<host>` projection 编码。
- Runtime pool selection 是精确匹配：只有字面量 `__system__` route 使用 `RUNTIME_SYSTEM`。未来如果有新的 reserved namespace 要进入 system-runtime，必须显式 opt in；不要改成泛化的 reserved-prefix 匹配。
- Route 和 pattern cache 是每个 gateway isolate 内的有界性能 cache。它们不是事实来源；Redis 才是当前 route source of truth。
- `routes:invalidate`、`patterns:invalidate` 和 `routes:flush` 是非持久 pub/sub hint。Gateway 在 subscriber connect/disconnect 时清 cache，因此漏掉消息后下一次 lookup 会重新读 Redis 修复。
- Pattern host ownership 移动会 publish `patterns:invalidate`，但这个 hint 仍然非持久。Gateway 如果错过 pub/sub message，可能继续从有界内存 cache 提供旧的 `patterns:<host>` projection，直到 subscriber reconnect 或 process restart 清空 cache；这是已接受的 stale-cache window，不是持久授权记录。
- WebSocket upgrade 使用和 HTTP 相同的 route resolution，然后把公开 socket 交给 `GatewayWsHolder`。Holder 负责 backend reconnect 尝试和有界 client-frame buffer；rolling gateway 或 runtime 仍可能断开物理 client connection。

## Redis / Storage 合同

Gateway 读取：

```text
namespaces               Set, active namespace gate
declared-hosts           Set, 任意 namespace 声明过的 custom/pattern host
routes:<ns>              Hash, worker name -> active version
patterns:<host>          Hash, path slot -> v2 tab-separated projection
```

Gateway 订阅：

```text
routes:invalidate        payload = namespace
routes:flush             payload ignored
patterns:invalidate      payload = host or "*"
```

Control 写 Redis 并 publish invalidation。Gateway 不反向调用 control 查询 route 是否变化。

## Ownership / 并发 / 失败语义

- Route cache 是 pull-triggered，并且能自愈。
- Gateway 在 subscriber connect 和 disconnect 时清 route/pattern cache，因为 pub/sub 消息不持久。
- Subscriber reconnect 会清本地 cache，下一次请求重新读 Redis；漏掉 invalidation 最多导致有界 stale cache，不会永久漂移。
- Namespace 之间的 pattern-host 重分配也有同样的非持久 hint window：普通 control writer 会 publish invalidation，但只有 gateway 丢弃或刷新本地 cache 后，Redis 权威状态才会生效。
- 数据面 route lookup 遇到 Redis outage 会表现为 gateway failure；admin-host forwarding 不依赖 route Redis 状态。
- Pattern 分支保持原始 path；subdomain 分支会去掉最前面的 worker segment。
- WebSocket backend reconnect 有上限，并且 client-frame buffer 有上限。

## 安全边界

- Reserved namespace 在 subdomain 分支里总是在 route lookup 前被拒绝。
- Public system route 白名单只适用于 pattern route；当前只有字面量 `__system__` 的 pattern route 会进入 `RUNTIME_SYSTEM`。
- Platform-tier namespace 是 resource-shaped，应该通过 binding 访问，而不是公开 subdomain。
- Gateway 不应保留 tenant path，例如 `/_scheduled` 或 `/_queued`。特权 runtime endpoint 在 runtime `:8088` 上，不靠 gateway path filter。
- Gateway 按 namespace 字面量选择 runtime pool，不按 reserved prefix 泛化匹配。
- Admin-host routing 只负责把请求送到 control；认证仍在 control/auth 内完成。
- 匹配 reserved namespace 的 host 必须进入 subdomain 分支，并在那里被拒绝；不要让 reserved namespace host 落到 pattern routing，变成普通的 "no route matches" 流量。

## 可观测性

Gateway 输出包含 request id、route context 和 outcome 的 request log。Metrics 只使用有界 label；namespace、worker、version、path 细节进日志，不进 metric label。

`/healthz` 和 `/_metrics` 会在公开 gateway listener 上、host 分类前返回。这是有意设计：load balancer 需要不依赖 route 的健康探针，而 gateway metrics 描述的是 ingress 进程，不是某个 tenant worker。这两个根路径是 gateway 全局保留路径，因此名为 `healthz` 或 `_metrics` 的 tenant worker 不能通过 subdomain routing 占用自己的根 fetch path；但另一个 worker 下的 `/app/_metrics` 这类路径仍是普通 tenant fetch。Gateway metrics 因此必须保持适合公开 data-plane socket：可以暴露有界的 service、route-stage、outcome、binding、websocket-state、Redis-command 和 cache size 信号，但不得暴露 namespace、worker、version、request path、token、secret、raw host、raw error text 或其它 tenant-controlled label。如果某个部署认为运营流量或 cache state 也敏感，应在 ingress、load balancer 或 service-mesh 层屏蔽 `/_metrics`，同时保留 `/healthz` 用于 readiness。

## 部署 / Rollout 注意事项

- 不改变 forwarded header 合同时，gateway 的 route-cache 或 request-parsing 改动可以独立 rolling。
- runtime internal socket path 的变化不应通过 gateway path filtering 实现。
- Route invalidation channel 改动必须与 control 对齐；style-contract 测试会保护这些字面量。

## 保护该模块的测试

- `tests/unit/gateway-dispatch.test.js`
- `tests/unit/gateway-lib.test.js`
- `tests/unit/gateway-runtime.test.js`
- `tests/unit/gateway-websocket.test.js`
- `tests/unit/gateway-holder.test.js`
- `tests/integration/gateway.test.js`
- `tests/integration/routing-gateway.test.js`
- `tests/unit/style-contracts.test.js`

## 已知约束和非目标

- Gateway 没有每个副本的同步 invalidation ack。
- Gateway 不是 control API 的授权层。
- Worker 加载之后，D1、DO、queues、cron、workflows 的路由不由 gateway 负责。

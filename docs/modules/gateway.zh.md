# Gateway

## 目的

Gateway 是公开数据面入口，也是 admin-host 入口 shim。它把 tenant HTTP/WebSocket 流量路由到正确 runtime pool，并把 admin-host 流量转发给 control；control 不需要知道 gateway 副本拓扑。

## 当前实现

workerd 入口是 `gateway/index.js`。纯 route 解析在 `gateway/dispatch.js` 和 `gateway/lib.js`；静态 routing option memoization 及 Redis/cache/subscriber 逻辑在 `gateway/runtime.js`；进程内 WebSocket lifecycle admission/reconciliation 在 `gateway/websocket-lifecycle.js`；WebSocket transport management 在 `gateway/websocket.js`。

Gateway 有三条 dispatch 分支：

- 归一化并转小写后的 host 等于 `env.ADMIN_HOST`：短路到 `env.CONTROL.fetch()`。这个分支不查 namespace 或 route Redis state，因此 admin-host 请求在 route cache 漂移、route lookup outage 或 DB 0 `FLUSHALL` 恢复工作中仍可到达 control。Auth 和大多数 control 操作仍依赖 Redis；相关 Redis state 不可用时会 fail closed。
- `<ns>.<PLATFORM_DOMAIN>/<worker>/<path>`：从 `routes:<ns>` 做 subdomain route lookup，并排除 `platform-domain-disabled:<ns>` 中的 worker。
- Pattern host：先从 `declared-hosts` 做 declared-host gate，再从 `patterns:<host>` 做 longest-prefix slot matching。

解析出的 `{ ns, worker, version }` 会转成 runtime 请求头 `x-worker-id: <ns>:<worker>:<version>` 和 `x-worker-prefix`。字面量 `__system__` route 进入 `RUNTIME_SYSTEM`；普通 tenant namespace 进入 `RUNTIME_USER`。

转发前，gateway 会清除客户端提供的 `x-worker-id`、`x-worker-prefix` 和所有 `x-wdl-*` header。同一套内部 header 策略也会过滤所有转发响应，包括失败和成功的 WebSocket upgrade，再将其返回公开 socket。

`ADMIN_HOST` 分支是 infrastructure traffic，不是 loaded-worker request。它不设置 `x-worker-id` 或 `x-worker-prefix`。`PLATFORM_DOMAIN` 和 `ADMIN_HOST` 可通过环境变量配置。各 routing tier 会把 `PLATFORM_DOMAIN` normalize 成最多 126 bytes、final label 仅包含字母的 ALB-compatible ASCII DNS hostname；单个尾点会被移除，结果转成小写，并默认使用 `workers.local`。Control 的 `/whoami` 只使用显式配置的值，绝不广告 `workers.local` 默认值。Admin-host short-circuit 默认仍未设置。

## 接口

- 公开 HTTP socket：`:8080`。
- Health 和 metrics：公开 listener 上的根 `/healthz` 和 `/_metrics` 是 gateway 保留路径。
- Admin-host forwarding：`ADMIN_HOST` 转发到 control。
- 数据面 forwarding：进入 runtime loader socket，而不是 runtime internal dispatch socket。
- WebSocket upgrade：gateway 在本地终结公开 `WebSocketPair`，并直接通过解析出的 runtime binding 代理。

## Routing 和 Cache 模型

Gateway 没有控制面权威。它只是把 Redis route state 投影成一个小的本地 routing cache：

- 每个请求先归一化并转小写 URL host。`ADMIN_HOST` 分支绕过 route Redis state，通过 `env.CONTROL.fetch()` 转发到 control/auth。
- Subdomain routing 先拒绝 reserved namespace，再检查 `namespaces`、`routes:<ns>` 和显式 opt-out 的 `platform-domain-disabled:<ns>` set。转发到 runtime 前会去掉最前面的 worker segment，因此 tenant code 看到的是 worker name 后面的 path。Worker opt out 后，pattern routing 仍保持 active。
- Pattern routing 先检查 `declared-hosts`，再读取 `patterns:<host>` 并选择最长匹配的 path slot。这个 gate 只回答“这个 host 是否被任意 namespace 声明过”，不分配 host owner。Ownership 和 conflict check 仍由 active `patterns:<host>` projection 编码。
- Runtime pool selection 是精确匹配：只有字面量 `__system__` route 使用 `RUNTIME_SYSTEM`。未来如果有新的 reserved namespace 要进入 system-runtime，必须显式 opt in；不要改成泛化的 reserved-prefix 匹配。
- Route 和 pattern cache 是每个 gateway isolate 内的有界性能 cache。它们不是事实来源；Redis 才是当前 route source of truth。
- `routes:invalidate`、`patterns:invalidate`、`routes:flush`、`session-policy:restart` 和 `worker:delete` 是非持久 pub/sub hint。Route/pattern event 只清对应 cache；精确 restart/delete event 只请求对应 worker group 的权威 reconciliation，subscriber reconnect 才 reconcile 全部本地 group。请求按 group 合并，瞬态失败也只重试失败 group。Gateway 在 subscriber connect/disconnect 时清 routing cache。
- Route invalidation 没有 cluster-wide acknowledgement barrier。Promote 后，warm Gateway 在观察到 `routes:invalidate` 前可能短暂把普通请求 admission 到前一个 immutable version；Runtime 不会为该请求重新检查 active state，bundled workerd 也允许已 admission 的调用在 sibling cache eviction 后自然 drain。WebSocket lifecycle snapshot 为 initial upgrade 和 backend reconnect 提供各自的 active-state admission point，但不会把普通 HTTP routing 线性化。
- Pattern host ownership 移动会 publish `patterns:invalidate`，但这个 hint 仍然非持久。Gateway 如果错过 pub/sub message，可能继续从有界内存 cache 提供旧的 `patterns:<host>` projection，直到 subscriber reconnect 或 process restart 清空 cache；这是已接受的 stale-cache window，不是持久授权记录。
- WebSocket upgrade 使用和 HTTP 相同的 route resolution。Gateway 在本地终结公开 socket，并直接通过解析出的 runtime binding 代理。Proxy 负责 backend reconnect 尝试和有界 client-frame buffer，并在 WebSocket lifecycle boundary 原子读取 active route 和 `worker:session-policy:<ns>:<worker>` projection。健康连接可以继续在原 immutable version 上 drain；异常 backend loss 后，只有该 version 仍 active 时才会透明重连。Active version 变化，或当前 `restart` projection 出现更高 sequence 时，公开连接会以 `1012` 关闭。Rolling gateway 或 runtime 仍可能断开物理 client connection。

## Redis / Storage 合同

Gateway 读取：

```text
namespaces               Set, active namespace gate
declared-hosts           Set, 任意 namespace 声明过的 custom/pattern host
routes:<ns>              Hash, worker name -> active version
worker:session-policy:<ns>:<worker>
                         String, active session policy version/mode/sequence projection
platform-domain-disabled:<ns>
                         Set, 不经 platform-domain 分支公开的 worker
patterns:<host>          Hash, path slot -> v2 tab-separated projection
```

Gateway 订阅：

```text
routes:invalidate        payload = namespace
routes:flush             payload ignored
patterns:invalidate      payload = host or "*"
session-policy:restart   payload = {ns,worker,version,restartSequence}
worker:delete            payload = {ns,worker}
```

Control 写 Redis 并 publish invalidation。Gateway 不反向调用 control 查询 route 是否变化。

## Ownership / 并发 / 失败语义

- Route cache 是 pull-triggered，并且能自愈。
- Gateway 在 subscriber connect 和 disconnect 时清 route/pattern cache，因为 pub/sub 消息不持久。
- Subscriber reconnect 会清本地 cache，下一次请求重新读 Redis；漏掉 invalidation 最多导致有界 stale cache，不会永久漂移。
- Gateway 还会按 namespace/worker 维护本进程 active public WebSocket session registry。`session-policy:restart` 只为 restart sequence 较旧的 session 请求一次对应 worker 的权威 reconciliation，`worker:delete` 对成功 whole-worker delete 做同样处理；两者都不会仅凭 event 内容关闭连接。Delete reconciliation 读取到 worker inactive 时，已建立 session 会以 `1012` 关闭；如果同名 worker 在一次成功的权威读取前已经完成重建，最新 projection 生效，当前状态无法证明中间发生过删除。完全消除这个已接受窗口需要 durable incarnation fence。Subscriber 只 settle 在 WebSocket request 中创建的 lifecycle signal，workerd 会先把 continuation 恢复到该 WebSocket 的 IoContext，再由 Gateway 操作两端 peer。Subscriber reconnect 时，Gateway 以有界并发重读全部本地 group；后续 `preserve` projection 会在同一 sequence 上覆盖尚未观察到的 restart。传输失败会保留健康 session，并按有界退避仅重试失败 group；只有畸形或回退的权威状态才会以 `1011` 关闭受影响 session。
- Membership gate 读取会在该 gate 变化时重新开始。Gate 已就绪后，冷 route 或 pattern projection 读取由对应 namespace/host 与全量 reset generation 共同保护：同 key invalidation 或全量 reset 会丢弃回复，无关 key invalidation 不会。Per-key generation 只在读取进行期间存在。Gateway 最多尝试五个 snapshot，之后返回 `503 gateway_routing_unavailable`。
- Namespace 之间的 pattern-host 重分配也有同样的非持久 hint window：普通 control writer 会 publish invalidation，但只有 gateway 丢弃或刷新本地 cache 后，Redis 权威状态才会生效。
- 数据面 route lookup 遇到 Redis outage 会表现为 gateway failure；admin-host forwarding 不依赖 route Redis 状态。
- Pattern 分支保持原始 path；subdomain 分支会去掉最前面的 worker segment。
- WebSocket backend reconnect 有上限，并且 client-frame buffer 有上限。
- Route 解析后，Gateway 会在同一个 Redis linearization point 读取 route 和 session policy projection。Initial route mismatch 会在 backend upgrade 前返回 `503 gateway_routing_unavailable`；这是有意让 client 在 stale route-cache hit 后重试完整 upgrade，而不是引入第二条 route-resolution path 或 admission 已失效的 immutable version。Upgrade 后的第二次检查构成该 backend 的 active-state admission point。异常 backend loss 后，Gateway 会在 retry 前检查，并在挂接 replacement backend 前再次验证成功的 upgrade；initial/replacement backend socket 在检查期间仍由该 request 持有，因此 terminal lifecycle signal 可以立即关闭它。如果 `preserve` promotion 在这次最终 snapshot 之后才提交，刚 admission 的 backend 仍可继续 drain。Gateway 不会增加跨服务 barrier，也不会逐 frame 执行 lifecycle check。Active route 旁缺失 session policy projection 表示 default `preserve`；route 和 projection 同时缺失表示 worker inactive，initial admission 返回 `503`，已建立 session 以 `1012` 关闭；malformed 或 torn state 会让已建立 session 以 `1011` fail closed。
- 正常和 application-terminal upstream Close frame 会直接传播，不再读取 lifecycle state。Gateway 仅把 `1001`、synthetic `1006` 和 `1011` 视为可重连 backend-loss signal；protocol/policy/resource close 及应用 `3xxx`/`4xxx` close 会送达 public peer，而不会静默启动新 backend session。可重连 loss 后，只要 active version 未变化，并且 sequence 未变化或当前 projection 为 `preserve`，Gateway 就会透明重连同一个 pinned worker id；active version 变化，或当前 projection 为 `restart` 且 sequence 增长时，Gateway 会以 `1012 service restart` 关闭 public/backend peers。Lifecycle command 带主动关闭 socket 的两秒 deadline。Redis transport failure 和 transient reply code（`BUSY`、`CLUSTERDOWN`、`LOADING`、`MASTERDOWN`、`READONLY`、`TRYAGAIN`）会在配置的 reconnect schedule 内重试；malformed persisted state、非 transient Redis reply error、sequence 回退或 retry 耗尽时，Gateway 才会以 `1011` 关闭两端，不会重连 stale state。
- `1012` 关闭会结束应用会话；client 需要重连并重新执行应用握手。Gateway 重置 backend reconnect epoch 时，旧 epoch 下排队的 client message 可能被丢弃，且没有逐帧 ack/nack。
- Gateway 自有 WebSocket peer 使用 `arraybuffer` 接收二进制消息，使文本帧和二进制帧都能保持原类型转发；tenant WebSocket 代码仍遵循 workerd 的常规 `binaryType` 合同。
- Client close 和 error event 会同时终止公开 WebSocket pair 与当前 backend socket。Backend Close frame 使用 workerd 默认的 reciprocal Close 处理，不会在正常关闭或重连时留下半开旧 backend socket。无状态码 Close frame 仍保持无状态码；不能出现在 wire 上的异常 close code 会按 `1011` 转发，转发 reason 遵守 WebSocket 的 123-byte UTF-8 上限。

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
- `tests/integration/gateway.test.js`
- `tests/integration/gateway-websocket.test.js`
- `tests/integration/routing-gateway.test.js`
- `tests/unit/style-contracts.test.js`

## 已知约束和非目标

- Gateway 没有每个副本的同步 invalidation ack。
- Gateway 不是 control API 的授权层。
- Worker 加载之后，D1、DO、queues、cron、workflows 的路由不由 gateway 负责。
- WebSocket lifecycle check 不是逐帧 owner fence。`preserve` 允许健康的旧版本连接自然 drain，但 active route 变化且 backend 脱离后不会重新加载该版本；`restart` 还会立即关闭较旧的公开 session，stale DO facet 则在下一次 dispatch 时 lazy 收敛。

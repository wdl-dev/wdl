# Log Tail 和 Observability

## 目的

Observability 提供有界 metrics、结构化日志、request-id 传播和 live tenant log tail，同时不把 Redis Streams 当成 audit log。

## 当前实现

共享 JS primitive 在 `shared/observability.js`。Runtime tailing 使用：

- `runtime/tail-worker.js` 捕获 console/exception。
- `runtime/tail-forwarder.js` 做 active-set check 和 append POST。
- 同一个 runtime forwarder 统一持有 invocation start/finish envelope timing 和 thrown-value normalization，因此 gateway dispatch 与 service-binding fetch 会输出同一种 `worker_fetch` shape。
- `redis-proxy` logs endpoints 做 active tail check 和 stream append。
- `control/handlers/logs-tail.js` 管理 SSE session 和 heartbeat activation。
- CLI `wdl tail` 给用户消费。

Rust 服务（`scheduler`、`redis-proxy`、`workflows`、`supervisor`）使用结构化 JSON logs 和各自的 Prometheus metrics。

## 接口

- Runtime 通过 workerd tails 捕获 loaded-worker 日志。
- Control SSE endpoint 服务 `wdl tail`。
- `redis-proxy` active-set 和 append APIs。

## Tail 投递模型

Live tail 是 activation-gated pipe，不是持久日志系统：

- workerd tails 把 console、exception、fetch、scheduled 和 queue event 交给 runtime tail worker。Runtime 始终保留结构化 stdout 作为持久平台日志路径。
- `runtime/tail-forwarder.js` append 前会检查 redis-proxy `/logs/tail/active`。Active-set 的命中和未命中都会短暂 cache，避免 inactive worker 每个 event 都付出 Redis write。
- Control 为每个 SSE tail session 做授权，在 `logs:tail:active` 写入/刷新 worker gate，读取 `logs:<ns>:<worker>:s`，并输出 SSE frame。Reconnect 会重新走正常 auth。
- redis-proxy 写入有界 stream entry，使用 `MAXLEN ~ 500` 并刷新 TTL。这个 stream 用来衔接 live consumer，不用于保存历史。
- 单 worker `wdl tail` 可以用 `Last-Event-ID` 在 stream 窗口内 resume。多 worker tail 是 fan-in session；reconnect 从新会话开始，因为单个 SSE cursor 无法表达每个 worker 一个 cursor。
- 与 session activation race 的 event、runtime rolling、redis-proxy failure 或慢 SSE reader 都可能导致丢 event。这是可接受的，因为 stdout/log aggregation 才是持久 observability 路径。

服务探针：

| 服务 | Health | Metrics |
|---|---|---|
| Gateway | `/healthz` | `/_metrics` |
| user-runtime / system-runtime internal | `/_healthz` | `/_metrics` |
| d1-runtime | `/healthz` | `/_metrics` |
| do-runtime | `/healthz` | `/_metrics` |
| scheduler | `/_healthz` | `/_metrics` |
| workflows | `/_healthz` | `/_metrics` |
| redis-proxy sidecars | `/_healthz` | `/_metrics` |
| control / auth | 无 | 无 |

Gateway probes 位于公开 listener。`/healthz` 为 load balancer readiness 保持公开；`/_metrics` 因为可能共享这个 listener，必须保持低基数且不包含 tenant identity label。如果某个部署认为 gateway 流量规模或 cache state 也敏感，应在 ingress 层只保护或屏蔽 `/_metrics`。

## Redis / Storage 合同

Live tail 使用 DB 1：

```text
logs:tail:active       Hash/HFE, active tail worker gates
logs:<ns>:<worker>:s   Stream, transient live-tail events
```

Tail streams 使用有界 `MAXLEN ~ 500`，并在写入时刷新 TTL。它们是 transient pipe，不是持久日志存储。

## Ownership / 并发 / 失败语义

- 结构化 stdout 是持久平台日志的事实来源。
- 没有 active tailer 时，runtime 仍输出 stdout，但在本地 active-set miss cache 后跳过 per-event stream append work。
- Active tail session 是有时限的授权租约，必须通过正常 auth reconnect。`LOG_TAIL_MAX_SESSION_MS` 设置 control-side 最大时长；非法值或空值会回退到 15 分钟。
- 当前 stock workerd 的行为（上游 issue [#6832](https://github.com/cloudflare/workerd/issues/6832) 跟踪）不会可靠地在 client disconnect 时触发 async response-body `ReadableStream.cancel()`。WDL 把它当作永久兼容边界处理：Control 的独立 watchdog 不是等待上游修复的临时 workaround。max-session watchdog 负责 reauthorization 上界，idle-pull watchdog 在 SSE body 连续三个 keepalive 周期没有被 pull 时关闭 session。活跃客户端会因为每次 heartbeat 腾出 queue 空间而自然按 keepalive 粒度继续 pull；遗弃客户端会停止 pull，因此不需要等完整 session lifetime 才清理。TCP 连接还在但应用层长时间不读的客户端可能被关闭，应自行 reconnect。
- 与 activation race 的 tail event 可以丢失。
- 高 QPS 或慢 SSE reader 可能因为 stream cap 丢中间事件。

## 安全边界

- Tail 授权在每个 control SSE session 上执行。
- Tail stream 按 namespace/worker scope。
- Metrics label 必须保持有界。Namespace、worker、version、token id、raw key、path 和 error text 进日志，不进 metric label。
- Request id 在传播前会被 sanitize 并限制长度。

## 可观测性策略

WDL 在 JS workerd tier 和 Rust 服务中使用同一套可观测性策略：日志回答“某个 request、worker、object 或 control-plane action 具体发生了什么”；metrics 回答“发生了多少次、有多慢、哪个有界类别变化了”。因此日志承载 correlation 和有界 debug identity；metrics 只承载适合 Prometheus 聚合的有界枚举 label。

中心 owner：

- JS 服务通过 `shared/observability.js`（`createLogger`、`createHttpRequestScope` 和 `recordRequestComplete`）输出平台日志。生产 JS 中直接使用 `console.*` 仅限这个 primitive，以及无法 import module 的 embedded source string。
- Rust 服务通过 `wdl-rust-common::log::emit_log_line` 或其薄 wrapper 输出日志，并通过 `wdl-rust-common::metrics::MetricStore` 暴露 metrics。
- HTTP request completion 统一由 request-scope helper 或 service middleware 记录，避免 request counter、duration summary、probe suppression、request-id field 和 `request_complete` 日志在各 tier 之间漂移。
- 服务特定 metrics 应优先使用一个 metric family，并用有界的 `outcome`、`reason`、`kind`、`mode`、`stage`、`status`、`scope`、`operation` 或有限 machine `code` label 区分结果；只有真正提供不同信号时才拆出独立 family。

## 可观测性合同

通用规则：

- `x-request-id` 尽可能跨 gateway、control/runtime、loaded worker、D1 和 Workflows 传播。缺失的 inbound id 会在入口生成；header adapter 只考虑第一个逗号分隔 token，包含 visible ASCII 以外字节、引号、反斜杠或超过 128 字节的 id 会被当成缺失。使用 `shared/request-scope.js` 的 JS entrypoint 会在 response 中 echo sanitize 后的 id，并在 `request_complete` 日志中记录 `request_id`；Rust sidecar 会 sanitize inbound id，并在 request middleware 拥有 completion 时记录。Control 的 Redis `PUBLISH` 路径只在本地日志记录该 id，不把它放进 pub/sub payload。
- 普通 loaded-worker handler 的 host facade 会直接获得 request id。持久化 class entrypoint 在 wrapped result settle 前使用一个小型可变诊断 context；wrapper 会检查 callable `then` 并可能返回 normalized Promise 以安排 cleanup，因此 concurrent 或 re-entrant class call 可能观察到另一次 invocation 的 id。do-runtime 的私有 fetch、alarm 和 RPC dispatch 会在 `x-request-id` 中携带可用 id。这只是 best-effort 的诊断关联，不是授权或完整性边界。
- 日志字段使用 snake_case。只有 `level=error` 写入 stderr；debug/info/warn JSON log line 都写入 stdout，这样 JS、Rust 和 embedded workerd shim 的日志路由保持一致。
- 内部运维日志，包括 system-worker cleanup 日志和防御性的 Redis callback warning，也使用同一套单行 JSON envelope：`ts`、`service`、`level`、`event`，再加 snake_case 字段。JS 服务使用 `shared/observability.js`；Rust 服务使用 `wdl-rust-common::log::emit_log_line`。错误文本写入 `error_message`；无法转成文本的 throwable 降级为 `Unknown error`，不能反向覆盖原始失败。不得输出 secret 值、raw credential、token material、raw Redis key 或无界 payload。
- 产品 API response body 默认使用 camelCase，除非 endpoint 明确记录不同 wire contract。
- Metrics 只使用有界枚举 label。
- 暴露 request metrics 的 Rust HTTP sidecar 使用同一组 `requests`、`request_duration_ms` 和 `request_errors` metric family，以及有界的 `service`/`route`/`status` labels；per-route error context 只进入 `request_complete` 日志中的 `error_code` / `error_message`。
- JS 和 Rust observability 实现刻意共享 metric prefix `wdl`、request metric families `requests` / `request_duration_ms` / `request_errors`、cardinality warning threshold `100`、Prometheus content type `text/plain; version=0.0.4; charset=utf-8`，以及结构化日志的 key 顺序、level priority、stream routing 和 timestamp shape；共享 fixture `tests/fixtures/observability-contract.json` pin 住这些值，但不引入 runtime observability owner。
- redis-proxy 用 `kv_value_bytes` summary 记录 KV payload size，label 只有有界的 `service`/`operation`/`kind`。它记录 value、metadata 和 raw batch byte count，用来判断是否需要 large-value offload；namespace、key 和 object identity 不进入 metric label。
- Rust `request_complete` log 输出整数 `duration_ms`，让各服务日志字段保持稳定；Prometheus duration summary 仍保留浮点值。
- JS `MetricsRegistry` 在单个 metric name 达到 100 个 series 时输出一次结构化 `metric_cardinality_warning` 日志，之后会丢弃该 metric 的全新 series，但继续更新已有 series。Rust `MetricStore` 当前仍保持同一 warning-only tripwire。该 warning 携带 metric name、观测到的 series 数和配置 limit；tenant-specific 细节本来就不应进入 label。因为这条 warning 由 metrics registry 输出，所以不会被 `LOG_LEVEL` 抑制。
- `*_max` 是单独的 gauge family，不是 Prometheus summary family 下的额外 sample。Summary 只能输出 `_count`、`_sum` 和 quantile sample。
- 成功的 probe route（`healthz`、`metrics`、`/_healthz`、`/_metrics`）会抑制 `request_complete` 日志，但 counter 仍会增加；错误仍会记录日志。
- `LOG_LEVEL` 只控制日志输出，不影响 metrics。高 QPS 部署可以设 `LOG_LEVEL=warn` 来关闭 per-request access log，同时保留 Prometheus signal。
- Service-binding trace propagation 由 caller 显式完成。`ServiceBinding#fetch` 会强制 `x-worker-id` 指向 target，但只有 caller 在 Request 上转发 `x-request-id` 时才保留它；JSRPC 不会跨 isolate 携带 Node async context。

Tail event families：

- `worker_console`
- `worker_exception`
- `worker_fetch`
- `worker_scheduled`
- `worker_queue`
- `tail_warning`

Tail identity 规则：

- fetch 请求的 `worker_console` identity 来自转发请求头，因为 workerd 对 `workerLoader` loaded worker 报告的 `scriptName=none`。
- `scheduled()` 和 `queue()` 的 console event 是没有 request shape 的 JSRPC event，因此 console tail event 会省略 `worker_id` 和 `request_id`，而不是伪造 `"unknown"`。
- Runtime 会在 invocation 边界输出显式的 `worker_fetch`、`worker_scheduled` 和 `worker_queue` start/finish event。`worker_fetch` 包含 method、worker-visible pathname、status/outcome 和 duration，不包含 host/query。
- Control 生成的 `tail_warning` SSE event 没有 Redis stream id，因此不会污染单 worker resume cursor。

## 部署 / Rollout 注意事项

- Runtime、redis-proxy 和 control 必须对 tail active/append protocol 保持一致。
- Tail 是 best-effort；rolling runtime/control 可能丢 live tail events。
- Metrics label 变化可能影响 dashboard，应视为 observability contract 变更。

## 保护该模块的测试

- `tests/unit/runtime-tail-worker.test.js`
- `tests/unit/control-logs-tail.test.js`
- `tests/unit/observability.test.js`
- `tests/integration/log-tail.test.js`
- `tests/integration/observability.test.js`
- `tests/unit/style-contracts.test.js`

## 已知约束和非目标

- `wdl tail` 不是 audit storage。
- 没有历史日志查询 API。
- Live tail 不做 server-side filtering/search。
- scheduled/queue handler 的 console event 不一定携带与 fetch event 相同的 identity fields。

# Queues 和 Cron

## 目的

Queues 和 cron 是 scheduler 负责的后台 dispatch 功能。Queues 为 Worker `queue()` handler 提供持久化、at-least-once 的后台消息投递。Cron 提供与 Cloudflare 兼容的分钟对齐 `scheduled()` 事件，语义是 best-effort，不回放 outage 期间错过的触发。

两个功能都由 worker deploy metadata 配置，由 control materialize 成 Redis projection，再由 `scheduler` 通过 runtime 的私有内部 socket `:8088` 触发。

## 当前实现

Control 负责校验 deploy payload，并把 queue/cron metadata 冻结进不可变 bundle metadata。Promote 是这些 metadata 变成 active 状态的边界：

- `control/handlers/deploy.js` 解析 `crons` 和 `queueConsumers`。
- `control/routing.js` 在 active-version WATCH/MULTI 路径里同时 promote cron projection、queue consumer projection、route state 和 lifecycle index。
- `control/lifecycle-indexes.js` 持有 JS 侧 Redis key helper：cron worker hash、cron slot ref、cron discovery index，以及 queue consumer projection。

Runtime 有两个角色：

- Queue producer binding 由 `runtime/bindings/queue.js` materialize。`env.MY_QUEUE.send()` 和 `sendBatch()` 构造有上限的 queue envelope，并通过 Rust `redis-proxy` sidecar 写入 data-plane Redis。
- Queue 和 cron 的 consumer dispatch 通过 `runtime/internal.js` 的 `:8088` 进入 runtime，然后由 `runtime/dispatch.js` 调用 workerd 原生 `scheduled()` 和 `queue()` entrypoint。

Scheduler 负责实际投递：

- Cron 代码在 `rust/scheduler/src/cron/`。
- Queue registry、consume、delayed delivery、DLQ 和 orphan 处理在 `rust/scheduler/src/queue/`。
- Scheduler 在构造 cron runtime worker id 前会用 canonical `wdl-rust-common` grammar 重新校验 projection 的 route namespace、worker name 和 version。非法 cron ref 会在 slot advance 或 runtime dispatch 前移除，cron sweep 也不会从 malformed worker metadata 重新播种 ref。Queue consumer 的 dispatch identity 也会在使用前重新校验；noncanonical identity 会使 projection 不可用，并按 consumer absent 处理，因此 backlog 可能进入 orphan stream。
- 部署上 scheduler 默认 1 个副本，当前 dispatch 路径具备多副本安全性。增加副本可以提高运行时并发，但不等于部署零中断：生产 rollout 仍可采用 stop-before-start 语义，并短暂暂停调度。

## 接口

用户侧 queue 接口：

- Wrangler producer：`[[queues.producers]]`
- Wrangler consumer：`[[queues.consumers]]`
- Runtime producer API：`env.<BINDING>.send(body, opts?)`
- Runtime producer API：`env.<BINDING>.sendBatch(messages, opts?)`
- Producer 上限是单条消息 128,000 bytes、单个 batch 100 条消息、单个 batch 总计 256,000 bytes。
- Runtime consumer handler：`export default { async queue(batch, env, ctx) {} }`
- Producer `delivery_delay` 已支持，作为该 binding 上 send 的默认 delay。
- Consumer `retry_delay` 已支持，作为没有显式 `delaySeconds` 的 retry 默认 delay。
- Consumer `max_concurrency` 当前会被拒绝。

用户侧 cron 接口：

- Wrangler simple form：`[triggers] crons = [...]`
- Runtime handler：`export default { async scheduled(event, env, ctx) {} }`

内部 runtime dispatch 接口：

- runtime internal socket `:8088` 上的 `POST /_scheduled`
- runtime internal socket `:8088` 上的 `POST /_queued`

这些内部路径是 socket-private 架构，不是 gateway 的 path reservation。Tenant worker 仍然可以定义公开路径 `/_scheduled` 或 `/_queued`；gateway 流量进入正常 loader socket，不会进入 `:8088`。

Cron trigger 和 queue consumer 是 dispatch 功能，因此 deploy 只能在 routeable namespace 声明它们：普通 tenant namespace，以及狭窄保留的 route namespace `__system__`。Platform-tier worker 是由 `[[platform_bindings]]` 选择的 cold-load target，不是 public/runtime dispatch target，不能声明 cron trigger 或 queue consumer。

## Scheduler Dispatch 模型

Scheduler 不是通用 job runner。它把 Redis projection 转成 runtime call，并让 Redis 状态判断这份 work 是否仍然 current。

Cron 使用 wall-clock 分钟 slot：

1. `wait_ms_until_next_slot()` 睡到下一个 UTC 分钟边界。tick loop 随后扫描当前 `cron-slot:<slot_ms>` bucket 和前一个 bucket。扫描前一个 bucket 是为了覆盖分钟 rollover 附近刚写入的 ref。
2. 另一条 sweep/reconcile 路径从 `cron:index:workers` 读取 active cron hash，用 croner 和配置的 timezone 计算每个 entry 的下一次触发时间，再 round 到分钟 slot，并把 ref 写入 slot bucket。这个流程是 repair 逻辑，不是权威状态。Control 只在 promote-time 初始 slot placement 使用 JavaScript `croner`；scheduler repair 和 advance 使用 Rust `croner`。缺少 canonical identity 或 metadata 的 worker hash 会被跳过，不会重新播种非法 ref，并输出 bounded `invalid_identity` 或 `invalid_meta` reason。
3. Cron ref 带 entry generation。真正 fire 前，scheduler 读取 `crons:<ns>:<worker>` 并比较 `gen`；metadata 缺失、JSON 损坏或 generation 不匹配都会让 ref 变成 stale，并从 slot 中移除。
4. 原子 claim 会在取得 lease 或修改 slot 前再次精确比较 metadata 和 entry snapshot。并发配置变化会触发一次有界重读和重新计算；如果配置再次变化，scheduler 会保留 source ref 给后续 tick，并记录 `config_changed_deferred`。
5. Scheduler 会先原子地 lease ref、从当前 slot 移除、加入下一个 slot，然后才调用 runtime。这个顺序保证每个 slot 只 fire 一次，并且 runtime/network 失败不会变成自动 cron retry。
6. 如果某个 ref 滞留在早于当前 wall-clock slot 的旧 slot 中，scheduler 只把它 advance 到下一个 future slot，不会 fire。也就是说 outage 或 scheduler 长时间停顿期间错过的 cron event 会被跳过，而不是补发。
7. 发给 runtime 的 `scheduledTime` 是 slot timestamp，不是 POST 时间。`cron_queue_lag_ms` 衡量 scheduler 相对该 slot 晚了多久。

因此 cron 语义是 Cloudflare 风格的 best-effort scheduled event：分钟对齐、不 catch-up replay、允许 overlap，用户 handler 失败只作为 outcome 上报，不由 scheduler 重试。

Queue dispatch 是 stream-driven，不是 wall-clock driven：

1. Producer 把 message envelope 写入 DB 1 stream；当 `delivery_delay` 或 retry delay 非零时，先写入 delayed ZSET。
2. Scheduler 会在启动时及每个 `SCHEDULER_SWEEP_MS`（默认五分钟）从权威 hash、stream 和 delayed ZSET 修复 `queue:index:*` discovery set。1.5 秒一次的 reconcile loop 只读取这些 index、删除 stale member、为 live stream 创建固定的 `wdl-scheduler` consumer group，并维护内存中的 known delayed queue 集合。正常写入仍由 writer 维护 index；周期 repair 为中断的 data-plus-index 写入提供有界恢复，同时避免在 reconcile 热路径扫描 keyspace。缺失的 optional consumer field 默认使用 `max_batch_size=10`、`max_batch_timeout_ms=5000`、`max_retries=3`、`retry_delay_secs=0` 且不配置 `dead_letter_queue`；已存在但 malformed/out-of-range 的字段或非法 dead-letter queue 会让该 consumer projection 不可用。Group creation 按 stream 隔离错误并继续后续有界 chunk；健康 consumer 会先写入内存 registry 并刷新派生 snapshot，最后才汇报聚合 reconcile failure。
3. Consume loop 使用 `XREADGROUP` 读取 main stream，并按 `max_batch_size` 组 batch 投递；该值必须在 `[1, 100]` 内，越界 projection 会被拒绝。每次 read 会用当前 active stream set 的 consumer batch-size snapshot 限制 `COUNT`，避免一次 poll 把超过当前 consumer 单批 dispatch 能力的 entries 放进 PEL。PEL reap 在 consumer 仍存在时使用同一 per-consumer cap；consumer 已消失的 orphan movement 仍可按 hard cap 分页。Consume 和 PEL reap 可以在 queue semaphore 下按 stream 并行 dispatch。每条 dispatch path 在向 runtime 发送 message 前都会重读该 stream 的权威 `queue-consumer` hash 并刷新内存 registry，因此 consumer promote 后，一旦 message 被选中，就不需要等下一次 reconcile tick 才使用新版本。当前模型里 `max_batch_timeout_ms` 不是 batching wait window。
4. Runtime 返回 queue outcome envelope。Scheduler 解析 explicit `ack`、explicit `retry`、batch retry 和 implicit ack。Retry 与 DLQ transition 会作为彼此独立的原子脚本放在同一 transport pipeline 中执行：所有 target 写入成功后才会 acknowledge 并删除 source entry。因此 target 侧错误会保留 source entry，也不会阻止同 batch 后续健康 transition 完成。不会因为重试同一批 bytes 变好的平台失败，例如 queue message decode failure 或非法 queue dispatch body，会直接进 DLQ，不消耗 retry budget。聚合 request-body-too-large 会先拆成更小 batch 重试。
5. Delayed loop 由 `queue-delayed-wake` 和下一条 due member 的 wall-clock sleep 唤醒。每个到期 member 会先取得 `queue-delayed-claim:*` lease，TTL 为 `SCHEDULER_FIRE_TIMEOUT_MS + 5000ms`；抢到的副本把它移回 main stream，或在 consumer 已消失时移入 orphan stream。
6. Orphan / Pending-Entry cleanup 是诊断和保护机制。它防止 consumer 删除或 scheduler crash 路径静默丢消息，但 main queue stream 仍是 durable backlog，并且故意不 trim。Delayed ZSET 和 orphan stream-tail migration 受 `QUEUE_SWEEP_BATCH_SIZE` 分页控制，默认 `100`。

## Redis / Storage 合同

Valkey DB 切分：

- DB 0：control-plane metadata、cron config/projection、queue consumer config。
- DB 1：queue data-plane stream、delayed queue、orphan stream、log-tail stream。

Cron keys：

```text
crons:<ns>:<worker>               Hash, active cron config 的权威状态
cron:seq:<ns>:<worker>            String, 永久 generation 高水位
cron:index:workers                Set, crons hash 的非权威 discovery index
cron:index:workers:backfilled     String, 一次性 legacy backfill marker
cron-slot:<slot_ms>               Set, 分钟 bucket consumption index，约在 slot+10min 过期
cron-lease:<slot_ms>:<ref>        String EX, 每个 ref 的 single-fire lease
```

Queue keys：

```text
queue-consumer:<ns>:<queue>       Hash, active consumer projection 的权威状态
queue:index:consumers             Set, queue-consumer hash 的 discovery index
queue:index:streams               DB 1 Set, main queue stream 的 discovery index
queue:index:delayed               DB 1 Set, delayed ZSET 的 discovery index
queue:<ns>:<queue>:s              Stream, main at-least-once message stream
queue-delayed:<ns>:<queue>        ZSET, delayed visibility queue
queue-delayed-claim:<hash>        String PX, 每个 delayed member 的 promotion lease
queue:<ns>:<queue>:dlq            Stream, dead-letter diagnostic stream
queue-orphaned:<ns>:<queue>       Stream, consumer 消失后的 message 降落区
queue-delayed-wake                Stream, delayed ZSET dispatcher 的 wake signal
```

`wdl-rust-common::queue_keys` 持有 wake stream 及其 `delayed_key` / `visible_at` entry fields，避免 redis-proxy producer 与 scheduler consumer 漂移。

Index 不是权威状态。Writer 只负责添加 index member；scheduler reconcile 在确认被引用 key 不存在后负责 stale cleanup。

## Ownership / 并发 / 失败语义

Cron：

- `crons:<ns>:<worker>` 是权威状态。
- `cron:seq:<ns>:<worker>` 是唯一的永久 generation allocator，在 projection 清空和 whole-worker delete 后仍保留；新分配从 `1024` 开始。
- Cron projection metadata 只保存 dispatch 使用的 active worker version。
- `cron:index:workers` 只用于 discovery。`cron:index:workers:backfilled` 表示 scheduler 已跨过 pre-index legacy state；之后空 index 表示没有已发现的 cron worker。
- `cron-slot:<slot_ms>` 是可重建的 consumption index。scheduler 会检查当前 bucket 和前一个 bucket，避免临近分钟边界的写入被延迟一整分钟。
- Cron ref 形状是 `<ns>:<worker>:<cron_id>:<gen>`。
- `gen` 是 stale bucket ref 的 fence。删除后重新添加同一个 cron，包括 whole-worker delete 后重建，都会得到新的 generation。
- Scheduler 先 lease 并 advance ref，再触发 runtime。如果 runtime call 失败，该 slot 也已经被消费。这保持 Cloudflare 风格的 best-effort cron 语义，而不是重试 scheduled event。
- 滞留在旧 slot 的 ref 会 advance 但不会 fire。Outage 期间错过的 cron event 会被跳过。
- Handler failure 会由 runtime 以 HTTP 200 + `outcome:"error"` 返回，scheduler 不把用户代码失败当成可重试 transport failure。

Queues：

- Main stream 是持久化 at-least-once，不做 server-side trim。
- DLQ 和 orphan stream 是诊断通道，可以使用有界 approximate trim。默认各保留 10k 条，通过 `SCHEDULER_MAX_DLQ_LEN` 和 `SCHEDULER_MAX_ORPHANED_LEN` 调整。
- Consumer group 固定为 `wdl-scheduler`。
- 一个 queue 只能有一个 active consumer worker。Promote 会替换完整 queue-consumer projection，因此移除的可选字段会在下一次 promote 消失。
- `max_batch_size` 在 runtime dispatch 前强制生效，也会约束 live consumer 的普通 `XREADGROUP` / PEL reclaim read。`max_batch_timeout_ms` 当前是配置 metadata，不是 Cloudflare 风格的聚合窗口。
- 内部 retry count 从 0 开始；Worker 看到的 `Message.attempts` 从 1 开始。
- `maxRetries = N` 表示 handler 最多能看到 `N + 1` 次 attempt，然后进入 DLQ。
- 平台判定的永久 dispatch failure（`queue_message_decode_failed` 或非法 queue dispatch body）对该 batch 是 terminal，message 会立即进入 DLQ。如果 runtime 因聚合 queue request 过大而拒绝，scheduler 会拆分 batch 并用更小 request 重试；只有单条 message 仍超过 runtime body 上限时才会变成 terminal。Auth failure 和未知应用层 `4xx` response 仍保持既有 retry 行为，除非后续被明确映射。
- retry 显式传入的 `delaySeconds` 会覆盖 consumer `retry_delay_secs`，包括 `0` 表示立即 retry。
- Delayed promotion claim 使用 runtime dispatch timeout horizon 再加 5s margin，因此 loaded scheduler 不会在 move/drop Lua owner check 运行前就失去 delayed-member claim。
- Delayed retry wakeup 是 best-effort hint。Scheduler 如果错过 wake stream，仍会在下一次 delayed-loop reconcile/sleep interval 发现 due message；这是有界延迟取舍，不是 correctness fence。
- Consumer 消失时，已有 stream message 会进入 orphan stream，而不是被丢弃。

## 安全边界

- Cron 和 queue 的 runtime dispatch path 只在 runtime 内部 socket `:8088` 上可达。
- Gateway 不应按 path 屏蔽 tenant 的 `/_scheduled` 或 `/_queued`；socket split 才是安全边界。
- Queue producer binding 通过 runtime `redis-proxy` sidecar 写入，并在写入前执行 message / batch 上限。
- Queue name 使用共享 queue name grammar。因为 queue key 用冒号分隔，名称中禁止 `:`。
- Scheduler 将普通 worker id 发给 user-runtime，将字面量 `__system__:` worker id 发给 system-runtime。

## 可观测性

Scheduler 为 cron 和 queue outcome 输出结构化日志和 Prometheus metrics。

重要 cron 信号：

- `cron_fires{outcome=...}`
- `cron_fire_duration_ms{outcome=...}`
- `cron_queue_lag_ms{outcome=...}`
- `cron_bucket_size`
- `cron_stale_refs_cleaned`
- `cron_sweep_entries_skipped`
- `cron_sweep_workers_skipped`
- Logs：`cron_fired`、`cron_lease_lost`、`cron_config_changed_deferred`、`cron_ref_stale`、`cron_ref_stale_advanced`、`cron_sweep_entry_skipped`、`cron_sweep_worker_skipped`、`cron_reconcile`

重要 queue 信号：

- `queue_messages{outcome=...}`
- `queue_dispatch_failures{kind=...}`
- `queue_batch_duration_ms{outcome=...}`
- `queue_delayed_wake_read_errors`
- Reconcile、`XREADGROUP`、delayed sweep、PEL reap、DLQ、orphan movement 相关日志，包括 `queue_batch_dispatched` 和 `queue_consumer_projection_invalid`。

Runtime tail logs 也会为 loaded worker 执行输出 `worker_scheduled` 和 `worker_queue` start/finish event。

## 部署 / Rollout 注意事项

- 修改 queue 或 cron wire shape 时，control/runtime/scheduler 应一起部署。
- Runtime `:8088` 必须先于依赖新 internal dispatch path 的 scheduler 部署。
- Cron discovery index 有一次性 backfill 路径。`cron:index:workers:backfilled` 存在后，control writer 负责维护 discovery projection。
- Queue index 是非权威、可修复的。任何创建 queue stream、delayed queue 或 consumer projection 的新 writer，都必须添加对应 index member。
- Scheduler 有独立的 cron 和 queue dispatch semaphore：`SCHEDULER_CRON_MAX_CONCURRENCY` 和 `SCHEDULER_QUEUE_MAX_CONCURRENCY`，默认都继承 `SCHEDULER_MAX_CONCURRENCY`。

## 保护该模块的测试

代表性测试锚点：

- `tests/unit/control-lib.test.js`：cron 和 queue manifest 解析。
- `tests/unit/control-routing.test.js`：cron 和 queue consumer 的 promote projection。
- `tests/unit/control-lifecycle-indexes.test.js`：JS cron/queue key helper 和 projection staging。
- `tests/fixtures/queue-key-parse.json`：JS/Rust 共用的 queue discovery-key parser 合同。
- `tests/unit/runtime-lib.test.js`：internal dispatch body normalization。
- `tests/unit/runtime-dispatch-handlers.test.js`：scheduled / queue dispatch 行为，以及 tail event envelope 行为。
- `tests/unit/style-contracts.test.js`：跨 tier Redis key/layout drift 检查。
- `rust/scheduler/src/cron/` 单元测试。
- `rust/scheduler/src/queue/*` 单元测试。
- `tests/integration/cron-triggers.test.js`
- Queue 集成测试文件组：`tests/integration/queues-delivery.test.js`、
  `tests/integration/queues-retry-and-delay.test.js`、
  `tests/integration/queues-orphan-and-control.test.js`、
  `tests/integration/queues-batch-and-isolation.test.js`
- `tests/integration/queue-native-dispatch.test.js`

## 已知约束和非目标

- Cron 是分钟对齐、best-effort，不回放 missed fire。
- Cron 允许 overlap。
- Queue `max_batch_timeout_ms` 还不是真正的聚合窗口。
- Queue `contentType = "v8"` 被拒绝。
- Queue consumer `max_concurrency` 被拒绝。
- Main queue stream 故意不 trim；backlog 是运维信号。
- 集成测试覆盖了 scheduler 多副本下的 cron due-ref claim、cron sweep recovery、queue reconcile 加 consumer-group delivery、delayed queue promotion、PEL reap 和 workflow tick。Durable Object alarm 由 Workflows service 驱动，scheduler 不负责该路径。新增 scheduler dispatch 路径前必须单独审计它自己的 Redis lease / fence 语义，不能默认具备副本安全性。

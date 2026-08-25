# Workflows

## 目的

Workflows 提供 Cloudflare-shaped workflow API，但由 WDL 自己的 Rust engine 和 Valkey DB 2 支撑。它支持 same-worker workflow definition、持久化 instance state、step replay、event、pause/resume/restart/terminate，以及 scheduler-driven execution。

## 当前实现

Workflow engine 是独立 axum 服务 `workflows`，监听 `:9120`。Runtime 通过 `runtime/workflows-client.js` 暴露 workflow binding，并通过 `runtime/dispatch/workflow-*.js` 处理 dispatch。Control 解析 workflow metadata，并拥有 deploy-time workflow definition keys。本模块文档是当前 workflows 设计参考。

V2 这个名称用于区分当前支持 DAG 的 engine 和早期仅测试使用的 V1 engine。新环境应按 greenfield schema 2 workflows state 组织。

workerd 提供用户代码执行环境、`WorkflowEntrypoint` class shape、module loading，以及让 runtime 在 frozen worker version 中调用 workflow class 的能力。它不提供 WDL 可复用的本地 workflow engine。WDL 在 workflows 中补齐外部 engine：DB 2 persistence、leases、ready/due scheduling、step replay、sleep、wait、event buffering、lifecycle transition、retention，以及 dispatch 回 runtime。

## 接口

用户侧：

- Wrangler `[[workflows]]`
- Runtime `Workflow` binding：create、createBatch、get
- `WorkflowInstance`：status、pause、resume、terminate、restart、sendEvent
- `cloudflare:workflows` import/export specifier 会被 rewrite 到本地 shim。Shim 暴露 `WorkflowEntrypoint` 和 `NonRetryableError`；只有真实 module specifier 会被 rewrite，用户字符串、注释、template、regex literal、member `.import()` 调用、`import.meta.resolve` 和 private `#import` member 都保持不变。

Control / CLI：

- `GET /ns/<ns>/workflows` 列 workflow definition，使用 `workflow.list`。
- `GET /ns/<ns>/workflows/<worker>/<workflow>/instances` 列 instance，使用 `workflow.read`。
- `GET /ns/<ns>/workflows/<worker>/<workflow>/instances/<id>` 返回 instance status，使用 `workflow.read`。可选查询参数只接受 camelCase：`includeSteps=true|false` 会返回 step 记录，`stepLimit=<n>` 限制返回的 step 数。
- `POST /ns/<ns>/workflows/<worker>/<workflow>/instances/<id>/{pause,resume,restart,terminate}` 使用 `workflow.write`。
- CLI `wdl workflows list|instances|status|pause|resume|restart|terminate` 是 control API 的薄封装。

内部：

- Runtime `Workflow` facade -> workflows endpoint：`/internal/workflows/create`、`/internal/workflows/create-batch`、`/internal/workflows/get`、`/internal/workflows/status`、`/internal/workflows/pause`、`/internal/workflows/resume`、`/internal/workflows/terminate`、`/internal/workflows/restart`、`/internal/workflows/send-event`。
- Scheduler -> workflows `/internal/workflows/tick`
- do-runtime -> workflows alarm mutation endpoint：`/internal/workflows/do-alarms/set` 和 `/internal/workflows/do-alarms/delete`
- Control -> workflows alarm cleanup endpoint：`/internal/workflows/do-alarms/cleanup-worker`
- workflows -> runtime `:8088` 上的 `POST /internal/workflows/run`
- workflows -> runtime `POST /internal/workflows/notify` 用于 progress callback
- workflows -> do-runtime `POST /internal/do/alarms/dispatch`，用于投递 Workflows-owned internal Durable Object alarm。
- Runtime step facade -> workflows endpoint：`/internal/workflows/claim-step`、`/internal/workflows/replay-steps`、`/internal/workflows/commit-step-success`、`/internal/workflows/commit-step-error`、`/internal/workflows/register-sleep`、`/internal/workflows/register-wait`。
- 权威 internal endpoint 集合在 `rust/workflows/src/server.rs`；该 surface 变化时需要同步更新本节。
- Control -> workflows：`/internal/workflows/instances`、instance status/lifecycle proxy，以及 worker/version delete 前的 `/internal/workflows/lifecycle/check-delete`。

## Redis / Storage 合同

Workflows 独占 Valkey DB 2 作为 instance execution state。Control 在 DB 0 拥有 `wf:defs:<ns>:<worker>`，用于 deploy-time workflow key allocation 和稳定 identity。该 Hash 会保留 retired name 直到 whole-worker delete；definition list 只会为当前 active worker 枚举这段 retired history，而 deploy 和单个 workflow 的 status/lifecycle 路径只读取自己需要的 name。

关键概念：

- `workflowKey` 是 physical workflow identity。
- `(ns, worker, workflowName)` 在 redeploy 间保持稳定 workflowKey。
- Instance state、step records、payload refs、events、ready/due indexes、run leases、retention indexes 和 callbacks 存在 DB 2。
- Workflow payload 是有显式 byte cap 的 JSON data。大型 application data 应放在 R2/S3/D1/KV，再在 workflow payload 中保存引用。
- 同一个 instance 的 DB 2 key 共享 `{ns:workflowKey:instanceId}` hash tag，但 workflow state 也会使用 global ready/due/retention keys。因此当前部署要求单个非 cluster 的 Valkey 分片（`num_node_groups = 1`），而不是 Redis Cluster；为 HA 配一个 primary/replica 对是可以的，因为复制不会对 keyspace 分片，但多分片会把未加 hash tag 的 global key 拆到不同 slot 并触发 CROSSSLOT。
- Internal Durable Object alarm jobs 也存在 DB 2 的 `wf:internal:do-alarm:*` 下。它们是 Workflows-owned backend jobs，不是 tenant workflow instances，只能通过 do-runtime/workflows internal endpoints 访问。

Key families：

| Key | Type | Owner | Authority | Cleanup/delete 语义 |
|---|---|---|---|---|
| `wf:defs:<ns>:<worker>` | Hash | Control | deploy metadata 的 workflow definition/key allocation 权威记录。 | Worker delete 在 lifecycle check 通过后删除 definitions。 |
| `wf:schema_version` | String | workflows | DB 2 workflow-state schema marker。 | 当前值是 `2`；greenfield deployment 从 schema 2 开始。 |
| `wf:instance:{<ns>:<workflowKey>:<instanceId>}:state` | Hash | workflows | instance state 权威记录。 | Terminal retention 和 lifecycle cleanup 删除过期 state。 |
| `wf:instance:{...}:payloads` | Hash | workflows | aggregate cap 下的 payload ref storage。 | 随 instance state family 删除。 |
| `wf:instance:{...}:steps`、`step-summaries`、`step-summary-index` | Hash/ZSET | workflows | step replay/history state 权威记录。 | 随 instance 删除；history read 有界，并拒绝 summary/index 数量不一致或请求页中 summary 缺失。 |
| `wf:instance:{...}:events`、`events-by-type` | Hash/ZSET | workflows | buffered event record 和 type index。 | consumed/stale event 在 wait matching 或 cleanup 中删除。 |
| `wf:ready:<shard>`、`wf:ready:active`、`wf:ready:cursor` | Set/String | workflows | ready-token hint、active shard set 和 fair-dispatch cursor。 | Token 是去重 hint；instance state 仍是权威；cursor 在 tick 之间轮转 shard 起点。 |
| `wf:due:<shard>` | ZSET | workflows | sleep/retry/event-timeout due index。 | Tick promotion 把到期 entry 移回 ready。 |
| `wf:by-worker:<ns>:<worker>` | Set | workflows | 按 worker 发现 instance 的索引。 | list/delete check 使用；retention/delete cleanup 移除 entry。 |
| `wf:by-workflow:<ns>:<worker>:<workflowKey>` | ZSET | workflows | 按 workflow key 分页列 instance 的有序索引。 | retention/delete cleanup 删除 sorted-set member。 |
| `wf:by-version:<ns>:<worker>:<version>` | Set | workflows | frozen-version referrer index。 | live instance 仍引用该 version 时阻止 version delete。 |
| `wf:pending-version:<ns>:<worker>:<version>` | ZSET | workflows | 按过期时间计分的短期 restart target-version blocker。 | Version-delete 检查 active member；restart 在创建持久 `wf:by-version` referrer 前原子复核自己的 marker。Member 30 秒后过期，ZSET key 使用 60 秒 TTL 做物理回收。 |
| `wf:retention` | ZSET | workflows | terminal retention due index。 | Retention tick 删除过期 terminal instance。 |
| `wf:internal:do-alarm:{<jobId>}:state` | Hash | workflows | 单个 Durable Object SQLite alarm row 的 backend job 权威状态。 | 成功 delivery、retry 耗尽、显式 delete 和 worker cleanup 会移除 job。 |
| `wf:internal:do-alarm:due:<shard>` | ZSET | workflows | DO alarm due index。score 是 due timestamp milliseconds。 | Tick promotion 把到期 job 移到 ready。 |
| `wf:internal:do-alarm:ready:<shard>`、`ready:active`、`ready:cursor` | Set/String | workflows | DO alarm ready hints、active shard set 和 fair-dispatch cursor。 | Dispatch 删除 ready hint 或在 retry 时重新调度；cursor 在 tick 之间轮转 shard 起点。 |
| `wf:internal:do-alarm:by-worker:<ns>:<worker>` | Set | workflows | internal DO alarm jobs 的 worker cleanup index。 | Whole-worker delete 在删除提交后请求 Workflows 删除 indexed jobs；残留 job 会在下一次 dispatch 时自清。 |
| `wf:internal:do-alarm:by-worker:<ns>:<worker>:cleanup-snapshot:<random>` | Set | workflows | 单次 cleanup-worker 使用的 by-worker DO alarm index 临时快照。 | 仅内部使用；TTL 为 60 秒，并在 cleanup drain snapshot 时续租。 |

## Ownership / 并发 / 失败语义

- V2 workflow 只支持 same-worker。
- Instance 冻结创建时的 worker version/class identity。
- Control 会对当前操作实际读到的 malformed active workflow entry 和 malformed `wf:defs` record fail closed；管理路径返回 `corrupt_meta`，deploy 在复用损坏的历史 definition 时返回 `workflow_definition_corrupt`。损坏的权威 metadata 不会被暴露为正常的 missing 或 retired workflow。正常 deploy 和单个 workflow 路径不会扫描无关的历史 definition。
- Workflows lifecycle check 会拒绝 malformed referrer member，而不是把它当成不存在。
- Scheduler 只负责唤醒 workflows；admission、fairness、shard tick、ready/due movement 和 runtime dispatch 都由 workflows 负责。Scheduler 会在 64 KiB 上限内读取 tick response，并要求合法 JSON object 根节点；单个缺失或未知字段仍保持 forward-compatible，并按未报告 progress 处理。
- Scheduler 也通过同一个 `/internal/workflows/tick` endpoint 唤醒 Workflows-owned internal DO alarm jobs；scheduler 不直接读写 DO alarm state。
- Workflows 在持久化 DO alarm job 前拒绝 non-canonical alarm identity，并在 dispatch 前重新校验持久化 alarm identity。一个 Control DB Lua snapshot 会同时读取当前 storage pointer、active route、retained-version score 和 active session policy projection。当前 `restart` projection 会把 superseded alarm retarget 到 active version，即使其 scheduled version 仍 retained；后续 `preserve` projection 会覆盖尚未观察到的 restart，并让 retained alarm 继续使用 scheduled version。Malformed session policy projection、route/projection 不一致，以及 retarget 需要使用的 malformed active version 会 fail closed。其中 namespace、worker、version 校验复用 `wdl-rust-common`；do-runtime protocol grammar 与 identity helper 拥有 canonical alarm-specific field 和 aggregate 512-byte DO host-id 合同，Workflows 在持久化和 dispatch 前镜像并重新校验该合同。Runtime run dispatch 与 progress callback 在 workflows crate 内共用同一个 system-vs-user runtime endpoint selector。
- 32 个 scheduling shards 划分 ready/due work。每次 tick 会交错处理 active shard 的候选，在 claim 前取得 dispatch permit，并把已 claim activation 交给 Workflows-owned background task，而不是逐 shard drain。Tick 只等待 maintenance、claim 和 admission，不等待 tenant execution 或 DO alarm delivery 完成。`WORKFLOWS_READY_DISPATCH_CONCURRENCY` 限制同一个 Workflows replica 上跨重叠 tick 的 workflow execution，默认值为 `128`，并限制在 1–128 ready batch 范围内。独立的 DO alarm pool 由 `WORKFLOWS_DO_ALARM_DISPATCH_CONCURRENCY` 控制，默认值为 `32`，有效范围为 1–100 个 job。已 admission 的 task 会持有 pool permit 和 shutdown in-flight guard，直到 runtime dispatch 与 fenced commit 结束；进程丢失后仍由现有 run lease 和 ready hint 恢复。Scheduler 通过 `WORKFLOWS_TICK_TIMEOUT_MS` 为 maintenance 和 admission 设置独立客户端 deadline，默认值为 60 秒；它不是 workflow execution deadline，也与 Workflows runtime dispatch timeout 相互独立。Tick 报告 maintenance、admission 或任一 dispatch pool 因容量不足仍有待处理工作时，Scheduler 使用 `WORKFLOWS_TICK_ACTIVE_INTERVAL_MS`（默认 100ms）；否则使用 `WORKFLOWS_TICK_INTERVAL_MS`（默认 1 秒）。
- Ready token 是去重 hint；instance hash state 是权威状态。
- Runtime terminal response 是 tagged variant：`completed` 必须包含 `output` 字段，`failed` 必须包含 `error` 字段，显式 JSON `null` 仍是合法 payload。`suspended` response 只有在权威 step backend 已把同一 generation/run token 转为 `waiting` 后才会清除 run claim；格式正确但乱序的 response 会成为 fenced no-op。
- Execution commit 同时用 `generation`、`runToken`、active instance status 和未过期 run lease fence。Step commit/register 接受同一 run 的 `running` 或 `waiting` 状态，因此一个并行 sibling 进入 retry/wait 后，另一个 sibling 仍可完成；completed runtime terminal 要求 `running`，failed runtime terminal 在 run lease 仍有效时也可以关闭由非法未 await suspending step 造成的同一 run `waiting` 状态。如果 lease 已过期，workflows 只恢复 ready hint，让下一次 claim 在新 lease 下 replay。首次 run admission、过期 run requeue、lifecycle commit、`sendEvent` 与 retention cleanup 还会同时比较持久化的 `createdAtMs` 和 `generation`，避免旧 snapshot 修改同 ID 下、creation timestamp 不同的后来 incarnation；会 invalidate in-flight execution 的 lifecycle path 会在同一个 Lua commit 内 rotate `generation`。
- Runtime replay cache 只是 advisory。DB 2 step state 是权威。同一个 runtime isolate 可以在同一 instance incarnation 的不同 run claim 间复用 terminal step record；成功 output 会保存为序列化 snapshot，并在每次 replay 时重新解码，因此租户修改返回对象不会污染后续 claim。每个 runtime isolate 的 module-level 跨请求 cache 最多保留 16 MiB serialized data；过大的 record 不进入 cache，压力下会逐出旧 cache，`workflow_replay_cache_bytes` gauge 在 `/_metrics` render 时从权威 counter 发布这部分跨请求保留量。全局逐出会停止跨请求保留；正在执行的 controller 仍可使用有界的 controller-local replay working set，避免逐出把 replay hit 变成 fresh claim，并在 dispatch 结束时释放 detached working set。Backend replay record 会先投影为 Runtime 实际消费的字段，再参与 byte accounting 和 retention，因此未消费的 response metadata 不能绕过 cache budget。新 claim 会重新开放有界分页，以发现其他 isolate 已提交的 record。
- Runtime 可以并发发起多个 `step.do`，常见形式是 `Promise.all`；每次调用按用户代码调用顺序分配 deterministic ordinal，从当前已完成 step frontier 记录 DAG dependencies，并在 run fence 下独立 commit。Step config 是通过 workerLoader JSRPC 边界按值传递的 JSON data，callable hook 不属于该合同；Workflows 拥有 canonical config encoding 与精确的 64 KiB 上限。`step.do` callback 不能启动另一个 workflow step，即使在 callback 的 `await` 之后也不允许；并行 sibling promise 应在 run body 中、callback 代码进入 in-flight 之前创建。如果 run 在已启动 step settle 前返回，会按 invalid run 失败，所以用户代码必须 await 并发 step promise。Suspending operation（`step.sleep`、`step.sleepUntil`、`step.waitForEvent`）仍保持互斥，不能和其它 in-flight step 重叠，因为它们会 suspend 整个 workflow run。
- Completed instance 使用 success retention；failed 和 terminated instance 使用 error retention。新建 instance 的两类 retention 默认都是 8 小时，可以通过 `create({ retention: { successRetention, errorRetention } })` 覆盖。
- Bundled Workers types 暴露 best-effort `locationHint` create option，但 WDL 没有 regional Workflow placement plane；`create()` 与 `createBatch()` 会在 backend I/O 前拒绝自有 `locationHint` field，而不是静默丢弃。
- `Workflow.createBatch()` 每次调用最多接受 100 项；Runtime prevalidation 与 Rust admission 共享这项 pinned limit。Rust 通过一个有界 pipeline 读取去重后的 instance-state snapshot，并在各项间共享 mutation preflight；每个新 instance 仍保留独立的 create token、create 后 control-plane revalidation、cleanup 与 finalize fence。
- 单个 workflow result 的上限是 1 MiB，runtime-to-workflows backend JSON request 的上限是 2 MiB。Runtime prevalidation 和 Rust backend 共享 pinned `workflow_payload_too_large` contract。每个 instance 的 aggregate payload cap 是 16 MiB。Runtime dispatch 会在转发前限制 serialized result 与 workflows-backend request bytes；这些文档最多包含 127 层 object/array（包括平台 envelope），并拒绝 key 或 value 中的孤立 UTF-16 surrogate，与锁定的 Rust JSON parser 保持一致。Workflows 拥有 backend JSON parsing、canonical step config 和 persisted aggregate accounting。Step/event 超 cap 写入会让请求失败；runtime terminal result 超 cap 会在同一事务内把 instance 转成 failed。
- Workflows 语义 request cap 使用 `request_too_large`；它不同于 control/runtime 协议中的 HTTP-body parser `request_body_too_large`。除此之外，HTTP 边界上的 workflow error 使用平台 `{ error, message }` envelope。Workflows service 的 5xx response 保留稳定 error code，但公开 message 固定；原始 diagnostic 只进入 service-side request log。

Workflow execution 使用两条 channel：

1. Generated `Workflow` facade 调用 binding-scoped host adapter。Adapter 只接受公开 Workflow operation，用不可变 binding props 覆盖 namespace、worker、version、workflow key 和 class，附加 mesh authentication 后转发到 workflows；tenant request field 不能选择其它 workflow。Scoped adapter 使用原生 `Fetcher.fetch()`，静态 host worker 启用 incoming request signal，因此显式可取消的 scoped-transport `Request` 被 abort 时，会在 workflows request 前取消有界 body 读取。公开 `Workflow` facade 仍是结构化 operation API，不新增 `Request` 或 `AbortSignal` option。
2. workflows 把已 claim 的 run dispatch 回 runtime `:8088` 上的 `/internal/workflows/run`。Runtime 加载 frozen worker version 并调用 `className.run(event, stepFacade)`。

Get、status 和 list read 会从请求中的 namespace、workflow key 与 instance id 派生 payload hash。读取 result/error payload 前，persisted `ns`、`workflowKey`、`instanceId` 和 `payloadsKey` 必须与该 canonical identity 一致；任何 divergence 都会以 invalid state fail closed。

Create/restart 与 replay 的 version pinning 不同。新的 `create()` 或 `restart()` 写 DB 2 前会按当前 active route canonicalize，因此新的 durable business process 从 active version 开始。已有 instance 使用自己存的 `frozenVersion` replay；promotion 不会改变它的代码。只要 non-expired instance 仍引用某个 version，`wf:by-version` 就会阻止 worker-version delete。Restart 在重新校验 active export 前写入一个短期 target-version blocker，最终 DB 2 transition 会原子地建立持久 `wf:by-version` referrer 并删除该 blocker，因此 version delete 不能从 active-version resolution 和 restart commit 之间穿过。Runtime 会用 bundle key 共用的正 JavaScript-safe-integer version parser 校验每个 dispatch 的 `frozenVersion`；malformed persisted tag 会在加载 worker 前失败。

Scheduling 是 hint-based，但状态权威在 instance hash：

1. `create`、`resume`、`restart` 和 event delivery 向 `wf:ready:<shard>` 写 immediate token。
2. Sleep、retry 和 wait timeout 向 `wf:due:<shard>` 写或更新 due token。
3. scheduler 调 `/internal/workflows/tick`；workflows promote due token、采样 ready token、取得同一 Workflows replica 内跨 tick/shard 共用的 dispatch permit，并 claim eligible instance。Tick 在 admission 后返回，不等待这些 run 完成。
4. Claim 根据 instance hash 校验 status、generation 和 lease state。重复或 stale 的 ready/due token 会自清理，不执行用户代码。
5. 每个已 admission 的 runtime dispatch 加上其 fenced result commit 都由 tracked Workflows task 持有。Runtime dispatch 受 `WORKFLOWS_DISPATCH_TIMEOUT_MS` 约束。在权威 step backend 操作中，backend transport failure、backend binding 缺失、`internal_auth_failed`、`502`/`503`/`504` 和明确的 `internal_error` 或 `redis_error` 会让 Runtime 返回固定 503，而不是 terminal workflow result；workflows 随后释放 ordinary run claim，让后续 tick replay。如果同一 run 已在 response failure 前权威提交 `waiting`，release 会保留 `waiting` 并复用 suspended-claim cleanup，而不是恢复 run 前 status。Replay-page prefetch 只是 advisory cache 路径：prefetch 失败会回退到权威 step 操作，只有该路径也失败时才适用上述 503 合同。存在并行 step 时，Runtime 会关闭新 step admission，并在 Workflows 权威 dispatch timeout 的剩余预算内等待 tracked sibling settle，同时预留一秒返回 response；在该有界等待内观察到的 host 标记 infrastructure failure 优先于普通 terminal step error，因此后到的 commit ACK 丢失不会被持久化为确定性失败。权威 dispatch budget 耗尽本身属于 result-unknown，Runtime 会返回同一个固定 503，使 Workflows 释放 claim，并让 replay 对账可能迟到的 durable step commit。Workflows 会在 request serialization 前计算并发送绝对 deadline，因此 transport、queueing、body parsing、tenant execution 和 sibling settlement 消耗同一预算。确定性的 step、fence、payload 和 persisted-state error 仍是 terminal；Runtime outcome 缺失或未知，或 terminal variant 缺少对应的必需 payload 字段，都属于 protocol error，不能隐式解释成 failed。Generation/run-token fence 会阻止双 durable commit，但用户代码里的外部副作用可能重复；workflow 代码和 step callback 应保持幂等。`WORKFLOWS_RUN_LEASE_MS` 会被 clamp 到高于 dispatch timeout，它是 stale-claim backstop，不是普通 long-run timeout 旋钮。

Step facade 实现 durable replay：

- `step.do(name, [config], callback)` 使用 ordinal、name、same-name count、DAG dependencies 和 canonical config hash 作为 replay identity。已完成且匹配的 step 返回 stored result；shape mismatch 会以 `workflow_step_mismatch` fail closed。
- 单个 step 最多记录 1000 条 dependency edge。如果超过 1000 个尚未 join 的 sibling 汇入后续 `step.do`，workflows 会以 `request_too_large` 拒绝该 step request；用中间 join 控制 fan-in。
- 单个 runtime dispatch turn 最多允许 1000 个 in-flight workflow step，也最多启动 1000 个 fresh backend step。这会在 root/sibling fan-out 形成 backend claim/commit 洪峰前先限流；completed/failed replay cache hit 不计入 fresh-start 上限。waiting replay record 会重新检查 workflows backend，并计入该上限，以便在再次 suspend 前修复 due / wait index。并行 `step.do` sibling 必须在同一个同步 fan-out batch 中创建，不能先 `await` 其中一个 sibling 再继续启动新的 durable step；await 后必须等整个 batch 完成，确保 replay 计算出同样的 dependency frontier。
- `step.sleep()` 和 `step.sleepUntil()` 记录 waiting state 和 due time，然后用 reserved internal sentinel suspend 当前 run。
- `step.waitForEvent()` 先检查 buffered event，再记录 wait 和可选 timeout。`sendEvent` 会在 wait 出现前保存 event payload 和 type index，因此支持 event-before-wait。
- Runtime 从头 replay 用户代码。它会 lazy fetch replay pages，也可以在进程内 advisory cache，但 DB 2 step state 始终是权威。
- Replay step record 与它引用的 payload 共用完整的 generation、run-token、creation-time、lease 和 active-status fence。只有该 fence 仍有效时才会解析引用的 payload，因此 restart 不会把另一 execution generation 的 payload 混入当前 page。
- V2 会为 `step.do` 持久化 DAG。runtime 按同步调用顺序分配 ordinal，把已完成 step 视作当前 dependency frontier，并把 frontier 存到后续 step 上。`Promise.all([step.do(...), step.do(...)])` 会产生拥有相同 parent 的 sibling nodes；join 后再调用的 `step.do` 会依赖这两个 sibling。依赖调度、join、cancel 仍由用户代码的 `await` / `Promise` 结构表达；workflows 持久化最终 graph，不另跑一个独立 graph planner。

Fence 模型：

- Execution commit（`claim-step`、step success/error、sleep/wait registration、runtime terminal）同时由 `generation`、`runToken`、active instance status 和未过期 run lease fence。Step commit/register 接受同一 run 的 `running` 或 `waiting`；completed runtime terminal 要求 `running`；failed runtime terminal 在 run lease 仍有效时也可以关闭由非法未 await suspending step 造成的同一 run `waiting` 状态。如果 lease 已过期，workflows 只恢复 ready hint，让下一次 claim 在新 lease 下 replay。
- Runtime `suspended` result 只有在同一权威状态已经是 `waiting` 时才释放 run claim；Runtime 不能仅凭 response assertion 创建 waiting 状态。
- 首次 run claim 与过期 run requeue、lifecycle commit（`pause`、`resume`、`restart`、`terminate`）、`sendEvent` 与 retention cleanup 会同时比较 `generation` 和持久化的 `createdAtMs` incarnation 字段；会 invalidate in-flight execution 的 lifecycle path 还会轮换 `generation`。如果并发 restart 或同 ID 下 creation timestamp 不同的新 incarnation 先赢，旧 mutation 会被拒绝。
- Payload bytes、payload refs、counters、state change 和 ready/due update 必须在 DB 2 中一起 commit；payload ref 缺失时 workflows 必须 fail closed。

## Progress Callback

Progress callback 是 best-effort same-worker Durable Object push。Create request 可把 `{ kind: "do", binding, idFromName, path? }` callback descriptor 存进 instance state。workflows 向 runtime `POST /internal/workflows/notify` 推 progress；runtime 调 reserved `__WdlWorkflowNotify__` entrypoint，再调用同 worker 的 DO binding。Lookup 和 delivery 各有独立有界 semaphore：`WORKFLOWS_PROGRESS_CALLBACK_LOOKUP_CONCURRENCY` 默认 `128`，`WORKFLOWS_PROGRESS_CALLBACK_CONCURRENCY` 默认 `32`。Saturation 会 drop 这个 best-effort callback 并记录 dropped outcome；delivery 不具备事务性，DB 2 status 才是权威状态。

## 安全边界

- workflows private API 不公开路由。
- Positional env 获得 runtime `Workflow` facade。Module evaluation 最多观察到 binding-scoped host adapter，绝不会拿到通用 authenticated workflows service Fetcher；adapter 固定 identity，并且只暴露公开 operation。
- Observer role 只拿 `workflow.list`。Instance list/status 是 payload-bearing，需要 `workflow.read`。
- Workflow read endpoint 除非明确设计为 metadata-only，否则应视为 payload-bearing。
- workflows lifecycle check 失败时，control delete fail closed。

## Cloudflare 兼容性说明

- WDL 跟随 Cloudflare 的 durable-step 模型：支持具名 `step.do`、retry、`step.sleep`、`step.sleepUntil` 和 `step.waitForEvent`，但不是 Cloudflare 内部 engine 的逐字节实现。
- WDL 从 runtime 实际执行中持久化 DAG edges。Cloudflare dashboard visualizer 会通过 AST parsing 推导更丰富的 graph，包括 conditionals、loops、nested functions 和 promise entry/exit ordering。WDL 不运行 AST planner；它记录实际 `step.do` 调用形成的 graph。
- `Promise.all([step.do(...), step.do(...)])` 受支持，并记录成 parallel sibling nodes。join 后的后续 `step.do` 会记录对这些 sibling 的 dependencies。
- WDL 对“未 resolve 的 durable step 后立刻 suspension”更严格。如果用户代码启动多个 `step.do` promise，只通过 `Promise.race()` 观察 winner，然后在其它已启动 step 仍 in-flight 时调用 `step.sleep` / `step.sleepUntil` / `step.waitForEvent`，WDL 会以 `workflow_invalid_step` 让 run 失败。把 nondeterministic race 包进一个 `step.do`，或在 suspend 前 await 所有已启动的 durable steps。
- WDL 也会把永久失败的 `step.do` 视为整个 run 的 terminal failure，即使用户代码捕获了 thrown error。如果 fallback 应该属于同一个 durable step，把 primary/fallback 逻辑放进同一个 `step.do` callback。

## 可观测性

workflows 遵循 Rust service observability shape：JSON logs、`/_healthz`、`/_metrics`、request in-flight tracking、shutdown drain 和有界 labels。Runtime 输出 workflow dispatch、replay cache、payload-limit 和 callback outcome。Workflows 通过有界 `workflow_dispatches` 输出包括 fenced no-op commit 在内的 completion outcome，并通过 `do_alarm_dispatches` 输出 internal DO alarm delivery/retry/discard/in-flight-unknown outcome。Scheduler tick 日志记录 admission 与 dispatch-pool capacity pressure，并把 workflow tick failure 与 queue/cron dispatch 分开记录。

## 部署 / Rollout 注意事项

- 跨 tier Workflow protocol 变化遵循 [infra rollout 注意事项](infra.zh.md#部署--rollout-注意事项)中的 reader-before-writer 流程；受影响的具体 service 写入该版本 CHANGELOG。
- 必需的 runtime dispatch deadline 是一个明确的 sender-first 例外：先滚动 workflows 并等待全部旧 Workflows task drain，再滚动 user-runtime 和 system-runtime。旧 Runtime 会忽略 additive field，新 Runtime 会拒绝缺少该字段的旧 sender。
- DB 2 是 workflow instance state 边界；不要从 control/runtime/scheduler 直接写 DB 2。
- workflows 在 DB 2 中持久化 `wf:schema_version`。Schema `2` 会在 step record 和 summary 中存储 DAG dependency edges。当前部署按该 schema 的 greenfield state 处理；没有新的设计前，不要为 in-flight legacy workflow instance 添加原地迁移路径。
- 如果开发或维护环境启动时 workflows DB 2 中已有未带版本的 `wf:*` runtime key，应先停止 workflows，清理该 DB 2 runtime state 后再启动。WDL workflow definitions 位于 DB 0 的 `wf:defs:*`，不属于 DB 2 runtime-state cleanup 范围。

## 保护该模块的测试

- `tests/unit/runtime-dispatch-workflows.test.js`
- `tests/unit/workflow-replay-cache.test.js`
- `tests/unit/runtime-load.test.js`
- `tests/unit/runtime-workflows-client.test.js`
- `tests/unit/control-handlers-workflows.test.js`
- `tests/unit/control-lib.test.js`
- `tests/unit/auth-lib.test.js`
- `rust/workflows/src/tests.rs`
- `tests/integration/workflows-service.test.js`
- Workflow 集成测试文件组：`tests/integration/workflows-runtime-core.test.js`、
  `tests/integration/workflows-runtime-scheduler.test.js`、
  `tests/integration/workflows-runtime-pausing.test.js`、
  `tests/integration/workflows-runtime-retention.test.js`
- `tests/integration/workflows-metadata.test.js`
- `tests/integration/workflows-durable-objects.test.js`
- `tests/unit/style-contracts.test.js`

## 已知约束和非目标

- V2 不宣称完整 Cloudflare Workflows compatibility。
- 不支持 cross-worker 或 `script_name` workflows。
- WDL 自有 binding facade 不暴露原生 workerd 的 `WorkflowInstance.delete()` 或 `Workflow.deleteBatch()`；instance lifecycle 仍由本文记录的 WDL API 和 retention engine 持有。
- 不提供平台托管的大 payload object-storage spill。
- 不使用 tenant Durable Object storage 作为 workflow backend。
- Runtime replay 不直接跳到 continuation；用户 JS 按 deterministic step ordinal replay，也包括并发 `step.do` 分配到的 ordinal。

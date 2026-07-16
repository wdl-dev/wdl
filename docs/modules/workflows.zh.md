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
| `wf:instance:{...}:steps`、`step-summaries`、`step-summary-index` | Hash/ZSET | workflows | step replay/history state 权威记录。 | 随 instance 删除；summary read 可截断。 |
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
- Scheduler 只负责唤醒 workflows；admission、fairness、shard tick、ready/due movement 和 runtime dispatch 都由 workflows 负责。
- Scheduler 也通过同一个 `/internal/workflows/tick` endpoint 唤醒 Workflows-owned internal DO alarm jobs；scheduler 不直接读写 DO alarm state。
- Workflows 在持久化 DO alarm job 前拒绝 non-canonical alarm identity，在 dispatch 前重新校验持久化 alarm identity，并在把 active route 用作 retarget 前校验其 version。其中 namespace、worker、version 校验复用 `wdl-rust-common`；do-runtime protocol grammar 与 identity helper 拥有 canonical alarm-specific field 和 aggregate 512-byte DO host-id 合同，Workflows 在持久化和 dispatch 前镜像并重新校验该合同。Runtime run dispatch 与 progress callback 在 workflows crate 内共用同一个 system-vs-user runtime endpoint selector。
- 32 个 scheduling shards 划分 ready/due work。
- Ready token 是去重 hint；instance hash state 是权威状态。
- Execution commit 同时用 `generation`、`runToken`、active instance status 和未过期 run lease fence。Step commit/register 接受同一 run 的 `running` 或 `waiting` 状态，因此一个并行 sibling 进入 retry/wait 后，另一个 sibling 仍可完成；completed runtime terminal 要求 `running`，failed runtime terminal 在 run lease 仍有效时也可以关闭由非法未 await suspending step 造成的同一 run `waiting` 状态。如果 lease 已过期，workflows 只恢复 ready hint，让下一次 claim 在新 lease 下 replay。Lifecycle commit 只用 generation fence，并在同一个 Lua commit 内 rotate `generation`。
- Runtime replay cache 只是 advisory。DB 2 step state 是权威。
- Runtime 可以并发发起多个 `step.do`，常见形式是 `Promise.all`；每次调用按用户代码调用顺序分配 deterministic ordinal，从当前已完成 step frontier 记录 DAG dependencies，并在 run fence 下独立 commit。`step.do` callback 不能启动另一个 workflow step，即使在 callback 的 `await` 之后也不允许；并行 sibling promise 应在 run body 中、callback 代码进入 in-flight 之前创建。如果 run 在已启动 step settle 前返回，会按 invalid run 失败，所以用户代码必须 await 并发 step promise。Suspending operation（`step.sleep`、`step.sleepUntil`、`step.waitForEvent`）仍保持互斥，不能和其它 in-flight step 重叠，因为它们会 suspend 整个 workflow run。
- Termination 是显式 non-success terminal outcome，使用 error retention。
- `Workflow.createBatch()` 每次调用最多接受 100 项；Runtime prevalidation 与 Rust admission 共享这项 pinned limit。
- 单个 workflow result 的上限是 1 MiB，runtime-to-workflows backend JSON request 的上限是 2 MiB。Runtime prevalidation 和 Rust backend 共享 pinned `workflow_payload_too_large` contract。每个 instance 的 aggregate payload cap 是 16 MiB。Step/event 超 cap 写入会让请求失败；runtime terminal result 超 cap 会在同一事务内把 instance 转成 failed。
- Workflows 语义 request cap 使用 `request_too_large`；它不同于 control/runtime 协议中的 HTTP-body parser `request_body_too_large`。除此之外，HTTP 边界上的 workflow error 使用平台 `{ error, message }` envelope。Client-facing proxy 应把 workflows 5xx 当作 backend/platform failure，不应依赖 response body 中的 raw backend diagnostic message。

Workflow execution 使用两条 channel：

1. Loaded worker 通过 reserved `__WDL_WORKFLOWS_BACKEND__` Fetcher binding 调 workflows。Runtime 从 bundle metadata 附加 identity；workflows 不信任 tenant body 中的 namespace、worker、version、workflow key、class 或 instance identity。
2. workflows 把已 claim 的 run dispatch 回 runtime `:8088` 上的 `/internal/workflows/run`。Runtime 加载 frozen worker version 并调用 `className.run(event, stepFacade)`。

Create/restart 与 replay 的 version pinning 不同。新的 `create()` 或 `restart()` 写 DB 2 前会按当前 active route canonicalize，因此新的 durable business process 从 active version 开始。已有 instance 使用自己存的 `frozenVersion` replay；promotion 不会改变它的代码。只要 non-expired instance 仍引用某个 version，`wf:by-version` 就会阻止 worker-version delete。Restart 在重新校验 active export 前写入一个短期 target-version blocker，最终 DB 2 transition 会原子地建立持久 `wf:by-version` referrer 并删除该 blocker，因此 version delete 不能从 active-version resolution 和 restart commit 之间穿过。Runtime 会用 bundle key 共用的正 JavaScript-safe-integer version parser 校验每个 dispatch 的 `frozenVersion`；malformed persisted tag 会在加载 worker 前失败。

Scheduling 是 hint-based，但状态权威在 instance hash：

1. `create`、`resume`、`restart` 和 event delivery 向 `wf:ready:<shard>` 写 immediate token。
2. Sleep、retry 和 wait timeout 向 `wf:due:<shard>` 写或更新 due token。
3. scheduler 调 `/internal/workflows/tick`；workflows promote due token、采样 ready token，并 claim eligible instance。
4. Claim 根据 instance hash 校验 status、generation 和 lease state。重复或 stale 的 ready/due token 会自清理，不执行用户代码。
5. Runtime dispatch 受 `WORKFLOWS_DISPATCH_TIMEOUT_MS` 约束。runtime dispatch error 或 timeout 时，workflows 会释放 ordinary run claim，让后续 tick 可以 retry。Generation/run-token fence 会阻止双 durable commit，但用户代码里的外部副作用可能重复；workflow 代码和 step callback 应保持幂等。`WORKFLOWS_RUN_LEASE_MS` 会被 clamp 到高于 dispatch timeout，它是 stale-claim backstop，不是普通 long-run timeout 旋钮。

Step facade 实现 durable replay：

- `step.do(name, [config], callback)` 使用 ordinal、name、same-name count、DAG dependencies 和 canonical config hash 作为 replay identity。已完成且匹配的 step 返回 stored result；shape mismatch 会以 `workflow_step_mismatch` fail closed。
- 单个 step 最多记录 1000 条 dependency edge。如果超过 1000 个尚未 join 的 sibling 汇入后续 `step.do`，workflows 会以 `request_too_large` 拒绝该 step request；用中间 join 控制 fan-in。
- 单个 runtime dispatch turn 最多允许 1000 个 in-flight workflow step，也最多启动 1000 个 fresh backend step。这会在 root/sibling fan-out 形成 backend claim/commit 洪峰前先限流；completed/failed replay cache hit 不计入 fresh-start 上限。waiting replay record 会重新检查 workflows backend，并计入该上限，以便在再次 suspend 前修复 due / wait index。并行 `step.do` sibling 必须在同一个同步 fan-out batch 中创建，不能先 `await` 其中一个 sibling 再继续启动新的 durable step；await 后必须等整个 batch 完成，确保 replay 计算出同样的 dependency frontier。
- `step.sleep()` 和 `step.sleepUntil()` 记录 waiting state 和 due time，然后用 reserved internal sentinel suspend 当前 run。
- `step.waitForEvent()` 先检查 buffered event，再记录 wait 和可选 timeout。`sendEvent` 会在 wait 出现前保存 event payload 和 type index，因此支持 event-before-wait。
- Runtime 从头 replay 用户代码。它会 lazy fetch replay pages，也可以在进程内 advisory cache，但 DB 2 step state 始终是权威。
- V2 会为 `step.do` 持久化 DAG。runtime 按同步调用顺序分配 ordinal，把已完成 step 视作当前 dependency frontier，并把 frontier 存到后续 step 上。`Promise.all([step.do(...), step.do(...)])` 会产生拥有相同 parent 的 sibling nodes；join 后再调用的 `step.do` 会依赖这两个 sibling。依赖调度、join、cancel 仍由用户代码的 `await` / `Promise` 结构表达；workflows 持久化最终 graph，不另跑一个独立 graph planner。

Fence 模型：

- Execution commit（`claim-step`、step success/error、sleep/wait registration、runtime terminal）同时由 `generation`、`runToken`、active instance status 和未过期 run lease fence。Step commit/register 接受同一 run 的 `running` 或 `waiting`；completed runtime terminal 要求 `running`；failed runtime terminal 在 run lease 仍有效时也可以关闭由非法未 await suspending step 造成的同一 run `waiting` 状态。如果 lease 已过期，workflows 只恢复 ready hint，让下一次 claim 在新 lease 下 replay。
- Lifecycle commit（`pause`、`resume`、`restart`、`terminate`、retention cleanup）使用 generation fence，并在会 invalidate in-flight execution 的路径中轮换 `generation`。
- `sendEvent` 面向 instance 当前 generation。如果并发 restart 先赢，send-event 返回 conflict，而不是写入 stale state。
- Payload bytes、payload refs、counters、state change 和 ready/due update 必须在 DB 2 中一起 commit；payload ref 缺失时 workflows 必须 fail closed。

## Progress Callback

Progress callback 是 best-effort same-worker Durable Object push。Create request 可把 `{ kind: "do", binding, idFromName, path? }` callback descriptor 存进 instance state。workflows 向 runtime `POST /internal/workflows/notify` 推 progress；runtime 调 reserved `__WdlWorkflowNotify__` entrypoint，再调用同 worker 的 DO binding。Lookup 和 delivery 各有独立有界 semaphore：`WORKFLOWS_PROGRESS_CALLBACK_LOOKUP_CONCURRENCY` 默认 `128`，`WORKFLOWS_PROGRESS_CALLBACK_CONCURRENCY` 默认 `32`。Saturation 会 drop 这个 best-effort callback 并记录 dropped outcome；delivery 不具备事务性，DB 2 status 才是权威状态。

## 安全边界

- workflows private API 不公开路由。
- Tenant code 只拿 runtime `Workflow` facade，不拿 raw backend Fetcher。
- 保留的 `__WDL_WORKFLOWS_BACKEND__` binding 在 user env 暴露前会被移除。
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

workflows 遵循 Rust service observability shape：JSON logs、`/_healthz`、`/_metrics`、request in-flight tracking、shutdown drain 和有界 labels。Runtime 输出 workflow dispatch、replay cache、payload-limit 和 callback outcome。Workflows 输出 internal DO alarm delivery/retry/discard outcome 和有界 `do_alarm_dispatches` metric。Scheduler 把 workflow tick failure 与 queue/cron dispatch 分开记录。

## 部署 / Rollout 注意事项

- Workflows rollout 跨 control、runtime、do-runtime、scheduler 和 workflows。
- workflows dispatch run 到 runtime 前，runtime 必须先支持 workflow internal dispatch path。
- 当 do-runtime 会调用新的 workflows API shape 时，必须先滚 workflows，再滚 do-runtime；internal Durable Object alarm mutation endpoints 也属于这个顺序。
- Scheduler 可在 workflows 部署后 rolling，因为它只调用 tick endpoint。
- DB 2 是 workflow instance state 边界；不要从 control/runtime/scheduler 直接写 DB 2。
- workflows 在 DB 2 中持久化 `wf:schema_version`。Schema `2` 会在 step record 和 summary 中存储 DAG dependency edges。当前部署按该 schema 的 greenfield state 处理；没有新的设计前，不要为 in-flight legacy workflow instance 添加原地迁移路径。
- 如果开发或维护环境启动时 workflows DB 2 中已有未带版本的 `wf:*` runtime key，应先停止 workflows，清理该 DB 2 runtime state 后再启动。WDL workflow definitions 位于 DB 0 的 `wf:defs:*`，不属于 DB 2 runtime-state cleanup 范围。

## 保护该模块的测试

- `tests/unit/runtime-dispatch-workflows.test.js`
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
- 不提供平台托管的大 payload object-storage spill。
- 不使用 tenant Durable Object storage 作为 workflow backend。
- Runtime replay 不直接跳到 continuation；用户 JS 按 deterministic step ordinal replay，也包括并发 `step.do` 分配到的 ordinal。

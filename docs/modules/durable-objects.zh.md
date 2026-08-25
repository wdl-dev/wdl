# Durable Objects

## 目的

Durable Objects 为 tenant worker 提供有状态、按名称寻址的对象执行，同时保留 stock workerd 原生 Durable Object 编程模型，包括同步 SQLite-backed `ctx.storage.sql`。

## 当前实现

DO 执行被隔离在 `do-runtime`，这是监听 `:8788` 的独立 workerd service。Loaded worker 通过 `runtime/do-client.js` 中的 facade 访问；do-runtime 加载同一个不可变 bundle，用 `WorkerStub.getDurableObjectClass()` 解析用户 class，再通过 host actor 以 native facet 运行。

关键文件：

- `runtime/do-client.js`、`runtime/_wdl-do-scoped-request.js`
- `runtime/bindings/do.js`、`runtime/_wdl-do-transport.js`
- `do-runtime/index.js`、`do-runtime/actor.js`、`do-runtime/load.js`
- `do-runtime/owner-registry.js`、`do-runtime/owner-client.js`
- `do-runtime/alarm*.js`
- 负责 drain/renew process supervision 的 `supervisor`

workerd 在 host actor 内提供 native Durable Object 执行模型：class construction、facet identity、SQLite-backed storage、同步 `ctx.storage.sql`、alarm 的 storage-facing API surface，以及 facet 内 WebSocket hibernation API。WDL 补的是 Cloudflare 平台通常在 isolate 外部提供的部分：namespace binding materialization、owner lookup、路由到 owning task、Redis-backed lease/fence state、gateway-managed public WebSocket forwarding、通过 Workflows 驱动的 alarm scheduling 和 lifecycle cleanup metadata。

WDL 会 shim `ctx.storage.setAlarm()`、`getAlarm()` 和 `deleteAlarm()`，因为 stock workerd 在 WDL 使用的 SQLite-backed facet 上会对 native alarm 抛错。Alarm state 存在 object SQLite 中；Workflows 在 DB 2 中拥有 backend due/retry/discard job state。Alarm 写入支持在 top-level async `ctx.storage.transaction()` callback 内使用，shim 会在 transaction commit 后 flush backend side effect。`transactionSync()` 无法 await 这些 side effect，因此在同步 transaction callback 内调用 `setAlarm()` 或 `deleteAlarm()` 会抛错。`deleteAll()` 只在 transaction 外通过 WDL best-effort storage shim 支持。Nested async transaction 释放的只是 child savepoint，不是 backend commit boundary，因此其 callback 内也不支持 alarm API。

## 接口

- Tenant binding：loaded worker env 中的 Durable Object namespace facade。
- Native `ctx.storage.sql` 支持 SQLite R*Tree virtual table（`rtree`、`rtree_i32`）和 `rtreecheck()`，并沿用同一套 facet storage 与 ownership 边界。
- Runtime -> do-runtime fetch/RPC：`/internal/do/invoke`
- Runtime -> do-runtime WebSocket：`/internal/do/connect`
- do-runtime -> workflows alarm 写入：`/internal/workflows/do-alarms/set`、`/internal/workflows/do-alarms/delete`
- workflows -> do-runtime alarm dispatch：`/internal/do/alarms/dispatch`
- Internal storage cleanup：`/internal/do/storage/delete`、`/internal/do/storage/delete-worker`
- 本地 supervisor endpoints：`/internal/do/drain`、`/internal/do/renew`
- Owner/diagnostic probe：`/internal/do/probe`

Storage cleanup endpoint 是 native facet storage cleanup 和 worker storage cleanup 使用的私有平台接口，不是 tenant-facing API。它们预留给未来的平台 cleanup 流程，当前普通 worker lifecycle 路径尚未调用。

DO protocol error 使用 `{ error, message, details? }`。不同于 admin HTTP 的 flat additive error shape，DO protocol detail 会嵌套在 `details` 下，因为消费者是 runtime/DO client protocol，不是通用 admin JSON parser。未知 internal exception 仍会降级成安全的 `internal_error` / `Internal error` message。Storage delete-worker 在 partial batch result 时可能返回 HTTP 207 和 `{ ok:false, deleted, errors }`；这是 result envelope，不是 generic JSON error envelope。
Tenant-originated DO fetch body 在 runtime host adapter 中限制为 1 MiB。Adapter 会在读取前拒绝超限的 `Content-Length`，streamed body 会增量读取，因此 limit 会在完整 buffering 前生效。

DO RPC method name 使用 JavaScript identifier grammar，do-runtime protocol reader 将其限制为最多 256 ASCII bytes。RPC 参数是最多 1 MiB 的 structural JSON data：接受 finite number、string、boolean、null、dense array 和 plain object。序列化不会调用 `toJSON()` hook；sparse array、circular structure、non-plain object 和 non-JSON value 会在 dispatch 前失败。Host adapter 也会在 1 MiB 上限内读取 RPC response envelope：超限的 `Content-Length` 会在 buffering 前被拒绝，streamed body 会在越界时立即取消。Body 读取、UTF-8 decode、JSON parse 或成功 envelope validation 失败会抛出 `do_rpc_result_unknown`；method 可能已经执行，因此 caller 不得盲目重放。

do-runtime 会通过 generated wrapper 截获的私有 fetch dispatch 调用 tenant alarm 和 RPC method；这些 request 携带外层 request id，使 host facade 可以传播该 id，且不会把平台 metadata 加入 tenant argument list。持久化 class instance 使用一个小型可变诊断 context，因此 concurrent 或 re-entrant call 可能观察到另一次 invocation 的 id。嵌套 DO fetch/connect request 会丢弃 tenant 提供的 `x-request-id`，并在 context id 可用时传播其净化值；request id 仍是 best-effort、不可信的诊断 metadata。

DO invoke envelope 通过 canonical namespace、worker、version 和 storage id 标识 persisted bundle，不接受 inline worker source。

Tenant-facing DO object name/id 必须是 well-formed Unicode string。`idFromName()` / `idFromString()` 会在 hash 或 dispatch 前拒绝 lone UTF-16 surrogate；do-runtime alarm ingress 和 Workflows revalidation 执行相同边界。Binding-scoped native Fetcher 在两段 object-name header hop 上使用同一套 canonical、可逆的 ASCII encoding，因此 HTTP header normalization 不会在原始 identity 到达 do-runtime validation 前裁剪空白或拒绝 Unicode。

DO host id 最多 512 UTF-8 bytes，并使用不带前导零的 canonical `shardN` suffix。DO binding class name 使用 ASCII JavaScript class-name grammar，并在 deploy 时限制为最多 468 bytes，确保所有 shard suffix 都能满足 aggregate host-id 上限。

## Redis / Storage 合同

Control 为每个 logical worker lifecycle 分配 opaque `doStorageId`，并冻结进 DO binding metadata。Native facet SQLite 文件位于 do-runtime `localDisk` storage；ECS 中挂载在 EFS 上。

Key families：

| Key | Type | Owner | Authority | Cleanup/delete 语义 |
|---|---|---|---|---|
| `worker:do-storage:<ns>:<worker>` | String | Control | logical worker 到当前 `doStorageId` 的权威指针。 | Whole-worker delete 删除指针；之后 redeploy 会分配新的 storage id。 |
| `worker:session-policy:<ns>:<worker>` | String | Control | 与 route flip 同事务提交的 active JSON projection `{version,mode,restartSequence}`。 | Whole-worker delete 删除；缺失 projection 视为默认 `preserve`。 |
| `worker:session-policy-seq:<ns>:<worker>` | String | Control | session policy restart event 的永久单调序号分配器。 | Whole-worker delete 后仍保留，确保重建后的下一次 restart 不会复用旧 Gateway session 已观察过的 sequence。 |
| `do:objects:<doStorageId>` | Set | do-runtime | 某个 storage id 下已观察 object 的 best-effort registry/tombstone。 | Whole-worker delete 后保留给未来平台 cleanup；object SQLite state 仍在 localDisk/EFS。 |
| `do:owner:scope:<encoded scope>` | String EX | do-runtime | `doStorageId:className:shard<N>` 的权威 owner lease。 | Lease expiry 以 Redis server `TIME` 为准；stale owner 不得 commit。 |
| `do:owner:scope:<encoded scope>:generation` | String | do-runtime | owner scope 的 monotonic generation counter。 | 不递减；stale generation 会被拒绝。 |
| `wf:internal:do-alarm:{<jobId>}:state` 以及相关 `wf:internal:do-alarm:*` keys | Hash/ZSET/Set | workflows | 单个 SQLite alarm row 的 backend job 权威状态。 | 成功 delivery、retry 耗尽、显式 delete 和 whole-worker cleanup 会移除 job。 |

Ownership 按 shard 划分：

- 每个 Worker DO class 有 16 个固定 host actor shard。
- Shard = `stableHash(objectName) % 16`。
- Owner lease scope 是 `doStorageId:className:shard<N>`。
- Redis owner state 携带 task identity 和 monotonic generation。

Alarm state 存在 object SQLite。Workflows 接收 do-runtime 的 set/delete 请求，并为每个 pending row 保存一个 internal job。Row token 用于 fence 用户驱动 delete 和 stale backend delivery；Workflows run token 在 DB 2 内 fence dispatch retry 和 completion。

workerd 2026-07-01 会大小写不敏感地拒绝 SQLite reserved `_cf_` namespace 下的 object name。WDL best-effort `ctx.storage.deleteAll()` 也会跳过这些名称，因此 `_CF_*` 这类旧变体在 private do-runtime facet-deletion owner 移除完整 database 前仍是 tenant SQL 不可访问的升级遗留物。

Pinned stock workerd 的 native SQLite reset path 在递归删除 child facet 时假设当前 actor 是 root，但 WDL tenant object 本身是 facet。因此 WDL 通过 public storage operation 实现历史 `deleteAll()` surface：以最多 128 个 key 为一批 list/delete KV、drop tenant SQL object，再更新 Workflows alarm index，避免无界加载 value 或超出 delete batch 上限。该路径在 KV、SQLite 与 DB 2 之间明确不原子；reject 可能留下部分结果，caller 不得让 `deleteAll()` 与其它 storage 或 alarm mutation 并发。WDL-specific `deleteAlarm:false` 会保留 local/backend alarm；省略或传 `true` 会请求 best-effort alarm cancellation。其它 native `deleteAll()` option 不会增强该合同。Private facet deletion 仍是权威 platform cleanup path。

`getAlarm()` 会做 best-effort alarm-scoped read repair：如果 SQLite 中有 pending alarm row，但 Workflows DB 2 due index 缺失，它会尝试幂等重写 backend due index，而不会给普通 DO fetch 增加 Redis IO。Repair failure 只记录日志并被吞掉；返回 timestamp 只确认本地 SQLite row，不确认 backend job。在 async storage transaction 内，`getAlarm()` 只读取本地 transactional state，不尝试 backend repair。每个 object 的全部 Workflows alarm-index mutation（包括 best-effort repair）共用一个 process-local promise tail，因此 concurrent request 会按 API 顺序到达 backend。Mutating API failure 会返回对应 caller；best-effort read-repair failure 只记录日志并吞掉，两者都不阻塞后续 mutation。Top-level transaction 会在首次 alarm mutation 时预占该 tail 的位置，native commit 后才用最终 coalesced effect 填充，因此后发 non-transactional mutation 不会反超。`preserve` 下 active/retained alarm 保留调度时的 worker version；`restart` promotion 会在旧 version 仍 retained 时将 superseded alarm retarget 到 active version，删除 retained version 也执行相同 retarget。两种转换都要求 `doStorageId` 仍匹配。逻辑 worker 已消失或指向不同 `doStorageId` 时，alarm 会自清理。

Alarm mutation 横跨 object SQLite 与 Workflows DB 2，明确不提供分布式事务。成功完成的 `setAlarm()` 才进入 at-least-once delivery 合同；input validation 会在两个 store mutation 前失败并保留当前 alarm。Backend index mutation 开始后，若 `setAlarm()` reject，最终状态未知，replacement attempt 也可能使旧 alarm 无法再触发。仍需要 alarm 的 caller 必须再次调用 `setAlarm()`；`getAlarm()` read repair 只适用于仍保留 SQLite alarm row 的情况，也不能确认失败的 set mutation。`deleteAlarm()` 在 backend mutation 开始后 reject 也属于结果未知：token-fenced compensation 可能在 backend job 已被删除后恢复 SQLite row。仍要求删除的 caller 必须再次调用 `deleteAlarm()`；要求保留 alarm 的 caller 必须重新调用 `setAlarm(desiredTime)` 并观察成功。`getAlarm()` 可以暴露仍存的本地时间并触发 best-effort repair，但返回 timestamp 或 `null` 都不确认 backend state。Async `ctx.storage.transaction()` 会先 commit 本地写入，再 flush post-commit backend alarm；若 transactional `setAlarm()` 或 `deleteAlarm()` flush reject，callback 中其它 tenant storage 写入仍已提交，而 alarm row 会执行与操作对应的独立 compensation 并保留前述 unknown-state 语义，因此 caller 不得盲目重跑整个 transaction callback。Transaction callback 会把进入时的 alarm row 作为 backend baseline；最终 delete 的 fence、backend request 与 compensation 只围绕该 baseline。Baseline 不存在时，callback 内 `setAlarm()` 后再 `deleteAlarm()` 只修改本地状态，不发送 backend delete。显式 `txn.rollback()` 会丢弃全部 queued alarm side effect，之后继续调用 shim alarm API 会抛错。SQLite-backed workerd 中 owning `ctx.storage` alias 与 callback `txn` object 的 alarm 调用属于同一个 native transaction；callback settle 前运行的 same-event branch 也会参与该 transaction，caller 必须观察 outer transaction result。Shim 会把 active transaction fence 保持到 native commit/rollback settle；callback settle 后才运行的 Promise reaction 会看到 closed transaction，不能在 native rollback 前 enqueue backend alarm。Transaction-local `getAlarm()` 不执行 backend repair；constructor-visible storage alias 与后续 `this.ctx.storage` 复用同一个 proxy/context。只有 native rollback 成功后才更新 shim bookkeeping；失败的 rollback 保留 queued effect，重复成功 rollback 维持 native no-op 语义。

Bundled workerd 为其 native alarm scheduler 暴露 `ctx.abort(reason, { retryAlarm: false })`。WDL 通过 authenticated、owner-fenced fetch 调用 tenant `alarm()`，并由 Workflows DB 2 持有 retry state，因此 workerd 不会把该 dispatch 识别为 native alarm event。该 option 仍会 abort facet，但不会禁止 WDL alarm retry；WDL 不会静默声称支持这项新的 native retry-control contract。

Pending delete row 会保存 internal fence token 并固定 `in_flight=1`。该 bit 是 same-service rolling reader fence：旧 do-runtime 虽不识别 token prefix，但会跳过 `getAlarm()` repair，并因原 backend token 与 fence token 不匹配而忽略 delivery，因此不会把 tombstone 当作 tenant alarm 执行。混部期间旧 mutator 仍可能把 fence token 作为一次无效 backend CAS 发出；当前 reader 会在删除前统一解开该 token。
创建 fence 时只在 current token 约束下更新 `token` 和 `in_flight`，不重新校验无关的 scheduled-time/retry 字段，因此损坏或 legacy row 仍可删除。

## Ownership / 并发 / 失败语义

- 同一时间只有一个 task 拥有一个 class shard。
- Generation fence 防止 stale owner 在 ownership 移动后继续 commit。
- `do-runtime/protocol/wire-grammar.js` 持有 producer 与 binding-scoped host transport 消费的 DO ownership error vocabulary，以及 host 的 pre-dispatch-safe retry subset。
- Facet identity 是 stable `doStorageId` 内的 `className:objectName`，因此两种 session policy mode 都保留 SQLite object state。
- Worker 级 session policy 合同——`sessionPolicy` bundle metadata 字段、route/projection 原子 promote commit、永久 sequence 分配、`session-policy:restart` publish 和公开 WebSocket reconciliation——由 `docs/modules/control-auth.zh.md` 与 `docs/modules/gateway.zh.md` 持有。本模块只消费已提交的 projection。
- 字段缺失或显式 `preserve` 时保持既有 facet 行为：已构造的 native facet 会保留 class version，直到 host actor restart 或 facet deletion。`restart` 则改为 lazy 退役 stale facet。
- 最新 active projection 生效。后续 `preserve` promotion 会保留已分配 sequence，并覆盖 Workflows 或 host actor 尚未观察到的 lazy restart；它不能撤销已经发生的 facet abort。
- do-runtime 不会在 promotion 时枚举所有 owner 或 facet。Owner resolution 与 owner-side dispatch 会在 owner/storage snapshot 中读取 active projection。Restart sequence 和当前 mode 是由这些 Redis read 得到的 owner-local state，不属于 invoke/connect wire field。当前 `restart` projection 会拒绝 superseded immutable version；更高 sequence 的首次 host-actor dispatch 会在重建前仅通过 `facets.abort()` 中止对应 stale facet，绝不调用 `facets.delete()`，因此 SQLite storage 保留。延迟到达的低 sequence dispatch 不能替换更新 facet。
- Native facet container 是 task-local，即使 host SQLite 由多个 task 共享也是如此。因此 host actor 会按 `(task_id, facet_name)` 保存已观察到的 restart sequence，并在 actor 重建后只读取当前 task 的记录。这样 lazy restart fence 可以跨 eviction 与 owner 转移保留，无需扫描全部 facet，不会让一个 task 代替另一个 task 确认其 stale native facet，也不会重复 abort 已经到达当前 sequence 的 facet；storage cleanup 会删除该 facet 的全部 task 记录。
- ECS task identity 是每个 task 独有的 ARN，而 workerd 没有提供 native facet container 已永久退役的权威信号。因此 ledger 不使用 TTL 或 LRU 清理：无论采用哪种 residency mode，长期存活的 facet 都可能为每个曾经 dispatch 它的 task 保留一条小记录，直到 storage cleanup。平台明确接受这项按 task 增长的存储成本，以免删除仍存活 task 在后续 owner 返回时所需的 fence。
- Actor-local facet-registration cache 和成功 object-registry memo 都是上限 10000 项的 advisory 内存表。Facet metadata eviction 会重新读取权威 SQLite row；object memo eviction 会重复幂等 Redis `SADD`。这些上限不会清理持久 session-policy ledger、object registry、native facet 或 loaded worker isolate。`workerLoader` binding 是唯一的 worker-stub factory，并持有 native isolate residency；host actor 不再镜像 worker id。Worker/class resolution 只在 native facet startup callback 内执行，不会发生在 warm-facet dispatch 上。
- Storage cleanup 按 stable `doStorageId` 定位，而不是按 immutable worker version 定位。它可以在请求 version 已 superseded 时经 owner dispatch，但 actor 进入 storage-delete branch 前仍会检查 whole-worker delete exclusion、active storage pointer、owner generation 和 lease fence。
- 已经运行的 call 和既有 facet 不会在 promotion 时被同步枚举或中断；它们会在下一次 dispatch 时收敛。
- Whole-worker delete 后 redeploy 会分配新的 `doStorageId`；旧 native storage tombstone 给后续 cleanup，而不是立即物理删除。
- WebSocket upgrade 必须在 owner endpoint 上完成。Cached endpoint 返回合法、可信的 owner hint 时，host 会清除 cache，并允许一次 router rediscovery 后连接新解析出的 owner。携带私有 hint marker 但 status、marker value、metadata 或 shard 不合法的 response 会净化为 `owner_unavailable`；router handoff 后的 transport failure 不会 fall back 到 router-established 101。
- 未缓存的 ordinary fetch/RPC 会进入一个 router。如果 shard 由另一 task 持有，该 router 会把 bounded invoke forward 一次，并把携带可信 owner headers 的最终响应返回；host 无需再次上传 invoke 即可学习 hint。旧 invoke hint opt-in 会被忽略，WebSocket connect 则继续使用上述 hint-only handoff。
- Forwarded 或 cached-direct call 会把可信 route fence 携带到目标 task。该 fence 与接收 task 的 process-local owner record 和 canonical owner shard 精确匹配时，do-runtime 只跳过外层 owner-resolution snapshot，并把 call 排队到 host actor；actor 仍会在 tenant dispatch 前读取 Redis time、精确 owner/generation、worker delete lock、active storage 和完整 session-policy projection。Actor dispatch 和 storage delete 还会在 mutation 前进入同一个 in-flight admission；task drain 会拒绝新操作，并在释放 ownership 前等待已进入的操作结束。缺少本地 lifecycle state 或 task 不匹配时会回到普通 resolution；stale fence 会产生同一个可信 pre-dispatch ownership error，并触发一次 host-side router rediscovery。旧 sender 不携带 fence，旧 reader 会忽略它，因此 rolling deployment 会保留完整 resolution 路径。
- Ordinary fetch/RPC 在收到携带 do-runtime 私有 ownership-error control header 的明确 pre-dispatch stale-owner/owner-race response 后，可以进行一次 router rediscovery；这也适用于非幂等 method 和 RPC。当前 router 会直接 forward ordinary invoke，因此 host 不会跟随 legacy owner-hint response，也不会第二次上传 invoke。Tenant response body 不能触发重放。无可信标记的 direct owner transport failure，或不带可信标记的 502/503/504，会清除 cached hint。安全的 `GET`/`HEAD` request 可以通过 router 重放；非幂等 method 和 RPC 会返回 `owner_unavailable`，因为 owner 可能已经应用了该请求。`owner_unavailable` 等 broad trusted ownership error 使用同一个分流：`GET`/`HEAD` 可以 rediscover，非幂等 method 和 RPC 直接终止。
- Host adapter 独占 owner-hint cache wiring、invoke/connect framing、race retry、direct-owner forwarding 和 response-header stripping。Injected facade 只把 public request、canonical object name 和诊断 request id 打包成 binding-scoped call。Connect transport 只在收到合法、可信的 owner-hint control response 或窄的 pre-dispatch ownership race 后允许一次 rediscovery，不会把 broad ownership error 或无可信标记的 transport failure 通过 router 重放。同一个 loader isolate 中的 host adapter 共享一个 process-local、上限 10000 项的 LRU，key 包含 `doStorageId`、class 和 canonical owner shard。同一 shard 的不同 object name 只复用 routing hint；authenticated invoke 仍携带精确 object name，owner actor 仍是权威。可信响应中的 `ownerKey` 必须与本地计算的 shard 一致才能缓存或跟随，明确的 stale-owner response 会清除该 shard entry。Malformed、wrong-status 或 mismatched WebSocket hint control response 会被丢弃并收敛为 `owner_unavailable`，因此 task identity 和私网 endpoint 不会跨过 tenant response boundary。Eviction 只会移除 routing hint，下一次请求会回到 router；高基数流量可能增加其它租户的 miss，但不能跨越 object identity 或 ownership fence。
- Runtime 为每个声明的 DO binding materialize 一个 host adapter。不可变 adapter props 会在附加 internal auth 前固定 namespace、worker version、storage identity 和 class；loaded-worker env 不包含通用 DO router 或 owner-network Fetcher。Module evaluation 可能观察到这个 scoped transport，但不能选择其它 DO binding identity。DO fetch 通过原生 `Fetcher.fetch()` 而不是自定义 Request RPC 进入 adapter。静态 host worker 启用 incoming request signal，因此显式可取消的 caller `Request` 被 abort 时，会在 owner dispatch 前取消有界 body 读取。结构化 DO RPC 仍以有界 JSON data 跨越自定义 RPC 边界，不传递 `Request`。
- `WEBSOCKET_RECONNECT_DELAYS_MS` 和 `WEBSOCKET_MAX_BUFFERED_MESSAGES` 可以在不改代码的情况下调整 gateway backend reconnect budget 和 client-message buffer cap。
- Alarm delivery 是 at-least-once。Scheduler 唤醒 Workflows；Workflows 把到期 internal alarm job promote 到 ready，在 DB 2 run token 下 claim，然后调用 do-runtime `/internal/do/alarms/dispatch`。do-runtime 构造 `DoInvoke{kind:"alarm"}` 请求，并走正常 owner router/fence 路径。
- Alarm mutation、retarget、dispatch 和 whole-worker storage cleanup 只接受 canonical positive JavaScript-safe-integer worker version grammar。非法 internal 或 persisted version 会在写入 job 或尝试 worker invoke 前失败。
- Alarm due time 是传给 `setAlarm()` 的 Unix millisecond timestamp。Workflows 和 do-runtime 都用各自本地 wall clock 判断这些 timestamp；如果 backend ready hint 在 SQLite alarm row 对 do-runtime 来说尚未到期时抵达，do-runtime 会 ignore 这次 dispatch，但不清 row，让 backend due-index repair 路径之后继续投递。这是 alarm compatibility 边界，不属于 Redis-time owner lease fence。
- Failed alarm 使用 `WORKFLOWS_DO_ALARM_RETRY_DELAY_MS`、`WORKFLOWS_DO_ALARM_RETRY_MAX_DELAY_MS` 和 `WORKFLOWS_DO_ALARM_RETRY_JITTER` 的 exponential backoff 和 jitter 重试，最多到 `WORKFLOWS_DO_ALARM_RETRY_MAX_TRIES`（默认 `6`），之后 discard 并增加 `do_alarm_dispatches{outcome="discarded"}`。
- 如果 Workflows client 调用 do-runtime 后 timeout，backend 会保留 running claim 到 `WORKFLOWS_DO_ALARM_CLAIM_LEASE_MS` 过期，而不是立即调度 retry。默认值是五分钟，且配置值会被 clamp 到高于 `WORKFLOWS_DISPATCH_TIMEOUT_MS`，这样正常 timeout 处理可以避免 do-runtime 仍在执行原 dispatch 时并发执行重叠 alarm body。Operator 应按最长预期 alarm handler body 配置 claim lease，而不只是按 HTTP dispatch timeout；alarm body 仍是 at-least-once，claim lease 过期后可能重叠执行。

Owner resolution 是单写入协议：

1. do-runtime 从 `doStorageId`、class name 和 shard 派生 owner scope。
2. Owner resolution 会 WATCH owner record、generation key、worker delete lock、active worker storage pointer 和 active session policy projection。`whole` delete lock 会拒绝 ownership；`version` lock 仍属于 WATCH snapshot，但不会中断 active storage。`restart` projection 会拒绝旧 immutable version，并把 sequence 传给 target-version dispatch。这个 WATCH 会阻止 claim 在 whole-worker delete 或 session policy state 变化后提交。Renew 会先 pipeline 读取 owner/storage snapshot，再通过 Lua CAS 原子比较 owner 原始字节与 active storage pointer，然后刷新 TTL；generation fence 已包含在 owner record 中，不需要再次读取 generation key。
3. 如果另一个 task 持有 live owner，router 会把 ordinary fetch/RPC forward 一次，并返回携带 owner headers 的最终响应；runtime host adapter 会缓存这些 headers，供后续请求直连。Forwarded 和 cached-direct call 会把 route fence 带入 owner actor，后者仍是权威 admission layer，因此目标 task 可以省掉原本重复的外层 snapshot。WebSocket connect 则接收 owner hint，并直接向该 endpoint 建立 `101`。所有路径中的 owner actor 都会在 tenant dispatch 前重新读取 Redis time、精确 owner/generation、worker delete lock、active storage 和完整 session-policy projection。Actor dispatch 和 storage delete 还会在 mutation 前进入同一个 in-flight admission；task drain 会拒绝新操作，并在释放 ownership 前等待已进入的操作结束。
4. 如果 owner 缺失或过期，claimant 在一个 Redis transaction 中递增 monotonic generation counter，并写入带 TTL 的 owner record。
5. Local dispatch 使用 native facet 前检查 `taskId`、`generation`、lease expiry、worker delete lock、active `doStorageId` 和剩余 lease budget。`whole` lock、stale generation、expired lease 或 storage pointer 改变都会 fail closed；`version` lock 不会中断 active storage。包括 `/delete-storage` 在内的每次 owner-side assertion 都在同一个 snapshot 中读取 owner record、delete lock、active storage pointer 和 Redis time。剩余 lease 小于 `DO_OWNER_LEASE_GUARD_MS`（默认 `1000`）时，owner 会先尝试 same-task、same-generation CAS renew；如果 renew 失败，才 fail closed。这个 guard 缩窄 takeover window，但不是 per-SQL-call 或 SQLite commit-time fence。
6. Supervisor 通过 `127.0.0.1:8788` renew 本地 owned scopes；`/internal/do/probe` 暴露 task 和 owner state 供诊断。Supervisor 允许本地 drain HTTP 调用执行最多 `DO_DRAIN_TIMEOUT_MS`（默认 `10000`）。在该请求内，do-runtime 停止新 ownership，并等待最多 `DO_DRAIN_IN_FLIGHT_TIMEOUT_MS`（默认 `8000`）让 host-actor dispatch 和 storage delete 完成，然后释放匹配 generation。Drain/renew response body 会在 256 KiB 上限内流式读取，再执行 JSON parse 或诊断截断。Drain 成功后，`do-supervisor` 会直接 kill workerd，而不是依赖 workerd 在 SIGTERM 后的 graceful window；后者会让 listener 处于 half-dead 状态，制造 takeover 504 窗口。Drain timeout 时返回 503 并保留 lease，让 failover 等正常 lease expiry。In-flight handler 还有 lease-budget watchdog：它会在 expiry 前 `DO_OWNER_LEASE_GUARD_MS` 重新检查 ownership；如果 renewal 停止或 ownership 移动，会 forget 受影响 owner scope 并 abort 受影响 facet；它不会把整个 task 标记为 draining。

Generation key 不是 cache，而是 fence。即使过期 Redis owner record 消失、另一 task 重新 claim 同一 scope，stale owner 后续 owner-side check 也会 fail closed。它阻止 stale owner 开始新的受保护 dispatch，或通过 lease-budget recheck；它不是已经运行中的 SQLite commit 的物理 fence。

Terraform 除了 Fargate task memory limit，还会给 do-runtime workerd container 设置显式 memory hard limit，并为同 task 的 redis-proxy sidecar 保留内存。这是 container failure boundary，不是 per-storage-call memory interrupt。

## Actor 驻留与驱逐

`DO_PREVENT_EVICTION` 是 do-runtime 的部署级配置。未设置或精确值 `true` 会选择设置 `preventEviction = true` 的 resident config；精确值 `false` 会选择其它配置完全相同、但省略 `preventEviction` 的 evictable config，允许 stock workerd 驱逐 idle actor。其它值属于配置错误，`do-supervisor` 会在启动 workerd 前退出。Terraform 的 `do_prevent_eviction` 变量、Compose 和本地 Kubernetes overlay 都默认使用 `true`。

修改该设置需要 rollout do-runtime。它不会改变 host actor unique key、owner scope、object identity 或 localDisk path，也不会删除 SQLite。Supervisor 会通过 `do_actor_residency_configured` 结构化事件记录实际选择的模式。

在该变量为 `false` 时，当前 bundled workerd 可能在 actor 空闲约 10 秒后 shutdown，并以约 70 秒的周期清理已断开 client 的 actor container。这些是上游当前实现时序，不是 WDL 的 latency 或 eviction SLA。WDL gate 会观察较长 idle 后的 actor 重建，但不声称直接观察内部 ActorContainer erase pass；该实现级测试由 workerd 拥有。Active request 和 non-hibernating WebSocket 可能继续让 actor 保持 active。通过 `ctx.acceptWebSocket()` 接受且处于静默状态的 socket 不同：workerd 可以保留 native socket、销毁 JavaScript actor，并在下一条消息到达时重建 actor。

Actor 重建边界如下：

- SQLite storage、object identity，以及静默 hibernatable WebSocket 的 attachment、tag 和 native socket 会保留。当前 workerd 的 legacy hibernation manager 仍可能在 actor eviction 期间静默丢失在途 application send 或 close。Workerd 2026-08-25 会跨 revival 保留在途 auto-response state，并防止其被 GC，已移除此前 blocked auto-response caveat。Resident 默认值避免既有部署自动暴露于剩余竞态。只有不依赖该 in-flight 边界且已通过 focused eviction gate 的 workload 才应设置 `DO_PREVENT_EVICTION=false`。
- JavaScript instance field、actor 持有的 worker/class reference、in-memory cache 和 non-durable timer 不会保留；应用不能把它们当作持久状态，底层 workerLoader code cache 可以有独立生命周期。
- Owner lease/generation renewal 与 JavaScript actor residency 相互独立。Renewal 既不会 pin actor heap，也不能替代重建后的 owner-side dispatch fence。
- 驱逐后的第一次 dispatch 是 cold dispatch，紧接着的下一次 dispatch 会重新变 warm。显式 `false` 是以 first-dispatch latency 换取更低的 inactive-actor residency。

用于资格确认的双 task ECS 对比执行了三组交错的 resident/evictable 测试。在该有界 fixture 中，evictable 模式让 75 秒时的 do-runtime memory 中位数下降 29%。该结果只确认 residency 取舍的方向，不是其它 workload 的 capacity 保证。

把该变量设为 `false` 不是 process memory ceiling。V8、SQLite、allocator arena、loaded code、active request 和 non-hibernating socket 仍可能保留内存；已释放 heap 也不保证立刻归还操作系统。Container memory limit、owner transfer 和按 workload 执行的 capacity test 仍然必需。

## 安全边界

- do-runtime internal endpoints 只在 private mesh 内可达，并要求共享的 `WDL_INTERNAL_AUTH_TOKEN` / `x-wdl-internal-auth` 内部认证 header。Health 和 metrics endpoint 例外。
- Tenant code 只能通过 runtime 生成的 facade 和 frozen metadata 访问 DO。
- Module-scope raw env 只包含 binding-scoped DO host adapter，以及 do-runtime 中固定 worker/storage identity 的 alarm-index adapter。DO adapter 会用固定 props 重建 internal invoke/connect metadata，并在 dispatch 前删除 tenant 提供的 control header；alarm adapter 不暴露通用 Fetcher，alarm delivery 仍由匹配的 SQLite row token fence。
- Tenant-visible DO metadata 和 error 不得包含 owner task id、backend endpoint 或原始 transport error 文本。
- Owner hint 只信任 do-runtime header，并且要通过 endpoint grammar validation。Owner hint 和 invoke fence 必须携带正的 JavaScript-safe-integer generation。
- Task identity 和 persisted owner record 在写入和读取时都会校验。Persisted record 的 `ownerKey`、`hostId`、storage id、class 和 shard 必须能重建出读取它的 Redis scope；owner resolution 还必须在 do-runtime 读取 invoking bundle 的 active storage pointer 前，确认 record 的 canonical namespace 和 worker 与该 bundle 一致。Owner forwarding 只接受 8788 端口上的 DO service/headless DNS，或 RFC1918/100.64 私网 IPv4；非法记录在附加 internal auth 前 fail closed。
- Owner-hint 与 ownership-error 防御是分层的：忽略 tenant response body 和 tenant-supplied control header，只信任 do-runtime control header；hint 还必须通过 endpoint grammar / acceptable-address 检查。最终可信的 ownership-control error 只有在 status 为 `503` 且 code 位于 allowlist 时才保留 code；其它 code/status 组合统一变为 `503 owner_unavailable`。Host 会在返回 tenant code 前替换 message 并丢弃私有 details。
- Binding-scoped host adapter 在 tenant realm 外持有 DO transport 与共享 D1/DO endpoint validation，并捕获 private-header stripping、request bound、invoke serialization、replay classification 和 endpoint validation 使用的 intrinsic。注入 tenant 的 facade 只包含公开 namespace/id/stub 行为和 scoped-request codec；tenant prototype mutation 不能改写受信 target 或 replay policy。
- 注入的 alarm shim 也会在 tenant module 前执行，并捕获 internal alarm 分类、SQLite 状态更新和 storage facade 安装依赖的 request、response、number、proxy 与 reflection 操作。Tenant 顶层对这些 intrinsic 的修改不能把 internal alarm 重定向到 tenant fetch handler，也不能阻止 facade 安装。
- do-runtime supervisor 必须调用本地 `127.0.0.1:8788` drain/renew endpoint；Service Connect alias 可能打到其他 task。

## 可观测性

do-runtime 围绕 actor residency 选择、owner resolution、session-policy fence、lazy facet restart、dispatch、alarm execution、drain、renew 和 WebSocket 处理输出结构化日志。Workflows 通过 `do_alarm_dispatches` 输出 backend alarm delivery/retry/discard/in-flight-unknown outcome；do-runtime metrics 覆盖 runtime operation。Dispatch admission 只更新进程内 in-flight 权威 counter，`/_metrics` 在 render 前即时发布 gauge。Gateway request log 不衡量 initial 101 之后的 backend WebSocket recovery 生命周期。

## 部署 / Rollout 注意事项

- 跨 tier DO protocol 变化遵循 [infra rollout 注意事项](infra.zh.md#部署--rollout-注意事项)中的 reader-before-writer 流程。
- workerd 进程终止前应先 drain，让 owned shard 释放，或通过 lease expiry failover。
- EFS shared storage 只有在 owner lease + generation fence 保证每个 owner scope 单 writer 时才安全。
- Best-effort 尝试把 localDisk volume 从 workerd 2026-07-03 或更高版本降回 2026-07-01 时，应执行 [infra rollout 注意事项](infra.zh.md#部署--rollout-注意事项)中的 scheduler metadata cleanup。
- 这项 cleanup 只恢复进程启动。降回 workerd 2026-07-01 前，必须重写或删除通过 `ctx.storage.put()` 持久化的 `Blob`，因为该版本无法反序列化这些值。
- Drain 和 renew 必须打本地 `127.0.0.1:8788` service。Service Connect 或 Kubernetes service alias 可能命中其他 task，不能表达 local-owner release semantics。
- `DO_OWNER_TTL_SECONDS` 必须是正的 canonical 十进制字符串，且不大于 `9007199254740`，确保从秒换算成毫秒后仍是安全整数。非 canonical 或超出范围的值会在 workerd 与 supervisor 中统一回退到 120 秒，避免 claim 和 renew schedule 分叉。
- Supervisor 侧的 `DO_DRAIN_TIMEOUT_MS` 必须是正的 canonical 十进制字符串，且不大于 `9007199254740991`；非法值回退到 10000 毫秒。它限制 supervisor 的本地 HTTP 调用，与 do-runtime 内等待 host-actor 的 `DO_DRAIN_IN_FLIGHT_TIMEOUT_MS` 不同。
- 修改 `DO_PREVENT_EVICTION` 需要 rollout do-runtime。使用 hibernatable WebSocket 或依赖 resident actor state 的 workload 应保持默认 `true`；只有接受文档中的 cold-dispatch 与 in-flight WebSocket 边界，并针对目标 workerd image 通过 focused eviction gate 后，才应设置为 `false`。

## 保护该模块的测试

- `tests/integration/durable-objects-core.test.js`
- `tests/integration/durable-objects-storage.test.js`
- `tests/integration/durable-objects-ownership.test.js`
- `tests/integration/durable-objects-alarms.test.js`
- `tests/integration/durable-objects-eviction.test.js`
- `tests/integration/durable-objects-websocket.test.js`
- `tests/unit/do-alarm-client.test.js`
- `tests/unit/do-alarm-shim.test.js`
- `tests/unit/do-owner-registry.test.js`
- `tests/unit/do-owner-client.test.js`
- `tests/unit/do-object-registry.test.js`
- `tests/unit/do-runtime-actor.test.js`
- `tests/unit/do-runtime-http.test.js`
- `tests/unit/do-runtime-load.test.js`
- `tests/unit/do-runtime-protocol.test.js`
- `tests/unit/do-state.test.js`
- `tests/unit/do-task-identity.test.js`
- `tests/unit/runtime-do-client.test.js`
- `tests/unit/runtime-do-transport.test.js`
- `rust/supervisor/src/drain.rs`
- `rust/supervisor/src/config.rs`
- `rust/supervisor/src/renew.rs`

## 已知约束和非目标

- 当前 lifecycle 不会在 worker delete 时物理清除 native facet SQLite storage。
- Whole-worker delete 会删除 active `worker:do-storage:<ns>:<worker>` pointer，并在 delete commit 后请求 Workflows 删除 internal DO alarm jobs；pointer 消失后，旧 facet 的 late `setAlarm()` 写入会被忽略。Cleanup 会 fence 到被删除的 `doStorageId`，因此同名 worker 以新 storage id redeploy 后不会被旧 delete 扫掉。如果 best-effort cleanup 失败，远期残留 alarm job 可能留在 DB 2 直到到期；随后 dispatch 会因为 storage pointer 已消失而自清理。Owner claim 会 WATCH 同一把 per-worker delete lock 和 storage pointer，每次 actor-side dispatch assertion 也会在 owner/storage snapshot 中读取该 lock；只有 `whole` lock kind 会拒绝 active storage，因此删除 inactive version 不会中断 active worker，而 whole-worker delete 也不会漏掉最终 scan 之后创建的 owner/generation 或 object-registry state。`do:objects:<doStorageId>` 会作为未来 platform cleanup 的 tombstone 保留。
- DO object registry 写入是 best-effort。Registry 写失败时 dispatch 会继续，因此 tombstone set 可能不完整；未来 cleanup 必须容忍缺失 member，并把 active storage pointer、owner/alarm state 当成更强的 lifecycle signal。
- Gateway-proxied WebSocket recovery 在 pinned immutable version 仍 active 时，才会在 `preserve` mode 下对 client connection continuity 做 best-effort；backend 脱离后不会重新加载已非 active 的版本。精确 restart 和 whole-worker-delete hint 只请求对应 worker 的权威检查；当前 projection 为 `restart` 或 worker 已 inactive 时，旧公开 Gateway session 会以 `1012` 关闭，restart 还会在 stale DO facet 下一次 dispatch 时 lazy abort。如果同名 worker 在 delete hint 读到 inactive snapshot 前已经重建，最新 projection 生效；该非持久 hint 不含可识别 delete 前 session 的 incarnation fence。Backend DO facet 在初始 `101` 之后不会逐消息 re-fence；owner handoff 安全依赖 reconnect/rebind 行为，以及创建 backend facet 前运行的 owner-side dispatch fence。Gateway 重置 backend reconnect epoch 时，旧 epoch 下排队的 client message 可能被丢弃，且没有逐帧 ack/nack。
- Cached WebSocket owner 只有返回合法、可信的 same-shard hint 或窄的 pre-dispatch ownership race 时才可以触发一次 router rediscovery，因为最终 101 仍由新解析出的 owner endpoint 建立；broad ownership error、malformed/wrong-shard control response 或 router handoff 后的 direct failure 会返回净化后的终态 error。
- 无可信标记的 ordinary fetch/RPC direct failure 只有安全的 `GET`/`HEAD` request 会 fall back 到 router。非幂等 method 和 RPC 在 outcome 可能未知时返回 `owner_unavailable`。只有明确、可信的 stale-owner/owner-race response 对所有 method 都可重试一次，因为它证明 dispatch 未进入 tenant code；ordinary invoke owner hint 会被净化而不会被跟随。
- Renamed/deleted migrations 延后。
- 长 handler 仍需要用户自己注意；lease-budget watchdog 保护平台 ownership 并缩窄 failover race，不 fence 每一次 storage call 或最终 SQLite commit point。

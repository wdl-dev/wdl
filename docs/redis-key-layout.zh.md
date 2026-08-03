# Redis Key Layout

本文是当前 active 的跨模块 Redis / Valkey key map。各模块文档负责 feature 内部的深层语义；本文记录跨模块的 DB split、key family 和 ownership 规则。

## 数据库切分

WDL 使用明确的逻辑切分：

- **`DB 0`，控制面：**bundle、routes/patterns、auth、D1/DO owner state、cron config、queue-consumer config、lifecycle metadata，以及 workflow definition（`wf:defs:*`）。
- **`DB 1`，数据面：**KV hash bucket、queue stream、delayed queue、orphan stream 和 live log-tail stream。
- **`DB 2`，workflows：**`wf:schema_version`、instance state、step record/summary、ready/due shard、event 和 event-type index、payload ref、retention index、restart target-version blocker、run lease。

Local compose、Kubernetes 和 Terraform 都启用这个切分。Rust service 和 Rust `redis-proxy` 使用 `DATA_REDIS_URL` / `DATA_REDIS_DB` 选择 data-plane Redis connection/database；嵌入的 JS control/log-tail 路径使用 `DATA_REDIS_ADDR` 加 `DATA_REDIS_DB`，因为它们的 RESP client 接收 host:port address。未设置这些 data-plane 变量的部署会把数据面 key 留在 control Redis connection/database，直到显式 opt in。Workflows 不同：未设置 `WORKFLOWS_REDIS_URL` 时 workflows service 仍默认使用 DB 2；只有显式设置 `WORKFLOWS_REDIS_DB=0` 时才使用 DB 0。

## 全局控制面 Keys

```text
routes:<ns>                     Hash, { workerName -> activeVersion }
platform-domain-disabled:<ns>   Set, 不经 platform-domain route 公开的 active worker
namespaces                      Set, 至少有一个 active worker 的 namespace
workers:<ns>                    Set, 有 worker-owned lifecycle state 的 worker name
worker:<ns>:<name>:next_version String, 单调 version counter，delete 后保留
cron:seq:<ns>:<name>            String, 永久 Cron generation 高水位
worker:do-rollout:<ns>:<name>   String, active DO rollout projection
worker:do-rollout-seq:<ns>:<name>
                                String, 永久单调 DO restart event 序号分配器
worker-versions:<ns>:<name>     ZSET, score=int version, member="v<int>"
worker:<ns>:<name>:v:<int>      Hash, bundle bytes + __meta__
worker-delete-lock:<ns>:<name>  String EX 30, 每个 worker 的 delete critical-section lock；value 是 whole:<token> 或 version:<token>；DO first-owner claim 会 WATCH，且只有 whole 阻止 ownership
do:owner:scope:<encoded scope>  String EX, authoritative DO owner lease
do:owner:scope:<encoded scope>:generation
                                String, 单调 DO owner generation counter
worker-version-referrers:<ns>:<name>:<version>
                                Set, canonical JSON 的 version-pinned caller ref
hosts:<ns>                      Set, operator 声明的 host intent
declared-hosts                  Set, 至少被一个 namespace 声明过的 host
declared-hosts:revision         String, host declaration mutation 的单调 revision
host-declarations:<host>        Set, 声明这个 host 的 namespace
ns-hosts:<ns>                   Set, promote 维护的 active host reverse index
patterns:<host>                 Hash, slot -> v2 tab-separated projection
auth:hash:<sha256_hex>          String, 明文 token hash 到 token id 的 lookup
auth:token:<tokenId>            Hash, token metadata + SHA-256 hash，不存明文
auth:delegated-issue-lock:<issuerTokenId>:<templateId>
                                  String EX, delegated-token issuer/template 发放锁
secrets:<ns>                    Hash, namespace-level WDL-ENC envelope
secrets:<ns>:<worker>           Hash, worker-level WDL-ENC envelope
```

`worker:<ns>:<name>:v:<int>` 的 key 使用 JavaScript safe-integer 范围内的正整数 version，而不是 `"v<int>"` tag。直接 seed Redis 的测试 fixture 必须使用 `shared/worker-contract.js#bundleKey`。

`cron:seq:<ns>:<name>` 是 Control 持有的永久 Cron generation allocator。它在 Cron projection 清空和 whole-worker delete 后仍保留，确保旧 `cron-slot:*` ref 不会匹配重建的 entry。Allocator 从 generation `1024` 开始分配，更低的值属于保留范围，永久 allocator 不会发放。

`worker:do-rollout:<ns>:<name>` 是 Control 持有的 active `{version,mode,restartSequence}` JSON projection。Promote 在更新 `routes:<ns>` 的同一个 transaction 中写入它。Gateway 在 WebSocket lifecycle boundary 原子读取这两个值；do-runtime 在 owner resolution 中读取 projection，并在 host actor 的 pipeline owner/storage dispatch snapshot 中再次读取。Sequence 是由这些 Redis read 得到的 owner-local state，不在 invoke/connect wire payload 中传递。Workflows 为 DO alarm 选择 dispatch version 时，会在一个 snapshot 中同时读取 projection、route、storage pointer 和 retained-version score。最新 active projection 是权威状态：后续 `preserve` projection 会在现有 sequence 上覆盖尚未观察到的 lazy restart，但不能撤销已经发生的连接关闭或 facet abort。Whole-worker delete 会删除 active projection。状态缺失表示默认 `preserve`，状态畸形则 fail closed。

`worker:do-rollout-seq:<ns>:<name>` 是 Control 持有的 opt-in DO restart event 永久单调序号分配器。它在 whole-worker delete 后仍保留，确保重建后的下一次 restart 不会复用 stale Gateway session 已观察过的 sequence。Control 是唯一 writer；Gateway、do-runtime 和 Workflows 只读取 active projection。

`do-rollout:restart` 是新 restart sequence 的非持久 Gateway notification channel。Control 在 route/projection update 的同一个 transaction 中 publish `{ns,worker,version,restartSequence}`。Gateway 只用它及时触发本进程公开 WebSocket session 的权威 reconciliation；initial/reconnect lifecycle read 和 subscriber reconciliation 始终根据最新 projection 决定动作。

`worker:delete` 是成功 whole-worker delete 的非持久 Gateway notification channel。Normal delete 在删除 active route 和 rollout projection 的同一个 transaction 中 publish `{ns,worker}`；仍有 worker-owned state 需要清理的 residual delete 会重新 publish。Gateway 只用它请求对应 worker 的权威 reconciliation；如果同名 worker 在权威读取观察到 inactive 前已经完成重建，最新 projection 生效，因为 hint 不含 durable incarnation fence。Route invalidation 仍只清 cache，subscriber reconnect 只能在 deleted state 仍可观察时修复漏掉的通知。

`namespaces` 是 active worker gate。有 active worker route 时会加入，最后一个 active worker 删除时可能移除。Namespace-level secrets 和 data-plane state 等资源可以比这个 set membership 活得更久。Auth 在 delegated token issue 时只把它作为 generated-namespace collision 的 best-effort 信号读取，而不是永久 namespace registry。

`routes:<ns>` 和 `worker-versions:<ns>:<name>` 只能通过 `shared/worker-contract.js#routesKey` / `#workerVersionsKey`（以及它们的 Rust 镜像 `rust/common/src/worker_contract.rs#routes_key` / `#worker_versions_key`）构造。Control 是唯一 writer；sanctioned reader 是 gateway（route resolution）和 workflows。workflows 有两条读取路径：workflow create / verify 时的 active-export resolution，以及 fired alarm 的 scheduled version 已不再 retained 或 active rollout projection 为 `restart` 时的 internal DO alarm retarget。改 key 语法时必须同时更新 JS helper、Rust helper 和所有 reader。

`workers:<ns>` 表示这个 worker 有 worker-owned lifecycle state：retained bundle、active projection、worker-level secrets 或 workflow definitions。Secret-only 和 definitions-only worker 会被有意列出，并可以 whole-delete。

## Route 和 Host Projection

Subdomain routing 读取 `routes:<ns>`，并过滤显式 opt-out 的 `platform-domain-disabled:<ns>` set。Active version 仍保留在 `routes:<ns>`，因此 lifecycle、binding 和 Workflows reader 继续共用同一个 active-version owner。Pattern routing 先检查 `declared-hosts`，再读取 `patterns:<host>`，并直接使用 slot value 中嵌入的 `version` 构造 `x-worker-id`。Pattern slot value 是由 `shared/route-projection.js` 编码的紧凑 `v2\t<ns>\t<worker>\t<version>\t<kind>\t<value>` record。Promote 在同一个 Redis transaction 中更新两套 projection，并同时加入或移除 platform-domain opt-out。Control mutation 和 delete 路径遇到无法 decode 的非空 slot 时会 fail closed，不会把未知 owner 当成空槽。

`hosts:<ns>` 是 operator intent：这个 namespace 被允许使用这些 host。`declared-hosts` 是 gateway 对“至少被一个 namespace 声明过的 host”的 gate。`host-declarations:<host>` 记录声明该 host 的 namespace，因此一个 namespace 移除声明时，不会在另一个 namespace 仍声明该 host 的情况下清掉全局 gate。Host reconcile 会在同一个 transaction 中修改源 declaration、派生索引并递增 `declared-hosts:revision`。`POST /reload` 从 `hosts:<ns>` 重建两个声明索引时会 WATCH 该 revision，因此并发 host reconcile 会让 repair 重试，成功后才发布 gateway cache invalidation。`ns-hosts:<ns>` 是 active reverse index：这个 namespace 当前在这些 host 上拥有至少一个 slot。`hosts:<ns>` 应是 superset。Host reconcile 会先用 `ns-hosts:<ns>` 做 fast path，再扫描 `patterns:<host>`。

Pattern `slot` 是原始 wrangler pattern，例如 `/mcp` 或 `/mcp/*`；它也是 Redis hash field。`kind` 是 `exact` 或 `prefix`，决定 gateway matching 语义。

## Bundle Metadata

`__meta__` 字段是小型 JSON metadata。模块 bytes 是 RESP-safe raw bytes，不是 base64。典型字段包括：

```json
{
  "mainModule": "worker.js",
  "compatibilityDate": "2026-04-24",
  "compatibilityFlags": [],
  "modules": { "worker.js": { "type": "module" } },
  "bindings": {},
  "vars": {},
  "routes": [],
  "crons": [],
  "queueConsumers": [],
  "assets": { "token": "...", "prefix": "assets/<ns>/<worker>/<token>/" },
  "exports": []
}
```

示例省略了 `durableObjectRollout`；Control 只为非默认的 `restart` policy 持久化该字段，字段缺失表示 `preserve`。

Control 把 `__meta__` 写为 JSON object。Control-plane consumer 通过 `control/lib.js::parseBundleMeta()` 解析必需的 bundle metadata；malformed JSON、array 和 scalar 值都会以 `corrupt_meta` fail closed。Bundle 缺失的语义仍由具体 use site 持有：当 projection 变更、唯一性证明、lifecycle cleanup、workflow view 或 environment budget 必须消费 metadata 才能产生正确结果时，只要权威 route 或 index 仍指向该 bundle，缺失就 fail closed。Deploy discovery/link preflight 不把缺失归类为 `corrupt_meta`；watched commit 仍是权威检查，并以 `target_drift` 拒绝缺失的 pinned service target。

Routes、platform-domain exposure、crons、queue consumers、Durable Object rollout mode、bindings、vars、exports、workflow definitions 和 asset prefixes 都是 version metadata。`workersDev: false` 表示显式关闭 platform-domain exposure；字段缺失表示启用。Rollback 本质上是 promote 一个旧的 immutable version。

## Feature Key Families

详细合同由各 feature 模块负责：

- D1：[D1](modules/d1.zh.md)
- Durable Objects：[Durable Objects](modules/durable-objects.zh.md)
- Queues 和 cron：[Queues 和 Cron](modules/queues-cron.zh.md)
- Workflows：[Workflows](modules/workflows.zh.md)
- Log tail：[Log Tail 和 Observability](modules/log-tail-observability.zh.md)
- Runtime/KV/R2/ASSETS/service/platform bindings：[Runtime](modules/runtime.zh.md)
- Control/auth/lifecycle/delete blockers：[Control 和 Auth](modules/control-auth.zh.md)

跨模块约束：

- Persisted D1/DO owner record 必须能重建出读取它的 Redis key 所编码的 scope。语法合法但错放在其他 scope 下的记录会在 forwarding、takeover、renew 或 release 前 fail closed；DO owner resolution 还必须在读取 invoking bundle 的 active storage pointer 前，把 record 的 canonical namespace 和 worker 绑定到该 bundle。
- Index 通常是可修复 projection，不是 authority。新增 writer 前，模块文档必须说明哪个 key 是权威状态。
- Lifecycle 和 delete blocker index 在模块文档声明为权威时就是权威；不要增加绕过这些 index 的 request-path fallback scan。
- Queue main stream 不做 trim，因为 at-least-once delivery 是合同。DLQ、orphan、log-tail 这类诊断 stream 可以使用有界 approximate trim。
- Secret hash value 在 steady state 下是 `WDL-ENC:` envelope。`/runtime/load` 没有 plaintext fallback。
- Workflows 拥有 DB 2 instance state。`wf:ready:cursor` 是内部 ready-shard 公平性 cursor。Control 只拥有 DB 0 的 `wf:defs:*`；其他 tier 不应直接写 DB 2。
- `wf:pending-version:<ns>:<worker>:<version>` 是 Workflows-owned、30 秒的 restart blocker。Version-delete 会将它与 `wf:by-version` 一起检查；restart 成功的 DB 2 script 会在创建持久 version referrer 前原子复验初始 marker。ZSET key 使用随写入刷新的 60 秒 TTL，确保遗留 marker key 会被物理回收。
- Workflows 还拥有 DB 2 中的 internal `wf:internal:do-alarm:*` jobs，用于 Durable Object alarm backend scheduling。do-runtime 通过 workflows HTTP API 写 alarm，而不是直接写这些 key。`wf:internal:do-alarm:ready:cursor` 是内部 ready-shard 公平性 cursor，不是租户状态。

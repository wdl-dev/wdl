# Runtime Loader 和 Bindings

## 目的

Runtime 从 Redis 加载不可变 worker version，构建 tenant-facing `env` bindings，dispatch fetch/scheduled/queue/workflow event，并保持 user-runtime 与 system-runtime 的信任边界分离。

## 当前实现

同一套 runtime 源码同时服务 user-runtime 和 system-runtime。主要入口：

- `runtime/index.js`：tenant fetch loader socket `:8081`。
- `runtime/internal.js`：internal dispatch socket `:8088`。
- `runtime/load.js` 和 `runtime/load/*`：bundle assembly、module rewrite、env construction、wrapper generation。
- `runtime/runtime.js`：workerLoader cache bookkeeping 和 sibling eviction。
- `runtime/dispatch.js` 和 `runtime/dispatch/*`：fetch/scheduled/queue/workflow dispatch helper。

Worker 通过不可变 id `<ns>:<worker>:<version>` 加载。Promote 会生成新的 id，因此 active-version 变化自然触发 fresh isolate cold-load。
`runtime/load.js` 也统一持有 `workerLoader.get()` wrapper：每次 cache miss 都会登记供后续 eviction 使用，只有 active-version dispatch path 会请求 sibling eviction；service binding 加载 pinned version 时不会驱逐 sibling。

两个 runtime pool 使用同一个 image 和源码，但 capnp config 不同：

- `runtime/config-user.capnp` 把 loaded-worker outbound 固定为 public-only networking。
- `runtime/config-system.capnp` 把 loader、control、auth 和 tail worker 放在 private+public network service 上，因此 loaded `__system__` worker 可以访问平台 mesh service。

这条 privilege asymmetry 存在于 capnp，而不是 Terraform 或 Kubernetes egress policy。`env.SERVICE_NAME` 是 capnp literal（`user-runtime` 或 `system-runtime`），用于日志/metrics，避免两个 pool 的观测流混在一起。

workerd config wiring 有几条不明显的约束：`workerd serve` 的 config 和 service binding 参数必须是分开的 argv token，不要压成 colon-separated string；Cap'n Proto 的 `external = ... http` 用于 HTTP/TLS-capable peer，Redis 这种 plain TCP peer 应放在 `network` service 后面并用 `connect()`；通过 `network` service 做 HTTPS fetch 需要 `tlsOptions = (trustBrowserCas = true)`，runtime image 必须带 `ca-certificates`；workerd embedded module name 不能包含 `..`，因此共享嵌入模块使用 `shared-redis` 这类扁平名称；部分 support source 会以两个名字使用：普通 workerd module 用 capnp 扁平名，generated loaded-worker module 用 `.js` module 名；例如 D1 会把同一个 `shared/d1-data-field.js` 源码作为 `shared-d1-data-field` 提供给 workerd module，同时作为 `_wdl-d1-data-field.js` 注入 tenant WorkerCode；user-runtime 同时保留 platform plumbing 用的 `internal-network` 和 tenant outbound 用的 `public-network`，loaded user worker 只能拿到 `public-network`，platform wrapper 保留自己需要的 private reach。

## 接口

- Loader socket `:8081`：gateway-routed tenant fetch 流量。
- Internal socket `:8088`：只处理 `GET /_healthz`、`GET /_metrics`、workflow run/notify、scheduled dispatch 和 queue dispatch。
- `redis-proxy` sidecar：cold-load、tenant secret envelope decrypt、KV、queue producer、log-tail active check 和 append。
- 隐藏 service Fetcher：D1 backend、DO backend、workflows backend，以及 DO owner-network direct path。
- Env-backed binding：queues 和 KV 调 `redis-proxy`；R2 签名 S3-compatible request；ASSETS 使用 deploy-time metadata 生成 tokenized CDN URL，不是隐藏 Fetcher。

Tenant 可见 binding 包括 KV、R2、D1、Durable Objects、Queues、ASSETS、service bindings、platform bindings 和 workflows。

## Binding 实现模型

workerd 提供 isolate、module evaluation、named entrypoint 和 JSRPC 机制。Cloudflare 生产平台通常在外部服务里实现的 binding 后端，则由 WDL 自己补齐。因此 runtime 把 binding 当作 adapter：

- KV、queue producer 这类纯数据 binding 调 colocated redis-proxy sidecar。Loaded worker 看到 Cloudflare-shaped object，但 method call 会先通过 workerd JSRPC 回到 runtime，再经 HTTP 调 redis-proxy。
- Secret value 也在 cold-load 时经过 redis-proxy。redis-proxy 解密 `WDL-ENC:` value 后，runtime 在 internal load envelope 中收到 plaintext `ns_secrets` 和 `worker_secrets`；tenant-facing `env` 形状保持不变。Env materialization 使用固定优先级：bundle vars，然后 namespace secrets，然后 worker secrets。同名 worker-level secret 覆盖 namespace-level secret，namespace-level secret 覆盖 var。Control 会在 deploy 和 secret mutation 过程中用 shape-only factory 调用 cold-load 同一个 `buildWorkerEnv()` materializer，再用留有 headroom 的预算估算完整 workerLoader env，包括用户 vars/secrets、runtime 注入的 binding/workflow env value，以及 platform/service binding props 中复制的 required caller secret，避免超限配置拖到 runtime cold-load 才失败。
- D1、Durable Objects、Workflows 这类 stateful binding 调专门 backend service。Hidden backend Fetcher 留在 runtime 内部，并在 tenant code 观察 `env` 前被删除。
- R2 是 S3-compatible object-storage adapter：runtime 使用平台 credential 签名请求，并发送到配置的 endpoint。
- ASSETS 是 deploy artifact URL helper：control 在 deploy 时把 assets 上传到 S3-compatible storage；runtime 读取 `__meta__.assets` 和 `ASSETS_CDN_BASE`，只暴露 `env.ASSETS.url(path)` 用来生成 tokenized CDN URL。
- Service 和 platform binding 使用 workerd JSRPC/fetch 机制，但 control metadata 决定允许访问哪个 worker、namespace、version 和 entrypoint。

KV 是最直接的例子。Runtime 把 `KV` 导出成 named entrypoint，并用 `{ ns, id }` props 为每个 binding 实例化一个对象。`get`、`put`、`delete`、`list`、batch `get` 和 metadata 调用都进入 redis-proxy DB 1。redis-proxy 把每个 namespace/id 拆成 32 个 hash bucket，key 形如 `kvh:<ns>:<id>:b:<bucket>`；value field 是 `v:<key>`，metadata field 是 `m:<key>`。Put 使用 `HSET`/`HSETEX` 和 hash-field expiration，delete 同时删除 value 和 metadata field，list 用 opaque cursor 扫 bucket field，batch/list metadata 路径会在 base64 response encoding 前检查 aggregate raw value/metadata byte budget。

## Binding Surface 合同

KV 支持常用 `KVNamespace` 调用：`get`、batch `get`、`getWithMetadata`、batch `getWithMetadata`、`put`、`delete` 和 `list`。`get` 支持 text、JSON、arrayBuffer 和 stream 形状；batch read 支持 text 和 JSON。Runtime shim 在 proxy 前把 value 限制到 25 MiB，stream value 也用同一个 cap 读取。所有 KV 操作的 key 都在 redis-proxy 边界限制为 512 个 UTF-8 字节，包括 list prefix 和 batch read。`list()` 基于 Redis `HSCAN`，不是 Cloudflare 有序 B-tree：key 不排序，cursor 是 opaque WDL cursor，并发写可能乱序出现或被再次看到。`limit` 上限是 1000。`cacheTtl` 只是 API shape；没有 Cloudflare edge read cache 或 global eventual-consistency window。

R2 binding 把 `bucket_name` 映射到平台 S3-compatible bucket 下的 namespace-scoped virtual bucket：`r2/<ns>/<bucket_name>/<object-key>`。同一个 namespace 中使用同一 `bucket_name` 的 worker 会有意共享数据；不同 namespace 通过前缀隔离。Runtime 支持常用 `head`、`get`、`put`、`delete` 和 `list` 路径。`get()` 返回 streaming body，便捷 reader 执行 25 MiB cap。`put(stream, ...)` 目前会先 buffer，再发单个 S3 PUT，并使用同一个 cap；不支持 multipart upload、SSE-C 和 checksum selection。Conditional requests 和 range GET 实现常用 R2 行为。`list({ include: [...] })` 为 metadata fields 额外执行 HEAD，并使用并发 cap。Tenant 提供的 `Headers` metadata 如果包含 `Expires`，其值必须是 canonical IMF-fixdate；malformed write metadata 会在 host binding call 前被拒绝。Tenant-facing R2 error 只暴露 operation/status，以及有帮助的 virtual object key；不会暴露原始 S3 response body 或 physical `r2/<ns>/<bucket>/...` key。Control-plane R2 admin error 可以为 operator 保留 backend detail。

ASSETS 是 deploy-artifact helper，不是完整 Cloudflare Pages asset pipeline。Control 把文件上传到 `assets/<ns>/<worker>/<token>/<path>`，注入 `ASSETS` binding，runtime 暴露同步的 `env.ASSETS.url(path)`。该方法在 runtime 中不做 IO，并用 `ASSETS_CDN_BASE` 返回浏览器可访问的 CDN URL。Path 按 `/` 切段，空段、`.` 和 `..` 被拒绝，每段会 percent-encode。Version 在 load 时绑定，因此 rollback 会切换 asset URL。需要对静态文件做 auth 或 rewrite 的 worker 应把文件留在 bundle 里，而不是使用 declared `assets`。

R2 和 ASSETS 生命周期语义故意不同。ASSETS 是 deploy artifact，version/worker delete 会 stage `worker-delete-s3-cleanup` work。R2 是 tenant runtime data，worker delete 永远不删除 R2 object。

Service binding 在 caller deploy 时冻结。Control 解析 target namespace、worker、version 和 entrypoint，存入 caller metadata，runtime 第一次使用时加载该精确 target version。之后 target promote 不会移动已有 caller；caller redeploy 才是 refresh 边界。这种 version pinning 让 rollback 和 version delete referrer check 都是确定的。Runtime 会在 materialize binding 前用 canonical positive JavaScript-safe-integer grammar 重新校验 persisted pinned version。

Cross-namespace service binding 要求 target 为被绑定的 entrypoint 声明 `[[exports]]` entry；default export 也要声明 `entrypoint = "default"`。该 entry 的 `allowed_callers` 控制 cross-namespace 访问；`["*"]` 对所有 namespace 开放，空数组则关闭 cross-namespace caller。没有 `[[exports]]` 的 target 只向 same-namespace caller 暴露 default entrypoint。Same-namespace caller 绕过 ACL，但 target 一旦声明 `[[exports]]`，仍受 strict entrypoint visibility 约束。ACL 变化是 deploy-time，不是 call-time；既有 caller 会保持 pin，直到 redeploy。

Platform binding 是 WDL-specific、指向 platform-tier namespace（例如 `__platform__`）的 service binding。Caller 声明 `[[platform_bindings]]`；control 根据 active `[[exports]] as = "..."` entries 解析 `SCREAMING_SNAKE_CASE` symbol，冻结 target，并只从 caller 转发 target 声明的 `required_caller_secrets`。Raw `[[services]] ns = <platform-tier-ns>` 会被拒绝，gateway 也会在 Redis lookup 前拒绝 platform-tier namespace 的 public traffic。

## Redis / Storage 合同

Runtime 通过 `redis-proxy` 从 DB 0 读取不可变 bundle 和 metadata。Data-plane binding 使用各自的 storage：

- DB 0 中的 secret hash value 是 envelope ciphertext。redis-proxy 在 runtime-load 时解密；provider 配置或 envelope 校验失败时 fail closed。
- KV 和 queue producer 通过 `redis-proxy` 使用 DB 1。
- Workflow binding 调用 `workflows`；runtime 不直接读取 DB 2。
- D1 和 DO binding 调用专门 runtime service。
- R2/ASSETS 使用 S3-compatible object storage。

Runtime 可以把 Redis bundle metadata 视为 control-authored，但 materialize 旧 metadata 时仍会重新校验 reserved runtime entrypoint 和 binding name。旧 metadata 如果包含早于 `2026-04-01` 的 `compatibilityDate`、Python module entry、上游 experimental compatibility flag、禁用 WDL 要求的 enhanced error serialization，或违反其他由 WDL 拥有的 metadata contract，也会 fail closed。日期下限与 enhanced serialization 要求是 WDL 的 forward-only policy；冗余或其他不兼容的上游 flag 仍由 workerd 在 cold load 时拒绝。

## Ownership / 并发 / 失败语义

- workerLoader cache 没有 LRU。Runtime 给每个 loaded worker 注入 `__WdlAbort__`，并在 active-version cold-load 时 evict sibling historical versions。
- Service-binding cold load 会记录 loaded version，但不 evict sibling，因为 service binding 可能有意指向 frozen historical version。
- Internal active-version scheduled/queue dispatch 会 opt into sibling eviction；frozen workflow dispatch 不会。
- 只要注入 privileged internal Fetcher，wrapper generation 就会避免 raw env 暴露给未包装 entrypoint。Host-wrapper runtime 会在 tenant module 前执行，并捕获参与 handler/env wrapping 决策的 intrinsic，因此 tenant 顶层 prototype mutation 不能绕过 env 边界。
- Generated wrapper 会把 request id 直接传给每次请求创建的 facade。持久 class instance 使用一个小型 mutable diagnostic context，并在 wrapped handler 开始时刷新；并发或有意重入的调用因此可能观察到另一次 invocation 的 id。该传播只属于 best-effort observability，不是安全或正确性边界，Runtime 也不会为它重写 tenant compatibility flags。Request id 不能用于授权、fence 或去重；其语法仍由 injected canonical request-id module 拥有。
- Request context wrapper 会把 facade object 换进 env，并在事件类型允许时传播 request id。do-runtime alarm 和 RPC 通过携带外层 request id 的私有 fetch dispatch 进入，不会把平台 metadata 加入 tenant method argument。
- Tenant `fetch()` 未捕获异常会映射为平台 `502 runtime_error` response，并带 request id。异常细节输出到结构化日志/live tail，不复制进客户端 body；throwable 自身无法转成字符串时，tail formatting 也不能反向覆盖原始异常。
- Internal scheduled、queue 和 workflow dispatch route 使用 result envelope 表达 handler outcome。Tenant handler error 是 scheduler/workflow 协议中的 outcome state，不是 generic platform transport error。
- Runtime 没有 route-cache invalidation protocol。`workerLoader` cache key 是不可变 worker id，因此 promote 后的新 version 是新 key，会自然 cold-load。
- `runtime/tail-worker.js` 通过 `workerCode.tails` 附加到每次 dynamic load。它总是输出结构化 stdout；只有 shared tail forwarder 看到 active subscription 后，才转发到 `wdl tail`。

## 安全边界

- user-runtime loaded worker 只拿 public-only outbound。Runtime 自身保留 internal outbound，用于 Redis 和 S3-compatible storage 相关工作。
- system-runtime loaded `__system__` worker 刻意拥有 private+public outbound。
- 新增 privileged runtime endpoint 必须加到 `runtime/internal.js` 的 `:8088`，不能加到 gateway-facing loader socket。
- 匹配 `__WDL_*__` 的 binding 和匹配 `__Wdl*__` 的 entrypoint 属于平台保留。
- D1/DO owner hint 只信任 runtime service 生成的 header，不信任 tenant response body。

## 可观测性

Runtime 为 loading、binding operation、`redis-proxy` call、workflow replay cache、loader eviction 和 dispatch envelope 输出日志/metrics。Tail worker 总是为 console/exception capture 输出结构化 stdout；只有匹配的 active tail session 存在时才转发到 `wdl tail`。

## 部署 / Rollout 注意事项

- 修改 bundle metadata、wrapper generation 或 binding shape 时，runtime 和 control 应一起滚。
- 如果 scheduler/workflows 依赖新的 `:8088` internal path 或 dispatch body，runtime 必须先滚。
- Runtime 不为 loaded worker 开启 workerd 的宽泛 `experimental` flag。Historical-version eviction 会注入 `__WdlAbort__`；当前 bundled workerd baseline 的 `abortIsolate()` 不需要该 flag。
- Loaded worker 不能访问非 GA experimental-only surface，例如不可撤销的长期 stub storage。除非先完成明确的功能设计，否则不要为兼容绕过打开宽泛 `experimental` flag。
- Control 会在 deploy 时拒绝上游 `$experimental` compatibility enable flags，runtime 也会拒绝仍带有这些 flags 的 retained metadata。`no_*` 这类 disable-style flag 不属于这个 mirror，除非上游把对应 enable flag 本身标为 experimental。
- Python Workers modules 不受支持。Control 会拒绝新的 `py` module manifest，runtime/do-runtime 会拒绝 retained metadata 中的 `py` module，而不是让 workerd 之后抛 mixed JS/Python bundle error。
- Runtime workerd 进程需要进程级 `--experimental`，因为上游 workerd 2026-07-01 用它 gate `workerLoader` binding。不要给 loaded WorkerCode 加 `experimental` compatibility flag 或 `allowExperimental`，除非新的上游 API 明确需要。
- 上游 workerd 2026-07-01 把 dynamic worker code 限制为 64 MiB、serialized dynamic env 限制为 1 MiB。Control 会在分配 version 前和 commit metadata materialization 之后估算最终 WorkerCode，包含 runtime/do-runtime 注入的 wrapper/client modules、workflow import rewrite 和生成的 workflow keys；vars、namespace/worker secrets、runtime 注入的 binding/workflow env value 会在 Redis WATCH commit 和 secret mutation 路径里按 headroomed `workerLoader` env budget 做权威检查。Deploy 和 namespace-secret mutation 使用它们实际会加载的 version；worker-secret mutation 还会在 WATCH/COPY bump 事务内重新检查 forced bump，确保 routing flip 前覆盖已分配的 bump version。估算以 JSON bytes 为基底，并对非 Latin-1 字符串补上 V8 two-byte string overhead，因此 ASCII 混 CJK 或 emoji 的 secret 不会绕过 control 后在 cold-load 才失败。
- 当前 stock workerd 中，客户端在 async `ReadableStream` response body 中途断开时，不一定调用 stream source 的 `cancel()`。Tenant streaming/SSE worker 应使用自己的 heartbeat、timeout 或应用层 close path，不要把 disconnect-driven `cancel()` 当成唯一资源清理信号。
- workerd 升级仍可能改变默认或 compatibility-flagged runtime surface；升级时要审 exposed surface，而不只审 loader/abort path。

## 保护该模块的测试

- `tests/unit/runtime-load.test.js`
- `tests/unit/runtime-dispatch-handlers.test.js`
- `tests/unit/runtime-dispatch-workflows.test.js`
- `tests/unit/runtime-service-binding.test.js`
- `tests/unit/runtime-queue-producer.test.js`
- `tests/unit/runtime-d1-client.test.js`
- `tests/unit/runtime-do-client.test.js`
- `tests/unit/runtime-bindings-do.test.js`
- `tests/unit/runtime-r2-client.test.js`
- `tests/unit/runtime-r2-host.test.js`
- `tests/unit/runtime-workflows-client.test.js`
- `tests/integration/service-bindings.test.js`
- `tests/integration/service-bindings-rpc.test.js`
- `tests/integration/platform-bindings.test.js`
- Queue 集成测试文件组：`tests/integration/queues-delivery.test.js`、
  `tests/integration/queues-retry-and-delay.test.js`、
  `tests/integration/queues-orphan-and-control.test.js`、
  `tests/integration/queues-batch-and-isolation.test.js`
- `tests/integration/cron-triggers.test.js`
- Workflow 集成测试文件组：`tests/integration/workflows-runtime-core.test.js`、
  `tests/integration/workflows-runtime-scheduler.test.js`、
  `tests/integration/workflows-runtime-pausing.test.js`、
  `tests/integration/workflows-runtime-retention.test.js`
- `tests/integration/d1-binding.test.js`
- `tests/integration/durable-objects-core.test.js`

## 已知约束和非目标

- Runtime 不在每个 hot request 上查 Redis 判断 version 是否 active。
- Historical isolate 可能留到 eviction 或 container recycle。
- Workflow replay cache 只是 advisory。
- Runtime 不是 control-plane 授权边界。
- 指向 pinned historical version 的 service-binding cold load 在 promote 后可能再次发生，因为这类版本有意不被 evict。非 route-churn 的 isolate leak 仍以 container recycle 作为 backstop。

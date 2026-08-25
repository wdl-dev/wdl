# Workers 兼容矩阵

本文回答的问题不是“代码在哪里”，而是“WDL 对 Cloudflare Workers 的某个 surface 兼容到什么程度，以及哪些部分是 WDL 自己补出来的”。

当前代码和测试仍然是事实来源。本文是 WDL 作为开源 Workers 平台时的当前兼容性合同。

每一行都拆成四类兼容性声明：

- **workerd 提供什么**：WDL 复用的上游 runtime surface。
- **WDL 增强 / 新增什么**：WDL 在裸 workerd surface 外补上的平台能力或 guardrail。
- **与 Cloudflare 的模型差异**：有意的模型差异，不必然代表更强或更弱。
- **未实现 / 缺口**：缺失的 Cloudflare 行为或不支持的配置形态。

## 状态说明

- **Supported**：普通 WDL tenant 应用应能使用，并有 unit 或 integration 测试保护。
- **Partial**：按 WDL 当前模型实现，但缺少 Cloudflare 的全局边缘语义或较少见配置形态。
- **Not supported**：deploy/config 时拒绝，或明确不实现。
- **Internal**：平台内部接口，不是 tenant API 兼容面。

## Runtime 和语言 surface

| Surface | 状态 | workerd 提供什么 | WDL 增强 / 新增什么 | 与 Cloudflare 的模型差异 | 未实现 / 缺口 |
|---|---|---|---|---|---|
| ES module Workers 和 `fetch()` | Supported | Module evaluation、request dispatch、`Response`/`Request`、service binding JSRPC 机制。 | Dynamic `workerLoader`、immutable version id、wrapper 生成 `env`、gateway routing、request logs、public/private outbound 隔离。 | 以 `_wdl-` 开头的 root module name 由平台保留；新 deploy 会被拒绝，retained version 在 cold load 时 fail closed。Tenant `fetch()` 未捕获异常会映射为平台 `502 runtime_error`；异常细节进入结构化日志/live tail，不进客户端 body。 | WDL 不模拟每个 compatibility date 的历史行为；workerd 按平台启用的 flags 运行。 |
| WebSocket upgrade | Supported | workerd 内的 WebSocket API 和 101 response 处理。 | Gateway 在本地终结公开 WebSocket pair，并通过有界 backend reconnect 和 client-frame buffer 直接转发到 runtime/do-runtime。 | Cloudflare 在部署新代码时会断开全部 WebSocket。除非 worker 选择了 `sessionPolicy: "restart"`（其 session 在 promote 时即关闭），WDL 在 promote 后只要 pinned version 的 backend 仍健康就保留 gateway-proxied 连接；仅当该 backend 丢失后，Gateway 才复核 active route，若 pinned version 已不活跃则以 `1012` 关闭，让 client 升级到 active version。 | Gateway rolling 仍会断开物理 client socket；应用级 resume 未实现。重连由应用负责：WebSocket 协议和裸 API 都没有定义会触发重连的 close code。 |
| `compatibility_date` / `compatibility_flags` | Partial | capnp 中启用的 workerd feature flags 和兼容行为。 | Dynamic Worker 会拒绝早于 `2026-04-01` 的显式 `compatibility_date`；Control 还会校验该值是真实的 `YYYY-MM-DD` 日期，且不晚于当前 UTC 日期和当前 bundled workerd 支持的最大日期，并拒绝上游 `$experimental` enable flags（例如 `experimental` / `unsafe_module`）、`legacy_error_serialization`、`allow_irrevocable_stub_storage`、`new_module_registry` 和 `streams_disable_constructors`。已由日期启用且生成相同 compiled flag set 的冗余 positive flag 会被保留并接受；其它不兼容组合仍由 workerd 在 cold load 时校验。 | WDL 把 compatibility metadata 当成部署期平台 metadata，不承诺完整 per-worker 历史行为模拟层。 | 没有 per-worker 模拟所有 Cloudflare 历史行为；tenant worker 不能 opt into 上游 experimental-only flags、禁用 WDL 要求的 enhanced error serialization 或标准 stream constructors、持久化不可撤销的 capability stub，或启用与 WDL generated main wrapper 冲突的 module registry identity。 |
| `nodejs_compat` | Partial | workerd 提供 Node.js compatibility module 和 global。从 compatibility date `2026-08-04` 开始，`nodejs_compat` 与 `nodejs_compat_v2` 都默认启用。 | CLI 把 compatibility flags 带入 metadata。需要恢复 `2026-08-04` 之前 surface 的 worker 必须同时设置 `no_nodejs_compat` 和 `no_nodejs_compat_v2`；只关闭前者仍会保留独立默认启用的 V2 surface。 | WDL 暴露 workerd 已启用的兼容 surface，而不是单独 Node.js runtime。 | 不承诺超出 workerd 已启用 surface 的完整 Node.js 平台能力。 |
| Execution context 与 module time | Partial | workerd 暴露 `ctx.abort(reason?)`、`ctx.tracing.startSpan()` 和链式 span attribute setter，并模拟生产 request time。在 active request 外（包括 module 顶层求值），`Date.now()`、无参数 `new Date()` 和 `performance.now()` 都返回 `0`。 | WDL generated wrapper 原样传递 native execution context，不在 module evaluation 时模拟 wall clock。 | `ctx.abort()` 只终止当前 stateless invocation，与 WDL host-only historical-isolate eviction 不同。 | WDL 拒绝 experimental `tail_worker_user_spans` flag，因此 tenant span 通过 WDL live tail 导出不属于支持合同。 |
| Python Workers modules | Not supported | workerd 已有 release-gated Python Workers 路径；`python_workers_20260610` 已不是 experimental，也不再按 compatibility date 隐式启用；`python_workers_20260817` 仍是 experimental。 | Control 用 `python_workers_unsupported` 拒绝 `py` module manifest；runtime 和 do-runtime 对 retained metadata 中的 `py` module 也 fail closed。 | WDL 保持 tenant bundle 只包含 JavaScript/WebAssembly/data，不允许 cold-load 时触发 Pyodide bootstrap。 | 不支持 Python Workers 和 JS/Python 混合 bundle。 |

WDL 通常不保证 workerd 降级。作为 best-effort 参考，目标 binary 只能 cold-load 其支持的 `compatibility_date` 对应的 retained Dynamic Worker version；具体说明见 [infra rollout 注意事项](modules/infra.zh.md#部署--rollout-注意事项)。

Node.js TLS 行为跟随 bundled workerd binary。从 WDL 的 2026-07-01 workerd pin 开始，compatibility date 不早于 2026-06-16 的 worker 会拿到 `throw_on_not_implemented_tls_options`：`node:tls` 中尚未实现的选项（例如 `checkServerIdentity`）会从“静默忽略”变为抛 `ERR_OPTION_NOT_IMPLEMENTED`。另外，workerd 的 `servername` / expected-certificate-hostname 行为变化不受任何 compatibility flag 门控，因此所有日期的证书 hostname 校验都跟随 bundled workerd 行为。

从 WDL 的 2026-08-11 workerd pin 开始，byte-stream BYOB read 会使用实时 `ArrayBufferView` 边界，而不是缓存的 offset 和 length，包括 buffer resize 或 transfer 后的情况。同一 pin 采用 ada-url 4：以 `xn--` 开头、但无法解码为有效 UTS #46 label 的 ASCII hostname label 会被保留并转为小写，而不是被拒绝。只要这类 label 同时满足 `shared/ns-pattern.js` 中的 ASCII DNS grammar，WDL 就会把它作为 literal route label 接受；`control/topology.js` 仍执行 numeric-host canonicalization，Unicode route name 仍不受支持。对于 compatibility date 不早于 `2026-08-11` 的 worker，workerd 还会默认启用 `workflows_enable_fast_engine_creation`，改变其原生 Workflow engine 分配 instance ID 的方式。WDL 不使用该 engine；WDL 自有 facade 和由 DB 2 支撑的 workflows service 继续持有 instance identity 与 lifecycle 语义。

Bundled workerd 允许 `Fetcher` 和 Durable Object class stub 在不启用 experimental flag 的情况下作为 opaque JSRPC 参数传递。WDL 把持有这种 stub 视为 capability delegation：接收方可以按 stub 内由 host 写入的 caller properties 调用目标，但不能改写这些 properties，也不能取出隐藏的平台 backend capability。该委托可以在内存中保留；但 WDL 会在 deploy 和 retained-state load 时拒绝 `allow_irrevocable_stub_storage`，static host worker 也不会启用它，因此长期持久化 stub 不属于受支持的 WDL surface。

## Bindings 和存储

| Surface | 状态 | workerd 提供什么 | WDL 增强 / 新增什么 | 与 Cloudflare 的模型差异 | 未实现 / 缺口 |
|---|---|---|---|---|---|
| KV namespace | Supported | 通过 workerd entrypoint/JSRPC 机制把 binding object 暴露给用户代码。 | Runtime `KV` facade 调 redis-proxy；redis-proxy 在 DB 1 hash bucket `kvh:<ns>:<id>:b:<bucket>` 存 value/metadata，字段为 `v:<key>` 和 `m:<key>`，支持 512-byte key/list-prefix cap、25 MiB value cap、batch raw-byte budget、TTL/EXAT、prefix list cursor。 | KV storage 是 WDL deployment 内的 Redis-backed、namespace-scoped 存储；`cacheTtl` 不是存储新鲜度合同。 | 没有 Cloudflare 全局边缘复制 / eventual consistency 模型。 |
| R2 bucket | Supported | workerd 的 fetch/stream primitives。 | Runtime R2 facade 用平台 credentials 签 S3-compatible request；CLI 解析 `[[r2_buckets]]`。 | Bucket lifecycle/placement 由 S3-compatible backend 负责。`Headers` 形态的 `httpMetadata` 只接受 canonical IMF-fixdate `Expires`，malformed value 会在 host call 前被拒绝。 | 不支持 `preview_bucket_name` 和 `jurisdiction`。 |
| Workers AI binding | Partial | workerd 提供 WorkerEntrypoint/JSRPC、Fetcher、stream 和 WebSocket。 | WDL 增加 namespace-scoped 加密 BYO credential、OpenAI/xAI/DeepSeek 官方 adapter、`fetch()`/`run()`/`models()`、OpenAI-compatible JSON/SSE、Responses 与 Realtime WebSocket、public-only egress 和有界 host pool。 | 这是 namespace 自有的 provider registry 与 credential boundary，不是 Cloudflare Workers AI 基础设施或 WDL managed catalog。Model id 使用 `<provider>/<alias>`，raw surface 使用 `https://ai.wdl/v1`。 | 不支持任意 endpoint、managed catalog、平台共享 credential、usage/quota/billing、`toMarkdown()`、异步 batch、background Responses、WebRTC 或自动 tool execution；DeepSeek 仅支持 HTTP/SSE。 |
| ASSETS | Partial | Worker 可接收平台提供的 binding object。 | CLI 把 assets 上传到 S3-compatible storage；runtime 暴露 WDL 的 `env.ASSETS.url(path)` helper，用于生成 tokenized CDN URL。 | WDL assets 是 S3/CDN helper 模型，不是 Cloudflare Pages asset hosting。 | 不是完整 Cloudflare Pages assets pipeline，也不提供 fetch-style assets binding 合同。 |
| D1 database | Partial | workerd 可承载 D1-like facade 和 localDisk-backed SQLite actor 代码；bundled SQLite 包含 R*Tree virtual table 和 `rtreecheck()`。 | WDL 实现 control-plane metadata、d1-runtime、owner lease/generation fencing、migrations、SQL execution、drain/renew、deploy-time alias freezing，并在 SQLite work 或 response emission 前限制 query body、decoded statement payload、row 和 result bytes。 | D1 storage 是 WDL-owned SQLite + owner routing，不是 Cloudflare global D1；metadata delete 不删除物理 SQLite 文件。 | 没有 Cloudflare global replication/bookmark 语义。workerd 会大小写不敏感地拒绝 reserved `_cf_` namespace 下的 SQLite 名称。 |
| Durable Objects | Partial | Native Durable Object class execution、facet identity、支持 R*Tree virtual table 的 SQLite-backed `ctx.storage.sql`、facet 内 WebSocket hibernation API。 | WDL 实现 runtime facade、do-runtime owner routing、shard leases、Redis generation fencing、由 Workflows-backed due/retry jobs 驱动的 alarm shim、gateway-managed public WebSocket forwarding、storage id、cleanup tombstone，以及由 bundle 拥有的 `preserve`/`restart` session policy。`preserve` 是默认值；`restart` 会以 `1012` 关闭已 supersede 的 public WebSocket，并在不删除 SQLite 的前提下 lazy abort stale facet。部署级 `DO_PREVENT_EVICTION` 默认是 `true`，会保持 host actor resident；显式 `false` 允许 idle actor 重建，同时保留 SQLite 与静默 hibernatable socket state。 | WDL DO identity、owner routing、session policy、cleanup 和 residency policy 由 WDL 管理，不兼容 Cloudflare migration 模型。`restart` 与 Cloudflare 的默认行为一致——部署新代码会重启每个 Durable Object 并断开其 WebSocket；`preserve` 是 WDL 默认值，Cloudflare 没有对应选项。后续 `preserve` promotion 可以 supersede 尚未被观察到的 restart work。 | 只支持同 worker class；不支持 `script_name`、rename/delete migrations 和平台级 WebSocket session recovery。Actor-local JavaScript state 不会跨 eviction 保留。Tenant `ctx.storage.deleteAll()` 是 WDL best-effort KV/SQL/Workflows sequence，不是 Cloudflare atomic storage operation；reject 可能留下部分结果，且不支持 concurrent mutation。Alarm mutation 横跨 object SQLite 与 Workflows DB 2；backend mutation 开始后，reject 的 `setAlarm()` 或 `deleteAlarm()` 最终状态未知，需要成功完成与操作意图一致的重试。`getAlarm()` repair 只是 best effort，不确认 backend state。Native `ctx.abort(..., { retryAlarm: false })` 不会禁止 WDL Workflows-backed alarm retry。当前 workerd 仍可能在 actor eviction 期间静默丢失在途 hibernatable WebSocket application send 或 close，因此 eviction 保持显式 opt-in；workerd 2026-08-25 已修复此前的在途 auto-response 遗留。workerd 会大小写不敏感地拒绝 reserved `_cf_` namespace 下的 SQLite 名称。 |
| Queues producer/consumer | Partial | loaded worker 中的 queue handler surface。 | Runtime producer facade 经 redis-proxy 写 DB 1；scheduler 负责 consumer dispatch、retry、DLQ、orphan、delayed queues，并会拆分过大的 dispatch body batch。 | `max_batch_timeout_ms` 是配置 metadata，不是真正 aggregation window；dispatch concurrency 由 scheduler 拥有。 | `max_concurrency` 被拒绝。 |
| Cron triggers | Supported | worker 的 `scheduled()` handler surface。 | Control 存 cron config；scheduler 拥有 indexed discovery、cron-slot bucket、due dispatch 和可修复 projection。 | Control 和 scheduler 使用 JS/Rust `croner` 引擎，并通过测试和模块文档约束行为。 | 新增 scheduler dispatch 路径仍必须单独审计 Redis lease/fence。 |
| Workflows | Partial | 用户 workflow class code 在 runtime dispatch 时运行在 loaded worker 内。 | workflows 拥有 DB 2 instance state、step/event/sleep commit、runtime-observed `step.do` DAG edges（包括 `Promise.all` sibling）、generation/run-token fence、lifecycle API、progress callback 和 scheduler tick。 | WDL Workflows V2 的 payload 语义和 terminal failure 规则由 WDL 定义；永久失败的 `step.do` 即使被捕获也会让 run terminal failed；单个 step 最多记录 1000 条 dependency edge。 | WDL Workflows V2 不是 Cloudflare Workflows parity。WDL 自有 facade 不暴露原生 workerd 的 `WorkflowInstance.delete()` 和 `Workflow.deleteBatch()`；也不支持 `locationHint`、`script_name`、跨 worker workflow 和 Cloudflare 的源码 AST visualizer。 |
| Service bindings | Supported | workerd service binding JSRPC 和 fetch dispatch。 | WDL 解析 target worker metadata，cold-load immutable version，在可用时传播 request id，并通过 control metadata 做 namespace/action ACL。 | Frozen-version service target 按设计不 evict active siblings。 | 当前无已记录缺口。 |
| Platform bindings | Supported | workerd named entrypoint/JSRPC 机制。 | WDL 从 control metadata 展开 ACL-checked platform bindings。 | Platform bindings 是 WDL-specific。 | 不是 Cloudflare tenant portability feature。 |
| Vars 和 secrets | Supported | Env values 可 materialize 到 worker `env`。 | Control 存 worker vars/secrets metadata，把 secret value 加密成 at-rest `WDL-ENC:` envelope；redis-proxy 在 cold-load 时解密；deploy/secret mutation 过程中按留有 headroom 的 workerd 1 MiB `workerLoader` serialized env budget 校验用户 vars/secrets、runtime 注入的 binding env value 和 binding-scoped Workflow identity props，并计入非 Latin-1 字符串的 V8 two-byte storage；worker secret 改变时 promote immutable version。 | Secrets 由 WDL 平台管理；at-rest envelope provider 是 WDL 部署关注点。 | 不是 Cloudflare account secrets。 |
| Worker code size | Supported | Dynamic worker module bodies 可由 `workerLoader` 接受，直到 workerd 的 64 MiB 上限。 | Control 会在写入 deploy 产生的 version 前估算最终 WorkerCode，包含 runtime/do-runtime 注入的 wrapper/client modules 和 workflow import rewrite。 | WDL 的 deploy JSON body limit 对普通 inline deploy 更低。 | 大型 server-side bundle assembly 路径必须保留这道 guard。 |

## 控制面和开发工具

| Surface | 状态 | workerd 提供什么 | WDL 增强 / 新增什么 | 与 Cloudflare 的模型差异 | 未实现 / 缺口 |
|---|---|---|---|---|---|
| Wrangler project parsing | Partial | 无。 | WDL CLI 解析支持的 `wrangler.toml` / JSONC 子集：KV、D1、R2、AI、services、DO、workflows、queues、vars、assets、routes，以及显式 `workers_dev` platform-domain opt-out。 | 不支持的字段会拒绝，而不是静默模拟。Cloudflare 用 `workers_dev` 控制 Worker 的 `*.workers.dev` route；版本化 preview URL 由独立的 `preview_urls` 控制，后者默认跟随 `workers_dev`。WDL 把 `workers_dev` 映射到 namespace 的常规服务路径 `<ns>.<platform-domain>/<worker>/`，不支持 `preview_urls`。因此一个关闭 Cloudflare `*.workers.dev` route 的项目，在这里关掉的是生产入口；WDL 要求至少有一条 route pattern 才接受该 opt-out。 | 上面的各 binding 行列出了主要拒绝形态。 |
| Worker deploy/promote/delete | Supported | workerd 加载平台提供的 worker。 | Control/auth 负责 bundle commit、route promotion、WATCH/MULTI fence、lifecycle indexes、retained versions、secrets 和 async S3 cleanup intent。 | WDL API/CLI 是管理面。 | 不以 Cloudflare API parity 为目标。 |
| Log tail | Supported | runtime tail worker 可获得 worker console output 和位置对应的 `Error` metadata。 | Runtime 输出有界 structured log，并把 workerd `errorInfo` 保留为 `error_info`；control 授权 tail session，redis-proxy 存 bounded stream。 | Tail activation 有时间边界。 | 与 activation 竞争的消息可能丢失；不启用 experimental user-span export。 |
| Metrics/health | Supported | 服务代码可以暴露 HTTP endpoint。 | Gateway、runtime、d1-runtime、do-runtime、scheduler、workflows、redis-proxy 暴露各自 probes/metrics。 | Metrics socket 按服务拆分。 | Control/auth 没有独立公开 metrics socket。 |

## 不支持或尚未建模的 Cloudflare surface

这些条目故意显式列出，避免兼容缺口藏在模块文档之间：

| Surface | Status | 当前 WDL 立场 |
|---|---|---|
| Cache API / Cloudflare edge cache 语义 | Not supported | `caches.default` 不是 WDL 暴露的 stock workerd surface，WDL 也没有实现 Cloudflare edge cache tier。Tenant code 不应依赖这个 binding，也不应把它当成持久化或 CDN 合同。 |
| Vectorize、Analytics Engine、Browser Rendering、Hyperdrive、Email Workers | Not supported | WDL 没有对应 binding facade、control-plane metadata 或后端服务。 |
| R2 multipart upload、customer-provided encryption keys 和 Cloudflare-specific checksum 行为 | Not supported | 当前 R2 facade 面向 WDL worker/assets 所需的 S3-compatible object 操作。高级 Cloudflare R2 行为需要先设计，才能写成兼容合同。 |
| Queue `contentType = "v8"` 和 per-consumer `max_concurrency` | Not supported | Queue message 支持文档化的 `json`、`text` 和 `bytes` content type；只有 `v8` 会被拒绝。Dispatch concurrency 仍由 scheduler 拥有，`max_concurrency` 会被拒绝，而不是静默忽略。 |
| Incoming TCP `connect()` handler 和 Socket RPC transfer | Not supported | Bundled workerd 已有 incoming `connect()` entrypoint 和 autogated Socket RPC transfer 路径，但 WDL 没有配置 tenant raw-TCP ingress，也没有启用该 autogate；这不影响已记录的 outbound `cloudflare:sockets` surface。 |
| 上游 experimental 和 WDL 显式拒绝的 compatibility flags | Not supported | Tenant `compatibility_flags` 中属于上游 workerd `$experimental` 的 enable flag，以及 WDL 显式拒绝的 `allow_irrevocable_stub_storage`、`new_module_registry` 和 `streams_disable_constructors`，会在 deploy 和 runtime decode 阶段被拒绝。New Module Registry 虽已在上游毕业，但 WDL generated wrapper 是实际 dynamic-loader main module，无法保持 tenant `import.meta.main`；WDL runtime facade 还要求标准 stream constructor。 |
| Python Workers | Not supported | WDL 拒绝 Python module manifest，而不是让 workerd 在 cold-load 时失败。 |
| Durable Object cross-script binding 和 migration rename/delete 语义 | Not supported | WDL DO class 仅支持 same-worker。Storage identity、owner routing 和 delete cleanup 由 WDL 管理，不兼容 Cloudflare migration 模型。 |
| Cloudflare account API parity | Not supported | WDL 暴露自己的 CLI/control API。Cloudflare API 兼容不是目标。 |

## 设计原则

workerd 已经提供 isolate 内编程模型时，WDL 尽量保留 tenant 可见的模型。Cloudflare 生产平台提供外部服务的地方，WDL 必须补上外部部分：control metadata、Redis/S3 storage adapter、owner routing、scheduler dispatch 或 lifecycle cleanup。因此任何兼容性工作都应同时说明两半：复用了哪个 workerd surface，以及由哪个 WDL 服务补了平台行为。

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
| ES module Workers 和 `fetch()` | Supported | Module evaluation、request dispatch、`Response`/`Request`、service binding JSRPC 机制。 | Dynamic `workerLoader`、immutable version id、wrapper 生成 `env`、gateway routing、request logs、public/private outbound 隔离。 | Tenant `fetch()` 未捕获异常会映射为平台 `502 runtime_error`；异常细节进入结构化日志/live tail，不进客户端 body。 | WDL 不模拟每个 compatibility date 的历史行为；workerd 按平台启用的 flags 运行。 |
| WebSocket upgrade | Supported | workerd 内的 WebSocket API 和 101 response 处理。 | Gateway `GatewayWsHolder` Durable Object 承载公网 socket 并转发到 runtime/do-runtime，避免长生命周期 101 留在普通 gateway request IoContext。 | WDL 优先保留 gateway-held 长连接；Cloudflare 依赖自己的全球 edge session 模型。 | Gateway rolling 仍会断开物理 client socket；应用级 resume 未实现。 |
| `compatibility_date` / `compatibility_flags` | Partial | capnp 中启用的 workerd feature flags 和兼容行为。 | CLI/control 保存 bundle metadata。Control 会校验 `compatibility_date` 是真实的 `YYYY-MM-DD` 日期，且不晚于当前 UTC 日期和当前 bundled workerd 支持的最大日期；上游 `$experimental` enable flags（例如 `experimental` / `unsafe_module`）会在 deploy 前被拒绝。 | WDL 把 compatibility metadata 当成部署期平台 metadata，不承诺完整 per-worker 历史行为模拟层。 | 没有 per-worker 模拟所有 Cloudflare 历史行为；tenant worker 不能 opt into 上游 experimental-only flags。 |
| `nodejs_compat` | Partial | runtime service 启用 flag 后由 workerd 提供的兼容 surface。 | CLI 把 compatibility flags 带入 metadata。 | WDL 暴露 workerd 已启用的兼容 surface，而不是单独 Node.js runtime。 | 不承诺超出 workerd 已启用 surface 的完整 Node.js 平台能力。 |
| Python Workers modules | Not supported | workerd 有 experimental Python Workers 路径。 | Control 用 `python_workers_unsupported` 拒绝 `py` module manifest；runtime 和 do-runtime 对 retained metadata 中的 `py` module 也 fail closed。 | WDL 保持 tenant bundle 只包含 JavaScript/WebAssembly/data，不允许 cold-load 时触发 Pyodide bootstrap。 | 不支持 Python Workers 和 JS/Python 混合 bundle。 |

Node.js TLS 行为跟随 bundled workerd binary。在 2026-07-01 workerd pin 下，compatibility date 不早于 2026-06-16 的 worker 会拿到 `throw_on_not_implemented_tls_options`：`node:tls` 中尚未实现的选项（例如 `checkServerIdentity`）会从“静默忽略”变为抛 `ERR_OPTION_NOT_IMPLEMENTED`。另外，workerd 的 `servername` / expected-certificate-hostname 行为变化不受任何 compatibility flag 门控，因此所有日期的证书 hostname 校验都跟随 bundled workerd 行为。

## Bindings 和存储

| Surface | 状态 | workerd 提供什么 | WDL 增强 / 新增什么 | 与 Cloudflare 的模型差异 | 未实现 / 缺口 |
|---|---|---|---|---|---|
| KV namespace | Supported | 通过 workerd entrypoint/JSRPC 机制把 binding object 暴露给用户代码。 | Runtime `KV` facade 调 redis-proxy；redis-proxy 在 DB 1 hash bucket `kvh:<ns>:<id>:b:<bucket>` 存 value/metadata，字段为 `v:<key>` 和 `m:<key>`，支持 512-byte key/list-prefix cap、25 MiB value cap、batch raw-byte budget、TTL/EXAT、prefix list cursor。 | KV storage 是 WDL deployment 内的 Redis-backed、namespace-scoped 存储；`cacheTtl` 不是存储新鲜度合同。 | 没有 Cloudflare 全局边缘复制 / eventual consistency 模型。 |
| R2 bucket | Supported | workerd 的 fetch/stream primitives。 | Runtime R2 facade 用平台 credentials 签 S3-compatible request；CLI 解析 `[[r2_buckets]]`。 | Bucket lifecycle/placement 由 S3-compatible backend 负责。 | 不支持 `preview_bucket_name` 和 `jurisdiction`。 |
| ASSETS | Partial | Worker 可接收平台提供的 binding object。 | CLI 把 assets 上传到 S3-compatible storage；runtime 暴露 WDL 的 `env.ASSETS.url(path)` helper，用于生成 tokenized CDN URL。 | WDL assets 是 S3/CDN helper 模型，不是 Cloudflare Pages asset hosting。 | 不是完整 Cloudflare Pages assets pipeline，也不提供 fetch-style assets binding 合同。 |
| D1 database | Partial | workerd 可承载 D1-like facade 和 localDisk-backed SQLite actor 代码。 | WDL 实现 control-plane metadata、d1-runtime、owner lease/generation fencing、migrations、SQL execution、drain/renew、deploy-time alias freezing，并在 SQLite work 或 response emission 前限制 query body、decoded statement payload、row 和 result bytes。 | D1 storage 是 WDL-owned SQLite + owner routing，不是 Cloudflare global D1；metadata delete 不删除物理 SQLite 文件。 | 没有 Cloudflare global replication/bookmark 语义。workerd 会大小写不敏感地拒绝 reserved `_cf_` namespace 下的 SQLite 名称。 |
| Durable Objects | Partial | Native Durable Object class execution、facet identity、SQLite-backed `ctx.storage.sql`、facet 内 WebSocket hibernation API。 | WDL 实现 runtime facade、do-runtime owner routing、shard leases、Redis generation fencing、由 Workflows-backed due/retry jobs 驱动的 alarm shim、gateway-held public WebSocket forwarding、storage id、cleanup tombstone。 | WDL DO identity、owner routing 和 cleanup 由 WDL 管理，不兼容 Cloudflare migration 模型。 | 只支持同 worker class；不支持 `script_name`、rename/delete migrations、平台级 WebSocket session recovery。workerd 会大小写不敏感地拒绝 reserved `_cf_` namespace 下的 SQLite 名称。 |
| Queues producer/consumer | Partial | loaded worker 中的 queue handler surface。 | Runtime producer facade 经 redis-proxy 写 DB 1；scheduler 负责 consumer dispatch、retry、DLQ、orphan、delayed queues，并会拆分过大的 dispatch body batch。 | `max_batch_timeout_ms` 是配置 metadata，不是真正 aggregation window；dispatch concurrency 由 scheduler 拥有。 | `max_concurrency` 被拒绝。 |
| Cron triggers | Supported | worker 的 `scheduled()` handler surface。 | Control 存 cron config；scheduler 拥有 indexed discovery、cron-slot bucket、due dispatch 和可修复 projection。 | Control 和 scheduler 使用 JS/Rust `croner` 引擎，并通过测试和模块文档约束行为。 | 新增 scheduler dispatch 路径仍必须单独审计 Redis lease/fence。 |
| Workflows | Partial | 用户 workflow class code 在 runtime dispatch 时运行在 loaded worker 内。 | workflows 拥有 DB 2 instance state、step/event/sleep commit、runtime-observed `step.do` DAG edges（包括 `Promise.all` sibling）、generation/run-token fence、lifecycle API、progress callback 和 scheduler tick。 | WDL Workflows V2 的 payload 语义和 terminal failure 规则由 WDL 定义；永久失败的 `step.do` 即使被捕获也会让 run terminal failed；单个 step 最多记录 1000 条 dependency edge。 | WDL Workflows V2 不是 Cloudflare Workflows parity；不支持 `script_name`、跨 worker workflow 和 Cloudflare 的源码 AST visualizer。 |
| Service bindings | Supported | workerd service binding JSRPC 和 fetch dispatch。 | WDL 解析 target worker metadata，cold-load immutable version，在可用时传播 request id，并通过 control metadata 做 namespace/action ACL。 | Frozen-version service target 按设计不 evict active siblings。 | 当前无已记录缺口。 |
| Platform bindings | Supported | workerd named entrypoint/JSRPC 机制。 | WDL 从 control metadata 展开 ACL-checked platform bindings。 | Platform bindings 是 WDL-specific。 | 不是 Cloudflare tenant portability feature。 |
| Vars 和 secrets | Supported | Env values 可 materialize 到 worker `env`。 | Control 存 worker vars/secrets metadata，把 secret value 加密成 at-rest `WDL-ENC:` envelope；redis-proxy 在 cold-load 时解密；deploy/secret mutation 过程中按留有 headroom 的 workerd 1 MiB `workerLoader` serialized env budget 校验用户 vars/secrets 加 runtime 注入的 binding/workflow env value，并计入非 Latin-1 字符串的 V8 two-byte storage；worker secret 改变时 promote immutable version。 | Secrets 由 WDL 平台管理；at-rest envelope provider 是 WDL 部署关注点。 | 不是 Cloudflare account secrets。 |
| Worker code size | Supported | Dynamic worker module bodies 可由 `workerLoader` 接受，直到 workerd 的 64 MiB 上限。 | Control 会在写入 deploy 产生的 version 前估算最终 WorkerCode，包含 runtime/do-runtime 注入的 wrapper/client modules、workflow import rewrite 和 materialized workflow keys。 | WDL 的 deploy JSON body limit 对普通 inline deploy 更低。 | 大型 server-side bundle assembly 路径必须保留这道 guard。 |

## 控制面和开发工具

| Surface | 状态 | workerd 提供什么 | WDL 增强 / 新增什么 | 与 Cloudflare 的模型差异 | 未实现 / 缺口 |
|---|---|---|---|---|---|
| Wrangler project parsing | Partial | 无。 | WDL CLI 解析支持的 `wrangler.toml` / JSONC 子集：KV、D1、R2、services、DO、workflows、queues、vars、assets、routes。 | 不支持的字段会拒绝，而不是静默模拟。 | 上面的各 binding 行列出了主要拒绝形态。 |
| Worker deploy/promote/delete | Supported | workerd 加载平台提供的 worker。 | Control/auth 负责 bundle commit、route promotion、WATCH/MULTI fence、lifecycle indexes、retained versions、secrets 和 async S3 cleanup intent。 | WDL API/CLI 是管理面。 | 不以 Cloudflare API parity 为目标。 |
| Log tail | Supported | runtime 中有 worker console output。 | Runtime tail worker 输出 structured logs；control 授权 tail session；redis-proxy 存 bounded streams。 | Tail activation 有时间边界。 | 与 activation 竞争的消息可能丢失。 |
| Metrics/health | Supported | 服务代码可以暴露 HTTP endpoint。 | Gateway、runtime、d1-runtime、do-runtime、scheduler、workflows、redis-proxy 暴露各自 probes/metrics。 | Metrics socket 按服务拆分。 | Control/auth 没有独立公开 metrics socket。 |

## 不支持或尚未建模的 Cloudflare surface

这些条目故意显式列出，避免兼容缺口藏在模块文档之间：

| Surface | Status | 当前 WDL 立场 |
|---|---|---|
| Cache API / Cloudflare edge cache 语义 | Not supported | `caches.default` 不是 WDL 暴露的 stock workerd surface，WDL 也没有实现 Cloudflare edge cache tier。Tenant code 不应依赖这个 binding，也不应把它当成持久化或 CDN 合同。 |
| Workers AI、Vectorize、Analytics Engine、Browser Rendering、Hyperdrive、Email Workers | Not supported | WDL 没有对应 binding facade、control-plane metadata 或后端服务。 |
| R2 multipart upload、customer-provided encryption keys 和 Cloudflare-specific checksum 行为 | Not supported | 当前 R2 facade 面向 WDL worker/assets 所需的 S3-compatible object 操作。高级 Cloudflare R2 行为需要先设计，才能写成兼容合同。 |
| Queue `contentType = "v8"` 和 per-consumer `max_concurrency` | Not supported | Queue message 支持文档化的 `json`、`text` 和 `bytes` content type；只有 `v8` 会被拒绝。Dispatch concurrency 仍由 scheduler 拥有，`max_concurrency` 会被拒绝，而不是静默忽略。 |
| 上游 experimental compatibility flags | Not supported | Tenant `compatibility_flags` 中属于上游 workerd `$experimental` 的 enable flag 会在 deploy 和 runtime decode 阶段被拒绝。 |
| Python Workers | Not supported | WDL 拒绝 Python module manifest，而不是让 workerd 在 cold-load 时失败。 |
| Durable Object cross-script binding 和 migration rename/delete 语义 | Not supported | WDL DO class 仅支持 same-worker。Storage identity、owner routing 和 delete cleanup 由 WDL 管理，不兼容 Cloudflare migration 模型。 |
| Cloudflare account API parity | Not supported | WDL 暴露自己的 CLI/control API。Cloudflare API 兼容不是目标。 |

## 设计原则

workerd 已经提供 isolate 内编程模型时，WDL 尽量保留 tenant 可见的模型。Cloudflare 生产平台提供外部服务的地方，WDL 必须补上外部部分：control metadata、Redis/S3 storage adapter、owner routing、scheduler dispatch 或 lifecycle cleanup。因此任何兼容性工作都应同时说明两半：复用了哪个 workerd surface，以及由哪个 WDL 服务补了平台行为。

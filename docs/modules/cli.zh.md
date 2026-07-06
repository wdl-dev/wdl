# CLI 和 Wrangler 输入

## 目的

CLI 实现位于下游。本文件记录下游 `wdl` CLI 必须满足的平台端合同：如何把 Wrangler 项目部署到 WDL、管理 namespace-scoped 资源，并在普通操作中只调用 admin API，而不直接连接 Redis、S3 或 runtime 服务。

## 当前实现

下游 CLI 以 `@wdl-dev/cli` package 发布，也可以从 standalone checkout 开发。本仓只保留 CLI 调用的 control-plane 和 runtime 合同；integration tests 默认使用 `PATH` 上的 `wdl` executable，CI 会在 integration job 前全局安装 `.github/workflows/ci.yml` 顶层 `WDL_CLI_PACKAGE` pin 住的版本。验证未发布 CLI 变更时应优先 link 或包装 checkout，让 `wdl` 出现在 `PATH` 上；`WDL_CLI_BIN` 只保留给需要绕过 `PATH` 解析的聚焦 integration run。命令语法、命令分组和面向用户的措辞属于下游职责。本文件只记录 CLI 调用必须保持的平台行为。

## Control Context 解析

普通 CLI 命令在发 HTTP 请求给 control 前，会解析 control URL、admin token 和 namespace。

CLI 读取 shell/CI 环境变量和可选的项目 `.env`。`.env` 支持基础 `KEY=value`，也支持按 namespace 分段的 INI section：

```ini
CONTROL_URL=http://admin.test:8080
WDL_NS=demo

[demo]
ADMIN_TOKEN=local-dev-token

[prod]
CONTROL_URL=https://ctl.prod.example
ADMIN_TOKEN=<prod-token>
```

优先级是：

1. CLI flag
2. shell/CI environment
3. 选中的 `[namespace]` section
4. base `.env`
5. code default

Canonical spelling 是 `--control-url` / `CONTROL_URL`。Platform integration tests 只提供 `CONTROL_URL`。

Namespace 选择顺序是 `--ns`，然后是 shell/base `.env` 的 `WDL_NS`，最后是命令自己的 fallback（如果有）。如果没有解析出 namespace，只加载 base `.env`。如果 `--ns foo` 没有 `[foo]` section，CLI 会静默使用 base 值。Section 名称使用 CLI-local `isAdminAcceptableNs()` 规则：普通 tenant namespace 和 delimiter-safe 的 `__...__` reserved-looking section name 都会被接受；这不是服务端 reserved namespace literal set 的精确副本。选中 section 里的 `WDL_NS` 会被忽略并打印 warning，避免 section 自己重定向 namespace。

裸 production control host 默认补 `https://`。裸 local-development host（例如 `admin.test:8080` 和 `localhost:8080`）默认补 `http://`；任何裸 `:8080` control URL 都被视为 local HTTP。不想使用这个启发式时，应显式写 scheme。

`CONTROL_CONNECT_HOST` 是调试/传输覆盖项，用来保持逻辑 control URL host 不变但连接到另一个 host；它不是普通 tenant 合同。

## Diagnostic Discovery

下游 CLI diagnostics 可以通过配置的 control URL 调用 `GET /whoami`，确认当前生效的 token 和 endpoint。响应是有意限制的 self-view：包含 `principal`、`tokenId`、`requestId`、`platformVersion`、`minCliVersion` 和 `urls`，但绝不包含 token plaintext、token hash、其它 token record 或 raw workerd version。

CLI 可以展示：

- `platformVersion`：control 返回的 WDL platform version。Canonical derivation 记录在 `control-auth.zh.md` 的 `/whoami` 段；CLI 应直接展示这个值，不应从 package metadata 自行重建。
- `minCliVersion`：当前 platform build 支持的最低下游 CLI 版本。
- `urls.control`：请求实际到达的 control origin。
- `urls.namespace`：tenant namespace origin，只对 namespace token 返回。
- `urls.assets`：配置的 public assets base URL，只在 control plane 有安全的绝对 `http`/`https` `ASSETS_CDN_BASE` 时返回；返回前会去掉 query 和 fragment。

CLI 应把这些字段当作 diagnostics 和 user-facing guidance 的默认值，而不是替代用户显式配置。如果 `minCliVersion` 大于当前 CLI 版本，CLI 应在执行 mutating command 前 warning 或 fail。可选 URL hint 缺失时，应展示为 unavailable，不应自行猜测。

## Deploy Pipeline

`wdl deploy <project>` 是受支持的 worker bundling 路径。它会调用项目本地 `wrangler`，或在设置 `WDL_WRANGLER_BIN` 时调用该显式 binary，并把 Wrangler dry-run 输出作为 bundle 来源。CLI 会为 dry-run bundling 设置 dummy `CLOUDFLARE_API_TOKEN`，因此普通项目本地构建不需要真实 Cloudflare 凭据。

WDL worker name 遵循平台语法，而不是 Wrangler 更窄的 deployment-name 语法：`[A-Za-z0-9][A-Za-z0-9_-]{0,254}`。大写字母、数字、下划线和连字符都是合法的。如果 Wrangler dry-run validation 会拒绝真实的平台 worker name，下游 CLI 可以在 bundling 时传入 dummy Wrangler name，但发给 control 的 payload 和最终部署的 WDL worker name 必须仍是用户请求的平台 worker name。

Wrangler dry-run 成功输出默认隐藏，只显示 WDL progress；`--verbose` 会透传 Wrangler raw output，便于调试。

Bundling 后，CLI 会遍历整个 Wrangler 输出目录，把每个产物发给 control：

- JavaScript chunk
- Wasm module
- imported text、JSON、CSS 和其他 data asset
- 除 source map 和 Wrangler 输出目录 `README.md` 之外的所有文件

Binary 文件在 control JSON payload 中使用 base64，并在 control 存储 raw bytes 前只 decode 一次。Runtime 不会看到 base64 bundle bytes。

CLI package 拥有 Wrangler dry-run bundling 和 bundle artifact collection。本平台仓不应复制这条 packaging path。

## Wrangler Config 合同

CLI 读取 `wrangler.toml`、`wrangler.jsonc` 或 `wrangler.json`；三种格式都使用同一套 snake_case 字段。Named environment 通过 `--env <name>` 或 `CLOUDFLARE_ENV` 选择。

如果配置存在 named environments，必须显式选择一个。WDL 不会在存在 `[env.<name>]` 时静默部署 top-level default。`env.<name>.name` 被拒绝：部署后的 worker name 始终是 top-level `name`。Staging/production 并行部署应使用不同 namespace。

WDL 遵循 Wrangler selected-env 继承规则：

- 非继承 key 必须在每个 env 中重新声明：`vars`、`kv_namespaces`、`r2_buckets`、`d1_databases`、`services`、`queues`、`workflows`、Durable Object bindings 以及类似 binding table。
- `assets` 这类可继承 key 遵循 Wrangler selected-env 行为，也可以显式覆盖。
- `name` 和 `migrations` 这类 top-level-only key 出现在 env table 中会被拒绝。

支持的 config surface：

| 字段 | WDL 行为 |
|---|---|
| `name`、`main`、`compatibility_date`、`compatibility_flags` | 存入 immutable bundle metadata。Control 会在 commit 前拒绝格式错误、未来日期或当前 bundled workerd 不支持的 `compatibility_date`；包含 runtime/do-runtime 注入模块和生成 workflow keys 后的最终 WorkerCode 必须落在 workerd 64 MiB `workerLoader` code limit 内。 |
| `[vars]` | 接受 string、number、boolean，并 stringified 进 `env`；vars、namespace/worker secrets、runtime 注入的 binding/workflow env value 必须落在 WDL 留有 headroom 的 workerd 1 MiB `workerLoader` env budget 内。 |
| `[[kv_namespaces]]` | `id` 是 platform-local KV namespace id，不是 Cloudflare UUID。 |
| `[[r2_buckets]]` | `binding` 加 `bucket_name` 映射为平台 S3 bucket 下的 namespace-scoped virtual R2 bucket。 |
| `[assets]` | `directory` 内容上传到 S3-compatible assets storage，并 auto-inject `ASSETS`。 |
| `[[d1_databases]]` | Binding 优先按 `database_id` 解析，其次按 namespace-local `database_name`；migration 使用匹配的配置。 |
| `[[durable_objects.bindings]]` | 支持 `[[migrations]].new_classes` 或 `new_sqlite_classes` 中的 same-worker class；不支持 `script_name` 和 rename/delete migration。 |
| `[[services]]` | 在 caller deploy 时冻结 target namespace、worker、version 和 entrypoint。Cross-namespace `ns` 是 WDL extension，需要 target opt in。 |
| `[[platform_bindings]]` | 从 platform-tier namespace 解析一个 `SCREAMING_SNAKE_CASE` symbolic platform export，并冻结到 caller。 |
| `route` / `routes` | 原样发给 control；control 负责 pattern grammar 和 platform-domain rejection。 |
| `[triggers] crons` 和 `[[triggers.schedules]]` | Cloudflare-compatible UTC cron 加 WDL timezone extension。 |
| `[[queues.producers]]` 和 `[[queues.consumers]]` | Producer 和 consumer metadata。`max_concurrency` 被拒绝。 |
| `[[workflows]]` | Same-worker Workflows V2 binding。 |

`[[analytics_engine_datasets]]` 在 top level 和 selected-env level 都会被 deploy 拒绝。Unsupported field 不应在暗示 WDL 未实现的平台行为时被静默忽略。

## 平台资源合同

下游 CLI 可以用自己的命令形状暴露这些 surface，但平台侧行为是固定的：

- Worker list 从 control 读取 active version、retained version 和 secret-only entry。
- Worker deletion hard-delete route、retained version、worker secret、queue consumer 和 cron，并在 Redis commit 后 stage asset cleanup。
- Version deletion hard-delete 一个 retained non-active version。
- Secret mutation 必须显式选择 worker scope 或 namespace scope，避免误写 namespace-wide secret。提交的空字符串是已设置 secret，不是 unset。
- D1 命令管理 namespace D1 database 和 forward-only migration file。Migration filename 是 migration id；已 apply 的文件不应 rename 或编辑。
- R2 命令在 namespace 前缀 `r2/<ns>/` 下操作。空的 declared virtual bucket 在第一次写入对象前，不会出现在由 prefix 推导的 list 结果里。
- Workflows 命令通过 workflows service 操作；CLI 不得直接写 DB2。
- Tail 命令通过 control 打开 live SSE session。

破坏性命令默认要求确认。自动化只有在已经检查目标后才应传 `--yes`。支持 `--json` 的命令会返回 raw control response，而不是人类可读摘要。

## Tail 合同

Tail 流式输出 live fetch invocation event、`console.*` 输出、未捕获 fetch-handler exception，以及 scheduled/queue invocation event。多个 worker name 会创建一个显式 fan-in terminal。

下游 CLI 可以暴露 raw output、有界 stream resume 和 reconnect knob，但 control 始终是唯一的 tail session owner。

Tail 是 live debug 路径，不是 audit storage。Tail protocol 细节见 [Log Tail 和 Observability](log-tail-observability.zh.md)。

## Ownership / 失败语义

- CLI 的普通操作不直接写 Redis。
- Control 仍然是 auth、validation、Redis commit、routing、lifecycle 和 cleanup intent 的 authority。
- CLI 可以 warning 缺失 caller secret，但当 pre-deploy secret flow 合法时 deploy 仍可成功。
- 如果 bundle artifact 从 Wrangler 输出到 control/runtime 的 round-trip 失败，那是 WDL bug，不是有意 silent drop。

## 保护该合同的测试

平台仓库通过标记了 `// @wdl-cli-integration` 的集成测试文件调用已发布的 `@wdl-dev/cli` 命令：

- `tests/integration/auth-platform.test.js`
- `tests/integration/cli-multi-env.test.js`
- `tests/integration/cli-smoke.test.js`
- `tests/integration/log-tail.test.js`
- `tests/integration/pages-assets-demo.test.js`
- `tests/integration/r2-cli-binding.test.js`
- `tests/integration/route-demo.test.js`
- `tests/integration/s3-cleanup.test.js`

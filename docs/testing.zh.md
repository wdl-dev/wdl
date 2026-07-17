# 测试

本文记录 WDL 当前测试合同。代码和测试仍然是事实来源；本文说明现有测试层级、它们覆盖的 artifact，以及支持的 runner flag。

## 测试层级

| 层级 | 命令 | 范围 |
| --- | --- | --- |
| Lint | `npm run lint` | JavaScript 源码和测试的 ESLint flat-config 检查。 |
| Typecheck | `npm run typecheck` | `tsc --noEmit`，对较宽的 JavaScript surface 启用 `allowJs` / `checkJs`。 |
| Strict typecheck | `npm run typecheck:strict` | 已纳入 strict 覆盖的 workerd 和 server-side JavaScript tier 的 JSDoc gate。 |
| Unit tests | `npm run test:unit` | helper、protocol contract、style contract 和本地 harness 的纯 Node 测试。不需要 Docker stack。 |
| Integration tests | `npm run test:integration` | 基于 Docker Compose 的端到端测试，使用已编译 workerd config 和预构建本地镜像。 |
| CLI integration subset | `npm run test:integration:cli` | 带 `// @wdl-cli-integration` 标记的测试子集，使用同一个 pool runner 和错开的端口段。 |
| Full local gate | `npm run test:all` | 快速检查加 integration tests。 |

`npm run test` 是快速 pre-push gate：lint、typecheck、strict typecheck 和 unit tests。它不跑 Docker integration tests。

Strict typecheck 覆盖 runtime/control JavaScript tier、`tests/` 下的 JavaScript-like 测试源码（`.js`、`.cjs` 和 `.mjs`），以及 `scripts/` 下的维护脚本（`.js` 和 `.mjs`）。JSON payload、Markdown、Cap'n Proto fixture 文件等非 JS fixture 不进入 TypeScript。

## Artifact 模型

所有 workerd 路径都由已编译的 `dist/workerd-configs/*.bin` 引导启动：

- 本地 Docker Compose 和 integration tests 使用 `*-local.bin`，内部 service hop 走本地 Envoy mesh。
- 生产形态 configs 使用无后缀 `.bin`，对应 Service Connect 风格路由。
- Source bind mounts 不是 runtime 合同。workerd-side JavaScript 和 Cap'n Proto 修改必须先编译进 `.bin` artifacts，stack 才会观察到。

integration runner 在启动 shard 前统一准备这些 artifacts：

1. `node scripts/compile-workerd-configs.js --local`
2. `docker compose build gateway workflows`

pool 管理的 shard 随后用 `--no-build` 启动 compose service，避免并发 shard 抢 Docker build。直接手跑单个 integration 文件时，也会在第一次启动 stack 前执行同样的 prepare。

## PR Integration Gate

Docker Compose integration job 需要 Docker Hub 和 Build Cloud credential，所以 CI 不在 pull request 上运行它。它只在 trusted push event 上运行。Protocol、runtime、Redis shape 或 state-machine 变化在 merge-ready 前仍应由 maintainer 本地跑定向或完整 integration。

push PR 前仍建议先跑定向 local integration：

| 改动区域 | 完整 gate 前建议的定向 integration |
| --- | --- |
| Gateway/admin routing 或 auth | `tests/integration/gateway.test.js`、`tests/integration/auth-worker.test.js`，或覆盖该 control route 的 integration 文件 |
| Deploy、promote、delete、lifecycle 或 S3 cleanup | 覆盖该路径的 deploy/delete/control lifecycle integration 文件 |
| Runtime binding metadata 或 facade 行为 | 对应 binding 的 focused integration 文件，加 runtime/load 单测 |
| D1 或 DO owner/state 行为 | focused D1/DO integration 文件；ownership 变化时包含 multi-runtime profile 测试 |
| Scheduler、queues、cron 或 workflows | focused queue/cron/workflows integration 文件，加触碰 Rust crate 的 tests |
| Redis key 或 payload shape | 覆盖该 key family writer/reader pair 的所有 integration 文件 |

`docs/protocol-contracts.zh.md` 拥有更宽的协议 review matrix。本文档拥有命令和 CI runner 行为。

## Integration Runner

`scripts/run-integration-tests.js` 是默认 integration 入口。它把选中的测试文件放进 FIFO 队列，并分配给彼此独立的 slot。

每个 slot 都有自己的：

- Docker Compose project：`wdl-it-<slot>`
- Gateway host port：`18080 + slot`
- s3mock host port：`29500 + slot`
- Valkey 容器和数据卷

通过的 slot 会立即清理。失败的 slot 默认保留供调试，除非显式开启失败清理。

slot 的第一个文件会支付依赖序 stack startup/restart 成本。该 slot 的后续文件走 `WDL_INTEGRATION_SLOT_PREPPED=1` 快路径；调用方承诺这个 slot 已经拾取最新编译 config 和预构建镜像。

## 运行集成测试

完整 integration：

```bash
npm run test:integration
```

单个 integration 文件：

```bash
node --test tests/integration/<file>.test.js
```

直接手跑单文件路径会自动准备本地 artifact，除非设置了 `WDL_INTEGRATION_SKIP_PREPARE=1`。

CLI 子集：

```bash
npm run test:integration:cli
```

CLI 子集已经包含在 `npm run test:integration` 中；只在聚焦 CLI 路径时单独运行。

CLI integration 默认使用 `PATH` 上的 `wdl` executable。本地运行应安装 `.github/workflows/ci.yml` 顶层 `WDL_CLI_PACKAGE` pin 住的已发布 CLI 版本：

```bash
npm install -g @wdl-dev/cli@1.4.0
```

验证未发布 CLI 变更时，优先 link 或包装 checkout，让 `wdl` 出现在 `PATH` 上。`WDL_CLI_BIN` 只保留给少数需要绕过 `PATH` 解析的聚焦 run。

如果选中的 CLI binary 不存在，integration runner 会在 preflight 阶段带 `WDL_CLI_BIN` 提示失败，而不是跳过 CLI 覆盖。

## 支持的 Flags

### 用户入口

| Flag | 默认值 | 含义 |
| --- | --- | --- |
| `WDL_INTEGRATION_SHARDS` | `4` | 完整 integration runner 的并行 slot 数。调试单个 stack 时可设为 `1`。 |
| `WDL_INTEGRATION_CLI_SHARDS` | `2` | CLI integration 子集的并行 slot 数。 |
| `WDL_KEEP_INTEGRATION_STACK=1` | 未设置 | run 结束后保留所有 slot stack。 |
| `WDL_TEARDOWN_INTEGRATION_STACK_ON_FAILURE=1` | 未设置 | 失败的 slot 也清理，而不是保留供调试。 |

### 高级入口

| Flag | 默认值 | 含义 |
| --- | --- | --- |
| `WDL_INTEGRATION_DURATIONS_FILE` | `.integration-test-durations.json` | 用于测试排序的历史耗时输入，改善 shard 平衡。 |
| `WDL_INTEGRATION_SKIP_PREPARE=1` | 未设置 | 跳过 compile/build preflight。只有确认编译产物和镜像已最新时才使用。 |
| `WDL_CLI_BIN` | `PATH` 上的 `wdl` | 需要绕过 `PATH` 解析的聚焦 integration run 使用的可选 executable override。 |

### Runner 内部

下面这些是 pool runner 和 integration helper 之间的实现细节。测试可以读取，但不应作为常规用户旋钮。

| Flag | 含义 |
| --- | --- |
| `WDL_INTEGRATION_NO_BUILD=1` | shared preflight build 后，让 compose helper 路径加上 `--no-build`。 |
| `WDL_INTEGRATION_SLOT_PREPPED=1` | 标记该 slot 已经支付完整 startup/restart prepare 成本。 |
| `WDL_GATEWAY_HOST_PORT` | runner 注入的每 slot gateway host port。 |
| `WDL_S3MOCK_HOST_PORT` | runner 注入的每 slot s3mock host port。 |
| `WDL_WORKERD_CONFIG_VARIANT=local` | 为 compose 选择本地编译的 workerd config 变体。 |

## Helper 与 Fixture

测试代码按所有权分三棵树：

### Helper 选型速查

优先选择和 response 或 fixture 来源最贴合的窄 helper：

| 测试需求 | 使用 | 说明 |
| --- | --- | --- |
| Unit test 读取真实或 fake JSON `Response` | `tests/helpers/response-json.js` 的 `readJsonResponse(...)` / `assertJsonResponse(...)` | 同时检查 status，并提供带 label 的 JSON parse/status 诊断。 |
| Integration HTTP JSON status + body | `tests/integration/helpers/http-response.js` 的 `readIntegrationJson(...)` / `assertIntegrationJson(...)` | 用于 Fetch `Response` 或收集好的 `{ status, body }` response，需要同时断言 status 和 JSON body 诊断时使用。 |
| Integration HTTP helper response JSON | `tests/integration/helpers/http-response.js` 的 `responseJson(...)` / `responseJsonOrNull(...)` | 用于已经单独断言 status 的 `{ status, body }` response，以及安装了 `.json()` / `.jsonOrNull()` 的 wrapper。 |
| Integration HTTP status 断言 | `tests/integration/helpers/assertions.js` 的 `assertStatus(...)`、`assertStatusIn(...)` 或 `assertNotStatus(...)` | response 带结构化诊断，或 status 失败时需要稳定 body 输出时使用。 |
| Integration Redis state | `tests/integration/helpers/redis.js` 的 typed helpers | 使用 `redisHGet(...)`、`redisXAdd(...)`、`redisPublish(...)`、`redisFlushAll(...)` 和 `db` option，不新增 direct `redis-cli` 字符串。 |
| Integration stack lifecycle | `tests/integration/helpers/stack.js` 的 `setupIntegrationSuite(...)` | 文件有额外 setup 时用 `afterStackUp` / `beforeEachReset` / `reset: false`，不要手写 `before(ensureStackUp)` + `beforeEach(resetStack)`。 |
| Integration queue scenarios | `tests/integration/helpers/queue-scenarios.js` 的 worker source 和 helpers | 用于 queue producer/consumer source、queue-specific stack setup 和重复 send/read helper；协议断言仍留在测试文件内。 |
| Integration workflow scenarios | `tests/integration/helpers/workflows-scenarios.js` 的 worker source 和 DB2 helpers | 用于 workflow demo source、workflow state keys、ready-shard helper 和直接 runtime replay helper；workflow 断言仍留在测试文件内。 |
| Integration fetch worker source wrapper | `tests/integration/helpers/worker-source.js` 的 `workerFetchCallerSource(...)` | 多个测试需要相同 `export default { async fetch(req, env) { try { ... } } }` caller 外壳时使用；业务 body 仍留在测试内联可读。 |
| Unit control handler module graph | `tests/helpers/control-handler-harness.js` 的 `createControlHandlerState(...)` / `importControlHandler(...)` | 用于需要 `control-shared` state、logs、env、metrics、Redis 或 backend service stub 的 `control/handlers/*` 测试。 |
| Unit Control shared module graph | `tests/helpers/load-control-shared.js` 的 `compileControlSharedGraph(...)` / `compileControlSharedDependencies(...)` | 用于测试 `control/shared.js` 和 synthetic shared stub，不重复构建其 production-backed dependency graph。 |
| Unit runtime R2 binding module graph | `tests/helpers/load-runtime-r2-binding.js` 的 `makeR2Bucket(...)` 和 fetch installer | 用于 `runtime/bindings/r2.js` host-surface 测试，不在每个文件里重建 R2 module replacement graph。 |
| Unit D1/DO owner-client module graph | `tests/helpers/load-d1-owner-client.js` 和 `tests/helpers/load-do-owner-client.js` 的 `loadD1OwnerClient(...)` / `loadDoOwnerClient(...)` | 用于 owner forwarding client 测试，不在每个文件里重建 state、protocol、internal-auth 和 owner-forwarder replacement graph。 |
| Unit auth entrypoint harness state | `tests/helpers/load-auth-index.js` 的 `authMockState(...)`、`authLogs(...)` 和 `lastAuthLog(...)` | 测试不直接读写 `globalThis.__authMockState`；该 global 只是 harness 内联 module mock 的私有存储。 |
| Unit/integration Redis command parity | `tests/helpers/redis-conformance-cases.js` 的 `redisConformanceCases` | fake Redis 和真实 integration Redis wrapper 必须对齐某个 command 语义时，加共享 case。运行 `tests/unit/fake-redis.test.js` 和聚焦 Redis conformance integration 文件。 |
| 记录 mocked `fetch` 调用 | `tests/helpers/mock-fetch.js` 的 `makeRecordingFetch(...)` / `withRecordingFetch(...)` | 测试需要自定义 call record shape 时使用 `capture`。 |
| 临时替换 global 或 property | `tests/helpers/mock-global.js` 的 `withMockedGlobal(...)`、`withMockedProperty(...)` 和 `withMockedPropertyDescriptor(...)` | 只有文件拥有 before/after cleanup 时才用 install-style helper。 |
| 捕获 console 或 stream 输出 | `tests/helpers/output-capture.js` 的 `withCapturedConsole(...)`、`installConsoleMethodCapture(...)` 或 `installStreamWriteCapture(...)` | 测试文件不要直接替换 `console.*` 或 `process.stderr/stdout.write`。 |
| 简单 sleep 或轮询 | `tests/helpers/timing.js` 的 `delay(...)` / `waitUntil(...)`，或 integration `stack.js` re-export | 不要替换 tenant worker source string 里的 sleep；那是被测 fixture 代码。 |
| 临时目录 | `tests/helpers/temp-dir.js` 的 `withTempDir(...)` | 优先用 scoped cleanup，不手写 `mkdtemp` / `rm` `finally` 块。 |
| 仓库 JSON fixture 文件 | `tests/helpers/load-shared-module.js` 的 `readRepositoryJson(...)` | 保持 fixture 读取带 label，并相对仓库路径定位。 |

- 被 JavaScript 和 Rust 两侧测试共同读取的 cross-language JSON fixture 放在 `tests/fixtures/`。JavaScript 测试用 `readRepositoryJson(...)`；Rust 测试用 `include_str!(...)` 读取同一个文件。这类 fixture 只 pin 测试合同和 drift guard，不新增 runtime shared owner。
- `tests/helpers/` 是 unit test helper 的归属：
  - 包含 `load-*.js` ESM data-URL loader 家族（每个被测 repo 模块一份 loader，例如 `load-auth-lib.js`、`load-control-lib.js`、`load-runtime-dispatch.js`）、共享 fixture（`runtime-dispatch-fixtures.js`、`control-shared-stub.js`）、静态分析工具（`source-scan.js`），以及 mock 化 Cloudflare runtime 表面的 `mocks/`。
  - `load-shared-module.js` 负责仓库模块源码读取、data-URL 构造和 import specifier rewrite helper；`shared/worker-contract.js`、`shared/ns-pattern.js` 这类无 import 的 contract owner 应通过 `repositoryFileUrl(...)` 直接加载，不在 stub 中重写其 grammar。测试需要重写 repo module import 时，用它代替手写 `readFileSync(...).replace(...)` 链。局部 source rewrite 使用 `readRepositoryFile(...)` 加 `applyModuleReplacements(...)`；需要读取 repo module source 并一次性应用 replacements 时用 `readRepositoryModuleSource(...)`；可复用 repo module 用 `repositoryModuleDataUrl(...)`；import-map 形态的 stub 用 `importSpecifierReplacements(...)`。这是被测试锁住的约定：`tests/unit/test-helper-style-contracts.test.js` 会拒绝共享 loader helper 之外新增的 source-producer `.replace(...)` module rewrite 链，包括直接链、变量中转形态，以及对 `applyModuleReplacements(...)` 产物的二次 rewrite。
  - `control-handler-harness.js` 负责 `control/handlers/*` unit tests 共用的 `control-shared` harness；handler 遵循 shared control entrypoint 形态时，用它注入 state/env/log/metrics/backend service 和 Redis，不再新增 file-local `control-shared` data-URL stub。`load-control-shared.js` 负责真实 `control/shared.js` 测试和 synthetic harness stub 共用的 production-backed dependency graph；stub 本地只保留 state-bound wiring 和 test instrumentation。
  - `load-runtime-r2-binding.js` 负责 `runtime/bindings/r2.js` host binding module graph，包括 SigV4Client/fetch 记录钩子；新增 R2 host 测试时扩展这个 loader，不把 replacement graph 复制回测试文件。`load-auth-index.js` 负责 auth entrypoint mock state accessor；测试使用 `authMockState(...)`、`authLogs(...)` 和 `lastAuthLog(...)`，不直接触碰 `globalThis.__authMockState`。
  - `mocks/fake-redis.js` 负责 unit tests 可复用的 in-memory Redis session/multi 子集，并为读写、batch helper 和 multi ops 提供 command trace；多个测试需要同一 Redis command shape 时扩展它，不再新增 file-local mock。Data-URL `shared-redis` replacement 使用 `sharedRedisStubUrl(...)`，共享 fake `WatchError` constructor 和 production `decodeBulk` semantics；只追加 graph-specific state 或 client exports。
  - `request-body.js` 负责 mock backend 捕获 `RequestInit.body` 时的 typed JSON request-body 解析；这类测试不要再直接手写 `JSON.parse(...)`。`do-envelope.js` 负责 Durable Object binary invoke envelope 的测试解码，不再新增 file-local `decodeDoEnvelope()`。style-contract 套件会拒绝新的直接 mock request-body `JSON.parse(...)` 和 file-local DO envelope decoder。
  - `mock-global.js` 负责临时替换 global 和 global property；单个 lexical async scope 用 `withMockedGlobal(...)` / `withMockedProperty(...)`，只有文件需要 before/after hook 所有权时才用 install-style helper。`mock-fetch.js` 是这套 helper 的 typed fetch wrapper。style-contract 套件会扫描仓库作者维护的 `.js` 测试，并拒绝这些 helper 之外新增的直接非 `__` `globalThis.<name> = ...` 赋值、直接 `console.log/warn/error/info = ...` 赋值，以及测试常用 built-in/global property hook（`process.stderr.*`、`AbortSignal.*`、`Object.*`、`JSON.*`、`Headers.prototype.*`、`Array.prototype.*`、`Function.prototype.*`）的直接赋值。Built-in prototype descriptor mock 使用 `withMockedPropertyDescriptor(...)`，不手写 `Object.defineProperty(...)` save/restore。
  - unit tests 不起 Docker，通过这些 loader 导入。
- `tests/integration/helpers/` 是 integration helper 的归属：
  - 每个关注点独占子模块：`admin-http.js`、`cli.js`、`compose.js`、`env.js`、`gateway-http.js`、`internal-http.js`、`http-response.js`、`websocket.js`、`stack.js`、`runtimes.js`、`redis.js`、`prometheus.js`、`misc.js`、`worker-source.js`，再加 tier 专用的 `d1-runtime.js` 和 `durable-objects.js`。`index.js` 是供 consumer 用的 barrel re-export。
  - helper 自己内部 import 走具体子模块，不绕 barrel。barrel 有意只收通用 helper。
  - tier 专用 fixture 模块（`d1-runtime.js`、`durable-objects.js`）保持 deep import，因为它们在 import 期跑 top-level `await` 编译 protocol graph，只有 D1/DO 测试该付这个成本。
  - 聚焦型的 `redis.js` 和 `prometheus.js` 同理走 deep import——测试只在断言 Redis 或 metrics 时才拉进来。`redis.js` 是 integration tests 共享的 Redis CLI wrapper；DB 1 / DB 2 断言通过它的 `db` option 选择数据库，不再添加 tier-local `redis-cli -n ...` wrapper。常用命令走 `redisFlushAll(...)`、`redisPublish(...)`、`redisXAdd(...)`、`redisHSet(...)`、`redisSetEx(...)` 等 typed helper；direct `redis-cli` 和 `composeExec("redis", ...)` 只允许出现在这个 helper 内。
  - `http-response.js` 负责 cached integration response JSON accessor：`readIntegrationJson(...)` / `assertIntegrationJson(...)` 把 status 断言和带 label 的 JSON parse 合在一起，支持 Fetch `Response` 和收集好的 `{ status, body }` response。status 已单独断言时用 `responseJson(...)` 要求 body 必须是 JSON；空 body 需要显式视为 `null` 时用 `responseJsonOrNull(...)`。integration tests 不直接 `await response.json()`；改用共享 helper，让失败信息带上 status/body 上下文。HTTP helpers 会在合适的 response 对象上挂 `.json()` / `.jsonOrNull()`。integration tests 不再手写解析 HTTP response 的 body/text，也不在本地 wrapper 里手写 `.json()` accessor；改用 `readIntegrationJson(...)`、`assertIntegrationJson(...)`、`responseJson(...)`、`responseJsonOrNull(...)` 或 `withResponseJsonAccessors(...)`。`json-payload.js` 负责其他带 label 的结构化 integration payload 解析，包括 command/stdout JSON 和 base64 JSON body。WebSocket text-frame JSON 走 `websocket.js` 的 `frameJson(...)` / `readJsonServerFrame(...)`；Redis 存储的 schema JSON 走 typed Redis helpers（`redisHGetJson(...)`、`redisHashJsonField(...)`、`redisJsonMember(s)(...)`）或 `readMeta(...)` 这类 domain helper。非共享协议的 stream event JSON 保持在对应测试域内（例如 log-tail SSE）。style-contract 套件会拒绝这些来源上的新增直接 JSON parse。
  - `worker-source.js` 负责小型 integration worker source 外壳，例如 `workerFetchCallerSource(...)`；它只去掉重复 wrapper boilerplate，被测 fetch body 仍应内联留在测试文件中。
  - 标准 Docker-backed 测试在 module scope 调用一次 `setupIntegrationSuite()`，不要重复写 `before(ensureStackUp)` + `beforeEach(resetStack)`；文件有 one-time setup、额外 per-test reset 或跳过默认 reset 时，通过 `afterStackUp`、`beforeEachReset` 或 `reset: false` 组合，不再手写 stack lifecycle。
- `test-workers/` 保存 integration tests 直接部署或读取的 worker fixture。两种结构并存且有意：完整 Wrangler workspace（`package.json + wrangler.toml + src/`），用于走 CLI deploy 路径的测试；以及只有 `src/` 的 source-only 结构，用于测试通过 `readFileSync(new URL("../../test-workers/<name>/src/index.js", ...))` 把源码作为字符串内联。按用法挑选；详见 `test-workers/README.zh.md`。
- `examples/` 保存手动 demo 和参考项目。integration tests 不应静默依赖这些路径；需要测试拥有形态时，应迁到 `test-workers/`。
- `tests/integration/manual/` 放 `*.manual.mjs` 复现脚本，**故意不被 runner 发现**。
- CLI integration tests 必须在文件顶部加入 `// @wdl-cli-integration` 标记，供 CLI subset runner 发现。
- Rust crate-local test helper 放在 crate 自己的 `#[cfg(test)]` module 里（例如 `rust/redis-proxy/src/lib.rs::test_support`）。同一个 crate 内重复的 parser/protocol 断言 helper 走这里，不在 sibling Rust module 里复制。

### 哪些保留 inline

两棵 helper 树**有意不当抽屉**。以下情况留在测试文件内：

- 该文件域内的单 caller 工具；
- 带字面 shell-escape 字符串载荷的 Redis 写操作；
- 另一个测试文件不共享的一次性 Redis mock command；
- DB 1 / DB 2 上 typed Redis helper 尚未暴露的 stream 协议命令（如 `XADD`、`XPENDING`、`XREADGROUP`）；
- 跨两个 caller 但实现语义分叉（如不同 header、不同 request shape）。

升级为 helper 的触发条件：非平凡 helper 在 ≥2 文件里字节相同地重复出现，或某 fixture 的 inline 源码超过约 35 行且**只是作为字符串加载**。

### Tripwire

`tests/helpers/style-contract-scanner.js` 负责 style-contract tests 共用的源码扫描和字面量提取 helper。生产/跨 tier tripwire 留在 `tests/unit/style-contracts.test.js`，它断言 workerd config 归属、service anchor、`composeNoBuildFlag`、grammar mirror、Redis key convention、active-doc parity 等跨源码契约。测试 helper/style tripwire 放在 `tests/unit/test-helper-style-contracts.test.js`；该文件递归扫描两棵 helper 树，守住 `load-shared-module.js` data-URL 构造、module rewrite、response JSON helper、Redis CLI wrapper 和上面的 module-loader 约定。repo module source rewrite 应继续集中在 `load-shared-module.js`，不要漂回 file-local source-reader 或 source-producer `.replace(...)` 链。共享 source scanner 会跳过生成的 dependency 和 worker build 目录（`node_modules`、`.deploy-dist`、`.wrangler`）。因此这些 tripwire 覆盖的是仓库作者维护的源码，而不是本地安装 churn。重命名或移动 helper 而忘记更新 tripwire，unit 套件即跳。

## 持续集成

WDL 使用分开的验证和 release workflow：

- GitHub Actions 是 pull request 和 `main` 的验证 gate：JavaScript、Rust 和 hygiene checks 在 PR 和 push 上运行。
- Docker Compose integration 只在 trusted push event 运行，因为它需要 Docker Hub 和 Build Cloud credential。
- GitHub release workflow 从 `wdl.*` tag push build/push release image；release tag 必须匹配 `VERSION` 并在 `CHANGELOG.md` 有对应 notes，manual run 可验证或发布同一条 build path。

`.github/workflows/ci.yml` 只在 trusted push event 运行 integration 套件。`integration` job `needs` `node`、`rust`、`rust-supply-chain` 和 `ci-hygiene` job，因此 lint、typecheck、unit、Rust、dependency 或 hygiene 检查失败时不会浪费 Build Cloud 额度，也不会启动整套 stack。npm audit gate 检查本仓 lock 里的依赖；已发布 CLI 只在 integration job 里全局安装，不进入本仓 package lock。

该 job 完全串行运行（`WDL_INTEGRATION_SHARDS=1`）：一次只起一套 Docker Compose stack，这样约十二个容器的 stack 不会翻倍叠加、压垮两核的 hosted runner。因此 wall time 接近所有文件时长之和，而非分片 pool 的 wall time。

镜像构建 offload 给 Docker Build Cloud。`docker/setup-buildx-action` 创建一个 `driver: cloud` builder，workflow 把 `docker compose build gateway workflows` 指向它，于是 `wdl-workerd:dev` 与 `wdl-rust:dev` 会在云端构建、共享 cargo/layer 缓存，并把镜像 load 回 runner；hosted runner 不在本地编译 Rust。Docker Hub credentials 存在临时 `DOCKER_CONFIG` 中，并在 image prep 后删除。随后 integration runner 设置 `WDL_INTEGRATION_SKIP_PREPARE=1`，因此 CLI tests 会在 Docker credentials 已经被丢弃后运行。

CI 会在 integration job 前以禁用 npm lifecycle scripts 的方式全局安装 `.github/workflows/ci.yml` 顶层 `WDL_CLI_PACKAGE` pin 住的 `@wdl-dev/cli` package，CLI integration 测试默认 exec 这个 `wdl` 命令。本地验证未发布 CLI 变更时也应让 `wdl` 从 `PATH` 解析；`WDL_CLI_BIN` 只作为聚焦 executable override。slow-first 顺序通过 `actions/cache` 用上一轮的 `.integration-test-durations.json` 做种子；cache miss 时回退到 runner 内置的 slow-first 列表。新跑出来的文件会重新入 cache 并作为 artifact 上传。

仓库设置里需要配置的 CI 配置项：

| 类型 | 名称 | 用途 |
| --- | --- | --- |
| variable | `DOCKER_USER` | Build Cloud 登录用的 Docker Hub 用户名。 |
| secret | `DOCKER_PAT` | Build Cloud 登录用的 Docker Hub access token。 |
| environment | `release` | Docker image 发布的 tag-restricted deployment 记录。 |

Build Cloud 的 builder endpoint（`getwdl/builder`）直接写在 workflow 里。

## 运维注意

- Node 的 `fetch()` 会抹掉 `Host` header。需要 gateway subdomain routing 时，integration helper 使用原生 `http` 模块，例如 `Host: admin.test`。
- D1 和 DO 多 runtime 测试使用 compose profile（`d1-multi`、`do-multi`），返回前必须恢复 single-runtime baseline。
- D1 test-hook endpoint 默认关闭，只能用于一次性 integration run。
- 如果 run 被中断，用对应的 `COMPOSE_PROJECT_NAME` 执行 `docker compose down -v` 清理受影响的 compose project。

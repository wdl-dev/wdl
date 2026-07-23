# Workerd JavaScript 标准

本文定义运行在 workerd tier 及其周边 JavaScript 代码的默认标准。它补充 `project-standards.zh.md` 和 `rust-sidecar-standards.zh.md`，不替代模块文档。

## 范围

这些标准覆盖：

- `gateway/`
- `runtime/`
- `control/`
- `auth/`
- `d1-runtime/`
- `do-runtime/`
- `shared/`
- JS 单元测试和集成测试

这些 tier 当前使用 JavaScript，不使用 TypeScript。如果以后引入 TS，也应保留相同的 ownership、contract 和测试规则。

TypeScript 仍作为 JavaScript checker 使用。`tsconfig.json` 是覆盖整棵 JS 树的宽松 `allowJs` / `checkJs` 基线。`tsconfig.strict.json` 是 workerd 和 server-side tier 的严格 JSDoc gate，覆盖 `auth`、`control`、`gateway`、`runtime`、`d1-runtime`、`do-runtime`、`shared`、选定脚本、测试和 system worker 代码。下游 CLI 拆分维护自己的 JavaScript 标准和兼容面。

## 语言基线

仓库里的脚本和测试以 Node `>=24` 为基线。运行时代码以支持同等 JavaScript 基线的 workerd 版本为目标。基线变化时，`tsconfig.json`、`tsconfig.strict.json`、`eslint.config.js`、`package.json#engines` 和 vendor build target 必须同步。

现代标准库应优先用于减少本地 helper 或 mutation 风险：

- 用 `Object.hasOwn(...)`，不要用 `Object.prototype.hasOwnProperty.call(...)`。
- 非原地排序或反转用 `toSorted()` / `toReversed()`。
- 动态字面量文本插入正则时用 `RegExp.escape(...)`。
- 只有真正的 deferred-promise 状态才用 `Promise.withResolvers()`。
- 只有代码天然是在分组或做集合比较时，才用 `Map.groupBy(...)`、`Object.groupBy(...)` 和 `Set` algebra。

不要为了显得现代而引入新 API。callback wrapper 的 `new Promise(...)`、parser stack、queue mutation 和性能敏感的本地算法，应保留更清楚的写法。`||` 默认值属于正确性审查，不是机械语法清理：只有确认 `0`、`false` 或 `""` 是必须保留的合法值时，才替换成 `??`。

`npm run typecheck:strict` 是合同门禁，不只是格式检查。公开边界 typedef 应描述实际会访问的最小 shape。优先使用 `unknown` 加本地 narrow，不要用只给未检查值改名的 `@typedef {any}` alias。总是抛错的函数应标 `@returns {never}`，让 strict 检查能在调用方正确收窄。

implementation 的 no-`any` 标准覆盖 `auth/`、`control/`、`gateway/`、`runtime/`、`d1-runtime/`、`do-runtime/`、`shared/` 和 `system-workers/` 下的生产 JS，生成产物和 vendor bundle 除外。测试可以在动态 fixture、global stub 和 thrown-error 探针里保留窄范围 `any` cast，但这个例外不能回流到实现代码。

重复的二进制/字符串路径应使用模块级 `TextEncoder` / `TextDecoder` 单例。一次性测试里内联创建可以接受；生产 decode path、Redis payload 解析和 binding adapter 应复用模块单例，除非确实需要有状态 decoder 选项。

## 所有权

Entrypoint 应保持薄：负责 dispatch、auth/routing、observability wiring，并调用具名 helper。纯 route parsing、key construction、normalization 和 policy decision 应放在不依赖 workerd 的可单测文件中。

合适的 owner 边界包括：

- route parsing 和 request-shape normalization
- binding materialization 和 wrapper generation
- Redis key family 和 projection staging
- protocol client 和 server handler
- lifecycle state machine 和 cleanup queue
- observability helper 和有界 metric label 策略
- test stub 和 hermetic harness

不要只因为文件长而拆分。只有当 reviewer 能命名一个行为并独立验证其合同时，拆分才有价值。

重复基础原语应由 shared helper 拥有。error message 格式化、random hex/prefixed id、env knob 解析、base64 byte 转换、request-id resolution 和平台 JSON response shaping 都应复用已有 helper。如果 helper 被镜像到测试 data URL stub，stub 必须 production-faithful，或直接 import shared test stub，不要在本地重写行为。

Control handler state 必须通过 `control/shared.js` accessor 流动。直接 `state.foo` 读取和 `const { foo } = state` 解构只属于 `control/shared.js` 和负责初始化它的 dispatcher。

## Workerd 边界

公开 tenant fetch、control/admin routing 和特权 runtime dispatch 必须保持分离：

- Gateway 公开流量进入 runtime loader socket。
- Scheduler 和 workflows dispatch 使用 runtime internal `:8088`。
- Control/auth 通过 system-runtime 运行，并拥有授权边界。
- D1 和 DO runtime 只暴露私有 internal service API。

不要通过 gateway 上的 tenant-visible path 保留来保护特权操作。应使用 socket/service 边界。

Hidden platform Fetcher binding 不能泄漏给用户代码。注入 internal Fetcher 的 runtime wrapper 必须在用户可见 `env` 中删除这些 binding，并避免 raw `export *` 路径暴露未包装 entrypoint。

## API 合同

平台 JSON error 使用：

```json
{ "error": "machine_code", "message": "human readable" }
```

Control、gateway、runtime 以及普通 D1/DO route error 应使用共享 JSON response helper，除非该模块拥有并记录了不同的 protocol envelope。Details 只能 additive，不能覆盖顶层 `error`、`message` 或 legacy `reason`。新 API 不应重新引入 `reason`；客户端合同应保持 `error` 和 `message`。如果 route 合法返回 result envelope、streaming body、`HEAD` response 或 WebSocket upgrade，而不是 JSON，应在 owning module 中记录这个例外。

Handler 不应在可以使用共享 helper 时手写 literal `{ error, message }` response body；reserved-field 和 content-type 规则应由 helper 拥有。D1 或 DO error mapper 这类 protocol-specific helper 必须有自己的测试和模块文档。

产品 success payload 使用 camelCase。日志使用 snake_case。Redis field 可以保留自己的 storage grammar，但新的公开 API 字段不应继承 Redis/log 命名。

Request id 传播前必须 sanitize 并限制长度。不要把原始错误文本、token id、namespace/worker/version、path 或 Redis key 放进 metric label。

## Redis 和状态

已有 shared key helper 时必须复用。新的 key family 如果跨模块使用，应加 style-contract 或 source-scan guard 同时检查 producer 和 consumer 字面量。

新增 Redis index 前要说明它是权威还是可修复。如果可修复，应记录权威记录和 stale cleanup 路径。

WATCH/MULTI 行为应由一个 owner 负责。不要把 preflight read 和 commit-time revalidation 拆散，除非测试能证明 watched key set。

Workerd I/O object 与创建它的 `IoContext` 绑定。共享 `RedisClient` 应保持 socket-per-call；相关命令应在一个 typed operation 内批处理，不要跨 invocation 保留 socket 或 request 创建的 Promise。只有当单个 invocation 或单个 long-lived owning task 明确需要在 WATCH/transaction 或 subscription lifecycle 内持有连接时，才使用 `RedisSession`。

不要向 application code 暴露 generic pipeline escape hatch。应在 Redis owner 处增加满足需求的最小 typed、bounded helper，并在那里校验 reply count、reply order 和领域解码。

## 测试

测试应保护真实合同：

- route grammar
- error shape
- binding exposure
- Redis key layout
- lifecycle blocker
- hidden Fetcher stripping
- internal socket ownership
- runtime 测试成本过高的 deployment/IaC drift

Style-contract 测试用于已知 drift pattern，regex 应足够窄，失败时要明确。source scan 如果刻意严格，或行为测试需要重型基础设施，应加简短注释。

Test stub 如果镜像 production helper，应共享或直接 import。不要在多个独立 stub 中手写复制 production 行为。

Source-scan guard 如果需要例外，例外应窄到合同所需的范围：优先用 `file:literal` 或 `file:function` allow，而不是整文件 allow-list。

## 验证

JS/workerd 改动的基础检查：

```bash
npm test
npm run typecheck:strict
node --test tests/unit/style-contracts.test.js
```

可执行行为变化需要跑定向集成：

- gateway routing、admin-host、WebSocket：gateway integration
- runtime loader、wrapper、binding 或 internal socket：相关 runtime/binding integration
- control/auth route 或 ACL：control/auth 和 CLI integration
- D1/DO facade 或 owner protocol：D1/DO integration
- queue/cron dispatch shape：scheduler/runtime integration
- workflows facade 或 dispatch protocol：workflows integration

纯文档改动可用 `git diff --check`、链接/路径检查；如果文档导航或 source-scan 规则变化，再跑 style-contract 测试。

## Refactor 纪律

JS refactor 遵循与 Rust service 和 sidecar 相同的 staged-review 纪律：

- 先定义一个可独立部署的边界
- 不把无关 cleanup 混入当前边界
- stage 完整 candidate 供 review
- feedback fix 在被确认前保持 unstaged
- 只有覆盖相关合同的检查通过后才 commit

如果结构变化同时改变行为，要明确写出，并运行覆盖该行为的集成测试。

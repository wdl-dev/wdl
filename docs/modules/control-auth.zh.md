# Control 和 Auth

## 目的

Control 是静态控制面 worker，负责 deploy、promote、lifecycle、secret、route、D1、R2、workflow、log-tail 和 auth-token API。Auth 是静态 JSRPC worker，负责校验和签发 scoped admin token。

## 当前实现

Control 跑在 system-runtime 的 `:8082`，通过 gateway admin-host 分支进入。它不是 workerLoader 动态加载的 worker。

主要文件：

- `control/index.js`：request dispatcher。
- `control/lib.js`：route parser 和共享 route 工具。
- `control/shared.js`：auth wrapper、JSON response、Redis singleton、publish helper。
- `control/handlers/*`：endpoint handlers。
- `control/routing.js`：promote/reconcile WATCH/MULTI 逻辑。
- `auth/index.js`、`auth/lib.js`、`shared/auth-roles.js`：token store、role table、authorization evaluation。

## 接口

- Admin-host HTTP API 使用 `x-admin-token`。
- Control 调用 `env.AUTH.verify({ token, action, ns, requestId })`。
- Auth 暴露 JSRPC 方法：verify、issue、list、revoke。
- CLI 使用 control URL；普通 deploy 不直接访问 Redis 或 AWS。

## Admin API Surface

所有 client-facing endpoint 都通过 control URL 访问；gateway 用 admin-host 分支匹配并转发这些请求，且请求必须携带 `X-Admin-Token`。没有单独 public/admin-facing port；客户端不直接调用内部 system-runtime `:8082` control socket。普通 JSON 错误是 `{ "error": "<machine-code>", "message": "<human text>" }`；额外字段只能 additive，客户端应按 `error` 分支。`message` 是安全的人类摘要，不是稳定 reason taxonomy；auth-specific reject reason 作为 `error` 暴露。

Worker lifecycle：

| Method | Path | 合同 |
|---|---|---|
| `GET` | `/ns/<ns>/workers` | 列出有 namespace-owned state 的 worker，包括 deploy-only、active 和 secret-only worker。 |
| `GET` | `/ns/<ns>/worker/<name>/versions` | 列出 retained versions 和 active status。 |
| `POST` | `/ns/<ns>/worker/<name>/deploy` | 从 shorthand code 或完整 module manifest 创建新的 immutable version；routes、crons、queue consumers、service refs、platform refs、assets、vars、bindings 和 `exports` 都是 version metadata。Python modules 和上游 experimental compatibility flags 会在 commit 前被拒绝。 |
| `POST` | `/ns/<ns>/worker/<name>/promote` | 通过 WATCH/MULTI routing path promote `{"version":"vN"}`。Host declaration 失败是 403；live pattern conflict 是 409；transaction contention 耗尽是 503。 |
| `DELETE` | `/ns/<ns>/worker/<name>/versions/<version>` | 在 active-route、service-ref、lifecycle 和 delete-lock blocker 全部通过后删除一个 retained non-active version。Referrer redaction 按 principal 决定。 |
| `POST` | `/ns/<ns>/worker/<name>/delete` | Whole-worker delete。`?dry_run=1` 只返回 computed impact 和 blockers，不写入。Redaction 与 single-version delete 一致。 |

Host、secret、data 和 auth 操作：

| Method | Path | 合同 |
|---|---|---|
| `GET` / `POST` | `/ns/<ns>/hosts` | 列出或 reconcile declared hosts。Reconcile 会 normalize host、拒绝 platform-domain host，并在移除仍有 live owned patterns 的 host 时返回 409。 |
| `POST` | `/reload` | Ops-only route resync：先从 `hosts:<ns>` 重建 declared-host gate，再发布 `routes:flush ""` 和 `patterns:invalidate "*"`，full-route channel 和 pattern channel 保持正交。 |
| `GET` | `/ns/<ns>/worker/<name>/secrets` | 只列出 worker-level secret keys；不存在读取 secret value 的 API。 |
| `PUT` / `DELETE` | `/ns/<ns>/worker/<name>/secrets/<KEY>` | 修改单个 worker-level secret。PUT 存储 `WDL-ENC:` envelope；active worker 会 bump 并 promote 来强制后续 cold-load 新 secrets。 |
| `GET` | `/ns/<ns>/secrets` | 只列出 namespace-level secret keys；不存在读取 secret value 的 API。 |
| `PUT` / `DELETE` | `/ns/<ns>/secrets/<KEY>` | 修改单个 namespace-level secret。不做 version bump；下一次 natural cold-load 生效。 |
| `GET` / `POST` / `DELETE` | `/ns/<ns>/d1/databases[/<databaseRef>]` | 列出、创建或删除 D1 database。Create 在 d1-runtime 初始化后把 provisional metadata 切 ready；delete 写 tombstone 并 best-effort 释放 owner lease。 |
| `POST` | `/ns/<ns>/d1/databases/<databaseRef>/query` | `wdl d1 execute` 使用的 operator SQL execute path。 |
| `GET` / `POST` | `/ns/<ns>/d1/databases/<databaseRef>/migrations[...]` | Migration list/status/apply。Apply 是 forward-only，并使用 advisory Redis UX lock；owner serialization 和 SQLite transaction 仍是正确性边界。 |
| `GET` | `/ns/<ns>/r2/buckets` | 列出 `r2/<ns>/` 下有对象的 virtual buckets；空 declared bucket 在第一次写入对象前不可见。 |
| `GET` | `/ns/<ns>/r2/buckets/<bucket>/objects` | 按 prefix/delimiter/limit/cursor 列对象。 |
| `HEAD` / `GET` / `DELETE` | `/ns/<ns>/r2/buckets/<bucket>/objects/<key>` | 读 metadata、流式读 bytes，或删除单个对象。`.` / `..` path segment 被拒；delete 是单次 idempotent S3 DELETE，不声明对象此前是否存在。Missing-object `HEAD` 按 HTTP `HEAD` 语义返回空 404；客户端在该路径上必须看 status，不应期待 JSON body。 |
| `GET` | `/ns/<ns>/logs/tail?worker=<name>[&worker=<name>...]` | SSE live-tail session。首次连接从 stream tail 开始。单 worker reconnect 可用 `Last-Event-ID` 或 `?since` resume；multi-worker session fresh-start。 |
| `GET` | `/whoami` | 校验当前 token，并只返回当前 authenticated principal、token id、request id、WDL platform version、minimum supported CLI version 和 public URL hints。这是 self-introspection，不是 token lifecycle 管理接口，绝不返回 token plaintext、hash、其它 token record 或 raw workerd version。 |
| `POST` / `GET` / `DELETE` | `/auth/tokens[...]` | Ops-only token issue/list/revoke。`kind="ops"` 被拒，因为 ops 是 bootstrap-only；token plaintext 只返回一次；`bootstrap` 被保护，必须通过 env update + redeploy 轮换。 |
| `POST` | `/auth/delegated-tokens` | `token-issuer` credential 使用的窄 delegated token issue。请求只指定 server-side template；目标 `kind`、生成的 namespace、label、expiry、active quota 和响应 metadata 都由 Auth 计算，而不是 Control 或 caller 计算。 |

## Control 操作模型

Control lifecycle 操作会拆开处理，确保每个关键 transition 只有一个权威入口：

- Deploy 解析支持的 Wrangler/JSONC 形状，校验 bindings 和 routes，通过 `worker:<ns>:<worker>:next_version` 分配下一个 immutable version，写入 bundle metadata/modules/assets，然后进入 explicit promotion 使用的同一条 promote 路径。分配 version 前，deploy 会按 workerd 64 MiB 上限估算最终 WorkerCode，包含 runtime/do-runtime 注入的 wrapper/client modules 和 workflow import rewrite。真正的 Redis WATCH commit 会在分配真实 version 并完成 metadata materialization（例如 D1 database id 和 workflow keys 解析）之后用 code-budget 和 headroomed `workerLoader` env-budget 做权威检查，然后才写入 version。
- Promote 是唯一 active-route flip。它会 WATCH 本次候选需要的 delete lock、bundle metadata、D1 ref、service-binding target ref、queue consumer key、host declaration 和 pattern key。EXEC 在一个受审计的 transition 中更新 active route、host 反向索引、cron/queue projection、lifecycle index 和 invalidation publication。
- Secret update/delete 在 worker 有 active route 时，会把 secret-store mutation stage 到 `bumpActiveAndPromote()` 内部，让 budget check、secret hash write、bundle copy 和 route flip 共享同一个 WATCH/MULTI transaction。如果没有 active route，则会在直接写 secret hash 前检查 retained versions 的预算；只有没有 retained versions 的 secret-only worker 会把第一次 load-time budget check 交给 deploy。Secret PUT 会先校验 plaintext 大小和形状，在进入 Redis mutation / WATCH retry loop 前加密成 `WDL-ENC:` envelope，并在重试间复用同一个 envelope。Runtime 因此看到新的 immutable version id，而不是原地可变的 secret。Secret DELETE 会先从 env 估算输入里移除目标字段，再解密剩余 secret hash，所以删除损坏的目标字段仍可成功；但剩余 namespace 或 worker secret 只要损坏就 fail closed，direct Redis repair 不是受支持的一致性路径。Namespace-secret mutation 会 WATCH 需要重新估算的 retained worker/version metadata；如果并发 metadata 变化持续使视图失效，control 返回 `namespace_secret_mutation_contention`。
- Version delete 和 whole-worker delete 都 fail-closed。提交 Redis lifecycle deletion 前，会先收集 active route、retained version、service ref、D1 ref、workflow lifecycle check、queue/cron projection 和 delete lock blocker。S3 object cleanup 只在 Redis commit 成功后 enqueue。
- Worker delete lock value 和 `s3cleanup:<id>` task id 必须由服务端生成随机值。`x-request-id` 只用于诊断，客户端或重试可能复用它；不能把它用作 lock token 或 cleanup-task id。
- Auth 不是一层约定俗成的 middleware。`parseControlRoute()` 分配 action，control 把 action 和 namespace 发给 auth，auth 用存储的 token record 对照 `shared/auth-roles.js` 评估权限。Dispatcher 代码不应自己从 URL prefix 推断权限。
- Delegated token issue 刻意不放进 direct `auth.token.*` lifecycle。`POST /auth/delegated-tokens` 使用 action `auth.delegated_token.issue`，因此 `/auth/tokens` issue/list/revoke 仍保持 strict ops-only；`token-issuer` credential 只能请求 Auth 按一个已配置 template materialize token。
- `/whoami` 是唯一 namespace-less non-ops action。它对任何有效 token 开放，因为只报告当前 token 自己的 principal、token id、request id 和 public diagnostics。`platformVersion` 是由 bundled workerd date version 派生的 WDL version，使用 `wdl.` namespace，例如 `workerd` `1.20260531.1` 会变成 `wdl.20260531.1`；它不是 project release tag，后者的末尾 counter 可以在同一个 workerd date 上为 WDL-only patch 递增。raw workerd version 不返回。`minCliVersion` 是最低支持的下游 CLI 版本。`urls.control` 是到达 control 的 public origin；当 ingress 提供单个 `x-forwarded-proto` 且值为 `http` 或 `https` 时，`/whoami` 会用该协议生成 `urls.control` 和 `urls.namespace`，否则回退到 control 看到的 request URL 协议。`urls.namespace` 只对 tenant namespace token 返回；`urls.assets` 只在 `ASSETS_CDN_BASE` 是安全的绝对 `http`/`https` URL 时返回，并且会去掉 query 和 fragment。它不能扩展成 token list、token lookup 或携带 secret 的 diagnostics。
- 只有 method、path length 和 verb 精确匹配 authorized shape 时，route shape 才带 `action`。缺 action 是有意行为：非 ops 会触发 auth unknown-action red line，ops 仍可到 dispatcher/handler 的 path 和 method validation，而不是被 auth 拦下。`/reload` 这类已知 top-level route 的 wrong-method case 会在 ops auth 通过后返回 `405 method_not_allowed`。

## Redis / Storage 合同

Control 拥有 DB 0 metadata：

- Bundles 和 versions。
- Active routes 和 host patterns。
- Worker lifecycle indexes。
- Secret hashes。
- D1/DO metadata 和 referrer indexes。
- Cron 和 queue consumer projections。
- Workflow definition key allocation（`wf:defs:*`），workflow instance state 在 DB 2。

Control 对 active-version flip、routing change、delete lock 和 lifecycle index update 使用 WATCH/MULTI/EXEC。Worker lifecycle index 是权威状态；handler 不应增加扫描 bundle state 的 fallback。

Auth 在 Redis 中存 token record，并按 `shared/auth-roles.js` 评估权限。

Key families：

| Key | Type | Owner | Authority | Cleanup/delete 语义 |
|---|---|---|---|---|
| `namespaces` | Set | Control | gateway/control listing 使用的 active namespace gate，也是 delegated-token 碰撞检测的 best-effort 信号。 | worker lifecycle state 变化时更新；whole-worker delete 在没有 active worker 时移除该 namespace。Namespace-scoped resources 可以比这个 membership 活得更久。 |
| `workers:<ns>` | Set | Control | worker lifecycle index。 | blocker 通过后，whole-worker delete 移除 member。 |
| `worker-versions:<ns>:<worker>` | ZSET | Control | retained version index。 | referrer check 通过后，version delete 移除 member。 |
| `worker:<ns>:<worker>:v:<n>` | Hash | Control | immutable bundle/version metadata 和 modules。 | 保留到 version/worker delete。 |
| `worker:<ns>:<worker>:next_version` | String counter | Control | logical worker name 的 monotonic next version number。 | whole-worker delete 后保留，保证 worker id 不复用。 |
| `routes:<ns>` | Hash | Control | active worker -> version route map。 | promote/delete 更新并 publish route invalidation。 |
| `hosts:<ns>` | Set | Control | namespace 声明的 host allow-list。 | Promote 检查 membership；host reconcile 更新声明集合。 |
| `declared-hosts` | Set | Control | gateway 对“至少被一个 namespace 声明过的 host”的全局 gate。 | Host reconcile 负责日常写入；`/reload` 从 `hosts:<ns>` 修复它。 |
| `host-declarations:<host>` | Set | Control | 当前声明这个 host 的 namespace。 | 防止一个 namespace 移除声明时清掉另一个 namespace 仍需要的全局 gate。 |
| `ns-hosts:<ns>` | Set | Control | namespace 的 active host 反向索引。 | promote/reconcile 在同一个 EXEC 内维护 SADD/SREM delta。 |
| `patterns:<host>` | Hash | Control | pattern-host route slots；value 是紧凑 `v2` tab-separated projection。 | reconcile/promote 更新并 publish pattern invalidation。 |
| `worker-version-referrers:<ns>:<worker>:<version>` | Set | Control | 可重建 service-binding referrer index。 | caller 仍引用该 version 时阻止 version delete。 |
| `worker-delete-lock:<ns>:<worker>` | String EX | Control | per-worker delete critical-section lock。 | 自动过期；execute delete 完成后释放。 |
| `secrets:<ns>`、`secrets:<ns>:<worker>` | Hash | Control | namespace 和 worker secret store；value 是 `WDL-ENC:` envelope。Control 写入前加密；redis-proxy 只在 `/runtime/load` 时解密。 | secret lifecycle 或 worker delete 删除。 |
| `queue:__system__:worker-delete-s3-cleanup:s` | DB 1 Stream | Control / s3-cleanup worker | best-effort post-commit object cleanup queue；逻辑队列名是 `worker-delete-s3-cleanup`。 | 只在 Redis delete commit 成功后 enqueue；enqueue 失败返回 `cleanup_queue_failed` warning。 |
| `auth:token:<tokenId>` | Hash | Auth | token record 权威记录。 | revoke/expiry 删除 active record 并写 tombstone 字段。 |
| `auth:hash:<sha256>` | String | Auth | plaintext-token hash -> token id lookup。 | revoke/expiry 删除。 |

Auth 子合同：

- `shared/auth-roles.js` 是 capability table；不要从 route 名字推断权限。
- Verify hot path 是 token hash lookup、token record shape validation，然后执行 `evaluateAccess({ action, ns, kind, principalNs })`。Record validation 必须证明 stored `kind` 存在于 `ROLES`，tenant-bound token 带 tenant namespace，platform-tier token 带 platform-tier reserved namespace，unbound ops token 不带 `ns`。
- Access red line 在 role narrowing 前按固定顺序执行：unknown role、reserved namespace、reserved tenant namespace name、unknown action、auth-token operation requires ops、system operation requires ops、namespace scope，然后 role action narrowing。
- `ops-observer` 是跨 namespace read-only，刻意不持 secret value、workflow payload、arbitrary SQL、R2 object head/body、token-list 和写权限。
- Bound role 必须和 stored `ns` 自洽：tenant role 绑定 tenant namespace，platform role 绑定 platform-tier reserved namespace，unbound ops role 不应存 `ns`。
- Reserved namespace 是 `shared/ns-pattern.js` 中的精确字面量：`__system__`、`__platform__` 和 `__community__`。当前只有 `__platform__` 在 `PLATFORM_TIER_RESERVED_NS` 中；`__system__` 是 system-runtime/control-plane reserved，`__community__` 已 reserved 但当前还不是 platform-tier role namespace。
- Reserved namespace check 同时存在于 route/auth red line 和 role scope check；两者不能互相替代。
- `issue` 可以签发 `ns`、`platform`、`platform-observer` 和 `ops-observer` token。`token-issuer` 也可由 ops direct issue，但必须提供 camelCase `issueTemplates`，且其中每个 id 都要指向已有 delegated issue template。Auth 把这个 allowlist 存成 Redis `issue_templates` JSON array string，并拒绝 public `issue_templates` 输入，避免 API shape 和 storage shape 混用。`ops` 只能来自 bootstrap。Token plaintext 只生成一次、展示一次，并且只以 SHA-256 hash 形式存储。
- 内置 delegated issue template 包括用于 workshop pool 的 `wdl-chat-ns-pool`，以及用于 hosted CLI live integration 短期 namespace 的 `wdl-cli-integration`。
- `token-issuer` 是 unbound role；除 `/whoami` self-introspection 外，它唯一 non-diagnostic action 是 `auth.delegated_token.issue`。Delegated issue 请求只接受 `{ template }`；caller-provided `kind`、`ns`、`label`、`expiresAt` 或 template allowlist 字段都会被拒。Auth 会重新读取 issuer token record，确认它仍 active 且 allowlist 包含该 template，再应用 Auth 代码内定义的 template registry，并写入带 `created_by`、`issue_template`、`issue_template_version` metadata 的目标 token。Active quota 基于 `created_by + issue_template + expires_at` 计算，是 live credential capacity guard，不是 environment quota。生成的 namespace 是写入 token record 的普通 tenant namespace 字符串。Auth 会先用 `auth:delegated-issue-lock:<issuerTokenId>:<templateId>` 串行化同一 issuer/template 的发放和 quota 计数，然后拒绝已经出现在 control 维护的 `namespaces` set 或任意已扫描 token record stored `ns` 中的 generated namespace candidate，不区分 token kind、issuer、template、是否过期或是否撤销。Quota 计算 fail-closed：同一 issuer/template 的 delegated token record 损坏会阻止新的 delegated issue，直到 storage contract violation 被修复。`GET /auth/tokens` 是 operator repair surface；单条 malformed `issue_templates` 会作为 invalid entry 列出，不会拖垮整个 list。V1 的 delegated namespace collision check 是 best-effort：unbound/full-plane token 仍能执行不会留下 auth-visible `ns` token record 的 namespace-scoped 写入，而 `namespaces` 只表示 active worker。常规 delegated namespace workflow 应使用 namespace-bound credential 做 namespace-scoped 写；未来的持久 namespace fact index 才是这个 residual risk 的完整修复。
- `expiresAt` 必须是严格 ISO-8601 UTC、毫秒精度，并通过真实日历 round trip 校验。过期 token 在过期后第一次 verify 时 lazy collect：删除 active hash index，并写入 `expired_at`；主动 revoke 使用 `revoked_at`。
- 保留 token id `bootstrap` 是 infra-managed ops token。`BOOTSTRAP_TOKEN` 在 auth cold start 时 upsert，`revoke("bootstrap")` 被禁止，轮换方式是更新环境变量并 redeploy。Verify 会缓存已 ensure 的 bootstrap hash，但 Redis 丢失后可以重新 ensure，因此 bootstrap token 能恢复被 flush 的 auth store。

## Ownership / 并发 / 失败语义

- `parseControlRoute()` 是唯一 URL-to-action parser。
- 如果 route shape 没有 action，非 ops 在 auth 处 fail closed；ops 可以到 dispatcher/handler 的 path 和 method validation；已知 top-level wrong-method route 返回 `405 method_not_allowed`。
- Control JSON error 形状是 `{ error, message }`；details 只能 additive。
- Details 可以增加字段，但不能覆盖 `error`、`message` 或 legacy `reason`。Auth reject reason 是 `error` machine code；日志可以把 `reason` 作为诊断上下文。
- Delegated issue 的 409 reason 有不同 retry 语义：`delegated_issue_busy` 表示 issuer/template lock 清除后可重试；`active_quota_exceeded` 在已有 delegated credential 过期或 revoke 前不应重试；`namespace_collision` 表示 Auth 已耗尽 configured candidate retry budget。
- Control 5xx response 使用 generic/safe message。Internal exception text、auth Redis diagnostic、backend message 和 provider error 应进入日志；除非 endpoint 明确拥有某个 diagnostic response field，否则不进入客户端 body。
- Deploy 在最终 WorkerCode 会碰撞 WDL 注入的 runtime/do-runtime 保留模块名或缺少必要 bundle metadata 时返回 `worker_code_invalid`，在最终 WorkerCode（包含 runtime/do-runtime 注入模块和生成的 workflow keys）超过 workerd 64 MiB dynamic code limit 时返回 `worker_code_too_large`。Deploy 和 secret mutation 在估算的 `workerLoader` env 超过 WDL headroomed 1 MiB budget 时返回 `worker_env_too_large`。`worker_env_too_large` details 包含 `namespace`、可选 `worker`、`env_bytes`、`max_env_bytes`、`upstream_max_env_bytes` 和 `headroom_bytes`。Deploy 阶段的逐版本检查还会包含 `version`。Secret mutation 重新估算既有 version 时，details 还会包含 `source_version` 和 `estimated_version`。`source_version` 是 operator 应检查、删除或 redeploy 的已存版本；`estimated_version` 是本次预算估算使用的 tag，在 worker-secret bump 路径中就是已分配的精确 bump version。
- Control 不直接调用 gateway。它写 Redis 并 publish invalidation message。
- Control 在进入 Redis mutation loop 前加密 secret PUT value。加密/provider 失败会返回 control error，不写 plaintext fallback。
- Worker delete 先 commit Redis lifecycle state；异步 S3 cleanup enqueue 是 best-effort，失败时返回 warning。
- `s3-cleanup` system worker 会把 cleanup task 持久化在 D1；row 存在后由 cron replay 负责重试。S3 失败使用分钟级 exponential backoff，最高 30 分钟；大前缀 cleanup 每完成一个 S3 List/Delete page 就 checkpoint continuation token，因此正常分页进度不会消耗 failure attempts，也不会在 scheduler timeout 后从头开始。每次 run 只处理一页，所以超大 prefix 会跨多个 cron 或 queue dispatch 排空，而不是让单次 scheduler dispatch 持续数分钟。
- Workflow lifecycle blocker 通过 workflows 检查；服务错误时 fail closed。
- AUTH JSRPC 错误或 Redis 爆炸属于控制面失败，映射为 503 fail closed，而不是 tenant-visible authorization fallback。

## 安全边界

- Auth role table 是 principal capability 的事实来源。
- Reserved namespace red line 在 route parsing 和 auth evaluation 中都执行。
- `ops` 是全平面权限。`ops-observer`、`ns`、`platform`、`platform-observer` 和 `token-issuer` 范围更窄。
- Platform double-pin 规则：platform role 要读跨 namespace 细节，必须同时满足 platform principal kind 和匹配的 platform-tier namespace。
- `x-admin-token` sanitizer 在 control 和 auth 间共享。
- Control 不携带 token-shaped 环境变量。唯一 server-side token env 是 auth 的 `BOOTSTRAP_TOKEN`；control 只通过 `AUTH` binding 调 auth。

## 可观测性

Control 和 auth 主要依赖结构化日志观测。它们不暴露公开 metrics socket。Request id 从 gateway -> control -> auth 传播，并出现在日志中。
Verify outcome 记录为 success、reject 或 error；5xx outcome 是 error log，4xx reject 是 warning log。

## 部署 / Rollout 注意事项

- 修改 bundle metadata shape 时，control 和 runtime 应一起滚。
- Control 和 gateway 必须保持 route invalidation channel 名称一致。
- Auth role 改动属于安全边界变更，应重点测试 reserved namespace 行为。

## 保护该模块的测试

- `tests/unit/control-lib.test.js`
- `tests/unit/control-routing.test.js`
- `tests/unit/control-delete-handler.test.js`
- `tests/unit/control-deploy-watch.test.js`
- `tests/unit/control-lifecycle-indexes.test.js`
- `tests/unit/auth-lib.test.js`
- `tests/unit/auth-index.test.js`
- `tests/integration/auth-worker.test.js`
- `tests/integration/auth-platform.test.js`
- `tests/integration/system-pool-auth.test.js`
- `tests/unit/style-contracts.test.js`

## 已知约束和非目标

- Control 不感知 gateway 拓扑。
- Auth token 是 bearer token；存储和 revoke 基于 Redis。
- Control API 是 admin/operator API，不是 tenant data-plane API。
- Control/runtime/auth/CLI 作为一个 release 交付。除非存在真实外部 rollout 要求，不要为 in-tree protocol response migration 增加假的双 shape fallback。

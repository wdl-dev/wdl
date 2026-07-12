# 安全模型

本文记录 WDL 当前依赖的安全边界。它不是每个部署的完整 formal threat model，但说明当前代码和基础设施假设。当前代码、测试和已部署的网络策略仍然是事实来源。

## 核心假设

- Tenant worker code 是不可信的。
- 私有 service mesh 内的平台服务是可信平台组件。Internal endpoint 除了 socket、网络、service 和 task-placement 边界外，还要求共享的 `WDL_INTERNAL_AUTH_TOKEN` 应用层 header。Health 和 metrics endpoint 保持未认证，供编排器和 scraper 做 liveness / metrics。
- 公开入口和 admin 入口是不同平面。Gateway 路由公开 tenant 流量；control/auth 授权控制面操作。
- 内网安全仍然是部署合同。共享 internal token 是 defense-in-depth 和 in-tree 平台 caller shape 认证，不替代 private port 不能暴露到 public ingress 的要求。
- Runtime escape 不应自动变成云凭证泄露。Tenant-running task 必须使用 least-privilege task role，不能拿到宽权限基础设施 credential。

## 信任区域

| 区域 | 示例 | 信任级别 | 主要边界 |
|---|---|---|---|
| 公开数据面 | Gateway public socket、tenant Worker URL | 不可信 client | Gateway routing + runtime worker isolation |
| Admin/control plane | Control URL / admin host、control worker、auth worker、CLI token API | 已认证 operator/tenant | `x-admin-token`、auth role table、namespace scope |
| Tenant runtime | user-runtime loaded workers | 不可信 tenant code | workerd isolate、wrapper-shaped `env`、public-only outbound |
| System runtime | system-runtime、control/auth/tail、`__system__` workers | 平台代码 | Reserved namespace + private+public outbound |
| 私有 service mesh | d1-runtime、do-runtime、workflows、scheduler、redis-proxy sidecar | 可信平台组件 | Service Connect / security group / internal socket + `WDL_INTERNAL_AUTH_TOKEN` |
| State stores | Valkey DB、EFS localDisk、S3-compatible storage | 平台拥有的数据服务 | Writer ownership、Redis DB split、secret envelope、storage credentials |
| Host/infra | ECS Fargate tasks、IAM roles、task metadata | Operator 控制 | IAM least privilege、ECS Exec policy |

## 公开入口和 Admin 入口

Gateway 不是 tenant 应用的授权层。它把公开 host/path 解析到 immutable worker version，并转发到 runtime loader socket。Tenant 应用需要公开应用鉴权时，应自己实现。

Admin-host 请求不同。Gateway 的 `ADMIN_HOST` 分支只负责转发到 control，并不授权。Control 把 route 解析成 action，然后让 auth 用 `shared/auth-roles.js` 校验 `x-admin-token`。

Reserved namespace 是精确字面量，不是宽泛的 `__*` 约定。当前集合是 `__system__`、`__platform__` 和 `__community__`。`__system__` 是 system-runtime namespace，也是唯一有狭窄 public system-route allowance 的 reserved namespace，细节见 gateway 模块。`__platform__` 是当前 platform binding 和 platform-scoped role 使用的 platform-tier namespace。`__community__` 预留给未来 community platform-tier 用途，但当前还不是 platform-tier role namespace。Reserved namespace 在公开 subdomain routing 中会先于 Redis route lookup 被拒绝。Reserved namespace 不是 tenant namespace：tenant-scoped `ns` token 必须绑定普通 tenant namespace；访问 reserved namespace 需要 `ops` 或 `shared/auth-roles.js` 允许的 platform role 形状。Tenant namespace 使用 DNS-label-compatible 文法：1-63 个小写字母/数字/hyphen，首尾必须是字母或数字。

## Tenant Runtime 隔离

WDL 使用 stock workerd，不 patch workerd。因此 runtime 隔离从 workerd isolate 边界开始，再叠加 WDL 的 wrapper 和网络规则：

- Tenant bundle 通过 `workerLoader` 作为 immutable worker version 加载。
- Runtime wrapper generation 构造 tenant-visible `env`；D1、DO、workflows 和 owner-network path 等 hidden platform Fetcher binding 留在 runtime 内，并在 tenant code 观察 `env` 前删除。
- user-runtime loaded worker 只拿 public-only outbound。Tenant `fetch()` 和 `cloudflare:sockets` 不应访问 platform-private address。
- system-runtime 的 `__system__` worker 刻意拥有 private+public outbound，因为它们是平台代码，不是 tenant code。
- 特权 runtime event 使用私有 `:8088` internal socket。Gateway 不应保留 `/_scheduled` 这样的 tenant-visible path；socket 边界才是安全边界。

## Internal Mesh 信任

很多 internal endpoint 是私有平台协议，不是公开 API：

- runtime `:8088` scheduled、queue 和 workflow dispatch
- d1-runtime owner、SQL、drain 和 renew path
- do-runtime invoke/connect/alarm/storage cleanup/drain/renew path
- do-runtime diagnostic probe path
- runtime `:8088` workflow run/notify dispatch path，由 workflows 调用
- workflows internal lifecycle、step 和 tick path
- redis-proxy sidecar 的 cold-load、KV、queue、logs 和 runtime support API

这些 endpoint 要求 `x-wdl-internal-auth` header，取值来自注入到每个平台注册服务 task 的共享 `WDL_INTERNAL_AUTH_TOKEN` secret。Tenant-originated forwarding path 会剥离这个 header；token 保留在 host-owned Durable Object proxy 和 host-side backend capability 中，不嵌入 generated tenant facade code，也不进入 tenant-visible `env`。

不要通过 gateway 或 internet-facing load balancer 暴露这些 endpoint。共享 internal token 只认证私有 mesh 内的 in-tree 平台 caller，不授权任意外部 caller。如果新 caller 不在可信 mesh 内，应设计显式认证和授权，而不是直接复用 internal protocol。

“私有 mesh 可信”不等于信任 tenant 输入。Runtime 和 stateful service 在协议边界仍要校验 worker id、namespace/worker grammar、owner header、generation fence、content type、request size 和 metadata shape。

## 控制面授权

Auth token 是 bearer token。Auth 在 Redis 中保存 token record 和 token hash；plaintext token 只在签发时展示一次。Role evaluation 集中在 `shared/auth-roles.js`。

关键 role 边界：

- `ops` 是全平面权限，由 bootstrap 管理。
- `ops-observer` 是跨 namespace read-only，但刻意没有 secret value、workflow payload、arbitrary SQL、R2 object body/head、token list 和写权限。
- `ns` role 绑定 tenant namespace。
- `platform` 和 `platform-observer` role 绑定 platform-tier reserved namespace。
- `token-issuer` 不绑定 namespace；除 `/whoami` self-introspection 外，它唯一 non-diagnostic action 是 `auth.delegated_token.issue`。它不能 direct issue token、list/revoke token 或访问 tenant resource。Delegated issue 返回短期 credential；namespace resource lifecycle 不属于 auth token lifecycle。
- Delegated namespace safety 假设常规 namespace-scoped 写操作使用 namespace-bound credential。Full-plane unbound credential 目前仍能执行 namespace-scoped 写，而且 active worker gate 清空后这些写可能不留下 auth-visible namespace fact；这是 V1 接受的 residual risk，直到存在持久 namespace fact index。
- Platform 跨 namespace 可见性必须同时满足 role kind 和绑定 namespace 的 platform-tier 规则；不要用 route-name check 替代。

Control handler 不应从 URL prefix 自己推断权限。应使用 `parseControlRoute()` action classification 和 auth verify。

Secret PUT handler 只在校验和加密期间看到 plaintext。Redis secret store 中保存的是 `WDL-ENC:` envelope；redis-proxy 只在服务 `/runtime/load` 时解密。Runtime cold-load 路径没有 steady-state plaintext fallback，因此 secret-envelope provider key 缺失或错误时，使用 secret 的 worker 会 fail closed。

## Binding 和状态安全

Binding 是 tenant capability 的主要 surface：

- KV 和 queue producer 使用 redis-proxy sidecar，并受 runtime-owned cap 约束。
- D1 和 DO facade 进入 stateful runtime，由后者拥有 single-writer lease 和 generation fence。
- Workflows facade 调 workflows；workflows 拥有 DB 2 state，并不信任 tenant body 中的 identity 字段。
- R2 在 runtime 使用平台 S3-compatible credential；tenant code 拿到 R2 binding，而不是 raw credential。
- ASSETS 只暴露 `env.ASSETS.url(path)` 生成 tokenized CDN URL；runtime 不暴露 assets 的 S3 credential 或 bytes。
- Service 和 platform binding 从 control metadata 与 ACL 解析；跨 namespace service call 需要 target-side authorization。
- Secret 只在 runtime `env` 构造时 materialize 成 plaintext。At rest 时，`secrets:<ns>` 和 `secrets:<ns>:<worker>` hash value 是 envelope ciphertext；Redis snapshot 和 debug read 不应暴露 tenant secret plaintext。

各模块文档列出了 Redis key family 的明确 owner。Index 通常是可修复 projection，不是 authority。新增第二 writer 或 fallback SCAN path 是安全和正确性变更，因为它可能绕过 lifecycle/delete fence。

## 基础设施边界

Terraform 在 ECS Fargate 上运行平台服务，包括执行 tenant 的 runtime task。Tenant-running task 的 cloud credential 暴露由 least-privilege task role、public-only workerd outbound binding 和 private mesh security group 约束。ECS Exec 只应在需要 platform operator access 的地方启用。

Service Connect 和 security group 是 internal mesh 边界的一部分。共享 `WDL_INTERNAL_AUTH_TOKEN` 当前值必须在 runtime、d1-runtime、do-runtime、scheduler、workflows 和 redis-proxy sidecar 间保持一致；可选的 `WDL_INTERNAL_AUTH_PREVIOUS_TOKEN` 只作为维护窗口轮换桥接被 receiver 接受。Caller 始终发送当前值，因此 token 轮换不是 rolling-safe，除非先暂停 traffic 或一起重启 private fleet。Scheduler 是 runtime internal dispatch 和 workflows tick 的 client；Workflows 会把 Durable Object alarm dispatch 到 do-runtime。Scheduler 和 workflows 都不是公开 service target。

## Tenant-facing 合同

Tenant-facing 行为记录在 standalone CLI guide。安全相关的 tenant 规则包括：

- Tenant token 默认只 scoped 到 namespace，除非 operator 签发更宽的 role。
- 默认公开 URL 形状是 namespace/worker path。
- Custom domain 和 Wrangler `routes` 由 operator 启用，不是通用 self-service。
- Cross-namespace service binding 需要 target-side authorization。
- Tenant socket/fetch 到 platform-private address 会在 runtime/workerd 网络边界被阻断。
- Live tail 是 best-effort debugging，不是 audit history。

## 可观测性和敏感数据

很多服务主要依赖日志做平台审计/调试。不要记录 plaintext token、token hash、secret value、raw platform credential 或无界 tenant payload。Metrics label 必须保持有界，不能包含 namespace、worker、version、path、Redis key、token id 或 raw error text。

Request id 可以跨服务传播，但传播前必须 sanitize 并限制长度。

## 非目标和缺口

- WDL 不宣称 Cloudflare account API parity。
- WDL 不实现 Cloudflare edge cache 语义，也不暴露 `caches.default`。
- Internal mesh protocol 目前没有在每个服务之间做 per-request mTLS；当前的应用层 mesh 控制是共享静态 token。
- 如果未来部署把 internal endpoint 放到无法假设 caller 是平台代码的网络上，复用这个模型前必须增加 caller-specific 显式认证。
- Tenant public application authentication 是 tenant 应用自己的责任。

## 保护该模型的测试和检查

- `tests/unit/style-contracts.test.js`：route channel、hidden Fetcher stripping、internal socket split、Fargate/task-role infrastructure guard、low-cardinality metrics 等 drift guard。
- `tests/unit/auth-lib.test.js`、`tests/unit/auth-index.test.js`、`tests/integration/auth-worker.test.js`、`tests/integration/auth-platform.test.js`：token 和 role 边界。
- `tests/unit/runtime-load.test.js`、`tests/unit/runtime-binding-surface.test.js`、`tests/integration/service-bindings.test.js`：wrapper 和 binding 暴露。
- `tests/unit/gateway-dispatch.test.js`、`tests/integration/gateway.test.js`、`tests/integration/routing-gateway.test.js`：route 和 reserved namespace 行为。
- `tests/integration/d1-*.test.js`、`tests/integration/durable-objects*.test.js` 和 workflows integration tests：stateful binding 的 owner/fence 行为。

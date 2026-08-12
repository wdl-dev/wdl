# Infra 和 Deployment

## 目的

Infra 文档描述 local compose、Terraform-managed environment 和 Kubernetes manifests 如何把平台服务映射到 Redis、local Envoy mesh、ECS Service Connect、EFS、S3/R2-compatible storage 和 release pipeline。

## 当前实现

当前有两套主要 infra family：

- `terraform/`：AWS ECS-shaped deployment environment。
- `deploy/kubernetes/`：Kubernetes-shaped deployment 的 Kustomize manifests。

Terraform 对应 AWS ECS-shaped deployment environment，由 developer/operator 从本地执行 `terraform plan/apply`。Kubernetes manifests 是 cluster-shaped deployment 的 release artifact，由目标 cluster 的 operator workflow rollout。

本地开发使用 `docker-compose.yml`，以及 `d1-multi`、`do-multi` 等 profile。

服务 family：

- workerd pools：gateway、user-runtime、system-runtime
- stateful runtimes：d1-runtime、do-runtime
- Rust services：scheduler、workflows、redis-proxy sidecars、supervisors
- data plane storage：Redis-compatible logical DBs、S3-compatible object buckets、
  D1/DO 的 EFS localDisk

Local compose 是开发便利环境，不是 production delivery contract。它用本地端口、Valkey、`s3mock` 和 Envoy mesh 启动同一组 service family；`d1-multi`、`do-multi` 等 profile 用于定向测试本地多副本行为。Production-shaped delivery path 是 Terraform 和 `deploy/kubernetes/` 下的 Kubernetes manifests。

这些交付路径用于生产运行，不只是本地演示。它们保留 runtime module 依赖的 service boundary、私有 mesh 假设、image contract、health/metrics endpoint，以及 ownership/failover 规则。Operator 仍然需要选择具体容量、managed Redis/Valkey durability、S3-compatible storage、EFS 或等价 localDisk persistence、ingress protection 和区域级 backup/restore 策略。

除 co-located sidecar 外，app service 有意保持一个可部署服务一个 container boundary：

- user-runtime 和 system-runtime co-locate `redis-proxy`。
- d1-runtime 和 do-runtime 通过 Rust `supervisor` 运行；supervisor 是 PID 1，负责本地 SIGTERM drain/renew，再停止 child workerd process。
- scheduler、workflows、gateway 和 stateless runtime pools 作为各自独立 service task/pod 运行。

## 接口

- 公开入口通过 ALB/gateway。Terraform-managed ALB ingress 可以在同一个 gateway service 上承载 admin host、platform wildcard、canonical site host 和额外 exact public host。
- Admin-host ingress 通过 gateway 的 `ADMIN_HOST` 分支进入 system-runtime control。
- Local Compose private service hop 使用 `envoy/envoy.yaml` 和编译进 `dist/workerd-configs` 的 `*-local.capnp` workerd config。
- ECS Service Connect 覆盖 runtime、D1、DO 和 workflows service target。
- 承载 WebSocket 的 Service Connect target 必须保留会传递 HTTP/1.1 Upgrade 的 HTTP 语义。不要在未重新验证 101 upgrade path 的情况下，把 gateway/runtime/DO traffic 悄悄降级成普通 L4 path。
- `redis-proxy` 是 runtime/DO task 旁边的本地 sidecar。
- AI provider traffic 从 user-runtime、system-runtime 和 do-runtime 的专用 public-only `AI_NETWORK` service 出站，不使用 system-runtime 同时允许 private/public 的默认 outbound service。
- Scheduler 作为 client 加入 Service Connect namespace，用于 runtime internal dispatch 和 workflows tick，Valkey/Redis 访问走自己的连接配置。Workflows 通过 `/internal/do/alarms/dispatch` 把 Durable Object alarm 投递给 do-runtime。
- GitHub release workflow 在 `wdl.*` tag push 时校验 `VERSION` 和 `CHANGELOG.md` 并发布 release image，也可手动运行同一条 build path 做 validation 或重新 publish。
- Terraform-managed infrastructure 通过 Terraform apply 管理。

GitHub Actions 是 pull request 和 `main` 的 JavaScript、Rust 和 hygiene 验证 gate。Docker Compose integration 套件需要 Docker Hub 和 Build Cloud credential，所以只在 trusted push 上运行。`.github/workflows/` 下的 release workflow 从 `wdl.*` tag push 校验 `VERSION` 和 `CHANGELOG.md`，再 build/push Docker Hub/GHCR image；手动 run 可验证或重新发布同一条 build path，它不是 PR validation gate。

Gateway 和 runtime 在不同环境中使用稳定的内部端口合同：

- `gateway :8080` 是唯一公开 HTTP/WebSocket socket。
- `runtime :8081` 是 gateway 使用的 loader socket。
- `runtime :8088` 是私有 socket，供 scheduler 和 workflows dispatch 使用。
- `system-runtime :8082` 在 gateway 的 admin-host 分支后承载 control。
- `d1-runtime :8787`、`do-runtime :8788` 和 `workflows :9120` 是 private mesh endpoint。
- `redis-proxy :7070` 是 co-located runtime/DO task 使用的本地 sidecar socket。

不要把 private mesh endpoint 暴露到 public ingress。K8s 和 ECS 交付都必须通过 Service Connect、ClusterIP Services、NetworkPolicy 或等价控制保持这个假设成立。

Private mesh caller 和 receiver 共享 `WDL_INTERNAL_AUTH_TOKEN` 作为当前 internal token。runtime、d1-runtime、do-runtime、scheduler、workflows 和 redis-proxy sidecar 之间的调用会携带 `x-wdl-internal-auth`。轮换期间 receiver 还会接受可选的 `WDL_INTERNAL_AUTH_PREVIOUS_TOKEN`；caller 始终只发送当前 token。Receiver 要求 auth header 恰好出现一次；两个 token 值都只能包含 visible ASCII byte，不能包含空白或逗号，确保 Fetch header normalization 不改变 token，并避免 header joining 把重复 header 伪装成单个已配置 token。Health 和 metrics endpoint 是唯一不要求该 token 的 service endpoint。这个 token 是平台 plumbing，不是 tenant binding：runtime wrapper code 会从 tenant-visible `env` 中剥离它，host-owned DO proxy 和 host-side backend capability 会在 DO forwarding 时添加它，并在 forwarding 前删除租户伪造的同名 header。

User-runtime、system-runtime 和 do-runtime 使用以下显式 AI runtime limit。默认值与 runtime hard maximum 由 `shared/ai-runtime-config.js` 统一拥有；部署 surface 显式重复默认值，并由测试与 owner 对齐：

| 变量 | 默认值 | 范围 |
| --- | ---: | --- |
| `AI_REQUEST_MAX_IN_FLIGHT` | `32` | 每个 runtime replica 的 model-list 与 HTTP body-admission 调用；non-streaming call 会一直持有到完成。 |
| `AI_STREAM_MAX_IN_FLIGHT` | `16` | 每个 runtime replica 在 request-pool body admission 后开放的 SSE stream。 |
| `AI_WS_MAX_SESSIONS` | `8` | 每个 runtime replica 的开放 provider WebSocket。 |
| `AI_REQUEST_BUDGET_MS` | `120000` | Model-list 与 HTTP setup deadline；non-streaming inference 持续受它约束到完成，SSE 在 response headers 后切换到 stream-duration bound。 |
| `AI_STREAM_IDLE_TIMEOUT_MS` | `30000` | SSE 连续没有 provider byte 的最大间隔。 |
| `AI_STREAM_MAX_DURATION_MS` | `300000` | SSE 绝对存活时间。 |
| `AI_WS_HANDSHAKE_BUDGET_MS` | `15000` | Provider WebSocket 握手 deadline。 |
| `AI_WS_IDLE_TIMEOUT_MS` | `120000` | WebSocket 连续没有 provider frame 的最大间隔。 |
| `AI_WS_MAX_DURATION_MS` | `1440000` | Adapter-specific clamp 前的 operator WebSocket lifetime cap。 |

这些是进程内 safety pool，不是 namespace quota 或计费控制。User-runtime 与 do-runtime 是独立服务，因此拥有彼此独立的 pool。代码拥有的 request、response、frame 和 aggregate byte cap 记录在 [AI 模块](ai.zh.md)。

AI client source 只由三个 base module owner 嵌入：`runtime/config-user.capnp`、`runtime/config-system.capnp` 和 `do-runtime/config.capnp`；local/evictable config 会 import 这些 worker graph。Cap'n Proto service list 不继承，因此每个具体 user-runtime、system-runtime 和 do-runtime config 都声明它绑定的 `ai-public-network` service。Production config 使用 public-only network service；local config 把同一 binding 路由到 deterministic fake provider Worker。

## Redis / Storage 合同

Logical DB 切分：

- DB 0：control-plane metadata。
- DB 1：data-plane KV、queue、log-tail streams。
- DB 2：workflows instance state。

Stateful storage：

- D1 localDisk 在 EFS 上。
- DO localDisk 使用独立 EFS。
- Assets/R2 位于 S3-compatible bucket namespace。
- Workflow payload refs 默认留在 DB 2 byte cap 内；应用如需大对象，应存外部引用。

## Ownership / 失败语义

- Scheduler 部署默认 1 个副本；当前 dispatch 路径具备多副本安全性，但 ECS rollout 仍使用 stop-before-start replacement，部署期间可能短暂停止调度。
- Workflows 是独立 Rust service。
- Scheduler 和 Workflows 默认最多 drain 25 秒 in-flight work；Compose、Kubernetes 和 Terraform ECS 均固定 30 秒 stop window，避免平台在默认应用 drain 结束前终止进程。部署如覆盖 `SCHEDULER_SHUTDOWN_DRAIN_MS` 或 `WORKFLOWS_SHUTDOWN_DRAIN_MS`，应同步评估对应的 stop window。
- Gateway、user-runtime 和 system-runtime 可以放在环境的 load balancing / service discovery 层后面水平扩展。本地 route cache、已加载 isolate 和 owner hint 都是优化，不是 authority。
- D1/DO 使用 owner lease、monotonic generation fence 和本地 drain/renew。超过 1 个 task 时需要稳定的 per-replica storage identity，并确保 supervisor drain/renew 只走私有本地入口，不能通过 service alias 打到其他 replica。
- 普通 D1/DO task 丢失会退回到 lease expiry，再由其他 replica takeover；graceful rollout 应优先走 supervisor drain，在 child workerd process 退出前释放 ownership。
- Terraform 在 ECS Fargate 上运行这套应用 stack 的服务。修改 capacity policy 时应记录哪些服务可以使用 `FARGATE_SPOT`；stateful runtime 和 singleton control loop 除非重新评估 interruption 语义，否则应保持 on-demand Fargate。
- 除了 Fargate task memory limit，D1 和 DO 的 workerd container 还会设置显式 container memory hard limit。DO 还会给本地 redis-proxy sidecar 保留内存。
- do-runtime 的 `DO_PREVENT_EVICTION` 默认是 `true`，会让 host actor resident，避免当前 workerd 的 actor eviction 中断在途 hibernatable WebSocket 操作。显式 `false` 会为已经验证的 workload 启用 eviction；它不能替代 container memory hard limit。
- Terraform Fargate service 在服务能容忍 overlapping capacity 时应使用 rolling replacement。D1/DO 使用 sequential replacement；scheduler 作为 singleton control loop 保持 stop-before-start。D1/DO 和 scheduler 都关闭 Availability Zone rebalancing，让 replacement 遵循各自显式 deployment strategy。

## 安全边界

- user-runtime loaded worker outbound 是 public-only。
- system-runtime 是刻意放宽的特权环境。
- Runtime internal `:8088`、d1-runtime `:8787`、do-runtime `:8788`、workflows `:9120` 和 Redis 都是 private mesh services。Private service call 还必须携带带有共享 `WDL_INTERNAL_AUTH_TOKEN` 的 `x-wdl-internal-auth`。
- Kubernetes NetworkPolicy 使用按组件区分的 caller 集合。ECS 刻意让 user-runtime、system-runtime、D1 和 DO task 共用一组 runtime security group，因此其网络规则粒度更粗，不要求与 Kubernetes caller 矩阵相同。
- Tenant-running runtime task 使用 least-privilege ECS task role、public-only workerd outbound binding 和 private mesh security group 作为 cloud credential 与 network 边界。ECS Exec 只应在需要 operator access 的地方启用。

## 可观测性

- 平台服务输出结构化日志。
- Gateway、user-runtime、system-runtime、d1-runtime、do-runtime、scheduler、workflows 和 redis-proxy 按配置暴露 Prometheus metrics；各服务 endpoint path 不完全一致，详见 `log-tail-observability.zh.md`。
- Terraform 默认启用 ECS Container Insights enhanced observability。这属于 AWS 基础设施遥测，提供 cluster、service、task 和 container 级健康与资源指标；它和 WDL 自身的 Prometheus metrics、bounded-label logging contract 是两层东西。
- Terraform 默认项之外的 CloudWatch/EFK log ingestion 由部署配置决定。

## 部署 / Rollout 注意事项

跨 tier protocol 变化使用一套依赖驱动流程，而不是维护永久 service 列表：

1. 先滚同时接受当前与下一 shape 的 reader/receiver。
2. 如果新旧 writer 混部可能提交不兼容状态，在 rolling writer tier 前暂停受影响 mutation surface。
3. 再滚会发出新 shape 的 writer/caller，等待旧 writer 完全 drain 后恢复暂停的 surface。

每个版本涉及的具体 service 与额外 gate 写在该版本 CHANGELOG 中；合同没有变化的 service 不会因此获得固定 rollout 顺序。

Internal auth 轮换采用双读单写，但它不是 rolling-safe 协议：caller 始终只发送 `WDL_INTERNAL_AUTH_TOKEN`，receiver 接受当前值和可选 previous 值。应在维护窗口内轮换，或先暂停 scheduler/workflows traffic。把旧值配置为 `WDL_INTERNAL_AUTH_PREVIOUS_TOKEN`、新值配置为 `WDL_INTERNAL_AUTH_TOKEN`，一起重启/滚动所有 private service；确认全量收敛后，再清空 `WDL_INTERNAL_AUTH_PREVIOUS_TOKEN` 并第二次滚动。

WDL 通常不保证 binary、runtime、配置、schema 或 storage 降级。以下内容只列出已知风险和 best-effort 恢复步骤，不是完整或有保证的 rollback 流程。

尝试降级 workerd 时，应确认所有 retained Dynamic Worker version 的 `compatibility_date` 不晚于目标 binary 支持的最大日期。恢复流量前，应重新部署或删除不兼容的 retained version；runtime health check 不会 cold-load 每个 retained bundle。

workerd 2026-07-17 的 stateful-runtime 升级相对 2026-07-01 是 forward-only。任何 2026-07-03 或更高版本首次启动后，都会在各 localDisk `metadata.sqlite` 的 native alarm scheduler `_cf_ALARM` 表中增加 `actor_name` 列；2026-07-01 随后无法用该 schema 启动。前向升级不需要 operator 操作。尝试把 D1 或 DO best-effort 降回 2026-07-01 时：

1. 停止共享相关 localDisk volume 的全部 D1 和 DO runtime。
2. 只删除 `/data/d1/wdl-d1-storage-v1/` 和 `/data/do/wdl-do-host-v1/` 下的 `metadata.sqlite`、`metadata.sqlite-wal`、`metadata.sqlite-shm`。
3. 启动 2026-07-01 runtime，确认健康后再恢复流量。

这些文件只承载 workerd native alarm scheduler metadata，而 WDL 不使用该 scheduler；WDL DO alarm 由 Workflows 承载。不要删除 per-actor `*.sqlite` database。
这项清理只恢复进程启动，不保证 per-actor 数据向后兼容。workerd 2026-07-17 可以通过 `ctx.storage.put()` 持久化 `Blob`，而 2026-07-01 无法反序列化。降级前应把这些值重写成 2026-07-01 兼容的类型或删除；否则旧 runtime 无法读取受影响的 DO storage value。

Terraform test 环境优先用 Terraform-managed change，不要用手动 rolling 替代，除非是在明确 debug。

## 保护该模块的测试

- Operator-driven checks：Terraform plan review 和 Kubernetes manifest review。
- `npm run test:integration`
- `tests/unit/style-contracts.test.js`：local compose Envoy mesh 形态、D1/DO test-hook IaC gate、DO residency 默认值，以及 Fargate-only Terraform launch contract。
- `tests/integration/durable-objects-eviction.test.js`：resident 默认值与显式 eviction 选择、actor 重建、owner 往返后的 task-local session-policy fence、SQLite 连续性，以及静默 hibernating WebSocket 连续性。
- `tests/integration/ai-binding.test.js`：provider/credential 生命周期、runtime 与 DO facade、public-only HTTP/SSE/WebSocket forwarding、SDK compatibility，以及 caller teardown 后的 watchdog cleanup。
- 目标 deployed environment rolling 后的 smoke tests。

## 已知约束和非目标

- 成本/capacity 是运维决策，不应隐藏在应用文档里。

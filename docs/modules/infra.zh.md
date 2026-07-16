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

runtime internal protocol 变化时的常见顺序：

1. 按需滚 workerd pools：gateway、user-runtime、system-runtime。
2. 如果 scheduler 调用新的 runtime internal path，再滚 scheduler。
3. 如果 workflow runtime protocol 改变，再滚 workflows。
4. 只有 D1/DO transport/config 改变时才滚 D1/DO。

方向很重要：

- 如果 workflows 会 dispatch 新的 runtime internal path 或 body shape，先滚 runtime，再滚 workflows。
- 如果 runtime/control/do-runtime 会调用新的 workflows API shape，先滚 workflows，再滚调用方。
- 如果 workflows 和 runtime 同时改变协议，先部署新增 endpoint 或 body shape 的一侧，再部署调用它的一侧。

Internal auth 轮换采用双读单写，但它不是 rolling-safe 协议：caller 始终只发送 `WDL_INTERNAL_AUTH_TOKEN`，receiver 接受当前值和可选 previous 值。应在维护窗口内轮换，或先暂停 scheduler/workflows traffic。把旧值配置为 `WDL_INTERNAL_AUTH_PREVIOUS_TOKEN`、新值配置为 `WDL_INTERNAL_AUTH_TOKEN`，一起重启/滚动所有 private service；确认全量收敛后，再清空 `WDL_INTERNAL_AUTH_PREVIOUS_TOKEN` 并第二次滚动。

Terraform test 环境优先用 Terraform-managed change，不要用手动 rolling 替代，除非是在明确 debug。

## 保护该模块的测试

- Operator-driven checks：Terraform plan review 和 Kubernetes manifest review。
- `npm run test:integration`
- `tests/unit/style-contracts.test.js`：local compose Envoy mesh 形态、D1/DO test-hook IaC gate、Fargate-only Terraform launch contract。
- 目标 deployed environment rolling 后的 smoke tests。

## 已知约束和非目标

- 成本/capacity 是运维决策，不应隐藏在应用文档里。

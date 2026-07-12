# Kubernetes 部署

这个目录包含 WDL 的第一版 Kubernetes 交付形态。它面向 greenfield 安装，并要求首次 apply 后就具备多副本准备状态：D1 和 DO 使用 StatefulSet 运行，具备稳定的逐 Pod owner endpoint，并共享 ReadWriteMany localDisk 存储。

这里的 Kubernetes 只负责应用层。托管 Valkey、对象存储、负载均衡器、DNS、证书和持久卷存储类等云资源，应由 Terraform、Pulumi、Crossplane 或云厂商原生工具提供。

## 目录结构

- `base/` 定义可复用的 Deployment、StatefulSet、Service 和 PVC。
- `components/metadata-guard/` 统一维护云 metadata egress policy。
- `overlays/local/` 固定本地镜像、本地开发 secret、namespace 和本地存储默认值。
- `overlays/local-ingress/` 为本地主机名增加 nginx Ingress 资源。
- `overlays/local-metadata-guard/` 应用本地 overlay，并额外加入共享的 metadata-guard component。
- `overlays/local-ingress-metadata-guard/` 应用本地 ingress overlay，并额外加入同一个 component。
- `storage/local-nfs/` 包含一个本地 ReadWriteMany provisioner 的 Helm values，可在开发集群中模拟云厂商 RWX 存储。

base 使用 production-style 镜像契约：workerd 服务默认使用 `docker.io/getwdl/wdl-workerd:latest`，Rust sidecar 和 Rust service 默认使用 `docker.io/getwdl/wdl-rust:latest`。本地 overlay 会把这些 public image name 映射到 Compose 集成测试本地构建的 `wdl-workerd:dev` 和 `wdl-rust:dev` tag。

所有 private WDL service 都从共享的 `wdl-secrets` Secret 读取 `WDL_INTERNAL_AUTH_TOKEN`，并在 internal mesh call 上以 `x-wdl-internal-auth` 发送。本地 overlay 会用 `local-internal-auth-token` 生成 `wdl-secrets`；生产 overlay 必须提供自己的值。轮换时使用可选的 `WDL_INTERNAL_AUTH_PREVIOUS_TOKEN`，但协议不是 rolling-safe：caller 始终发送 current，receiver 接受 current 和可选 previous。应在维护窗口内轮换，或先暂停 scheduler/workflows traffic；设置 previous=旧值/current=新值后，一起重启 private fleet，收敛后清空 previous。

workerd 镜像使用非 local 的编译后配置（`gateway.bin`、`user-runtime.bin`、`system-runtime.bin`、`do-runtime.bin`）。这些配置直接使用 Kubernetes Service DNS，因此这套栈不需要本地 Compose 的 Envoy sidecar。

base 还会安装只限制 ingress 的 NetworkPolicy。Kubernetes ClusterIP Service 本身不是隔离边界：如果没有 NetworkPolicy，同 namespace 中的任意 Pod 通常都能访问 Valkey 和 runtime internal socket。因此 baseline 默认拒绝 ingress，并只打开预期 WDL 组件之间的端口。它暂时没有默认拒绝 egress，因为生产 overlay 必须自行决定 DNS、对象存储、托管 Valkey 和外部 admin access 如何访问。

如果本地或 staging 集群只需要最小的云 metadata 防护、但暂时不做完整生产 egress allowlist，可以用 `overlays/local-metadata-guard/` 代替 `overlays/local/`，或用 `overlays/local-ingress-metadata-guard/` 代替 `overlays/local-ingress/`。这些 overlay 会为 WDL Pod 加一条 egress NetworkPolicy：允许普通 IPv4 出站，但排除常见 metadata endpoint `169.254.169.254/32` 和 `169.254.170.2/32`。这不是完整生产 egress 策略；生产 overlay 仍需要按具体云厂商决定 DNS、对象存储、托管 Valkey、admin endpoint 和 IPv6 metadata 的访问规则。

## Stateful Runtime 边界

D1 和 DO 有两类 Service surface：

- `d1-runtime` 和 `do-runtime` 是普通 ClusterIP first-hop router Service，由编译后的 workerd 配置使用。workerd 会连接这些稳定名字；它不应把 headless multi-A DNS 当负载均衡器消费。
- `d1-runtime-headless` 和 `do-runtime-headless` 是 owner-endpoint Service。Owner record 和 owner hint 会保存逐 Pod 名字，例如 `d1-runtime-0.d1-runtime-headless:8787`。

D1 和 DO owner record 保存的是拥有某个 SQLite-backed localDisk shard 的精确 runtime Pod endpoint。因此 Kubernetes manifests 使用：

- `d1-runtime` 和 `do-runtime` 使用 StatefulSet；
- first-hop runtime routing 使用普通 ClusterIP Service；
- 稳定逐 Pod owner DNS 使用独立 headless Service；
- `D1_TASK_ID` / `DO_TASK_ID` 来自 Pod 名；
- `D1_TASK_ENDPOINT` / `DO_TASK_ENDPOINT` 设为 `$(POD_NAME).<headless-service>:<port>`；
- `/data/d1` 和 `/data/do` 使用共享 ReadWriteMany PVC。

不要把负载均衡 Service DNS 写进 owner record，也不要用逐 Pod RWO PVC 替换共享 RWX PVC。负载均衡 owner endpoint 会把 takeover、probe 和 direct-owner traffic 路由到错误 Pod；逐 Pod PVC 会在 ownership 改变后打开不同 SQLite 文件。

大多数本地 Kubernetes 发行版默认只提供 RWO storage class，而 WDL 的 D1 和 DO localDisk 数据需要共享 RWX 存储。因此本地 overlay 期望存在一个名为 `wdl-nfs-rwx` 的 RWX class。在 Docker Desktop、kind 或其他没有现成 RWX 存储的本地集群上，先安装本地 NFS provisioner 再 apply WDL 栈：

```bash
helm repo add nfs-ganesha-server-and-external-provisioner \
  https://kubernetes-sigs.github.io/nfs-ganesha-server-and-external-provisioner/
helm repo update nfs-ganesha-server-and-external-provisioner
helm upgrade --install wdl-nfs \
  nfs-ganesha-server-and-external-provisioner/nfs-server-provisioner \
  --namespace wdl-storage \
  --create-namespace \
  -f deploy/kubernetes/storage/local-nfs/values.yaml
kubectl -n wdl-storage rollout status statefulset/wdl-nfs-nfs-server-provisioner
kubectl get storageclass wdl-nfs-rwx
```

这个 provisioner 只是共享文件存储的本地模拟，不是生产存储建议。生产 overlay 应改用目标平台提供的云厂商原生 RWX driver，例如 AWS EFS CSI、Aliyun NAS CSI，或其他 Kubernetes CSI driver。

## 本地 Greenfield 路径

这是推荐的本地 smoke 路径。它已在 Docker Desktop Kubernetes 上验证，但步骤本身只依赖标准 Kubernetes：任何具备 `kubectl`、可用镜像来源、RWX storage class 和 ingress controller 的集群，都可以按同一形态跑。

1. 启动或连接到一个本地 Kubernetes 集群。
2. 按上一节安装本地 NFS provisioner；如果你的集群已经提供名为 `wdl-nfs-rwx` 的 RWX storage class，可以跳过。
3. 构建 Compose 集成测试使用的同一组本地镜像：

   ```bash
   docker build -f Dockerfile.workerd -t wdl-workerd:dev .
   docker build -f Dockerfile.rust -t wdl-rust:dev .
   ```

   如果本地集群没有和 Kubernetes node 共享 Docker daemon，需要把这些 tag 导入或推送到每个 node 都能解析的位置。Docker Desktop 的多 node backend 使用 `kindest/node` 容器，导入本地镜像的方式如下：

   ```bash
   docker save -o /tmp/wdl-k8s-images.tar wdl-workerd:dev wdl-rust:dev
   for node in $(docker ps --format '{{.Names}}' | grep '^desktop-'); do
     docker exec -i "$node" ctr -n k8s.io images import - < /tmp/wdl-k8s-images.tar
   done
   ```

4. 如果集群还没有 nginx Ingress controller，先安装一个。Kubernetes 1.35 应使用 ingress-nginx controller v1.15.x 或更新的 1.15 support line。本地 overlay 已使用 controller v1.15.1 验证：

   ```bash
   kubectl apply -f \
     https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.15.1/deploy/static/provider/kind/deploy.yaml
   kubectl -n ingress-nginx rollout status deploy/ingress-nginx-controller --timeout=240s
   ```

5. 添加本地 smoke 名字的 hosts：

   ```text
   127.0.0.1 admin.test
   127.0.0.1 demo.workers.local
   127.0.0.1 s3mock.local
   ```

6. 应用 ingress overlay。它会包含本地 base，并把 `ASSETS_CDN_BASE` patch 为 `http://s3mock.local/wdl-assets`：

   ```bash
   kubectl apply -k deploy/kubernetes/overlays/local-ingress
   ```

7. 等待所有 WDL workload ready：

   ```bash
   kubectl -n wdl-local rollout status deploy/gateway
   kubectl -n wdl-local rollout status deploy/user-runtime
   kubectl -n wdl-local rollout status deploy/system-runtime
   kubectl -n wdl-local rollout status deploy/redis
   kubectl -n wdl-local rollout status deploy/s3mock
   kubectl -n wdl-local rollout status deploy/assets-proxy
   kubectl -n wdl-local rollout status deploy/scheduler
   kubectl -n wdl-local rollout status deploy/workflows
   kubectl -n wdl-local rollout status statefulset/d1-runtime
   kubectl -n wdl-local rollout status statefulset/do-runtime
   ```

8. 检查 gateway 和 CLI 路径：

   ```bash
   curl http://admin.test/healthz
   CONTROL_URL=http://admin.test \
   ADMIN_TOKEN=local-dev-token \
   WDL_NS=demo \
   wdl workers
   ```

本地 admin host 是 `admin.test`，本地 admin token 是 `local-dev-token`。

## 本地 Smoke 检查

greenfield 栈 ready 后，建议通过 Ingress hostnames 跑几条租户级检查。这些检查比 Pod Ready 更能说明业务链路完整。

部署并检查 ASSETS：

```bash
CONTROL_URL=http://admin.test \
ADMIN_TOKEN=local-dev-token \
WDL_NS=demo \
wdl deploy test-workers/pages-assets
curl http://demo.workers.local/pages-assets/
```

HTML 中应包含 `http://s3mock.local/wdl-assets/...` 下的 URL。再直接访问其中一个 URL，确认对象存储路径也能通过 Ingress 访问。

随后 smoke stateful runtimes 和 workflows：

```bash
CONTROL_URL=http://admin.test \
ADMIN_TOKEN=local-dev-token \
WDL_NS=demo \
wdl d1 create compat-main
CONTROL_URL=http://admin.test ADMIN_TOKEN=local-dev-token WDL_NS=demo \
wdl deploy test-workers/d1-compat
CONTROL_URL=http://admin.test ADMIN_TOKEN=local-dev-token WDL_NS=demo \
wdl deploy examples/do-demo
CONTROL_URL=http://admin.test ADMIN_TOKEN=local-dev-token WDL_NS=demo \
wdl deploy examples/workflows-demo
curl 'http://demo.workers.local/d1-compat/?op=raw'
curl 'http://demo.workers.local/do-demo/hit?room=greenfield'
curl 'http://demo.workers.local/workflows-demo/start?id=k8s-smoke&mode=parallel&steps=1'
sleep 1
curl 'http://demo.workers.local/workflows-demo/status?id=k8s-smoke&steps=1'
```

Tenant HTTP 请求应使用你加到 `/etc/hosts` 的域名，例如 `http://demo.workers.local/...`，而不是显式 `Host` header。这样更接近浏览器行为，也能覆盖 asset URL 使用的同一条 ingress 路径。

## Port-Forward 备选路径

如果不想安装 Ingress controller，可以应用 local overlay 并转发面向 host 的端口：

```bash
kubectl apply -k deploy/kubernetes/overlays/local
kubectl -n wdl-local rollout status deploy/gateway
kubectl -n wdl-local rollout status deploy/user-runtime
kubectl -n wdl-local rollout status deploy/system-runtime
kubectl -n wdl-local rollout status deploy/redis
kubectl -n wdl-local rollout status deploy/s3mock
kubectl -n wdl-local rollout status deploy/scheduler
kubectl -n wdl-local rollout status deploy/workflows
kubectl -n wdl-local rollout status statefulset/d1-runtime
kubectl -n wdl-local rollout status statefulset/do-runtime
kubectl -n wdl-local port-forward svc/gateway 30080:8080
kubectl -n wdl-local port-forward svc/s3mock 30900:9090
```

然后使用：

```bash
curl http://localhost:30080/healthz
CONTROL_URL=http://admin.test:30080 \
CONTROL_CONNECT_HOST=127.0.0.1 \
ADMIN_TOKEN=local-dev-token \
WDL_NS=demo \
wdl workers
```

port-forward 路径下的 Tenant HTTP 请求通常需要显式 `Host` header，除非你为 `*.workers.local` 添加了本地 wildcard DNS。

## 本地端口

- Gateway：`localhost:30080`
- s3mock：`localhost:30900`

这些是 port-forward 出来的 host 端口，不是 Kubernetes NodePort。Docker Desktop 的多 node `kindest/node` backend 不一定会把 NodePort 发布回 host。本地 overlay 默认把 `ASSETS_CDN_BASE` 设为 `http://localhost:30900/wdl-assets`，因此只要 s3mock port-forward 正在运行，返回给浏览器的 asset URL 就能从 host 访问。

## 本地 Ingress

如果已经安装 Ingress controller，可以直接应用 ingress overlay：

```bash
kubectl apply -k deploy/kubernetes/overlays/local-ingress
kubectl -n wdl-local rollout status statefulset/d1-runtime
kubectl -n wdl-local rollout status statefulset/do-runtime
```

本地 ingress overlay 期望存在 `nginx` IngressClass，并路由：

- `admin.test` 到 `gateway:8080`；
- `*.workers.local` 到 `gateway:8080`；
- `s3mock.local/wdl-assets/assets/...` 到只读 `assets-proxy`；proxy 只把不带 query 的 asset GET/HEAD 请求转发到 `s3mock:9090`。

这个 overlay 会把 `assets-proxy` 的 NetworkPolicy 只开放给 `ingress-nginx` namespace 中的 ingress-nginx Pod，并且只把 s3mock 开给 WDL runtime Pods 和这个 proxy。它不会通过 Ingress 暴露完整 s3mock S3 API。如果你的 controller 使用不同 namespace 或 label，请和 IngressClass 一起 patch `overlays/local-ingress/s3mock-network-policy.yaml`。

如果你没有在 greenfield 路径里添加 hosts，这里也需要为测试名添加 hosts：

```text
127.0.0.1 admin.test
127.0.0.1 demo.workers.local
127.0.0.1 s3mock.local
```

然后使用：

```bash
curl -H 'Host: admin.test' http://127.0.0.1/healthz
CONTROL_URL=http://admin.test \
CONTROL_CONNECT_HOST=127.0.0.1 \
ADMIN_TOKEN=local-dev-token \
WDL_NS=demo \
wdl workers
```

这个 overlay 会把 `ASSETS_CDN_BASE` patch 为 `http://s3mock.local/wdl-assets`。该 host 只是 asset read facade，不会转发 S3 query 操作；需要直接调 s3mock API debug 时，请使用本地 port-forward 路径。

## 当前边界

这是 local-first deployment baseline，不是完整生产 chart。生产化仍需做云厂商相关决策：

- 托管 Valkey 或 StatefulSet 策略；
- 对象存储 provider 和 bucket bootstrap；
- D1/DO localDisk 数据使用的云厂商原生 RWX durable storage class 和备份策略；
- ingress、TLS、DNS 和 CDN 策略；
- secret-envelope provider 选择和 key rotation 流程；
- PodDisruptionBudget、topology spread、资源规格和 autoscaling；
- Pod security context（`runAsNonRoot`、drop capabilities、尽可能 read-only root filesystem）；
- egress NetworkPolicy，以及在云厂商没有提供等价隔离时启用 Redis/Valkey authentication/TLS。

除非项目明确采用 Crossplane 之类的 operator，否则不要把云资源创建放进这棵 Kustomize tree。

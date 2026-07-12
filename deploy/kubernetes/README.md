# Kubernetes Deployment

This directory contains the first Kubernetes delivery shape for WDL. It targets
greenfield installs that are multi-replica ready from the first apply: D1 and DO
run as StatefulSets with stable per-Pod owner endpoints and shared
ReadWriteMany localDisk storage.

Kubernetes owns only the application layer here. Cloud resources such as managed
Valkey, object storage, load balancers, DNS, certificates, and persistent volume
classes should be provisioned by Terraform, Pulumi, Crossplane, or the cloud
provider's native tooling.

## Layout

- `base/` defines reusable Deployments, StatefulSets, Services, and PVCs.
- `components/metadata-guard/` owns the shared cloud-metadata egress policy.
- `overlays/local/` pins local images, local development secrets, a namespace,
  and local storage defaults.
- `overlays/local-ingress/` adds nginx Ingress resources for local hostnames.
- `overlays/local-metadata-guard/` applies the local overlay plus the shared
  metadata-guard component.
- `overlays/local-ingress-metadata-guard/` applies the local ingress overlay
  plus the same component.
- `storage/local-nfs/` contains Helm values for a local ReadWriteMany
  provisioner that can simulate provider RWX storage on a development cluster.

The base uses the production-style image contract: workerd services default to
`docker.io/getwdl/wdl-workerd:latest`, and Rust sidecars/default Rust services
default to `docker.io/getwdl/wdl-rust:latest`. The local overlay maps those
public image names to the same `wdl-workerd:dev` and `wdl-rust:dev` tags that
Compose integration tests build locally.

All private WDL services read `WDL_INTERNAL_AUTH_TOKEN` from the shared
`wdl-secrets` Secret and send it as `x-wdl-internal-auth` on internal mesh
calls. The local overlay generates `wdl-secrets` with `local-internal-auth-token`;
production overlays must provide their own value. Rotation uses optional
`WDL_INTERNAL_AUTH_PREVIOUS_TOKEN`, but the protocol is not rolling-safe: callers
always send current while receivers accept current plus optional previous. Rotate
during a maintenance window or after pausing scheduler/workflows traffic. Set
previous=old/current=new, restart the private fleet together, then clear previous
after the fleet converges.

The workerd image uses the non-local compiled configs (`gateway.bin`,
`user-runtime.bin`, `system-runtime.bin`, `do-runtime.bin`). Those configs use
Kubernetes service DNS directly, so this stack does not need the local Compose
Envoy sidecar.

The base also installs ingress-only NetworkPolicies. Kubernetes ClusterIP
Services are not an isolation boundary by themselves: without NetworkPolicy, any
pod in the namespace can usually reach Valkey and the runtime internal sockets.
The baseline therefore default-denies ingress and opens only the expected WDL
component-to-component ports. It intentionally does not default-deny egress yet
because production overlays must decide how DNS, object storage, managed
Valkey, and external admin access are reached.

For local or staging clusters that need the minimum cloud metadata guard without
a full production egress allowlist, use `overlays/local-metadata-guard/` instead
of `overlays/local/`, or `overlays/local-ingress-metadata-guard/` instead of
`overlays/local-ingress/`. These overlays add an egress NetworkPolicy for WDL
Pods that allows ordinary IPv4 egress but excludes common metadata endpoints
`169.254.169.254/32` and `169.254.170.2/32`. This is not a complete production
egress policy; production overlays still need provider-specific DNS, object
storage, managed Valkey, admin endpoint, and IPv6 metadata decisions.

## Stateful Runtime Boundary

D1 and DO have two service surfaces:

- `d1-runtime` and `do-runtime` are ordinary ClusterIP first-hop router
  Services used by the compiled workerd configs. Workerd connects to these
  stable names; it is not expected to consume headless multi-A DNS as a load
  balancer.
- `d1-runtime-headless` and `do-runtime-headless` are owner-endpoint Services.
  Owner records and owner hints store Pod-specific names such as
  `d1-runtime-0.d1-runtime-headless:8787`.

D1 and DO owner records store the endpoint of the exact runtime Pod that owns a
SQLite-backed localDisk shard. The Kubernetes manifests therefore use:

- StatefulSets for `d1-runtime` and `do-runtime`;
- ordinary ClusterIP Services for first-hop runtime routing;
- separate headless Services for stable per-Pod owner DNS;
- `D1_TASK_ID` / `DO_TASK_ID` set from the Pod name;
- `D1_TASK_ENDPOINT` / `DO_TASK_ENDPOINT` set to
  `$(POD_NAME).<headless-service>:<port>`;
- shared ReadWriteMany PVCs for `/data/d1` and `/data/do`.

Do not store load-balanced Service DNS in owner records, and do not replace the
shared RWX PVCs with per-Pod RWO PVCs. Load-balanced owner endpoints route
takeover, probe, and direct-owner traffic to the wrong Pod; per-Pod PVCs open
different SQLite files after ownership changes.

Most local Kubernetes distributions ship with a default RWO storage class, while
WDL needs shared RWX storage for D1 and DO localDisk data. The local overlay
therefore expects an RWX class named `wdl-nfs-rwx`. On Docker Desktop, kind, or
other local clusters that do not already provide RWX storage, install the local
NFS provisioner before applying the WDL stack:

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

That provisioner is a local simulation of shared file storage, not a production
storage recommendation. Production overlays should use the provider-native RWX
driver instead, such as AWS EFS CSI, Aliyun NAS CSI, or another Kubernetes CSI
driver supplied by the target platform.

## Local Greenfield Path

This is the recommended local smoke path. It was verified on Docker Desktop
Kubernetes, but the sequence is intentionally standard Kubernetes: any cluster
with `kubectl`, a working image source, an RWX storage class, and an ingress
controller can follow the same shape.

1. Start or connect to a local Kubernetes cluster.
2. Install the local NFS provisioner from the previous section, unless your
   cluster already provides an RWX storage class named `wdl-nfs-rwx`.
3. Build the same local images used by Compose integration tests:

   ```bash
   docker build -f Dockerfile.workerd -t wdl-workerd:dev .
   docker build -f Dockerfile.rust -t wdl-rust:dev .
   ```

   If your local cluster does not share the Docker daemon with the Kubernetes
   nodes, import or push those tags so every node can resolve them. Docker
   Desktop's multi-node backend uses `kindest/node` containers, so importing the
   local images looks like:

   ```bash
   docker save -o /tmp/wdl-k8s-images.tar wdl-workerd:dev wdl-rust:dev
   for node in $(docker ps --format '{{.Names}}' | grep '^desktop-'); do
     docker exec -i "$node" ctr -n k8s.io images import - < /tmp/wdl-k8s-images.tar
   done
   ```

4. Install an nginx Ingress controller if the cluster does not already have one.
   Kubernetes 1.35 should use ingress-nginx controller v1.15.x or newer in the
   1.15 support line. This local overlay was verified with controller v1.15.1:

   ```bash
   kubectl apply -f \
     https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.15.1/deploy/static/provider/kind/deploy.yaml
   kubectl -n ingress-nginx rollout status deploy/ingress-nginx-controller --timeout=240s
   ```

5. Add host entries for the local smoke names:

   ```text
   127.0.0.1 admin.test
   127.0.0.1 demo.workers.local
   127.0.0.1 s3mock.local
   ```

6. Apply the ingress overlay. It includes the local base and patches
   `ASSETS_CDN_BASE` to `http://s3mock.local/wdl-assets`:

   ```bash
   kubectl apply -k deploy/kubernetes/overlays/local-ingress
   ```

7. Wait for every WDL workload to become ready:

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

8. Check the gateway and the CLI path:

   ```bash
   curl http://admin.test/healthz
   CONTROL_URL=http://admin.test \
   ADMIN_TOKEN=local-dev-token \
   WDL_NS=demo \
   wdl workers
   ```

The local admin host is `admin.test`, and the local admin token is
`local-dev-token`.

## Local Smoke Checks

After the greenfield stack is ready, run a few tenant-level checks through the
Ingress hostnames. These verify more than Pod readiness.

Deploy and check ASSETS:

```bash
CONTROL_URL=http://admin.test \
ADMIN_TOKEN=local-dev-token \
WDL_NS=demo \
wdl deploy test-workers/pages-assets
curl http://demo.workers.local/pages-assets/
```

The HTML should contain URLs under `http://s3mock.local/wdl-assets/...`. Fetch
one of those URLs directly to confirm the object-storage path is reachable
through Ingress.

Then smoke the stateful runtimes and workflows:

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

Tenant HTTP requests should use the hostname you added to `/etc/hosts`, such as
`http://demo.workers.local/...`, rather than an explicit `Host` header. This
matches browser behavior and exercises the same ingress path that asset URLs
use.

## Port-Forward Fallback

If you do not want to install an Ingress controller, apply the local overlay and
forward the host-facing ports:

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

Then use:

```bash
curl http://localhost:30080/healthz
CONTROL_URL=http://admin.test:30080 \
CONTROL_CONNECT_HOST=127.0.0.1 \
ADMIN_TOKEN=local-dev-token \
WDL_NS=demo \
wdl workers
```

Tenant HTTP requests on the port-forward path usually need an explicit `Host`
header unless you add a local wildcard DNS entry for `*.workers.local`.

## Local Ports

- Gateway: `localhost:30080`
- s3mock: `localhost:30900`

These are port-forwarded host ports, not Kubernetes NodePorts. Docker Desktop's
multi-node `kindest/node` backend does not always publish NodePorts back to the
host. `ASSETS_CDN_BASE` defaults to `http://localhost:30900/wdl-assets` in the
local overlay so asset URLs returned to a browser are reachable from the host
while the s3mock port-forward is running.

## Local Ingress

If an Ingress controller is already installed, the ingress overlay can be
applied directly:

```bash
kubectl apply -k deploy/kubernetes/overlays/local-ingress
kubectl -n wdl-local rollout status statefulset/d1-runtime
kubectl -n wdl-local rollout status statefulset/do-runtime
```

The local ingress overlay expects the `nginx` IngressClass and routes:

- `admin.test` to `gateway:8080`;
- `*.workers.local` to `gateway:8080`;
- `s3mock.local/wdl-assets/assets/...` to a read-only `assets-proxy`, which
  forwards only query-free asset GET/HEAD requests to `s3mock:9090`.

The overlay opens the `assets-proxy` NetworkPolicy only to ingress-nginx Pods in
the `ingress-nginx` namespace, and opens s3mock only to WDL runtime Pods plus
that proxy. It deliberately does not expose the full s3mock S3 API through
Ingress. If your controller runs under a different namespace or label set, patch
`overlays/local-ingress/s3mock-network-policy.yaml` alongside the IngressClass.

Add host entries for the names you want to exercise if you did not already do
so in the greenfield path:

```text
127.0.0.1 admin.test
127.0.0.1 demo.workers.local
127.0.0.1 s3mock.local
```

Then use:

```bash
curl -H 'Host: admin.test' http://127.0.0.1/healthz
CONTROL_URL=http://admin.test \
CONTROL_CONNECT_HOST=127.0.0.1 \
ADMIN_TOKEN=local-dev-token \
WDL_NS=demo \
wdl workers
```

`ASSETS_CDN_BASE` is patched to `http://s3mock.local/wdl-assets` by this overlay.
That host is only an asset-read facade and does not forward S3 query operations;
use the local port-forward path when you need direct s3mock API access for
debugging.

## Current Boundaries

This is a local-first deployment baseline, not a full production chart.
Productionizing it still needs provider-specific decisions:

- managed Valkey or StatefulSet policy;
- object storage provider and bucket bootstrap;
- provider-native RWX durable storage class and backup policy for D1/DO
  localDisk data;
- ingress, TLS, DNS, and CDN policy;
- secret-envelope provider choice and key rotation process;
- pod disruption budgets, topology spread, resource sizing, and autoscaling;
- pod security contexts (`runAsNonRoot`, dropped capabilities, read-only root
  filesystem where possible).
- egress NetworkPolicy and Redis/Valkey authentication/TLS if the provider does
  not supply equivalent isolation.

Keep cloud-resource creation outside this Kustomize tree unless the project
explicitly adopts an operator such as Crossplane.

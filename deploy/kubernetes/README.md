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
- `overlays/local/` provides local smoke secrets, a namespace, and local storage
  defaults.
- `overlays/local-gateway/` adds Gateway API resources for local hostnames.
- `overlays/local-metadata-guard/` applies the local overlay plus the shared
  metadata-guard component.
- `overlays/local-gateway-metadata-guard/` applies the local gateway overlay
  plus the same component.
- `storage/local-nfs/` contains Helm values for a local ReadWriteMany
  provisioner that can simulate provider RWX storage on a development cluster.

The base and local overlays use the published image contract: workerd services
pull `docker.io/getwdl/wdl-workerd:latest`, and Rust sidecars/default Rust
services pull `docker.io/getwdl/wdl-rust:latest`. WDL containers set
`imagePullPolicy: Always`, so applying the stack does not depend on images built
or imported through a developer workstation.

All private WDL services read `WDL_INTERNAL_AUTH_TOKEN` from the shared
`wdl-secrets` Secret and send it as `x-wdl-internal-auth` on internal mesh
calls. The local overlay generates `wdl-secrets` with `local-internal-auth-token`;
production overlays must provide their own value. Rotation uses optional
`WDL_INTERNAL_AUTH_PREVIOUS_TOKEN`, but the protocol is not rolling-safe: callers
always send current while receivers accept current plus optional previous. Rotate
during a maintenance window or after pausing scheduler/workflows traffic. Set
previous=old/current=new, restart the private fleet together, then clear previous
after the fleet converges.

The workerd image uses the non-local/production-mesh compiled configs (`gateway.bin`,
`user-runtime.bin`, `system-runtime.bin`, `d1-runtime.bin`, and resident
`do-runtime.bin` by default). Setting `DO_PREVENT_EVICTION=false` selects
`do-runtime-evictable.bin` instead. These artifacts use Kubernetes service DNS
directly, so this stack does not need the local Compose Envoy sidecar. D1 has no local
config variant.

The base also installs ingress-only NetworkPolicies. Kubernetes ClusterIP
Services are not an isolation boundary by themselves: without NetworkPolicy, any
pod in the namespace can usually reach Valkey and the runtime internal sockets.
The baseline therefore default-denies ingress and opens only the expected WDL
component-to-component ports. It intentionally does not default-deny egress yet
because production overlays must decide how DNS, object storage, managed
Valkey, and external admin access are reached.

For local or staging clusters that need the minimum cloud metadata guard without
a full production egress allowlist, use `overlays/local-metadata-guard/` instead
of `overlays/local/`, or `overlays/local-gateway-metadata-guard/` instead of
`overlays/local-gateway/`. These overlays add an egress NetworkPolicy for WDL
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
Kubernetes, where the Gateway `LoadBalancer` is published on `127.0.0.1`.
Other Gateway API clusters can follow the same shape, but must map the smoke
hostnames to their external Gateway address or expose it through a NodePort or
local tunnel instead.

1. Start or connect to a local Kubernetes cluster.
2. Install the local NFS provisioner from the previous section, unless your
   cluster already provides an RWX storage class named `wdl-nfs-rwx`.
3. Install the Gateway API standard CRDs and NGINX Gateway Fabric. This local
   overlay is pinned to NGINX Gateway Fabric 2.6.7:

   ```bash
   kubectl kustomize \
     "https://github.com/nginx/nginx-gateway-fabric/config/crd/gateway-api/standard?ref=v2.6.7" \
     | kubectl apply -f -
   helm upgrade --install ngf \
     oci://ghcr.io/nginx/charts/nginx-gateway-fabric \
     --version 2.6.7 \
     --namespace nginx-gateway \
     --create-namespace \
     --wait
   ```

4. Add host entries for the local smoke names:

   ```text
   127.0.0.1 admin.test
   127.0.0.1 demo.workers.local
   127.0.0.1 s3mock.local
   ```

5. Apply the Gateway API overlay. It includes the local base and patches
   `ASSETS_CDN_BASE` to `http://s3mock.local/wdl-assets`:

   ```bash
   kubectl apply -k deploy/kubernetes/overlays/local-gateway
   ```

6. Wait for the Gateway and every WDL workload to become ready:

   ```bash
   kubectl -n wdl-local wait --for=condition=Programmed gateway/wdl --timeout=240s
   kubectl -n wdl-local rollout status deployment \
     -l gateway.networking.k8s.io/gateway-name=wdl --timeout=240s
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

7. Bootstrap the permanent `__system__/s3-cleanup` worker before accepting
   tenant ASSETS deploys:

   ```bash
   export CONTROL_URL=http://admin.test
   export ADMIN_TOKEN=local-dev-token

   wdl d1 create --ns __system__ s3-cleanup-state
   (
     cd system-workers/s3-cleanup
     wdl d1 migrations apply --ns __system__ s3-cleanup-state
   )

   printf '%s' 'http://s3mock:9090' \
     | wdl secret put --ns __system__ --worker s3-cleanup S3_ENDPOINT
   printf '%s' 'us-east-1' \
     | wdl secret put --ns __system__ --worker s3-cleanup S3_REGION
   printf '%s' 'wdl-assets' \
     | wdl secret put --ns __system__ --worker s3-cleanup S3_BUCKET
   printf '%s' 'test' \
     | wdl secret put --ns __system__ --worker s3-cleanup S3_ACCESS_KEY_ID
   printf '%s' 'test' \
     | wdl secret put --ns __system__ --worker s3-cleanup S3_SECRET_ACCESS_KEY

   wdl deploy system-workers/s3-cleanup --ns __system__
   ```

   This worker is required for the ASSETS lifecycle. Without it, version and
   worker deletion can enqueue cleanup intents, but no consumer removes the S3
   objects.

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
Gateway API hostnames. These verify more than Pod readiness.

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
through the Gateway.

Then delete the test Worker and confirm that the system cleanup worker removes
its ASSETS objects:

```bash
ASSET_URL=$(
  curl -fsS http://demo.workers.local/pages-assets/ \
    | sed -n 's/.*href="\([^"]*hello\.txt\)".*/\1/p'
)
test -n "$ASSET_URL"
curl -fsS "$ASSET_URL"

wdl delete worker pages-assets --ns demo --yes
for _ in $(seq 1 90); do
  ASSET_STATUS=$(curl -sS -o /dev/null -w '%{http_code}' "$ASSET_URL")
  [ "$ASSET_STATUS" = 404 ] && break
  sleep 1
done
test "$ASSET_STATUS" = 404
```

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

If you do not want to install a Gateway API implementation, apply the local
overlay and forward the host-facing ports:

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

## Local Gateway API

If NGINX Gateway Fabric is already installed, the Gateway API overlay can be
applied directly:

```bash
kubectl apply -k deploy/kubernetes/overlays/local-gateway
kubectl -n wdl-local wait --for=condition=Programmed gateway/wdl --timeout=240s
kubectl -n wdl-local rollout status deployment \
  -l gateway.networking.k8s.io/gateway-name=wdl --timeout=240s
```

The local Gateway API overlay expects the `nginx` GatewayClass and routes:

- `admin.test` to `gateway:8080`;
- `*.workers.local` to `gateway:8080`;
- `s3mock.local/wdl-assets/assets/...` to a read-only `assets-proxy`, which
  forwards only query-free asset GET/HEAD requests to `s3mock:9090`.

The overlay opens the `assets-proxy` NetworkPolicy only to the data-plane Pods
created for the `wdl` Gateway, and opens s3mock only to WDL runtime Pods plus
that proxy. It deliberately does not expose the full s3mock S3 API through the
Gateway. A different Gateway API implementation must provide an equivalent
data-plane Pod selector in `overlays/local-gateway/gateway-network-policy.yaml`.

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

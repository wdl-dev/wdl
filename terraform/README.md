# WDL Terraform

This directory contains the AWS Terraform deployment for a single-account WDL
environment. It is intentionally split into three root modules so a fresh AWS
account can be brought up in stages:

| Root | Purpose |
| --- | --- |
| `bootstrap/` | Remote state bucket, CloudTrail logging, and optional monthly budget |
| `foundation/` | VPC, subnets, one NAT Gateway, public ALB, and ACM certificates |
| `terraform/` | WDL application services, shared data stores, IAM, and logs |

DNS is managed outside AWS. Terraform prints the records to add at your DNS
provider, but it does not create hosted zones or DNS records.

## Directory Layout

```text
terraform/
  bootstrap/
  foundation/
  modules/
    compute/   # ECS Fargate, Service Connect, ALB target group, IAM, logs
    data/      # Valkey, S3 buckets, optional CloudFront assets CDN, S3 writer secrets
  main.tf
  outputs.tf
  providers.tf
  terraform.tfvars.example
  variables.tf
  versions.tf
```

Local `backend.hcl`, `terraform.tfvars`, `.terraform/`, and state snapshots are
ignored by git. Do not commit real backend config, tfvars, or state.

## Bootstrap

Create your AWS admin user/group and local AWS profile out-of-band. Then create
the remote state bucket:

```sh
AWS_PROFILE=<profile> terraform -chdir=terraform/bootstrap init -backend=false
AWS_PROFILE=<profile> terraform -chdir=terraform/bootstrap apply \
  -var name_prefix=wdl-demo
```

The first bootstrap apply starts with local state because the state bucket does
not exist yet. After it completes, write local ignored backend files from the
`s3_backend_example` output. Each root needs its own `backend.hcl` because
Terraform resolves `-backend-config=backend.hcl` relative to the `-chdir` root.

```text
terraform/bootstrap/backend.hcl    key = "bootstrap/terraform.tfstate"
terraform/foundation/backend.hcl   key = "foundation/terraform.tfstate"
terraform/backend.hcl              key = "app/terraform.tfstate"
```

Then migrate bootstrap state:

```sh
AWS_PROFILE=<profile> terraform -chdir=terraform/bootstrap init \
  -backend-config=backend.hcl \
  -migrate-state
```

## Foundation

Foundation creates the network and public entry point. Apply it first without
certificate validation so Terraform can print the DNS validation records:

```sh
AWS_PROFILE=<profile> terraform -chdir=terraform/foundation init \
  -backend-config=backend.hcl

AWS_PROFILE=<profile> terraform -chdir=terraform/foundation apply \
  -var name_prefix=wdl-demo \
  -var admin_host=api.wdl.dev \
  -var 'platform_domain=*.wdl.sh' \
  -var site_host=wdl.dev \
  -var 'additional_public_hosts=["chat.wdl.dev"]' \
  -var assets_cdn_domain=assets.wdl.dev \
  -var validate_certificates=false
```

Add the printed CNAME records at your DNS provider:

- ACM validation records for the regional ALB certificate.
- Optional ACM validation record for the public `site_host`, such as `wdl.dev`.
- Optional ACM validation records for each `additional_public_hosts` entry.
- Optional ACM validation records for the us-east-1 assets CDN certificate.
- `admin_host` CNAME to the ALB DNS name.
- `platform_domain` wildcard CNAME to the ALB DNS name.
- `site_host` to the ALB DNS name. For Cloudflare-managed apex hosts such as
  `wdl.dev`, use Cloudflare's supported flattened/proxied target form.
- `www.<site_host>` to the same ALB DNS name. The ALB redirects it to `site_host`
  with HTTP 301.
- Each `additional_public_hosts` entry to the same ALB DNS name. These hosts
  terminate TLS on the shared regional site certificate and forward to the WDL
  gateway.

Then enable validation and apply again:

```sh
AWS_PROFILE=<profile> terraform -chdir=terraform/foundation apply \
  -var name_prefix=wdl-demo \
  -var admin_host=api.wdl.dev \
  -var 'platform_domain=*.wdl.sh' \
  -var site_host=wdl.dev \
  -var 'additional_public_hosts=["chat.wdl.dev"]' \
  -var assets_cdn_domain=assets.wdl.dev \
  -var validate_certificates=true
```

The second apply waits for ACM validation and creates the HTTPS ALB listener.
The application stack needs these foundation outputs:

- `vpc_id`
- `private_subnet_ids`
- `alb_https_listener_arn`
- `alb_security_group_id`
- `alb_dns_name`
- `assets_cdn_acm_certificate_arn` if the assets CDN is enabled

Fresh AWS accounts may need AWS Support verification before CloudFront
distribution creation is allowed. Until the account is cleared for CloudFront,
leave `assets_cdn_domain` and `assets_cdn_acm_certificate_arn` empty in the
application tfvars.

## Application

Create a local ignored `terraform/terraform.tfvars` from
`terraform/terraform.tfvars.example`, using the foundation outputs:

```hcl
vpc_id                 = "vpc-..."
private_subnet_ids     = ["subnet-...", "subnet-...", "subnet-..."]
alb_https_listener_arn = "arn:aws:elasticloadbalancing:..."
alb_security_group_id  = "sg-..."

admin_host      = "api.wdl.dev"
platform_domain = "*.wdl.sh"

site_host = "wdl.dev"
additional_public_hosts = ["chat.wdl.dev"]

assets_cdn_domain              = ""
assets_cdn_acm_certificate_arn = ""
```

Initialize the application root with its own `terraform/backend.hcl` and apply:

```sh
AWS_PROFILE=<profile> terraform -chdir=terraform init \
  -backend-config=backend.hcl

AWS_PROFILE=<profile> terraform -chdir=terraform apply
```

`site_host` configures ALB TLS for both the apex host and `www.<site_host>`, forwards
the apex host to the WDL gateway target group, and redirects `www.<site_host>` to the
apex host with HTTP 301. `additional_public_hosts` configures extra exact public
hosts that share the regional site certificate and forward to the same gateway target
group. WDL host declaration, route ownership, and route pattern deployment still
belong to the WDL control plane and CLI. Declare each public host and deploy its
Worker through WDL after DNS reaches the gateway.

If the assets CDN is enabled, point the assets CDN host at the CloudFront
distribution after the application apply:

```sh
AWS_PROFILE=<profile> terraform -chdir=terraform output -raw assets_cdn_distribution_domain
```

Create a CNAME from `assets_cdn_domain` (for example `assets.wdl.dev`) to that
CloudFront domain at your DNS provider. Then smoke-test that DNS and TLS reach
CloudFront:

```sh
curl -I https://assets.wdl.dev/
```

The default image variables pull public release images from Docker Hub:

| Variable | Default |
| --- | --- |
| `workerd_image` | `docker.io/getwdl/wdl-workerd:latest` |
| `rust_image` | `docker.io/getwdl/wdl-rust:latest` |

Pin immutable release tags or mirror these images into a private registry for
production. Terraform compares the image reference string, not the digest behind
a mutable tag. Repushing `:latest` does not roll unchanged services; use
`aws ecs update-service --force-new-deployment` when intentionally keeping a
mutable tag.

## Services

The application stack runs seven ECS services on Fargate capacity providers.

- `gateway`: `wdl-workerd`; ALB ingress for tenant traffic and the admin host.
- `user-runtime`: `wdl-workerd` plus `redis-proxy`; loaded tenant workers and
  internal dispatch.
- `system-runtime`: `wdl-workerd` plus `redis-proxy`; static control/auth/tail
  workers and system namespace loader.
- `d1-runtime`: `wdl-workerd` plus `d1-supervisor`; D1 host with EFS-backed
  localDisk at `/data/d1`.
- `do-runtime`: `wdl-workerd` plus `do-supervisor`; DO host with EFS-backed
  localDisk at `/data/do`.
- `scheduler`: `wdl-rust`; cron, queue dispatch, and workflow tick wakeups.
- `workflows`: `wdl-rust`; workflow state, run leases, and internal DO alarm
  dispatch.

Tenant egress policy is enforced inside workerd. User-runtime loaded workers and
tenant Durable Object classes receive a `public-network` outbound binding with
`allow=["public"]`. System-runtime and platform internals deliberately keep
private plus public mesh reach.

## Capacity

The ECS cluster enables both `FARGATE` and `FARGATE_SPOT` capacity providers:

- gateway, user-runtime, and system-runtime keep `base = 1` on on-demand Fargate and
  place overflow according to `od_weight` / `spot_weight`.
- scheduler, workflows, d1-runtime, and do-runtime run on on-demand Fargate only.
- All task definitions use ARM64 Linux Fargate-compatible CPU and memory defaults.
- gateway, user-runtime, system-runtime, and workflows use zero-downtime rolling
  replacement (`maximum_percent = 200`, `minimum_healthy_percent = 100`).
- d1-runtime and do-runtime use sequential replacement (`maximum_percent = 100`,
  `minimum_healthy_percent = 50`) with Availability Zone rebalancing disabled.
- scheduler remains stop-before-start (`maximum_percent = 100`,
  `minimum_healthy_percent = 0`) with Availability Zone rebalancing disabled because it
  is a singleton control loop.

Fargate task-level `cpu` and `memory` are the task reservation/limit boundary. D1 and
Durable Object stateful runtime containers also set explicit container `memory` hard
limits. D1 defaults to `runtime_memory - 128 MiB`. DO defaults to
`runtime_memory - 192 MiB`: 64 MiB for the colocated redis-proxy sidecar reservation
plus 128 MiB of task-level headroom. Both are overrideable with
`d1_runtime_container_memory` / `do_runtime_container_memory`; D1 must leave the
task-level headroom, and DO must leave both the redis-proxy reservation and additional
task-level headroom. This matters because newer workerd releases no longer cap
SQLite's process hard heap at 512 MiB; the container cap keeps a runaway SQLite query
inside the stateful runtime container budget. The supervisor is PID 1 in that same
container.

Tenant-running tasks use least-privilege ECS task roles, public-only workerd outbound
bindings, and private mesh security groups as the cloud credential and network
boundary. D1 and DO task identities use `ECS_CONTAINER_METADATA_URI_V4`.

## Observability

The Terraform ECS cluster enables Container Insights with enhanced observability. That
collects AWS infrastructure telemetry for cluster, service, task, and container health
and utilization, including per-container memory metrics used for capacity checks. AWS
bills these metrics as CloudWatch Container Insights/custom metrics. This is separate
from WDL service Prometheus metrics and bounded-label application logs.

## Admin Token

Retrieve the generated bootstrap admin token from Terraform state:

```sh
AWS_PROFILE=<profile> terraform -chdir=terraform output -raw admin_token
```

The token is stored in Secrets Manager and injected into system-runtime as
`BOOTSTRAP_TOKEN`. The auth worker upserts it as the reserved `bootstrap` ops
token on cold start. Control verifies requests through `env.AUTH.verify()`.

Client tooling sends the token as `X-Admin-Token`, usually via `ADMIN_TOKEN`:

```sh
export ADMIN_TOKEN=$(AWS_PROFILE=<profile> terraform -chdir=terraform output -raw admin_token)
wdl deploy ./test-workers/hello-jsonc --ns demo --control-url https://api.wdl.dev
```

The ALB listener only checks that `X-Admin-Token` is present on the admin host.
Missing headers return a fixed 401 at the edge. Token validity is still checked
inside control/auth.

## System s3-cleanup Worker

Terraform creates the ASSETS bucket, the scoped s3-cleanup IAM user, and the
secret containing that user's access key. It does not deploy the permanent
`__system__/s3-cleanup` worker because that worker is a WDL control-plane
artifact with its own D1 binding, migration, queue consumer, cron trigger, and
worker-level secrets.

After exporting `ADMIN_TOKEN`, bootstrap it once:

```sh
export CONTROL_URL=https://api.wdl.dev
export S3_ENDPOINT=https://s3.ap-east-1.amazonaws.com
export S3_REGION=ap-east-1
export S3_BUCKET=$(AWS_PROFILE=<profile> terraform -chdir=terraform output -raw assets_bucket)
export S3_CLEANUP_SECRET_ARN
S3_CLEANUP_SECRET_ARN=$(
  AWS_PROFILE=<profile> terraform -chdir=terraform output -raw s3_cleanup_secret_arn
)

AWS_PROFILE=<profile> aws secretsmanager get-secret-value \
  --secret-id "$S3_CLEANUP_SECRET_ARN" \
  --query SecretString \
  --output text

wdl d1 create --ns __system__ s3-cleanup-state --control-url "$CONTROL_URL"

(
  cd system-workers/s3-cleanup
  wdl d1 migrations apply --ns __system__ s3-cleanup-state --control-url "$CONTROL_URL"
  npm install --ignore-scripts
)

printf '%s' "$S3_ENDPOINT" \
  | wdl secret put --ns __system__ --worker s3-cleanup S3_ENDPOINT --control-url "$CONTROL_URL"
printf '%s' "$S3_REGION" \
  | wdl secret put --ns __system__ --worker s3-cleanup S3_REGION --control-url "$CONTROL_URL"
printf '%s' "$S3_BUCKET" \
  | wdl secret put --ns __system__ --worker s3-cleanup S3_BUCKET --control-url "$CONTROL_URL"
printf '%s' "<access_key_id from the JSON above>" \
  | wdl secret put --ns __system__ --worker s3-cleanup S3_ACCESS_KEY_ID --control-url "$CONTROL_URL"
printf '%s' "<secret_access_key from the JSON above>" \
  | wdl secret put \
    --ns __system__ \
    --worker s3-cleanup \
    S3_SECRET_ACCESS_KEY \
    --control-url "$CONTROL_URL"

wdl deploy system-workers/s3-cleanup --ns __system__ --control-url "$CONTROL_URL"
```

Without this worker, version and worker deletion can still enqueue ASSETS cleanup
intents, but nothing consumes them.

## Secrets

Terraform creates core runtime/control secrets:

| Secret | Consumers | Purpose |
| --- | --- | --- |
| `${name}/admin-token` | system-runtime | Bootstrap ops token |
| `${name}/internal-auth-token` | all private services | `x-wdl-internal-auth` for mesh calls |
| `${name}/secret-envelope` | redis-proxy and control | Local-provider tenant secret envelope key |

Terraform also creates storage credential secrets for S3-compatible ASSETS/R2
access:

| Secret | Consumers | Purpose |
| --- | --- | --- |
| `${name}/control-s3` | system-runtime/control | Put-only ASSETS upload credential |
| `${name}/s3-cleanup` | `__system__/s3-cleanup` | List/delete ASSETS cleanup credential |
| `${name}/runtime-r2` | runtimes/control | Tenant R2 physical bucket credential |

The local-provider secret envelope key and storage access keys are generated by
Terraform and therefore exist in Terraform state. Treat remote state, state
backups, and state access logs as secret-root material. Do not copy state into
tickets, CI artifacts, review logs, or ad hoc debug output.

Do not taint or recreate `${name}/secret-envelope` as a normal rotation action.
Existing envelopes would keep their `kid` but no longer decrypt. A future key
rotation needs an explicit rewrap procedure.

Internal auth rotation is dual-read and single-write, but it is not rolling-safe
by itself. Set `internal_auth_previous_token_secret_arn` to a separate secret
containing the old token, rotate the managed current token, restart all private
services together, then clear the previous ARN after the fleet converges.

## Operating Notes

### Service Connect Alias Renames

ECS Service Connect bakes alias names into each task's Envoy sidecar when the
task starts. After renaming a `client_alias.dns_name`, force a new deployment on
every consuming service:

```sh
AWS_PROFILE=<profile> aws ecs update-service \
  --cluster <cluster> \
  --service <service> \
  --force-new-deployment
```

Symptoms when this is missed are usually `getaddrinfo ENOTFOUND <old-alias>` in
the consuming service logs.

### D1 and DO Routing

Service Connect DNS can route a request to any D1 or DO runtime task. Correctness
comes from Valkey owner records, generation fencing, and forward-to-owner logic,
not from Service Connect stickiness.

D1 and DO task identity is lazy-resolved from ECS metadata. Terraform sets
`D1_TASK_CONTAINER_NAME=d1-runtime` and `DO_TASK_CONTAINER_NAME=do-runtime`.
The task does not need explicit task ID or endpoint environment variables.

### Runtime Recycle

`workerLoader` caches by immutable `<ns>:<name>:<version>`, so new deploys are
natural cache misses. Old isolates remain resident until user-runtime or
system-runtime tasks recycle. Use `aws ecs update-service --force-new-deployment`
periodically if memory growth matters.

### Storage

D1 and DO each mount a separate EFS file system root into the workerd `localDisk`
path. D1 uses `/data/d1`; DO uses `/data/do`. Both use Elastic Throughput to
avoid the low baseline of tiny early file systems.

Control owns S3 writes. Developers need HTTPS reach to the control URL and an
admin or tenant token; AWS credentials stay server-side in scoped Secrets
Manager secrets.

### Test Hooks

`d1_test_hooks_enabled` defaults to `false` and is guarded to test-named compute
stacks. Keep it disabled for normal live-ready service state.

## Validation

Useful checks after an apply:

```sh
AWS_PROFILE=<profile> terraform -chdir=terraform plan -input=false

AWS_PROFILE=<profile> aws ecs wait services-stable \
  --cluster <cluster> \
  --services <service>...

curl -i https://api.wdl.dev/
curl -i https://demo.wdl.sh/
```

`https://api.wdl.dev/` should return an unauthorized JSON response without an
admin token. A tenant host with no deployed worker should return a gateway JSON
404 rather than a DNS, TLS, or ALB error.

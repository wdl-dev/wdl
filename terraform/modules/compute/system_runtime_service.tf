// system-runtime: workerd container plus local redis-proxy sidecar.
// :8081 is the loader for dynamic __system__-ns workers, :8088 is internal
// dispatch, and :8082 is the static control worker. Same workerd image as
// user-runtime, different capnp.

resource "aws_ecs_task_definition" "system_runtime" {
  family                   = "${var.name}-system-runtime"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.system_runtime_cpu
  memory                   = var.system_runtime_memory
  execution_role_arn       = aws_iam_role.exec.arn
  # S3 writes use static IAM user creds via Secrets Manager (SigV4 signer
  # signs, no role assumption), so the default runtime_task role is fine.
  task_role_arn = aws_iam_role.runtime_task.arn

  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }

  container_definitions = jsonencode([
    {
      name        = "redis-proxy"
      image       = var.rust_image
      essential   = true
      command     = ["/redis-proxy"]
      stopTimeout = 20

      environment = local.redis_proxy_env

      secrets = concat(local.secret_envelope_secrets, local.internal_auth_secrets)

      healthCheck = local.redis_proxy_health_check

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.runtime.name
          awslogs-region        = var.region
          awslogs-stream-prefix = "ecs-system-redis-proxy"
        }
      }
    },
    {
      name        = "system-runtime"
      image       = var.workerd_image
      essential   = true
      entryPoint  = ["workerd"]
      command     = ["serve", "-b", "/app/dist/workerd-configs/system-runtime.bin", "--experimental"]
      stopTimeout = 20

      dependsOn = [{
        containerName = "redis-proxy"
        condition     = "HEALTHY"
      }]

      portMappings = [
        {
          name          = "system-loader"
          containerPort = 8081
          protocol      = "tcp"
          appProtocol   = "http"
        },
        {
          name          = "system-internal"
          containerPort = 8088
          protocol      = "tcp"
          appProtocol   = "http"
        },
        {
          name          = "system-control"
          containerPort = 8082
          protocol      = "tcp"
          appProtocol   = "http"
        },
      ]

      environment = concat([
        { name = "REDIS_ADDR", value = local.redis_addr },
        { name = "DATA_REDIS_ADDR", value = local.redis_addr },
        { name = "DATA_REDIS_DB", value = "1" },
        { name = "REDIS_PROXY_URL", value = "http://127.0.0.1:7070" },
        local.log_level_env,
        { name = "ASSETS_CDN_BASE", value = var.assets_cdn_base },
        { name = "PLATFORM_DOMAIN", value = local.platform_domain },
        { name = "S3_BUCKET", value = var.assets_bucket },
        { name = "S3_REGION", value = var.region },
        # Pass the regional S3 endpoint explicitly so SigV4 signs against
        # the same host that the S3-compatible client will contact.
        { name = "S3_ENDPOINT", value = local.aws_s3_endpoint },
      ], local.r2_s3_env)

      secrets = concat([
        # BOOTSTRAP_TOKEN uses the admin token Secrets Manager ARN
        # (aws_secretsmanager_secret.admin_token.arn) so it lands on auth's
        # env (capnp). ALB now only checks that X-Admin-Token is present;
        # auth still verifies the actual token value and upserts this one as
        # the reserved bootstrap ops token.
        { name = "BOOTSTRAP_TOKEN", valueFrom = aws_secretsmanager_secret.admin_token.arn },
        { name = "S3_ACCESS_KEY_ID", valueFrom = "${var.control_s3_secret_arn}:access_key_id::" },
        { name = "S3_SECRET_ACCESS_KEY", valueFrom = "${var.control_s3_secret_arn}:secret_access_key::" },
        { name = "R2_S3_ACCESS_KEY_ID", valueFrom = "${var.runtime_r2_secret_arn}:access_key_id::" },
        { name = "R2_S3_SECRET_ACCESS_KEY", valueFrom = "${var.runtime_r2_secret_arn}:secret_access_key::" },
      ], local.secret_envelope_secrets, local.internal_auth_secrets)

      healthCheck = merge(local.workerd_health_check_thresholds, {
        command = ["CMD", "/usr/local/bin/http-hc", "http://127.0.0.1:8088/_healthz"]
      })

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.runtime.name
          awslogs-region        = var.region
          awslogs-stream-prefix = "ecs-system"
        }
      }
  }])

  lifecycle {
    create_before_destroy = true
  }
}

module "system_runtime_service" {
  source = "../ecs-service"

  name                = "${var.name}-system-runtime"
  cluster_id          = aws_ecs_cluster.this.id
  task_definition_arn = aws_ecs_task_definition.system_runtime.arn
  # Day-one single replica. Control is stateless (Redis is the truth),
  # so scaling out is purely a throughput choice, not correctness.
  desired_count          = 1
  enable_execute_command = true
  deployment             = local.zero_downtime_deployment

  capacity_provider_strategies = local.fargate_stateless_capacity_provider_strategies

  subnet_ids         = var.private_subnet_ids
  security_group_ids = [aws_security_group.runtime.id]

  # Runtime sockets publish under dns_name=system-runtime so clients select
  # tenant fetch (:8081), internal dispatch (:8088), or control (:8082) by port.
  service_connect_namespace_arn = aws_service_discovery_http_namespace.this.arn
  service_connect_services = [
    {
      port_name                   = "system-loader"
      discovery_name              = "system-runtime"
      timeout_per_request_seconds = 0
      client_aliases              = [{ port = 8081, dns_name = "system-runtime" }]
    },
    {
      port_name                   = "system-internal"
      discovery_name              = "system-runtime-internal"
      timeout_per_request_seconds = 0
      client_aliases              = [{ port = 8088, dns_name = "system-runtime" }]
    },
    {
      port_name                   = "system-control"
      discovery_name              = "system-runtime-control"
      timeout_per_request_seconds = 0
      client_aliases              = [{ port = 8082, dns_name = "system-runtime" }]
    },
  ]

  depends_on = [aws_ecs_cluster_capacity_providers.this]
}

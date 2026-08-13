resource "aws_ecs_task_definition" "user_runtime" {
  family                   = "${var.name}-user-runtime"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.runtime_cpu
  memory                   = var.runtime_memory
  execution_role_arn       = aws_iam_role.exec.arn
  task_role_arn            = aws_iam_role.runtime_task.arn

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
          awslogs-stream-prefix = "ecs-redis-proxy"
        }
      }
    },
    {
      name        = "user-runtime"
      image       = var.workerd_image
      essential   = true
      entryPoint  = ["workerd"]
      command     = ["serve", "-b", "/app/dist/workerd-configs/user-runtime.bin", "--experimental"]
      stopTimeout = 20

      dependsOn = [{
        containerName = "redis-proxy"
        condition     = "HEALTHY"
      }]

      portMappings = [
        {
          name          = "runtime-http"
          containerPort = 8081
          protocol      = "tcp"
          appProtocol   = "http"
        },
        {
          name          = "runtime-internal"
          containerPort = 8088
          protocol      = "tcp"
          appProtocol   = "http"
        },
      ]

      environment = concat([
        { name = "REDIS_PROXY_URL", value = "http://127.0.0.1:7070" },
        local.log_level_env,
        { name = "ASSETS_CDN_BASE", value = var.assets_cdn_base },
      ], local.r2_s3_env)

      secrets = concat([
        { name = "R2_S3_ACCESS_KEY_ID", valueFrom = "${var.runtime_r2_secret_arn}:access_key_id::" },
        { name = "R2_S3_SECRET_ACCESS_KEY", valueFrom = "${var.runtime_r2_secret_arn}:secret_access_key::" },
      ], local.internal_auth_secrets)

      healthCheck = merge(local.workerd_health_check_thresholds, {
        command = ["CMD", "/usr/local/bin/http-hc", "http://127.0.0.1:8088/_healthz"]
      })

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.runtime.name
          awslogs-region        = var.region
          awslogs-stream-prefix = "ecs"
        }
      }
  }])

  lifecycle {
    create_before_destroy = true
  }
}

module "user_runtime_service" {
  source = "../ecs-service"

  name                   = "${var.name}-user-runtime"
  cluster_id             = aws_ecs_cluster.this.id
  task_definition_arn    = aws_ecs_task_definition.user_runtime.arn
  desired_count          = var.runtime_desired_count
  enable_execute_command = true
  deployment             = local.zero_downtime_deployment

  capacity_provider_strategies = local.fargate_stateless_capacity_provider_strategies

  subnet_ids         = var.private_subnet_ids
  security_group_ids = [aws_security_group.runtime.id]

  # Publish dns_name=user-runtime on both sockets. Gateway uses :8081 for
  # tenant fetch; scheduler/workflows use :8088 for internal dispatch.
  service_connect_namespace_arn = aws_service_discovery_http_namespace.this.arn
  service_connect_services = [
    {
      port_name                   = "runtime-http"
      discovery_name              = "user-runtime"
      timeout_per_request_seconds = 0
      client_aliases              = [{ port = 8081, dns_name = "user-runtime" }]
    },
    {
      port_name                   = "runtime-internal"
      discovery_name              = "user-runtime-internal"
      timeout_per_request_seconds = 0
      client_aliases              = [{ port = 8088, dns_name = "user-runtime" }]
    },
  ]

  depends_on = [aws_ecs_cluster_capacity_providers.this]
}

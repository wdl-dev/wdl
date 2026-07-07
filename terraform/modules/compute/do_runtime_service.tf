resource "aws_ecs_task_definition" "do_runtime" {
  family                   = "${var.name}-do-runtime"
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

  volume {
    name = "do-storage"

    efs_volume_configuration {
      file_system_id     = aws_efs_file_system.do_storage.id
      root_directory     = "/"
      transit_encryption = "ENABLED"
    }
  }

  container_definitions = jsonencode([
    {
      name              = "redis-proxy"
      image             = var.rust_image
      essential         = true
      command           = ["/redis-proxy"]
      memoryReservation = local.redis_proxy_memory_reservation
      stopTimeout       = 20

      environment = local.redis_proxy_env

      secrets = concat(local.secret_envelope_secrets, local.internal_auth_secrets)

      healthCheck = local.redis_proxy_health_check

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.do_runtime.name
          awslogs-region        = var.region
          awslogs-stream-prefix = "ecs-redis-proxy"
        }
      }
    },
    {
      name       = "do-runtime"
      image      = var.workerd_image
      essential  = true
      entryPoint = ["do-supervisor"]
      memory     = local.do_runtime_container_memory

      dependsOn = [{
        containerName = "redis-proxy"
        condition     = "HEALTHY"
      }]

      portMappings = [{
        name          = "do-http"
        containerPort = 8788
        protocol      = "tcp"
        appProtocol   = "http"
      }]

      mountPoints = [{
        sourceVolume  = "do-storage"
        containerPath = "/data/do"
        readOnly      = false
      }]

      environment = concat([
        { name = "REDIS_ADDR", value = local.redis_addr },
        { name = "REDIS_PROXY_URL", value = "http://127.0.0.1:7070" },
        local.log_level_env,
        { name = "ASSETS_CDN_BASE", value = var.assets_cdn_base },
        { name = "DO_TASK_CONTAINER_NAME", value = "do-runtime" },
        { name = "DO_OWNER_TTL_SECONDS", value = "120" },
        { name = "D1_QUERY_TIMEOUT_MS", value = "30000" },
        ], var.do_test_hooks_enabled ? [
        { name = "DO_TEST_HOOKS", value = "1" },
      ] : [], local.r2_s3_env)

      secrets = concat([
        { name = "R2_S3_ACCESS_KEY_ID", valueFrom = "${var.runtime_r2_secret_arn}:access_key_id::" },
        { name = "R2_S3_SECRET_ACCESS_KEY", valueFrom = "${var.runtime_r2_secret_arn}:secret_access_key::" },
      ], local.internal_auth_secrets)

      stopTimeout = 20

      healthCheck = merge(local.workerd_health_check_thresholds, {
        command = ["CMD", "/usr/local/bin/http-hc", "http://127.0.0.1:8788/healthz"]
      })

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.do_runtime.name
          awslogs-region        = var.region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])

  lifecycle {
    create_before_destroy = true

    precondition {
      condition = (
        local.do_runtime_container_memory > 0 &&
        local.do_runtime_container_memory <= var.runtime_memory - local.redis_proxy_memory_reservation - local.stateful_runtime_memory_headroom
      )
      error_message = "do_runtime_container_memory must leave task-level memory reservation for the redis-proxy sidecar and additional task headroom."
    }

    precondition {
      condition     = !var.do_test_hooks_enabled || can(regex("(^|-)test($|-)", var.name))
      error_message = "do_test_hooks_enabled may only be enabled for test-named compute stacks."
    }
  }
}

module "do_runtime_service" {
  source = "../ecs-service"

  name                   = "${var.name}-do-runtime"
  cluster_id             = aws_ecs_cluster.this.id
  task_definition_arn    = aws_ecs_task_definition.do_runtime.arn
  desired_count          = var.do_runtime_desired_count
  enable_execute_command = true
  deployment             = local.sequential_replacement_deployment

  availability_zone_rebalancing = "DISABLED"

  capacity_provider_strategies = local.fargate_ondemand_capacity_provider_strategies

  subnet_ids         = var.private_subnet_ids
  security_group_ids = [aws_security_group.runtime.id]

  service_connect_namespace_arn = aws_service_discovery_http_namespace.this.arn
  service_connect_services = [{
    port_name                   = "do-http"
    discovery_name              = "do-runtime"
    timeout_per_request_seconds = 0
    client_aliases              = [{ port = 8788, dns_name = "do-runtime" }]
  }]

  depends_on = [
    aws_ecs_cluster_capacity_providers.this,
    aws_efs_mount_target.do_storage,
  ]
}

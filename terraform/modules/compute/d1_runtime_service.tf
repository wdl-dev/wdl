resource "aws_ecs_task_definition" "d1_runtime" {
  family                   = "${var.name}-d1-runtime"
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
    name = "d1-storage"

    efs_volume_configuration {
      file_system_id     = aws_efs_file_system.d1_storage.id
      root_directory     = "/"
      transit_encryption = "ENABLED"
    }
  }

  container_definitions = jsonencode([{
    name       = "d1-runtime"
    image      = var.workerd_image
    essential  = true
    entryPoint = ["d1-supervisor"]
    memory     = local.d1_runtime_container_memory

    portMappings = [{
      name          = "d1-http"
      containerPort = 8787
      protocol      = "tcp"
      appProtocol   = "http"
    }]

    mountPoints = [{
      sourceVolume  = "d1-storage"
      containerPath = "/data/d1"
      readOnly      = false
    }]

    environment = concat([
      { name = "REDIS_ADDR", value = local.redis_addr },
      local.log_level_env,
      # Production identity comes from ECS_CONTAINER_METADATA_URI_V4 at first
      # owner claim. Do not set D1_TASK_ID/D1_TASK_ENDPOINT here unless the
      # task endpoint source is changed consistently.
      { name = "D1_TASK_CONTAINER_NAME", value = "d1-runtime" },
      { name = "D1_OWNER_TTL_SECONDS", value = "120" },
      { name = "D1_PROBE_TIMEOUT_MS", value = "500" },
      { name = "D1_QUERY_TIMEOUT_MS", value = "30000" },
      ], var.d1_test_hooks_enabled ? [
      { name = "D1_TEST_HOOKS", value = "1" },
    ] : [])

    secrets = local.internal_auth_secrets

    stopTimeout = 20

    healthCheck = merge(local.workerd_health_check_thresholds, {
      command = ["CMD", "/usr/local/bin/http-hc", "http://127.0.0.1:8787/healthz"]
    })

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.d1_runtime.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "ecs"
      }
    }
  }])

  lifecycle {
    create_before_destroy = true

    precondition {
      condition = (
        local.d1_runtime_container_memory > 0 &&
        local.d1_runtime_container_memory <= var.runtime_memory - local.stateful_runtime_memory_headroom
      )
      error_message = "d1_runtime_container_memory must be positive and leave task-level memory headroom."
    }

    precondition {
      condition     = !var.d1_test_hooks_enabled || can(regex("(^|-)test($|-)", var.name))
      error_message = "d1_test_hooks_enabled may only be enabled for test-named compute stacks."
    }
  }
}

module "d1_runtime_service" {
  source = "../ecs-service"

  name                   = "${var.name}-d1-runtime"
  cluster_id             = aws_ecs_cluster.this.id
  task_definition_arn    = aws_ecs_task_definition.d1_runtime.arn
  desired_count          = var.d1_runtime_desired_count
  enable_execute_command = true
  deployment             = local.sequential_replacement_deployment

  availability_zone_rebalancing = "DISABLED"

  capacity_provider_strategies = local.fargate_ondemand_capacity_provider_strategies

  subnet_ids         = var.private_subnet_ids
  security_group_ids = [aws_security_group.runtime.id]

  service_connect_namespace_arn = aws_service_discovery_http_namespace.this.arn
  service_connect_services = [{
    port_name                   = "d1-http"
    discovery_name              = "d1-runtime"
    timeout_per_request_seconds = 0
    client_aliases              = [{ port = 8787, dns_name = "d1-runtime" }]
  }]

  depends_on = [
    aws_ecs_cluster_capacity_providers.this,
    aws_efs_mount_target.d1_storage,
  ]
}

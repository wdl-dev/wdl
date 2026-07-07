resource "aws_ecs_task_definition" "scheduler" {
  family                   = "${var.name}-scheduler"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.scheduler_cpu
  memory                   = var.scheduler_memory
  execution_role_arn       = aws_iam_role.exec.arn
  task_role_arn            = aws_iam_role.scheduler_task.arn

  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }

  container_definitions = jsonencode([{
    name        = "scheduler"
    image       = var.rust_image
    essential   = true
    command     = ["/scheduler"]
    stopTimeout = 20

    environment = [
      { name = "REDIS_URL", value = "redis://${local.redis_addr}" },
      { name = "DATA_REDIS_URL", value = local.data_redis_url },
      { name = "RUNTIME_HOST", value = "user-runtime" },
      { name = "RUNTIME_PORT", value = "8088" },
      # `__system__`-ns loaded workers need system-runtime's private
      # outbound. dns_name is set by system_runtime_service.tf.
      { name = "SYSTEM_RUNTIME_HOST", value = "system-runtime" },
      { name = "SYSTEM_RUNTIME_PORT", value = "8088" },
      { name = "WORKFLOWS_HOST", value = "workflows" },
      { name = "WORKFLOWS_PORT", value = "9120" },
      local.log_level_env,
    ]

    secrets = local.internal_auth_secrets

    healthCheck = local.scheduler_health_check

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.scheduler.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "ecs"
      }
    }
  }])

  lifecycle {
    create_before_destroy = true
  }
}

module "scheduler_service" {
  source = "../ecs-service"

  name                   = "${var.name}-scheduler"
  cluster_id             = aws_ecs_cluster.this.id
  task_definition_arn    = aws_ecs_task_definition.scheduler.arn
  desired_count          = var.scheduler_desired_count
  enable_execute_command = true

  # Stop-before-start rollout. Multi-replica safety covers concurrent runtime
  # dispatch, not a zero-gap scheduler deploy; brief skips still follow CF
  # "skip missed on outage" semantics.
  deployment = local.stop_before_start_deployment

  availability_zone_rebalancing = "DISABLED"

  capacity_provider_strategies = local.fargate_ondemand_capacity_provider_strategies

  subnet_ids         = var.private_subnet_ids
  security_group_ids = [aws_security_group.scheduler.id]

  # Scheduler consumes user-runtime/system-runtime through Service Connect.
  # It's a client in the SC namespace — no service of its own to expose.
  service_connect_namespace_arn = aws_service_discovery_http_namespace.this.arn

  depends_on = [aws_ecs_cluster_capacity_providers.this]
}

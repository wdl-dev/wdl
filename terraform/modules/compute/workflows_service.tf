resource "aws_ecs_task_definition" "workflows" {
  family                   = "${var.name}-workflows"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.workflows_cpu
  memory                   = var.workflows_memory
  execution_role_arn       = aws_iam_role.exec.arn
  task_role_arn            = aws_iam_role.workflows_task.arn

  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }

  container_definitions = jsonencode([{
    name        = "workflows"
    image       = var.rust_image
    essential   = true
    command     = ["/workflows"]
    stopTimeout = 30

    portMappings = [{
      name          = "workflows-http"
      containerPort = 9120
      protocol      = "tcp"
      appProtocol   = "http"
    }]

    environment = [
      { name = "REDIS_URL", value = "redis://${local.redis_addr}" },
      { name = "CONTROL_REDIS_URL", value = "redis://${local.redis_addr}" },
      { name = "WORKFLOWS_REDIS_DB", value = "2" },
      { name = "WORKFLOWS_PORT", value = "9120" },
      # Long tool steps (e.g. a full deploy) can exceed the 60s code default and
      # get re-dispatched mid-step; give a dispatch one turn's headroom.
      { name = "WORKFLOWS_DISPATCH_TIMEOUT_MS", value = "120000" },
      { name = "RUNTIME_HOST", value = "user-runtime" },
      { name = "RUNTIME_PORT", value = "8088" },
      { name = "SYSTEM_RUNTIME_HOST", value = "system-runtime" },
      { name = "SYSTEM_RUNTIME_PORT", value = "8088" },
      { name = "DO_RUNTIME_HOST", value = "do-runtime" },
      { name = "DO_RUNTIME_PORT", value = "8788" },
      local.log_level_env,
    ]

    secrets = local.internal_auth_secrets

    healthCheck = local.workflows_health_check

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.workflows.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "ecs"
      }
    }
  }])

  lifecycle {
    create_before_destroy = true
  }
}

module "workflows_service" {
  source = "../ecs-service"

  name                   = "${var.name}-workflows"
  cluster_id             = aws_ecs_cluster.this.id
  task_definition_arn    = aws_ecs_task_definition.workflows.arn
  desired_count          = var.workflows_desired_count
  enable_execute_command = true
  deployment             = local.zero_downtime_deployment

  capacity_provider_strategies = local.fargate_ondemand_capacity_provider_strategies

  subnet_ids         = var.private_subnet_ids
  security_group_ids = [aws_security_group.workflows.id]

  service_connect_namespace_arn = aws_service_discovery_http_namespace.this.arn
  service_connect_services = [{
    port_name                   = "workflows-http"
    discovery_name              = "workflows"
    timeout_per_request_seconds = 0
    client_aliases              = [{ port = 9120, dns_name = "workflows" }]
  }]

  depends_on = [aws_ecs_cluster_capacity_providers.this]
}

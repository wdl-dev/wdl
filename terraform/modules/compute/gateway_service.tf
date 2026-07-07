resource "aws_ecs_task_definition" "gateway" {
  family                   = "${var.name}-gateway"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.gateway_cpu
  memory                   = var.gateway_memory
  execution_role_arn       = aws_iam_role.exec.arn
  task_role_arn            = aws_iam_role.gateway_task.arn

  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }

  container_definitions = jsonencode([{
    name        = "gateway"
    image       = var.workerd_image
    essential   = true
    entryPoint  = ["workerd"]
    command     = ["serve", "-b", "/app/dist/workerd-configs/gateway.bin"]
    stopTimeout = 20

    portMappings = [{
      name          = "http"
      containerPort = 8080
      protocol      = "tcp"
      appProtocol   = "http"
    }]

    environment = [
      { name = "REDIS_ADDR", value = local.redis_addr },
      { name = "PLATFORM_DOMAIN", value = local.platform_domain },
      local.log_level_env,
      # Must stay OUTSIDE platform_domain — a tenant named "admin"
      # would otherwise squat the ingress by subdomain grammar alone.
      { name = "ADMIN_HOST", value = var.admin_host },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.gateway.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "ecs"
      }
    }
  }])

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_lb_target_group" "gateway" {
  name        = "${var.name}-gw"
  vpc_id      = var.vpc_id
  port        = 8080
  protocol    = "HTTP"
  target_type = "ip"

  deregistration_delay = 10

  health_check {
    path                = "/healthz"
    healthy_threshold   = 2
    unhealthy_threshold = 2
    interval            = 5
    timeout             = 3
    matcher             = "200"
  }
}

resource "aws_lb_listener_rule" "gateway" {
  listener_arn = var.alb_https_listener_arn
  priority     = 4600

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.gateway.arn
  }

  condition {
    host_header {
      values = [var.platform_domain]
    }
  }
}

resource "aws_lb_listener_rule" "site" {
  count = var.site_host != "" ? 1 : 0

  listener_arn = var.alb_https_listener_arn
  priority     = 4611

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.gateway.arn
  }

  condition {
    host_header {
      values = [var.site_host]
    }
  }
}

resource "aws_lb_listener_rule" "site_www_redirect" {
  count = var.site_host != "" ? 1 : 0

  listener_arn = var.alb_https_listener_arn
  priority     = 4610

  action {
    type = "redirect"

    redirect {
      host        = var.site_host
      path        = "/#{path}"
      port        = "443"
      protocol    = "HTTPS"
      query       = "#{query}"
      status_code = "HTTP_301"
    }
  }

  condition {
    host_header {
      values = ["www.${var.site_host}"]
    }
  }
}

resource "aws_lb_listener_rule" "additional_public_hosts" {
  count = length(var.additional_public_hosts) > 0 ? 1 : 0

  listener_arn = var.alb_https_listener_arn
  priority     = 4612

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.gateway.arn
  }

  condition {
    host_header {
      values = var.additional_public_hosts
    }
  }
}

module "gateway_service" {
  source = "../ecs-service"

  name                   = "${var.name}-gateway"
  cluster_id             = aws_ecs_cluster.this.id
  task_definition_arn    = aws_ecs_task_definition.gateway.arn
  desired_count          = var.gateway_desired_count
  enable_execute_command = true
  deployment             = local.zero_downtime_deployment

  capacity_provider_strategies = local.fargate_stateless_capacity_provider_strategies

  subnet_ids         = var.private_subnet_ids
  security_group_ids = [aws_security_group.gateway.id]

  load_balancers = [{
    target_group_arn = aws_lb_target_group.gateway.arn
    container_name   = "gateway"
    container_port   = 8080
  }]

  # Gateway is a client here, not a server, so no Service Connect service
  # entry — it resolves user-runtime/system-runtime via their client aliases.
  service_connect_namespace_arn = aws_service_discovery_http_namespace.this.arn

  depends_on = [
    aws_ecs_cluster_capacity_providers.this,
    aws_lb_listener_rule.gateway,
    aws_lb_listener_rule.site,
    aws_lb_listener_rule.site_www_redirect,
    aws_lb_listener_rule.additional_public_hosts,
  ]
}

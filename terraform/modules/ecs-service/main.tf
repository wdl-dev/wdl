resource "aws_ecs_service" "this" {
  name                   = var.name
  cluster                = var.cluster_id
  task_definition        = var.task_definition_arn
  desired_count          = var.desired_count
  enable_execute_command = var.enable_execute_command

  availability_zone_rebalancing = var.availability_zone_rebalancing

  deployment_maximum_percent         = var.deployment.maximum_percent
  deployment_minimum_healthy_percent = var.deployment.minimum_healthy_percent

  dynamic "capacity_provider_strategy" {
    for_each = var.capacity_provider_strategies

    content {
      capacity_provider = capacity_provider_strategy.value.capacity_provider
      weight            = capacity_provider_strategy.value.weight
      base              = capacity_provider_strategy.value.base
    }
  }

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = var.security_group_ids
    assign_public_ip = var.assign_public_ip
  }

  dynamic "load_balancer" {
    for_each = var.load_balancers

    content {
      target_group_arn = load_balancer.value.target_group_arn
      container_name   = load_balancer.value.container_name
      container_port   = load_balancer.value.container_port
    }
  }

  dynamic "service_connect_configuration" {
    for_each = var.service_connect_namespace_arn == null ? [] : [var.service_connect_namespace_arn]

    content {
      enabled   = true
      namespace = service_connect_configuration.value

      dynamic "service" {
        for_each = var.service_connect_services

        content {
          port_name      = service.value.port_name
          discovery_name = service.value.discovery_name

          dynamic "timeout" {
            for_each = service.value.timeout_per_request_seconds == null ? [] : [service.value.timeout_per_request_seconds]

            content {
              per_request_timeout_seconds = timeout.value
            }
          }

          dynamic "client_alias" {
            for_each = service.value.client_aliases

            content {
              port     = client_alias.value.port
              dns_name = client_alias.value.dns_name
            }
          }
        }
      }
    }
  }
}

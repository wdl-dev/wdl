resource "aws_ecs_cluster" "this" {
  name = "${var.name}-cluster"

  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}

resource "aws_ecs_cluster_capacity_providers" "this" {
  cluster_name       = aws_ecs_cluster.this.name
  capacity_providers = ["FARGATE", "FARGATE_SPOT"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = var.spot_weight
  }

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = var.od_weight
  }
}

resource "aws_service_discovery_http_namespace" "this" {
  name = "${var.name}.wdl.local"
}

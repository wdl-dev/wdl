# ECS task security groups keep egress broad. Tenant-code egress is enforced
# inside workerd by config-user.capnp / do-runtime public-network bindings;
# these SGs only define which inbound service-to-service hops are accepted.
resource "aws_security_group" "gateway" {
  name        = "${var.name}-gateway"
  description = "Gateway tasks: ingress from ALB only"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group_rule" "gateway_from_alb" {
  type                     = "ingress"
  from_port                = 8080
  to_port                  = 8080
  protocol                 = "tcp"
  security_group_id        = aws_security_group.gateway.id
  source_security_group_id = var.alb_security_group_id
}

resource "aws_security_group" "runtime" {
  name        = "${var.name}-runtime"
  description = "Runtime tasks: user, system, D1, and DO ingress from gateway/private mesh"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ECS intentionally shares this SG across user, system, D1, and DO tasks. Rules
# sourced from or targeting it are therefore coarser than Kubernetes per-component
# NetworkPolicies; splitting those caller sets requires splitting this SG first.

# Range covers :8081 (loader sockets) + :8082 (system-runtime control).
resource "aws_security_group_rule" "runtime_from_gateway" {
  type                     = "ingress"
  from_port                = 8081
  to_port                  = 8082
  protocol                 = "tcp"
  security_group_id        = aws_security_group.runtime.id
  source_security_group_id = aws_security_group.gateway.id
}

resource "aws_security_group" "scheduler" {
  name        = "${var.name}-scheduler"
  description = "Scheduler tasks: no inbound service ports"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group_rule" "runtime_from_scheduler" {
  type                     = "ingress"
  from_port                = 8088
  to_port                  = 8088
  protocol                 = "tcp"
  security_group_id        = aws_security_group.runtime.id
  source_security_group_id = aws_security_group.scheduler.id
}

resource "aws_security_group" "workflows" {
  name        = "${var.name}-workflows"
  description = "Workflows runtime tasks: ingress from runtime and scheduler"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group_rule" "workflows_from_runtime" {
  type                     = "ingress"
  from_port                = 9120
  to_port                  = 9120
  protocol                 = "tcp"
  security_group_id        = aws_security_group.workflows.id
  source_security_group_id = aws_security_group.runtime.id
}

resource "aws_security_group_rule" "workflows_from_scheduler" {
  type                     = "ingress"
  from_port                = 9120
  to_port                  = 9120
  protocol                 = "tcp"
  security_group_id        = aws_security_group.workflows.id
  source_security_group_id = aws_security_group.scheduler.id
}

resource "aws_security_group_rule" "runtime_from_workflows" {
  type                     = "ingress"
  from_port                = 8088
  to_port                  = 8088
  protocol                 = "tcp"
  security_group_id        = aws_security_group.runtime.id
  source_security_group_id = aws_security_group.workflows.id
}

resource "aws_security_group_rule" "runtime_d1_from_runtime" {
  type                     = "ingress"
  from_port                = 8787
  to_port                  = 8787
  protocol                 = "tcp"
  security_group_id        = aws_security_group.runtime.id
  source_security_group_id = aws_security_group.runtime.id
}

resource "aws_security_group_rule" "runtime_do_from_runtime" {
  type                     = "ingress"
  from_port                = 8788
  to_port                  = 8788
  protocol                 = "tcp"
  security_group_id        = aws_security_group.runtime.id
  source_security_group_id = aws_security_group.runtime.id
}

resource "aws_security_group_rule" "runtime_do_from_workflows" {
  type                     = "ingress"
  from_port                = 8788
  to_port                  = 8788
  protocol                 = "tcp"
  security_group_id        = aws_security_group.runtime.id
  source_security_group_id = aws_security_group.workflows.id
}

resource "aws_security_group_rule" "valkey_from_scheduler" {
  type                     = "ingress"
  from_port                = var.valkey_port
  to_port                  = var.valkey_port
  protocol                 = "tcp"
  security_group_id        = var.valkey_security_group_id
  source_security_group_id = aws_security_group.scheduler.id
}

# Valkey accepts from both gateway and runtime task SGs.
resource "aws_security_group_rule" "valkey_from_gateway" {
  type                     = "ingress"
  from_port                = var.valkey_port
  to_port                  = var.valkey_port
  protocol                 = "tcp"
  security_group_id        = var.valkey_security_group_id
  source_security_group_id = aws_security_group.gateway.id
}

resource "aws_security_group_rule" "valkey_from_runtime" {
  type                     = "ingress"
  from_port                = var.valkey_port
  to_port                  = var.valkey_port
  protocol                 = "tcp"
  security_group_id        = var.valkey_security_group_id
  source_security_group_id = aws_security_group.runtime.id
}

resource "aws_security_group_rule" "valkey_from_workflows" {
  type                     = "ingress"
  from_port                = var.valkey_port
  to_port                  = var.valkey_port
  protocol                 = "tcp"
  security_group_id        = var.valkey_security_group_id
  source_security_group_id = aws_security_group.workflows.id
}

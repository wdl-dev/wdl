data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  azs                = slice(data.aws_availability_zones.available.names, 0, 3)
  public_subnets     = { for idx, cidr in var.public_subnet_cidrs : idx => cidr }
  private_subnets    = { for idx, cidr in var.private_subnet_cidrs : idx => cidr }
  assets_cdn_enabled = var.assets_cdn_domain != ""
  site_host_enabled  = var.site_host != ""
  site_www_host      = local.site_host_enabled ? "www.${var.site_host}" : ""
  public_alb_hosts = distinct(compact(concat(
    [var.site_host, local.site_www_host],
    var.additional_public_hosts,
  )))
  public_alb_hosts_enabled = length(local.public_alb_hosts) > 0
  public_alb_certificate_sans = (
    length(local.public_alb_hosts) > 1
    ? slice(local.public_alb_hosts, 1, length(local.public_alb_hosts))
    : []
  )
}

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.name_prefix}-vpc"
  }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name = "${var.name_prefix}-igw"
  }
}

resource "aws_subnet" "public" {
  for_each = local.public_subnets

  vpc_id                  = aws_vpc.this.id
  cidr_block              = each.value
  availability_zone       = local.azs[tonumber(each.key)]
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.name_prefix}-public-${tonumber(each.key) + 1}"
    Tier = "public"
  }
}

resource "aws_subnet" "private" {
  for_each = local.private_subnets

  vpc_id            = aws_vpc.this.id
  cidr_block        = each.value
  availability_zone = local.azs[tonumber(each.key)]

  tags = {
    Name = "${var.name_prefix}-private-${tonumber(each.key) + 1}"
    Tier = "private"
  }
}

resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name = "${var.name_prefix}-nat"
  }
}

resource "aws_nat_gateway" "this" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public["0"].id

  tags = {
    Name = "${var.name_prefix}-nat"
  }

  depends_on = [aws_internet_gateway.this]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = {
    Name = "${var.name_prefix}-public"
  }
}

resource "aws_route_table_association" "public" {
  for_each = aws_subnet.public

  subnet_id      = each.value.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.this.id
  }

  tags = {
    Name = "${var.name_prefix}-private"
  }
}

resource "aws_route_table_association" "private" {
  for_each = aws_subnet.private

  subnet_id      = each.value.id
  route_table_id = aws_route_table.private.id
}

resource "aws_security_group" "alb" {
  name        = "${var.name_prefix}-alb"
  description = "Public ALB ingress"
  vpc_id      = aws_vpc.this.id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.name_prefix}-alb"
  }
}

resource "aws_lb" "this" {
  name               = "${var.name_prefix}-alb"
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = [for subnet in aws_subnet.public : subnet.id]

  tags = {
    Name = "${var.name_prefix}-alb"
  }
}

resource "aws_acm_certificate" "alb" {
  domain_name               = var.admin_host
  subject_alternative_names = [var.platform_domain]
  validation_method         = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_acm_certificate" "assets_cdn" {
  provider = aws.us_east_1
  count    = local.assets_cdn_enabled ? 1 : 0

  domain_name       = var.assets_cdn_domain
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_acm_certificate" "site" {
  count = local.public_alb_hosts_enabled ? 1 : 0

  domain_name               = local.public_alb_hosts[0]
  subject_alternative_names = local.public_alb_certificate_sans
  validation_method         = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_acm_certificate_validation" "alb" {
  count           = var.validate_certificates ? 1 : 0
  certificate_arn = aws_acm_certificate.alb.arn
}

resource "aws_acm_certificate_validation" "assets_cdn" {
  provider = aws.us_east_1
  count    = var.validate_certificates && local.assets_cdn_enabled ? 1 : 0

  certificate_arn = aws_acm_certificate.assets_cdn[0].arn
}

resource "aws_acm_certificate_validation" "site" {
  count = var.validate_certificates && local.public_alb_hosts_enabled ? 1 : 0

  certificate_arn = aws_acm_certificate.site[0].arn
}

resource "aws_lb_listener" "http" {
  count             = var.validate_certificates ? 1 : 0
  load_balancer_arn = aws_lb.this.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = "redirect"
    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }
}

resource "aws_lb_listener" "https" {
  count             = var.validate_certificates ? 1 : 0
  load_balancer_arn = aws_lb.this.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = aws_acm_certificate_validation.alb[0].certificate_arn

  default_action {
    type = "fixed-response"
    fixed_response {
      content_type = "application/json"
      status_code  = "404"
      message_body = "{\"error\":\"not_found\"}"
    }
  }
}

resource "aws_lb_listener_certificate" "site" {
  count = var.validate_certificates && local.public_alb_hosts_enabled ? 1 : 0

  listener_arn    = aws_lb_listener.https[0].arn
  certificate_arn = aws_acm_certificate_validation.site[0].certificate_arn

  lifecycle {
    create_before_destroy = true
  }
}

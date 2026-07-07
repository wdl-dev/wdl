locals {
  redis_addr                       = "${var.valkey_host}:${var.valkey_port}"
  data_redis_url                   = "redis://${local.redis_addr}/1"
  stateful_runtime_memory_headroom = 128
  redis_proxy_memory_reservation   = 64
  d1_runtime_container_memory = coalesce(
    var.d1_runtime_container_memory,
    var.runtime_memory - local.stateful_runtime_memory_headroom,
  )
  do_runtime_container_memory = coalesce(
    var.do_runtime_container_memory,
    var.runtime_memory - local.redis_proxy_memory_reservation - local.stateful_runtime_memory_headroom,
  )

  # PLATFORM_DOMAIN is the base hostname gateway uses to split ns from host
  # (regex builds "<ns>.<PLATFORM_DOMAIN>"). platform_domain carries a leading
  # "*." for the ALB host_header rule; strip it for the env var.
  platform_domain = trimprefix(var.platform_domain, "*.")

  aws_s3_endpoint = "https://s3.${var.region}.amazonaws.com"
  log_level_env   = { name = "LOG_LEVEL", value = var.log_level }

  redis_proxy_env = [
    { name = "REDIS_URL", value = "redis://${local.redis_addr}" },
    { name = "DATA_REDIS_URL", value = local.data_redis_url },
    { name = "REDIS_PROXY_PORT", value = "7070" },
  ]

  secret_envelope_secrets = [
    { name = "SECRET_ENVELOPE_LOCAL_KEY_B64", valueFrom = "${var.secret_envelope_secret_arn}:local_key_b64::" },
    { name = "SECRET_ENVELOPE_KID", valueFrom = "${var.secret_envelope_secret_arn}:kid::" },
  ]

  internal_auth_secret = [
    { name = "WDL_INTERNAL_AUTH_TOKEN", valueFrom = aws_secretsmanager_secret.internal_auth_token.arn },
  ]
  internal_auth_previous_secret = var.internal_auth_previous_token_secret_arn == "" ? [] : [
    { name = "WDL_INTERNAL_AUTH_PREVIOUS_TOKEN", valueFrom = var.internal_auth_previous_token_secret_arn },
  ]
  internal_auth_secrets = concat(local.internal_auth_secret, local.internal_auth_previous_secret)

  redis_proxy_health_check = {
    command     = ["CMD", "/redis-proxy", "healthcheck"]
    interval    = 5
    timeout     = 5
    retries     = 3
    startPeriod = 5
  }

  # Tight thresholds are safe because the d1/do healthz handler lives in the
  # main worker isolate, separate from the actor isolates that run sync SQL —
  # long queries do not block probes. d1/do additionally return 503 during
  # drain so ECS pulls them from Service Connect routing.
  workerd_health_check_thresholds = {
    interval    = 10
    timeout     = 3
    retries     = 2
    startPeriod = 5
  }

  scheduler_health_check = {
    command     = ["CMD", "/scheduler", "healthcheck"]
    interval    = 5
    timeout     = 5
    retries     = 3
    startPeriod = 5
  }

  workflows_health_check = {
    command     = ["CMD", "/workflows", "healthcheck"]
    interval    = 5
    timeout     = 5
    retries     = 3
    startPeriod = 5
  }

  r2_s3_env = [
    { name = "R2_S3_BUCKET", value = var.r2_bucket },
    { name = "R2_S3_REGION", value = var.region },
    { name = "R2_S3_ENDPOINT", value = local.aws_s3_endpoint },
  ]

  # ECS starts a full replacement set, waits for health, then drains old tasks.
  zero_downtime_deployment = {
    maximum_percent         = 200
    minimum_healthy_percent = 100
  }

  stop_before_start_deployment = {
    maximum_percent         = 100
    minimum_healthy_percent = 0
  }

  sequential_replacement_deployment = {
    maximum_percent         = 100
    minimum_healthy_percent = 50
  }

  fargate_stateless_capacity_provider_strategies = [
    { capacity_provider = "FARGATE", base = 1, weight = var.od_weight },
    { capacity_provider = "FARGATE_SPOT", weight = var.spot_weight },
  ]

  fargate_ondemand_capacity_provider_strategies = [
    { capacity_provider = "FARGATE", weight = 1 },
  ]
}

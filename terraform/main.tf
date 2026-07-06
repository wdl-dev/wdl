locals {
  name = var.name_prefix
}

module "data" {
  source = "./modules/data"

  name                           = local.name
  vpc_id                         = var.vpc_id
  private_subnet_ids             = var.private_subnet_ids
  assets_cdn_domain              = var.assets_cdn_domain
  assets_cdn_acm_certificate_arn = var.assets_cdn_acm_certificate_arn
}

module "compute" {
  source = "./modules/compute"

  name                    = local.name
  region                  = var.region
  vpc_id                  = var.vpc_id
  private_subnet_ids      = var.private_subnet_ids
  alb_https_listener_arn  = var.alb_https_listener_arn
  alb_security_group_id   = var.alb_security_group_id
  platform_domain         = var.platform_domain
  admin_host              = var.admin_host
  site_host               = var.site_host
  additional_public_hosts = var.additional_public_hosts

  rust_image    = var.rust_image
  workerd_image = var.workerd_image

  valkey_host              = module.data.valkey_primary_endpoint
  valkey_port              = module.data.valkey_port
  valkey_security_group_id = module.data.valkey_security_group_id

  assets_bucket                           = module.data.assets_bucket_name
  r2_bucket                               = module.data.r2_bucket_name
  control_s3_secret_arn                   = module.data.control_s3_secret_arn
  runtime_r2_secret_arn                   = module.data.runtime_r2_secret_arn
  secret_envelope_secret_arn              = module.data.secret_envelope_secret_arn
  internal_auth_previous_token_secret_arn = var.internal_auth_previous_token_secret_arn

  gateway_desired_count       = var.gateway_desired_count
  runtime_desired_count       = var.runtime_desired_count
  d1_runtime_desired_count    = var.d1_runtime_desired_count
  do_runtime_desired_count    = var.do_runtime_desired_count
  d1_test_hooks_enabled       = var.d1_test_hooks_enabled
  do_test_hooks_enabled       = var.do_test_hooks_enabled
  scheduler_desired_count     = var.scheduler_desired_count
  workflows_desired_count     = var.workflows_desired_count
  gateway_cpu                 = var.gateway_cpu
  gateway_memory              = var.gateway_memory
  system_runtime_cpu          = var.system_runtime_cpu
  system_runtime_memory       = var.system_runtime_memory
  runtime_cpu                 = var.runtime_cpu
  runtime_memory              = var.runtime_memory
  d1_runtime_container_memory = var.d1_runtime_container_memory
  do_runtime_container_memory = var.do_runtime_container_memory
  scheduler_cpu               = var.scheduler_cpu
  scheduler_memory            = var.scheduler_memory
  workflows_cpu               = var.workflows_cpu
  workflows_memory            = var.workflows_memory
  spot_weight                 = var.spot_weight
  od_weight                   = var.od_weight

  log_level          = var.log_level
  log_retention_days = var.log_retention_days
  assets_cdn_base    = module.data.assets_cdn_base
}

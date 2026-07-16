variable "name" { type = string }
variable "region" { type = string }

variable "vpc_id" { type = string }
variable "private_subnet_ids" { type = list(string) }

variable "alb_https_listener_arn" { type = string }
variable "alb_security_group_id" { type = string }
variable "platform_domain" { type = string }
# Must live OUTSIDE platform_domain or a tenant named "admin" squats
# the short-circuit by grammar alone. Same string is injected into
# gateway as the ADMIN_HOST env var.
variable "admin_host" { type = string }
variable "site_host" {
  type    = string
  default = ""
}
variable "additional_public_hosts" {
  type    = list(string)
  default = []
}

variable "workerd_image" { type = string }
variable "rust_image" { type = string }

variable "valkey_host" { type = string }
variable "valkey_port" { type = number }
variable "valkey_security_group_id" { type = string }

variable "assets_bucket" { type = string }
variable "r2_bucket" { type = string }
# JSON secret with access_key_id / secret_access_key — the SigV4 signer
# signs with these. Created in module.data.
variable "control_s3_secret_arn" { type = string }
variable "runtime_r2_secret_arn" { type = string }
variable "secret_envelope_secret_arn" { type = string }
variable "internal_auth_previous_token_secret_arn" {
  type    = string
  default = ""
}

variable "gateway_desired_count" { type = number }
variable "runtime_desired_count" { type = number }
variable "d1_runtime_desired_count" { type = number }
variable "do_runtime_desired_count" { type = number }
variable "d1_test_hooks_enabled" { type = bool }
variable "scheduler_desired_count" { type = number }
variable "workflows_desired_count" { type = number }
variable "gateway_cpu" { type = number }
variable "gateway_memory" { type = number }
variable "system_runtime_cpu" { type = number }
variable "system_runtime_memory" { type = number }
variable "runtime_cpu" { type = number }
variable "runtime_memory" { type = number }
variable "d1_runtime_container_memory" {
  type    = number
  default = null
}
variable "do_runtime_container_memory" {
  type    = number
  default = null
}
variable "scheduler_cpu" { type = number }
variable "scheduler_memory" { type = number }
variable "workflows_cpu" { type = number }
variable "workflows_memory" { type = number }
variable "spot_weight" { type = number }
variable "od_weight" { type = number }

variable "log_level" { type = string }
variable "log_retention_days" { type = number }
variable "assets_cdn_base" { type = string }

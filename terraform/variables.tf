variable "region" {
  type        = string
  default     = "ap-east-1"
  description = "AWS region for the WDL infrastructure."
}

variable "environment" {
  type        = string
  default     = "test"
  description = "Value for the AWS default Environment tag. Resource naming is controlled by name_prefix."
  validation {
    condition     = contains(["test", "staging", "prod"], var.environment)
    error_message = "environment must be one of: test, staging, prod."
  }
}

variable "name_prefix" {
  type        = string
  description = "Resource name prefix, e.g. wdl-test."
}

variable "platform_domain" {
  type        = string
  description = "Wildcard host for tenant traffic, e.g. *.wdl.sh."
  validation {
    condition = (
      startswith(var.platform_domain, "*.") &&
      !strcontains(var.platform_domain, "://") &&
      !strcontains(var.platform_domain, "/") &&
      !strcontains(var.platform_domain, ":") &&
      var.platform_domain == trimspace(var.platform_domain)
    )
    error_message = "platform_domain must be a wildcard host such as *.wdl.sh, without scheme, path, port, or surrounding whitespace."
  }
}

variable "admin_host" {
  type        = string
  description = <<-EOT
    Exact host for the control plane, e.g. api.wdl.dev.
    Must be covered by the ACM cert on the existing ALB listener and SHOULD
    sit outside platform_domain's wildcard — otherwise a tenant named "admin"
    could squat the short-circuit if env.ADMIN_HOST ever drifts.
  EOT
  validation {
    condition = (
      length(var.admin_host) > 0 &&
      var.admin_host == trimspace(var.admin_host) &&
      !startswith(var.admin_host, "*.") &&
      !strcontains(var.admin_host, "://") &&
      !strcontains(var.admin_host, "/") &&
      !strcontains(var.admin_host, ":")
    )
    error_message = "admin_host must be an exact host such as api.wdl.dev, without wildcard, scheme, path, port, or surrounding whitespace."
  }
}

variable "site_host" {
  type        = string
  default     = ""
  description = "Optional exact canonical public site host that should forward from the ALB to the WDL gateway, e.g. wdl.dev. The www host is derived automatically; WDL control still owns host declarations and route patterns."
  validation {
    condition = (
      var.site_host == "" || (
        var.site_host == trimspace(var.site_host) &&
        !startswith(var.site_host, "www.") &&
        !startswith(var.site_host, "*.") &&
        !strcontains(var.site_host, "://") &&
        !strcontains(var.site_host, "/") &&
        !strcontains(var.site_host, ":")
      )
    )
    error_message = "site_host must be empty or an exact canonical host such as wdl.dev, without www prefix, wildcard, scheme, path, port, or surrounding whitespace."
  }
}

variable "additional_public_hosts" {
  type        = list(string)
  default     = []
  description = "Additional exact public hosts that should forward from the ALB to the WDL gateway, e.g. chat.wdl.dev."
  validation {
    condition = alltrue([
      for host in var.additional_public_hosts : (
        length(host) > 0 &&
        host == trimspace(host) &&
        !startswith(host, "*.") &&
        !strcontains(host, "://") &&
        !strcontains(host, "/") &&
        !strcontains(host, ":")
      )
    ]) && length(var.additional_public_hosts) == length(distinct(var.additional_public_hosts))
    error_message = "additional_public_hosts must contain unique exact hosts such as chat.wdl.dev, without wildcard, scheme, path, port, or surrounding whitespace."
  }
}

variable "assets_cdn_domain" {
  type        = string
  default     = ""
  description = "Custom domain CNAME for the CloudFront distribution in front of the assets bucket, e.g. assets.wdl.dev."
}

variable "assets_cdn_acm_certificate_arn" {
  type        = string
  default     = ""
  description = "ACM certificate ARN for the CloudFront assets CDN alias. CloudFront certificates must be in us-east-1. Leave empty to skip creating the distribution."
}

variable "internal_auth_previous_token_secret_arn" {
  type        = string
  default     = ""
  description = "Optional Secrets Manager ARN for the previous internal mesh auth token during rotation. Leave empty outside rotation windows."
}

# ---- Pre-existing infrastructure (you supply these) ------------------------

variable "vpc_id" {
  type        = string
  description = "Existing VPC id."
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "Private subnet ids across at least three AZs for ECS tasks, EFS mount targets, and ElastiCache."
  validation {
    condition     = length(var.private_subnet_ids) >= 3
    error_message = "Provide at least three private subnet ids across different AZs."
  }
}

variable "alb_https_listener_arn" {
  type        = string
  description = "Existing HTTPS:443 listener arn on the ALB."
}

variable "alb_security_group_id" {
  type        = string
  description = "Security group attached to the ALB (used to allow ingress into gateway tasks)."
}

# ---- Capacity / scaling ----------------------------------------------------

variable "gateway_desired_count" {
  type    = number
  default = 2
}

variable "runtime_desired_count" {
  type    = number
  default = 2
}

variable "d1_runtime_desired_count" {
  type    = number
  default = 2
}

variable "do_runtime_desired_count" {
  type    = number
  default = 2
}

variable "d1_test_hooks_enabled" {
  type        = bool
  default     = false
  description = "Enable D1 internal test hooks in d1-runtime. Keep false outside disposable/test environments."
}

variable "do_test_hooks_enabled" {
  type        = bool
  default     = false
  description = "Enable DO internal test hooks in do-runtime. Keep false outside disposable/test environments."
}

variable "gateway_cpu" {
  type    = number
  default = 512
  validation {
    condition     = contains([256, 512, 1024, 2048, 4096, 8192, 16384, 32768], var.gateway_cpu)
    error_message = "gateway_cpu must be a valid ECS Fargate task CPU value."
  }
}

variable "gateway_memory" {
  type    = number
  default = 1024
  validation {
    condition = (
      (var.gateway_cpu == 256 && contains([512, 1024, 2048], var.gateway_memory)) ||
      (var.gateway_cpu == 512 && contains([1024, 2048, 3072, 4096], var.gateway_memory)) ||
      (var.gateway_cpu == 1024 && contains([2048, 3072, 4096, 5120, 6144, 7168, 8192], var.gateway_memory)) ||
      (var.gateway_cpu == 2048 && var.gateway_memory >= 4096 && var.gateway_memory <= 16384 && var.gateway_memory % 1024 == 0) ||
      (var.gateway_cpu == 4096 && var.gateway_memory >= 8192 && var.gateway_memory <= 30720 && var.gateway_memory % 1024 == 0) ||
      (var.gateway_cpu == 8192 && var.gateway_memory >= 16384 && var.gateway_memory <= 61440 && var.gateway_memory % 4096 == 0) ||
      (var.gateway_cpu == 16384 && var.gateway_memory >= 32768 && var.gateway_memory <= 122880 && var.gateway_memory % 8192 == 0) ||
      (var.gateway_cpu == 32768 && contains([61440, 122880, 249856], var.gateway_memory))
    )
    error_message = "gateway_memory must be a valid ECS Fargate task memory value for gateway_cpu."
  }
}

variable "system_runtime_cpu" {
  type    = number
  default = 512
  validation {
    condition     = contains([256, 512, 1024, 2048, 4096, 8192, 16384, 32768], var.system_runtime_cpu)
    error_message = "system_runtime_cpu must be a valid ECS Fargate task CPU value."
  }
}

variable "system_runtime_memory" {
  type    = number
  default = 1024
  validation {
    condition = (
      (var.system_runtime_cpu == 256 && contains([512, 1024, 2048], var.system_runtime_memory)) ||
      (var.system_runtime_cpu == 512 && contains([1024, 2048, 3072, 4096], var.system_runtime_memory)) ||
      (var.system_runtime_cpu == 1024 && contains([2048, 3072, 4096, 5120, 6144, 7168, 8192], var.system_runtime_memory)) ||
      (var.system_runtime_cpu == 2048 && var.system_runtime_memory >= 4096 && var.system_runtime_memory <= 16384 && var.system_runtime_memory % 1024 == 0) ||
      (var.system_runtime_cpu == 4096 && var.system_runtime_memory >= 8192 && var.system_runtime_memory <= 30720 && var.system_runtime_memory % 1024 == 0) ||
      (var.system_runtime_cpu == 8192 && var.system_runtime_memory >= 16384 && var.system_runtime_memory <= 61440 && var.system_runtime_memory % 4096 == 0) ||
      (var.system_runtime_cpu == 16384 && var.system_runtime_memory >= 32768 && var.system_runtime_memory <= 122880 && var.system_runtime_memory % 8192 == 0) ||
      (var.system_runtime_cpu == 32768 && contains([61440, 122880, 249856], var.system_runtime_memory))
    )
    error_message = "system_runtime_memory must be a valid ECS Fargate task memory value for system_runtime_cpu."
  }
}

variable "runtime_cpu" {
  type    = number
  default = 1024
  validation {
    condition     = contains([256, 512, 1024, 2048, 4096, 8192, 16384, 32768], var.runtime_cpu)
    error_message = "runtime_cpu must be a valid ECS Fargate task CPU value."
  }
}

variable "runtime_memory" {
  type    = number
  default = 2048
  validation {
    condition = (
      (var.runtime_cpu == 256 && contains([512, 1024, 2048], var.runtime_memory)) ||
      (var.runtime_cpu == 512 && contains([1024, 2048, 3072, 4096], var.runtime_memory)) ||
      (var.runtime_cpu == 1024 && contains([2048, 3072, 4096, 5120, 6144, 7168, 8192], var.runtime_memory)) ||
      (var.runtime_cpu == 2048 && var.runtime_memory >= 4096 && var.runtime_memory <= 16384 && var.runtime_memory % 1024 == 0) ||
      (var.runtime_cpu == 4096 && var.runtime_memory >= 8192 && var.runtime_memory <= 30720 && var.runtime_memory % 1024 == 0) ||
      (var.runtime_cpu == 8192 && var.runtime_memory >= 16384 && var.runtime_memory <= 61440 && var.runtime_memory % 4096 == 0) ||
      (var.runtime_cpu == 16384 && var.runtime_memory >= 32768 && var.runtime_memory <= 122880 && var.runtime_memory % 8192 == 0) ||
      (var.runtime_cpu == 32768 && contains([61440, 122880, 249856], var.runtime_memory))
    )
    error_message = "runtime_memory must be a valid ECS Fargate task memory value for runtime_cpu."
  }
}

variable "d1_runtime_container_memory" {
  type        = number
  default     = null
  description = "Optional hard memory limit, in MiB, for the d1-runtime container. Defaults to runtime_memory minus 128 MiB of task-level headroom."
  validation {
    condition     = var.d1_runtime_container_memory == null || var.d1_runtime_container_memory > 0
    error_message = "d1_runtime_container_memory must be null or a positive number of MiB."
  }
}

variable "do_runtime_container_memory" {
  type        = number
  default     = null
  description = "Optional hard memory limit, in MiB, for the do-runtime container. Defaults to runtime_memory minus the redis-proxy sidecar memory reservation and 128 MiB of task-level headroom."
  validation {
    condition     = var.do_runtime_container_memory == null || var.do_runtime_container_memory > 0
    error_message = "do_runtime_container_memory must be null or a positive number of MiB."
  }
}

variable "scheduler_desired_count" {
  type        = number
  default     = 1
  description = "Scheduler task count. Defaults to 1; current dispatch paths are multi-replica safe, so raise after capacity review."
}

variable "workflows_desired_count" {
  type        = number
  default     = 2
  description = "Workflow runtime replicas. DB2 run leases fence duplicate dispatch, so replicas are for availability and throughput."
}

variable "scheduler_cpu" {
  type    = number
  default = 1024
  validation {
    condition     = contains([256, 512, 1024, 2048, 4096, 8192, 16384, 32768], var.scheduler_cpu)
    error_message = "scheduler_cpu must be a valid ECS Fargate task CPU value."
  }
}

variable "scheduler_memory" {
  type    = number
  default = 2048
  validation {
    condition = (
      (var.scheduler_cpu == 256 && contains([512, 1024, 2048], var.scheduler_memory)) ||
      (var.scheduler_cpu == 512 && contains([1024, 2048, 3072, 4096], var.scheduler_memory)) ||
      (var.scheduler_cpu == 1024 && contains([2048, 3072, 4096, 5120, 6144, 7168, 8192], var.scheduler_memory)) ||
      (var.scheduler_cpu == 2048 && var.scheduler_memory >= 4096 && var.scheduler_memory <= 16384 && var.scheduler_memory % 1024 == 0) ||
      (var.scheduler_cpu == 4096 && var.scheduler_memory >= 8192 && var.scheduler_memory <= 30720 && var.scheduler_memory % 1024 == 0) ||
      (var.scheduler_cpu == 8192 && var.scheduler_memory >= 16384 && var.scheduler_memory <= 61440 && var.scheduler_memory % 4096 == 0) ||
      (var.scheduler_cpu == 16384 && var.scheduler_memory >= 32768 && var.scheduler_memory <= 122880 && var.scheduler_memory % 8192 == 0) ||
      (var.scheduler_cpu == 32768 && contains([61440, 122880, 249856], var.scheduler_memory))
    )
    error_message = "scheduler_memory must be a valid ECS Fargate task memory value for scheduler_cpu."
  }
}

variable "workflows_cpu" {
  type    = number
  default = 512
  validation {
    condition     = contains([256, 512, 1024, 2048, 4096, 8192, 16384, 32768], var.workflows_cpu)
    error_message = "workflows_cpu must be a valid ECS Fargate task CPU value."
  }
}

variable "workflows_memory" {
  type    = number
  default = 1024
  validation {
    condition = (
      (var.workflows_cpu == 256 && contains([512, 1024, 2048], var.workflows_memory)) ||
      (var.workflows_cpu == 512 && contains([1024, 2048, 3072, 4096], var.workflows_memory)) ||
      (var.workflows_cpu == 1024 && contains([2048, 3072, 4096, 5120, 6144, 7168, 8192], var.workflows_memory)) ||
      (var.workflows_cpu == 2048 && var.workflows_memory >= 4096 && var.workflows_memory <= 16384 && var.workflows_memory % 1024 == 0) ||
      (var.workflows_cpu == 4096 && var.workflows_memory >= 8192 && var.workflows_memory <= 30720 && var.workflows_memory % 1024 == 0) ||
      (var.workflows_cpu == 8192 && var.workflows_memory >= 16384 && var.workflows_memory <= 61440 && var.workflows_memory % 4096 == 0) ||
      (var.workflows_cpu == 16384 && var.workflows_memory >= 32768 && var.workflows_memory <= 122880 && var.workflows_memory % 8192 == 0) ||
      (var.workflows_cpu == 32768 && contains([61440, 122880, 249856], var.workflows_memory))
    )
    error_message = "workflows_memory must be a valid ECS Fargate task memory value for workflows_cpu."
  }
}

variable "spot_weight" {
  type        = number
  default     = 3
  description = "FARGATE_SPOT weight for stateless gateway and runtime service capacity."
  validation {
    condition     = var.spot_weight > 0
    error_message = "spot_weight must be greater than zero."
  }
}

variable "od_weight" {
  type        = number
  default     = 1
  description = "FARGATE on-demand weight for stateless gateway and runtime service capacity."
  validation {
    condition     = var.od_weight > 0
    error_message = "od_weight must be greater than zero."
  }
}

# ---- Image tags ------------------------------------------------------------

variable "rust_image" {
  type        = string
  default     = "docker.io/getwdl/wdl-rust:latest"
  description = "Full image reference for redis-proxy, scheduler, and workflows."
}

variable "workerd_image" {
  type        = string
  default     = "docker.io/getwdl/wdl-workerd:latest"
  description = "Full image reference for gateway, control, runtime, D1, and Durable Objects workerd services."
}

# ---- Observability ---------------------------------------------------------

variable "log_level" {
  type    = string
  default = "info"
}

variable "log_retention_days" {
  type    = number
  default = 30
}

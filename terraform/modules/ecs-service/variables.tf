variable "name" {
  type = string
}

variable "cluster_id" {
  type = string
}

variable "task_definition_arn" {
  type = string
}

variable "desired_count" {
  type = number
}

variable "enable_execute_command" {
  type = bool
}

variable "deployment" {
  type = object({
    maximum_percent         = number
    minimum_healthy_percent = number
  })
}

variable "availability_zone_rebalancing" {
  type    = string
  default = "ENABLED"
  validation {
    condition     = contains(["ENABLED", "DISABLED"], var.availability_zone_rebalancing)
    error_message = "availability_zone_rebalancing must be ENABLED or DISABLED."
  }
}

variable "capacity_provider_strategies" {
  type = list(object({
    capacity_provider = string
    weight            = number
    base              = optional(number)
  }))
}

variable "subnet_ids" {
  type = list(string)
}

variable "security_group_ids" {
  type = list(string)
}

variable "assign_public_ip" {
  type    = bool
  default = false
}

variable "load_balancers" {
  type = list(object({
    target_group_arn = string
    container_name   = string
    container_port   = number
  }))
  default = []
}

variable "service_connect_namespace_arn" {
  type     = string
  default  = null
  nullable = true
}

variable "service_connect_services" {
  type = list(object({
    port_name                   = string
    discovery_name              = string
    timeout_per_request_seconds = optional(number)
    client_aliases = list(object({
      port     = number
      dns_name = string
    }))
  }))
  default = []
}

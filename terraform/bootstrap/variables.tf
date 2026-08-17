variable "region" {
  type        = string
  default     = "ap-east-1"
  description = "AWS region for Terraform state, CloudTrail, and bootstrap resources."
}

variable "environment" {
  type        = string
  default     = "test"
  description = "Value for the AWS default Environment tag."
  validation {
    condition     = contains(["test", "staging", "prod"], var.environment)
    error_message = "environment must be one of: test, staging, prod."
  }
}

variable "name_prefix" {
  type        = string
  description = "Resource name prefix, e.g. wdl-demo."
}

variable "state_bucket_name" {
  type        = string
  default     = ""
  description = "Optional explicit Terraform state bucket name. Defaults to a globally unique name based on name_prefix, account id, and region."
}

variable "state_noncurrent_version_retention_days" {
  type        = number
  default     = 90
  description = "Retention for old Terraform state object versions."
}

variable "cloudtrail_bucket_name" {
  type        = string
  default     = ""
  description = "Optional explicit CloudTrail bucket name. Defaults to a globally unique name based on name_prefix, account id, and region."
}

variable "cloudtrail_retention_days" {
  type        = number
  default     = 180
  description = "Retention for CloudTrail log objects."
}

variable "monthly_budget_limit_usd" {
  type        = number
  default     = 300
  description = "Monthly AWS cost budget in USD. A budget is created only when budget_alert_emails is non-empty."
}

variable "budget_alert_emails" {
  type        = list(string)
  default     = []
  description = "Email recipients for budget alerts. Leave empty to skip creating a budget."
}

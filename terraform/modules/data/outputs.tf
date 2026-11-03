output "valkey_primary_endpoint" {
  value = aws_elasticache_replication_group.valkey.primary_endpoint_address
}

output "valkey_port" {
  value = aws_elasticache_replication_group.valkey.port
}

output "valkey_security_group_id" {
  value = aws_security_group.valkey.id
}

output "assets_bucket_name" {
  value = aws_s3_bucket.assets.bucket
}

output "r2_bucket_name" {
  value = aws_s3_bucket.r2.bucket
}

output "control_s3_secret_arn" {
  value = aws_secretsmanager_secret.control_s3.arn
}

output "s3_cleanup_secret_arn" {
  value = aws_secretsmanager_secret.s3_cleanup.arn
}

output "runtime_r2_secret_arn" {
  value = aws_secretsmanager_secret.runtime_r2.arn
}

output "secret_envelope_secret_arn" {
  value = aws_secretsmanager_secret.secret_envelope.arn
}

output "assets_cdn_base" {
  value = local.cdn_enabled ? "https://${var.assets_cdn_domain}" : ""
}

output "assets_cdn_distribution_domain" {
  value       = local.cdn_enabled ? aws_cloudfront_distribution.assets[0].domain_name : ""
  description = "CloudFront-assigned domain. Point your CNAME at this."
}

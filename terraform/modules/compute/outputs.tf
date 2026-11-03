output "cluster_name" {
  value = aws_ecs_cluster.this.name
}

output "gateway_target_group_arn" {
  value = aws_lb_target_group.gateway.arn
}

output "d1_runtime_service_name" {
  value = module.d1_runtime_service.name
}

output "do_runtime_service_name" {
  value = module.do_runtime_service.name
}

output "workflows_service_name" {
  value = module.workflows_service.name
}

output "d1_storage_file_system_id" {
  value = aws_efs_file_system.d1_storage.id
}

output "do_storage_file_system_id" {
  value = aws_efs_file_system.do_storage.id
}

output "admin_token" {
  value       = random_password.admin_token.result
  sensitive   = true
  description = "Bootstrap control-plane token. Retrieve with `terraform output -raw admin_token`; ALB only checks that X-Admin-Token is present, and auth verifies this token value server-side."
}

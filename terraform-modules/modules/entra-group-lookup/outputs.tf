output "group_id" {
  description = "Identity Store group ID for the synced Entra group."
  value       = data.aws_identitystore_group.this.group_id
}

output "display_name" {
  description = "Resolved Identity Store group display name."
  value       = data.aws_identitystore_group.this.display_name
}

output "identity_store_id" {
  description = "Identity Store ID used for lookup."
  value       = local.identity_store_id
}

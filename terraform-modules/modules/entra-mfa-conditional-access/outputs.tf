output "policy_id" {
  description = "Object ID of the Conditional Access policy."
  value       = azuread_conditional_access_policy.this.id
}

output "policy_name" {
  description = "Name of the Conditional Access policy."
  value       = azuread_conditional_access_policy.this.display_name
}

output "target_application_client_id" {
  description = "Application client ID protected by the Conditional Access policy."
  value       = data.azuread_service_principal.aws_identity_center.client_id
}

output "target_application_object_id" {
  description = "Enterprise application service principal object ID."
  value       = data.azuread_service_principal.aws_identity_center.object_id
}

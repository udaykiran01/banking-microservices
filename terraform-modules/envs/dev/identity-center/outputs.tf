output "entra_synced_group_display_name" {
  description = "Display name of the Entra group assigned to the AWS account."
  value       = module.devops_team.display_name
}

output "entra_synced_group_identity_store_id" {
  description = "Identity Store group ID assigned to the AWS account."
  value       = module.devops_team.group_id
}

output "permission_set_arn" {
  description = "ARN of the DevOpsDeploymentAccess permission set."
  value       = module.devops_deployment_access.arn
}

output "permission_set_instance_arn" {
  description = "ARN of the IAM Identity Center instance."
  value       = module.devops_deployment_access.instance_arn
}

output "account_assignment_id" {
  description = "AWS IAM Identity Center account assignment ID."
  value       = module.devops_account_assignment.id
}

output "devops_identity_center_role_arn" {
  description = "IAM Identity Center reserved role ARN used for EKS access."
  value       = one(data.aws_iam_roles.devops_identity_center_role.arns)
}

output "eks_access_entry_id" {
  description = "EKS access entry for the DevOps Identity Center role."
  value       = module.devops_eks_access.id
}

output "kubernetes_pod_rbac_group" {
  description = "Kubernetes group granted pod operation access."
  value       = module.devops_pod_rbac.kubernetes_group
}

output "kubernetes_pod_rbac_namespace" {
  description = "Namespace where pod operation access is granted."
  value       = module.devops_pod_rbac.namespace
}

output "conditional_access_policy_id" {
  description = "Entra Conditional Access policy ID requiring MFA for AWS IAM Identity Center."
  value       = module.aws_identity_center_mfa.policy_id
}

output "conditional_access_target_application_client_id" {
  description = "Client ID of the protected AWS IAM Identity Center enterprise application."
  value       = module.aws_identity_center_mfa.target_application_client_id
}

variable "aws_region" {
  description = "AWS region used for AWS provider operations."
  type        = string
  default     = "us-east-1"
}

variable "aws_account_id" {
  description = "AWS account ID that receives the DevOpsDeploymentAccess assignment."
  type        = string
}

variable "entra_tenant_id" {
  description = "Microsoft Entra tenant ID."
  type        = string
}

variable "eks_cluster_name" {
  description = "EKS cluster where DevOps users receive Kubernetes pod operation access."
  type        = string
}

variable "kubernetes_namespace" {
  description = "Kubernetes namespace where DevOps users can perform pod operations."
  type        = string
  default     = "banking-app"
}

variable "devops_kubernetes_group" {
  description = "Kubernetes RBAC group mapped from the IAM Identity Center DevOps role."
  type        = string
  default     = "banking-devops"
}

variable "entra_synced_group_display_name" {
  description = "Display name of the Entra group synced into IAM Identity Center."
  type        = string
  default     = "AWS-DevOps-Team"
}

variable "aws_identity_center_enterprise_app_display_name" {
  description = "Display name of the AWS IAM Identity Center enterprise app in Entra."
  type        = string
  default     = "AWS IAM Identity Center"
}

variable "conditional_access_state" {
  description = "Initial state for the MFA Conditional Access policy."
  type        = string
  default     = "enabledForReportingButNotEnforced"

  validation {
    condition     = contains(["enabled", "disabled", "enabledForReportingButNotEnforced"], var.conditional_access_state)
    error_message = "conditional_access_state must be enabled, disabled, or enabledForReportingButNotEnforced."
  }
}

variable "conditional_access_excluded_users" {
  description = "Break-glass or service account object IDs excluded from the MFA policy."
  type        = set(string)
  default     = []
}

variable "tags" {
  description = "Tags applied to supported AWS resources."
  type        = map(string)
  default = {
    Application = "banking-app"
    Environment = "dev"
    ManagedBy   = "terraform"
  }
}

output "id" {
  description = "EKS access entry ID."
  value       = aws_eks_access_entry.this.id
}

output "principal_arn" {
  description = "IAM principal ARN mapped into the EKS cluster."
  value       = aws_eks_access_entry.this.principal_arn
}

output "kubernetes_groups" {
  description = "Kubernetes RBAC groups associated with the principal."
  value       = aws_eks_access_entry.this.kubernetes_groups
}

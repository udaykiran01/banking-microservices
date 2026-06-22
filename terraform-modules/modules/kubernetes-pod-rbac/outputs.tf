output "role_name" {
  description = "Kubernetes Role name."
  value       = kubernetes_role_v1.pod_operator.metadata[0].name
}

output "role_binding_name" {
  description = "Kubernetes RoleBinding name."
  value       = kubernetes_role_binding_v1.pod_operator.metadata[0].name
}

output "namespace" {
  description = "Namespace where pod operations are allowed."
  value       = var.namespace
}

output "kubernetes_group" {
  description = "Kubernetes group bound to pod operation permissions."
  value       = var.kubernetes_group
}

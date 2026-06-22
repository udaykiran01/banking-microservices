variable "namespace" {
  description = "Kubernetes namespace where pod operations are allowed."
  type        = string
}

variable "kubernetes_group" {
  description = "Kubernetes group that receives pod operation permissions."
  type        = string
}

variable "role_name" {
  description = "Name of the Kubernetes Role."
  type        = string
  default     = "pod-operator"
}

variable "role_binding_name" {
  description = "Name of the Kubernetes RoleBinding."
  type        = string
  default     = "devops-pod-operator"
}

variable "pod_verbs" {
  description = "Allowed verbs for pod resources."
  type        = set(string)
  default     = ["get", "list", "watch", "delete"]
}

variable "pod_log_verbs" {
  description = "Allowed verbs for pod logs."
  type        = set(string)
  default     = ["get", "list", "watch"]
}

variable "pod_exec_verbs" {
  description = "Allowed verbs for pod exec."
  type        = set(string)
  default     = ["create"]
}

variable "cluster_name" {
  description = "Name of the EKS cluster."
  type        = string
}

variable "principal_arn" {
  description = "IAM principal ARN that should be allowed to authenticate to the EKS cluster."
  type        = string
}

variable "kubernetes_groups" {
  description = "Kubernetes RBAC groups associated with the IAM principal."
  type        = set(string)
}

variable "tags" {
  description = "Tags to apply to the EKS access entry."
  type        = map(string)
  default     = {}
}

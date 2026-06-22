variable "kubernetes_cluster_name"{
    description = "cluster name"
    type = string
}

variable "resource_group_name" {
  description = "resource_group_name"
  type        = string
}

variable "location" {
  description = "region name"
  type        = string
}

variable "environment" {
  description = "region name"
  type        = string
}
variable "acr_id" {
  description = "Azure Container Registry resource ID"
  type        = string
}

variable "dns_prefix" {
  description = "Azure Container Registry resource ID"
  type        = string
}

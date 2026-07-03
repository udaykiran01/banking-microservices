variable "resource_group_name" {
  description = "resource_group_name"
  type        = string
}

variable "location" {
  description = "region name"
  type        = string
}

variable "environment" {
  description = "Deployment environment."
  type        = string
}

variable "key_vault_name" {
  description = "region name"
  type        = string

}

variable "oidc_issuer_url" {
  description = "AKS OIDC issuer URL for workload identity federated credentials."
  type        = string
  default     = null
}

variable "workload_identities" {
  description = "Managed identities and federated credentials for Kubernetes service accounts."
  type = map(object({
    namespace       = string
    service_account = string
    roles           = optional(list(string), ["Key Vault Secrets User"])
  }))
  default = {}
}

variable "log_analytics_workspace_id" {
  description = "Optional Log Analytics workspace ID for diagnostic settings."
  type        = string
  default     = null
}

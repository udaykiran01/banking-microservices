variable "kubernetes_cluster_name" {
  description = "AKS cluster name."
  type        = string
}

variable "resource_group_name" {
  description = "Resource group name."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
}

variable "environment" {
  description = "Deployment environment."
  type        = string
}

variable "acr_id" {
  description = "Azure Container Registry resource ID."
  type        = string
}

variable "dns_prefix" {
  description = "AKS DNS prefix."
  type        = string
}

variable "aks_subnet_id" {
  description = "Subnet ID used by AKS node pools."
  type        = string
  default     = null
}

variable "private_dns_zone_id" {
  description = "Private DNS zone ID for private AKS. Use \"System\" to let AKS manage it."
  type        = string
  default     = "System"
}

variable "private_cluster_enabled" {
  description = "Create a private AKS API server."
  type        = bool
  default     = false
}

variable "azure_policy_enabled" {
  description = "Enable Azure Policy add-on for AKS."
  type        = bool
  default     = false
}

variable "oidc_issuer_enabled" {
  description = "Enable OIDC issuer for Workload Identity."
  type        = bool
  default     = false
}

variable "workload_identity_enabled" {
  description = "Enable Microsoft Entra Workload Identity."
  type        = bool
  default     = false
}

variable "network_plugin" {
  description = "Kubernetes network plugin."
  type        = string
  default     = "kubenet"
}

variable "network_policy" {
  description = "Kubernetes network policy provider."
  type        = string
  default     = null
}

variable "outbound_type" {
  description = "AKS outbound egress type."
  type        = string
  default     = "loadBalancer"
}

variable "automatic_upgrade_channel" {
  description = "AKS automatic upgrade channel."
  type        = string
  default     = "stable"
}

variable "node_os_upgrade_channel" {
  description = "Node OS automatic upgrade channel."
  type        = string
  default     = "NodeImage"
}

variable "sku_tier" {
  description = "AKS SKU tier."
  type        = string
  default     = null
}

variable "system_node_pool" {
  description = "System node pool settings."
  type = object({
    name                 = string
    vm_size              = string
    zones                = list(string)
    min_count            = number
    max_count            = number
    node_count           = number
    max_pods             = number
    os_disk_size_gb      = number
    orchestrator_version = optional(string)
  })
  default = {
    name            = "system"
    vm_size         = "Standard_D2s_v5"
    zones           = ["1", "2", "3"]
    min_count       = 3
    max_count       = 6
    node_count      = 3
    max_pods        = 30
    os_disk_size_gb = 128
  }
}

variable "user_node_pools" {
  description = "User node pools keyed by pool name."
  type = map(object({
    vm_size              = string
    zones                = list(string)
    min_count            = number
    max_count            = number
    node_count           = number
    max_pods             = number
    os_disk_size_gb      = number
    mode                 = optional(string, "User")
    node_labels          = optional(map(string), {})
    node_taints          = optional(list(string), [])
    orchestrator_version = optional(string)
  }))
  default = {}
}

variable "maintenance_window_auto_upgrade" {
  description = "Allowed automatic upgrade window."
  type = object({
    frequency   = string
    interval    = number
    duration    = number
    day_of_week = optional(string)
    utc_offset  = optional(string)
    start_time  = optional(string)
  })
  default = {
    frequency   = "Weekly"
    interval    = 1
    duration    = 4
    day_of_week = "Sunday"
    utc_offset  = "+00:00"
    start_time  = "02:00"
  }
}

variable "maintenance_window_node_os" {
  description = "Allowed node OS upgrade window."
  type = object({
    frequency   = string
    interval    = number
    duration    = number
    day_of_week = optional(string)
    utc_offset  = optional(string)
    start_time  = optional(string)
  })
  default = {
    frequency   = "Weekly"
    interval    = 1
    duration    = 4
    day_of_week = "Sunday"
    utc_offset  = "+00:00"
    start_time  = "04:00"
  }
}

variable "log_analytics_workspace_id" {
  description = "Optional Log Analytics workspace ID for Container Insights."
  type        = string
  default     = null
}

variable "key_vault_secrets_provider_enabled" {
  description = "Enable Azure Key Vault Secrets Store CSI Driver add-on."
  type        = bool
  default     = false
}

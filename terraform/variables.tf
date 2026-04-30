variable "project_name" {
  description = "Short name used for tags and generated names."
  type        = string
  default     = "banking-app"
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region for all resources."
  type        = string
  default     = "canadacentral"
}

variable "resource_group_location" {
  description = "Azure region metadata for the resource group. Existing resource groups keep their original location."
  type        = string
  default     = "eastus"
}

variable "resource_group_name" {
  description = "Resource group that will contain the AKS infrastructure."
  type        = string
  default     = "rg-banking-app-dev"
}

variable "acr_name" {
  description = "Globally unique Azure Container Registry name. Use only letters and numbers."
  type        = string
  default     = "bankingappdev"
}

variable "acr_sku" {
  description = "Azure Container Registry SKU."
  type        = string
  default     = "Basic"
}

variable "aks_cluster_name" {
  description = "AKS cluster name."
  type        = string
  default     = "banking-app-dev-aks"
}

variable "kubernetes_version" {
  description = "Optional AKS Kubernetes version. Leave null to use Azure's default."
  type        = string
  default     = null
}

variable "node_count" {
  description = "Number of nodes in the default AKS node pool."
  type        = number
  default     = 1
}

variable "node_vm_size" {
  description = "VM size for the default AKS node pool."
  type        = string
  default     = "Standard_D2s_v3"
}

variable "node_os_disk_size_gb" {
  description = "OS disk size for AKS nodes."
  type        = number
  default     = 64
}

variable "postgres_server_name" {
  description = "Globally unique PostgreSQL Flexible Server name."
  type        = string
  default     = "banking-app-dev-pg-31065"
}

variable "postgres_database_name" {
  description = "Application database name."
  type        = string
  default     = "banking"
}

variable "postgres_admin_login" {
  description = "PostgreSQL admin username."
  type        = string
  default     = "bankingadmin"
}

variable "postgres_admin_password" {
  description = "PostgreSQL admin password. Set this in terraform.tfvars or TF_VAR_postgres_admin_password."
  type        = string
  sensitive   = true
}

variable "postgres_version" {
  description = "PostgreSQL Flexible Server major version."
  type        = string
  default     = "16"
}

variable "postgres_sku_name" {
  description = "PostgreSQL Flexible Server SKU."
  type        = string
  default     = "B_Standard_B1ms"
}

variable "postgres_storage_mb" {
  description = "PostgreSQL storage in MB."
  type        = number
  default     = 32768
}

variable "postgres_zone" {
  description = "Availability zone for PostgreSQL. Set null to let Azure choose."
  type        = string
  default     = "1"
}

variable "allow_azure_services_to_postgres" {
  description = "Allow Azure services, including AKS egress through Azure networking, to reach PostgreSQL."
  type        = bool
  default     = true
}

variable "postgres_allowed_ip_ranges" {
  description = "Extra PostgreSQL firewall rules keyed by rule name."
  type = map(object({
    start_ip = string
    end_ip   = string
  }))
  default = {}
}

variable "key_vault_name" {
  description = "Globally unique Key Vault name."
  type        = string
  default     = "banking-dev-kv-31065"
}

variable "azure_openai_endpoint" {
  description = "Optional Azure OpenAI endpoint stored in Key Vault for ai-analyzer-service."
  type        = string
  default     = ""
}

variable "azure_openai_key" {
  description = "Optional Azure OpenAI API key stored in Key Vault for ai-analyzer-service."
  type        = string
  default     = ""
  sensitive   = true
}

variable "azure_openai_deployment" {
  description = "Optional Azure OpenAI deployment name stored in Key Vault for ai-analyzer-service."
  type        = string
  default     = ""
}

variable "github_repository" {
  description = "GitHub owner/repository for OIDC federation."
  type        = string
  default     = "udaykiran01/banking-microservices"
}

variable "github_branch" {
  description = "GitHub branch allowed to use the federated credential."
  type        = string
  default     = "main"
}

variable "github_federated_credential_name" {
  description = "Federated credential display name."
  type        = string
  default     = "github-main"
}

variable "tags" {
  description = "Additional tags for Azure resources."
  type        = map(string)
  default     = {}
}

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.116"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.53"
    }
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
  }
}

provider "azuread" {}

data "azurerm_client_config" "current" {}

locals {
  common_tags = merge(
    {
      app         = var.project_name
      environment = var.environment
      managed_by  = "terraform"
    },
    var.tags
  )

  name_prefix = "${var.project_name}-${var.environment}"
}

resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.resource_group_location
  tags     = local.common_tags
}

resource "azurerm_container_registry" "main" {
  name                = var.acr_name
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = var.acr_sku
  admin_enabled       = false
  tags                = local.common_tags
}

resource "azurerm_kubernetes_cluster" "main" {
  name                = var.aks_cluster_name
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  dns_prefix          = "${local.name_prefix}-aks"
  kubernetes_version  = var.kubernetes_version
  oidc_issuer_enabled = true
  tags                = local.common_tags

  default_node_pool {
    name                = "system"
    vm_size             = var.node_vm_size
    node_count          = var.node_count
    os_disk_size_gb     = var.node_os_disk_size_gb
    type                = "VirtualMachineScaleSets"
    enable_auto_scaling = false

    upgrade_settings {
      max_surge = "10%"
    }
  }

  identity {
    type = "SystemAssigned"
  }

  key_vault_secrets_provider {
    secret_rotation_enabled  = true
    secret_rotation_interval = "2m"
  }

  network_profile {
    network_plugin    = "azure"
    load_balancer_sku = "standard"
  }
}

resource "azurerm_role_assignment" "aks_acr_pull" {
  scope                = azurerm_container_registry.main.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_kubernetes_cluster.main.kubelet_identity[0].object_id
}

resource "azurerm_postgresql_flexible_server" "main" {
  name                   = var.postgres_server_name
  resource_group_name    = azurerm_resource_group.main.name
  location               = var.location
  version                = var.postgres_version
  administrator_login    = var.postgres_admin_login
  administrator_password = var.postgres_admin_password
  zone                   = var.postgres_zone
  storage_mb             = var.postgres_storage_mb
  sku_name               = var.postgres_sku_name
  tags                   = local.common_tags
}

resource "azurerm_postgresql_flexible_server_database" "app" {
  name      = var.postgres_database_name
  server_id = azurerm_postgresql_flexible_server.main.id
  charset   = "UTF8"
  collation = "en_US.utf8"
}

resource "azurerm_postgresql_flexible_server_firewall_rule" "azure_services" {
  count            = var.allow_azure_services_to_postgres ? 1 : 0
  name             = "AllowAzureServices"
  server_id        = azurerm_postgresql_flexible_server.main.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

resource "azurerm_postgresql_flexible_server_firewall_rule" "allowed_ips" {
  for_each         = var.postgres_allowed_ip_ranges
  name             = each.key
  server_id        = azurerm_postgresql_flexible_server.main.id
  start_ip_address = each.value.start_ip
  end_ip_address   = each.value.end_ip
}

resource "azurerm_key_vault" "main" {
  name                          = var.key_vault_name
  resource_group_name           = azurerm_resource_group.main.name
  location                      = var.location
  tenant_id                     = data.azurerm_client_config.current.tenant_id
  sku_name                      = "standard"
  soft_delete_retention_days    = 7
  purge_protection_enabled      = false
  enable_rbac_authorization     = true
  public_network_access_enabled = true
  tags                          = local.common_tags
}

resource "azurerm_role_assignment" "terraform_key_vault_admin" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_role_assignment" "aks_key_vault_secrets_user" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_kubernetes_cluster.main.key_vault_secrets_provider[0].secret_identity[0].object_id
}

resource "azurerm_key_vault_secret" "db_host" {
  name         = "DB-HOST"
  value        = azurerm_postgresql_flexible_server.main.fqdn
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.terraform_key_vault_admin]
}

resource "azurerm_key_vault_secret" "db_port" {
  name         = "DB-PORT"
  value        = "5432"
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.terraform_key_vault_admin]
}

resource "azurerm_key_vault_secret" "db_user" {
  name         = "DB-USER"
  value        = var.postgres_admin_login
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.terraform_key_vault_admin]
}

resource "azurerm_key_vault_secret" "db_password" {
  name         = "DB-PASSWORD"
  value        = var.postgres_admin_password
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.terraform_key_vault_admin]
}

resource "azurerm_key_vault_secret" "db_name" {
  name         = "DB-NAME"
  value        = azurerm_postgresql_flexible_server_database.app.name
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.terraform_key_vault_admin]
}

resource "azurerm_key_vault_secret" "azure_openai_endpoint" {
  count        = var.azure_openai_endpoint == "" ? 0 : 1
  name         = "AZURE-OPENAI-ENDPOINT"
  value        = var.azure_openai_endpoint
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.terraform_key_vault_admin]
}

resource "azurerm_key_vault_secret" "azure_openai_key" {
  count        = var.azure_openai_key == "" ? 0 : 1
  name         = "AZURE-OPENAI-KEY"
  value        = var.azure_openai_key
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.terraform_key_vault_admin]
}

resource "azurerm_key_vault_secret" "azure_openai_deployment" {
  count        = var.azure_openai_deployment == "" ? 0 : 1
  name         = "AZURE-OPENAI-DEPLOYMENT"
  value        = var.azure_openai_deployment
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.terraform_key_vault_admin]
}

resource "azuread_application" "github_actions" {
  display_name = "${local.name_prefix}-github-actions"
}

resource "azuread_service_principal" "github_actions" {
  client_id = azuread_application.github_actions.client_id
}

# resource "azuread_application_federated_identity_credential" "github_main" {
#  application_id = azuread_application.github_actions.id
#  display_name   = var.github_federated_credential_name
#  description    = "GitHub Actions OIDC trust for ${var.github_repository} ${var.github_branch} deploys"
#  audiences      = ["api://AzureADTokenExchange"]
#  issuer         = "https://token.actions.githubusercontent.com"
#  subject        = "repo:${var.github_repository}:ref:refs/heads/${var.github_branch}"
# }


resource "azurerm_role_assignment" "github_contributor" {
  scope                = azurerm_resource_group.main.id
  role_definition_name = "Contributor"
  principal_id         = azuread_service_principal.github_actions.object_id
}

resource "azurerm_role_assignment" "github_acr_push" {
  scope                = azurerm_container_registry.main.id
  role_definition_name = "AcrPush"
  principal_id         = azuread_service_principal.github_actions.object_id
}

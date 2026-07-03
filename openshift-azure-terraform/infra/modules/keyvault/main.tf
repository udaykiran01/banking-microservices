
data "azurerm_client_config" "current" {}
resource "azurerm_key_vault" "kv" {
  name                = var.key_vault_name
  location            = var.location
  resource_group_name = var.resource_group_name
  tenant_id           = data.azurerm_client_config.current.tenant_id

  sku_name = "standard"

  soft_delete_retention_days = 90
  purge_protection_enabled   = true

  rbac_authorization_enabled = true
}

resource "azurerm_user_assigned_identity" "workload" {
  for_each = var.workload_identities

  name                = "uai-${each.key}-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name
}

resource "azurerm_federated_identity_credential" "workload" {
  for_each = var.oidc_issuer_url == null ? {} : var.workload_identities

  name                = "fic-${each.key}-${var.environment}"
  resource_group_name = var.resource_group_name
  parent_id           = azurerm_user_assigned_identity.workload[each.key].id
  audience            = ["api://AzureADTokenExchange"]
  issuer              = var.oidc_issuer_url
  subject             = "system:serviceaccount:${each.value.namespace}:${each.value.service_account}"
}

locals {
  workload_role_assignments = flatten([
    for identity_key, identity in var.workload_identities : [
      for role in try(identity.roles, ["Key Vault Secrets User"]) : {
        key          = "${identity_key}-${replace(role, " ", "-")}"
        identity_key = identity_key
        role         = role
      }
    ]
  ])
}

resource "azurerm_role_assignment" "workload_keyvault" {
  for_each = {
    for assignment in local.workload_role_assignments : assignment.key => assignment
  }

  scope                = azurerm_key_vault.kv.id
  role_definition_name = each.value.role
  principal_id         = azurerm_user_assigned_identity.workload[each.value.identity_key].principal_id
}

resource "azurerm_monitor_diagnostic_setting" "key_vault" {
  count = var.log_analytics_workspace_id == null ? 0 : 1

  name                       = "diag-keyvault-to-law"
  target_resource_id         = azurerm_key_vault.kv.id
  log_analytics_workspace_id = var.log_analytics_workspace_id

  enabled_log {
    category_group = "allLogs"
  }

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}

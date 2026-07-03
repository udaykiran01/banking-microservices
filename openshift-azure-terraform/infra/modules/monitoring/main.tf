resource "azurerm_log_analytics_workspace" "this" {
  name                = "law-banking-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "PerGB2018"
  retention_in_days   = var.retention_in_days
}

resource "azurerm_monitor_diagnostic_setting" "targets" {
  for_each = var.diagnostic_target_resource_ids

  name                       = "diag-${each.key}-to-law"
  target_resource_id         = each.value
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id

  enabled_log {
    category_group = "allLogs"
  }

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}

resource "azurerm_security_center_subscription_pricing" "defender_containers" {
  count = var.enable_defender_for_containers ? 1 : 0

  tier          = "Standard"
  resource_type = "Containers"
}

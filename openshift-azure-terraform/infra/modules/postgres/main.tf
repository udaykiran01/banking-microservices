resource "azurerm_postgresql_flexible_server" "this" {
  name                = var.postgres_server_name
  resource_group_name = var.resource_group_name
  location            = var.location

  administrator_login    = var.admin_username
  administrator_password = var.admin_password

  sku_name = var.sku_name

  storage_mb                    = var.storage_mb
  version                       = var.postgres_version
  delegated_subnet_id           = var.postgres_subnet_id
  private_dns_zone_id           = var.private_dns_zone_id
  public_network_access_enabled = false
  backup_retention_days         = var.backup_retention_days
  zone                          = var.zone

  dynamic "high_availability" {
    for_each = var.high_availability_mode == "Disabled" ? [] : [1]
    content {
      mode = var.high_availability_mode
    }
  }

  lifecycle {
    ignore_changes = [
      zone
    ]

  }

}



resource "azurerm_postgresql_flexible_server_database" "this" {
  name      = var.database_name
  server_id = azurerm_postgresql_flexible_server.this.id
  charset   = "UTF8"
  collation = "en_US.utf8"
}

resource "azurerm_monitor_diagnostic_setting" "postgres" {
  count = var.log_analytics_workspace_id == null ? 0 : 1

  name                       = "diag-postgres-to-law"
  target_resource_id         = azurerm_postgresql_flexible_server.this.id
  log_analytics_workspace_id = var.log_analytics_workspace_id

  enabled_log {
    category_group = "allLogs"
  }

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}

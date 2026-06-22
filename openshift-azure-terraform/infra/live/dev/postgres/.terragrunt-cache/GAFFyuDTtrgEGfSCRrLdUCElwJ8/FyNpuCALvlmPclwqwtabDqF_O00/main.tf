resource "azurerm_postgresql_flexible_server" "this" {
  name                = var.postgres_server_name
  resource_group_name = var.resource_group_name
  location            = var.location

  administrator_login    = "postgresadmin"
  administrator_password = var.admin_password

  sku_name = "B_Standard_B1ms"

  storage_mb = 32768
  version    = "16"
}

resource "azurerm_postgresql_flexible_server_database" "this" {
  name      = var.database_name
  server_id = azurerm_postgresql_flexible_server.this.id
  charset   = "UTF8"
  collation = "en_US.utf8"
}
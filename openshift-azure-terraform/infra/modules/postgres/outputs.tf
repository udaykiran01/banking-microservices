output "postgres_server_id" {
  value = azurerm_postgresql_flexible_server.this.id
}

output "postgres_server_name" {
  value = azurerm_postgresql_flexible_server.this.name
}

output "postgres_fqdn" {
  value = azurerm_postgresql_flexible_server.this.fqdn
}

output "database_name" {
  value = azurerm_postgresql_flexible_server_database.this.name
}
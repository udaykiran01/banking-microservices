output "postgres_private_dns_zone_id" {
  value = azurerm_private_dns_zone.postgres.id
}
output "postgres_private_dns_zone_id" {
  value = azurerm_private_dns_zone.postgres.id
}

output "aks_private_dns_zone_id" {
  value = try(azurerm_private_dns_zone.aks[0].id, null)
}

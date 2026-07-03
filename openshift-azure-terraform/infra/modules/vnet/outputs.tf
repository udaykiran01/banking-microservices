output "vnet_id" {
  value = azurerm_virtual_network.this.id
}

output "vnet_name" {
  value = azurerm_virtual_network.this.name
}

output "aks_subnet_id" {
  value = azurerm_subnet.aks.id
}

output "app_gateway_subnet_id" {
  value = try(azurerm_subnet.app_gateway[0].id, null)
}

output "postgres_subnet_id" {
  value = azurerm_subnet.postgres.id
}

output "firewall_private_ip" {
  value = try(azurerm_firewall.this[0].ip_configuration[0].private_ip_address, null)
}

output "firewall_public_ip" {
  value = try(azurerm_public_ip.firewall[0].ip_address, null)
}

output "aks_route_table_id" {
  value = try(azurerm_route_table.aks_egress[0].id, null)
}

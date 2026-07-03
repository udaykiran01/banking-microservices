output "front_door_profile_id" {
  value = azurerm_cdn_frontdoor_profile.this.id
}

output "front_door_endpoint_host_name" {
  value = azurerm_cdn_frontdoor_endpoint.this.host_name
}

output "front_door_endpoint_id" {
  value = azurerm_cdn_frontdoor_endpoint.this.id
}

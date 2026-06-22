output "frontdoor_endpoint_host_name" {
  value = azurerm_cdn_frontdoor_endpoint.this.host_name
}

output "frontdoor_custom_domain_validation_token" {
  value = var.custom_domain_host_name == null ? null : azurerm_cdn_frontdoor_custom_domain.this[0].validation_token
}

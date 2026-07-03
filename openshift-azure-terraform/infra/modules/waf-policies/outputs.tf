output "application_gateway_waf_policy_id" {
  value = azurerm_web_application_firewall_policy.app_gateway.id
}

output "front_door_waf_policy_id" {
  value = azurerm_cdn_frontdoor_firewall_policy.front_door.id
}

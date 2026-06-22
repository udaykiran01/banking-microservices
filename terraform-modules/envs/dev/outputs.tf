output "frontdoor_endpoint_host_name" {
  value = module.frontdoor-waf.frontdoor_endpoint_host_name
}

output "frontdoor_custom_domain_validation_token" {
  value = module.frontdoor-waf.frontdoor_custom_domain_validation_token
}

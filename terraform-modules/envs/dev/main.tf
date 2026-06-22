module "frontdoor-waf" {
  source = "../../modules/frontdoor-waf"

  resource_group_name        = var.resource_group_name
  location                   = var.location
  profile_name               = var.profile_name
  endpoint_name              = var.endpoint_name
  origin_group_name          = var.origin_group_name
  origin_name                = var.origin_name
  origin_host_name           = var.origin_host_name
  origin_host_header         = var.origin_host_header
  custom_domain_name         = var.custom_domain_name
  custom_domain_host_name    = var.custom_domain_host_name
  health_probe_path          = var.health_probe_path
  route_name                 = var.route_name
  waf_policy_name            = var.waf_policy_name
  security_policy_name       = var.security_policy_name
  log_analytics_workspace_id = var.log_analytics_workspace_id
  tags                       = var.tags
}

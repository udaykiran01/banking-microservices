include {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = "../../../modules/application-gateway-waf"
}

locals {
  env = read_terragrunt_config(find_in_parent_folders("env.hcl"))
}

dependency "resource_group" {
  config_path = "../resource-group"
}

dependency "vnet" {
  config_path = "../vnet"
}

dependency "waf_policies" {
  config_path = "../waf-policies"
}

dependency "monitoring" {
  config_path = "../monitoring"
}

inputs = {
  environment         = local.env.locals.environment
  resource_group_name = dependency.resource_group.outputs.resource_group_name
  location            = local.env.locals.location
  subnet_id           = dependency.vnet.outputs.app_gateway_subnet_id
  waf_policy_id       = dependency.waf_policies.outputs.application_gateway_waf_policy_id
  public_ip_domain_name_label = "agw-banking-${local.env.locals.environment}"

  # Replace with the internal ingress controller IP after the private ingress is deployed.
  backend_ip_addresses = ["10.20.1.10"]
  backend_port         = 80
  health_probe_path    = "/health"

  log_analytics_workspace_id = dependency.monitoring.outputs.log_analytics_workspace_id
}

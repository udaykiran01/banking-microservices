include {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = "../../../modules/front-door"
}

locals {
  env = read_terragrunt_config(find_in_parent_folders("env.hcl"))
}

dependency "resource_group" {
  config_path = "../resource-group"
}

dependency "application_gateway" {
  config_path = "../application-gateway-waf"
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

  origin_host_name   = dependency.application_gateway.outputs.public_ip_fqdn
  origin_host_header = dependency.application_gateway.outputs.public_ip_fqdn
  waf_policy_id      = dependency.waf_policies.outputs.front_door_waf_policy_id

  log_analytics_workspace_id = dependency.monitoring.outputs.log_analytics_workspace_id
}

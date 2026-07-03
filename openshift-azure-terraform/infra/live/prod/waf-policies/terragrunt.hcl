include {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = "../../../modules/waf-policies"
}

locals {
  env = read_terragrunt_config(find_in_parent_folders("env.hcl"))
}

dependency "resource_group" {
  config_path = "../resource-group"
}

inputs = {
  environment         = local.env.locals.environment
  resource_group_name = dependency.resource_group.outputs.resource_group_name
  location            = local.env.locals.location
  mode                = "Prevention"
}

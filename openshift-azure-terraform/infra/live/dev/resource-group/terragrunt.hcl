include {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = "../../../modules/resource-group"
}

locals {
  env = read_terragrunt_config(find_in_parent_folders("env.hcl"))
}

inputs = {
  environment         = local.env.locals.environment
  resource_group_name = "rg-banking-${local.env.locals.environment}"
  location            = local.env.locals.location
}
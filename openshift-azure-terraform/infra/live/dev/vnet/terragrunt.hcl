include {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = "../../../modules/vnet"
}

locals {
  env = read_terragrunt_config(find_in_parent_folders("env.hcl"))
}

dependency "resource_group" {
  config_path = "../resource-group"
}

inputs = {
  resource_group_name = dependency.resource_group.outputs.resource_group_name
  location            = local.env.locals.location

  vnet_name = "openshift-banking-${local.env.locals.environment}"

  address_space = ["10.10.0.0/16"]

  aks_subnet_name      = "snet-aks-${local.env.locals.environment}"
  aks_subnet_prefixes  = ["10.10.1.0/24"]

  postgres_subnet_name     = "snet-postgres-${local.env.locals.environment}"
  postgres_subnet_prefixes = ["10.10.2.0/24"]
}
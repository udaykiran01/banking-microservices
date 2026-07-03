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
  environment         = local.env.locals.environment
  resource_group_name = dependency.resource_group.outputs.resource_group_name
  location            = local.env.locals.location

  vnet_name = "openshift-banking-${local.env.locals.environment}"

  address_space = ["10.20.0.0/16"]

  aks_subnet_name      = "snet-aks-${local.env.locals.environment}"
  aks_subnet_prefixes  = ["10.20.1.0/22"]

  postgres_subnet_name     = "snet-postgres-${local.env.locals.environment}"
  postgres_subnet_prefixes = ["10.20.5.0/24"]

  app_gateway_subnet_name     = "snet-agw-${local.env.locals.environment}"
  app_gateway_subnet_prefixes = ["10.20.6.0/24"]

  firewall_enabled         = true
  firewall_subnet_prefixes = ["10.20.7.0/26"]
  firewall_sku_tier        = "Standard"
}

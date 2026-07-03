include {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = "../../../modules/private-dns"
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

inputs = {
  resource_group_name      = dependency.resource_group.outputs.resource_group_name
  location                 = local.env.locals.location
  vnet_id                  = dependency.vnet.outputs.vnet_id
  enable_aks_private_dns   = true
}

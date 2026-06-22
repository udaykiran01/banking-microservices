include {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = "../../../modules/aks"
}

locals {
  env = read_terragrunt_config(find_in_parent_folders("env.hcl"))
}

dependency "resource_group" {
  config_path = "../resource-group"
}
dependency "acr" {
  config_path = "../acr"
}

inputs = {
  environment             = local.env.locals.environment
  resource_group_name     = dependency.resource_group.outputs.resource_group_name
  location                = local.env.locals.location
  kubernetes_cluster_name = "banking-openshift-${local.env.locals.environment}"
  dns_prefix              = "banking-${local.env.locals.environment}"
  acr_id = dependency.acr.outputs.acr_id

}
include {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = "../../../modules/postgres"
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

dependency "private_dns" {
  config_path = "../private-dns"
}

inputs = {
  resource_group_name = dependency.resource_group.outputs.resource_group_name
  location            = "canadacentral"

  postgres_server_name = "psql-banking-openshift-${local.env.locals.environment}"
  database_name        = "bankingdbdev"

  postgres_subnet_id  = dependency.vnet.outputs.postgres_subnet_id
  private_dns_zone_id = dependency.private_dns.outputs.postgres_private_dns_zone_id

  admin_username = "pgadminuser"
  admin_password = "ChangeThisPassword123!"

  sku_name               = "B_Standard_B1ms"
  storage_mb             = 32768
  backup_retention_days  = 7
  high_availability_mode = "Disabled"
}
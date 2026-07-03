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

dependency "vnet" {
  config_path = "../vnet"
}

dependency "private_dns" {
  config_path = "../private-dns"
}

dependency "monitoring" {
  config_path = "../monitoring"
}

inputs = {
  environment             = local.env.locals.environment
  resource_group_name     = dependency.resource_group.outputs.resource_group_name
  location                = local.env.locals.location
  kubernetes_cluster_name = "banking-openshift-${local.env.locals.environment}"
  dns_prefix              = "banking-${local.env.locals.environment}"
  acr_id                  = dependency.acr.outputs.acr_id

  aks_subnet_id           = dependency.vnet.outputs.aks_subnet_id
  private_cluster_enabled = true
  private_dns_zone_id     = dependency.private_dns.outputs.aks_private_dns_zone_id

  network_plugin = "azure"
  network_policy = "azure"
  outbound_type  = "userDefinedRouting"

  azure_policy_enabled      = true
  oidc_issuer_enabled       = true
  workload_identity_enabled = true
  key_vault_secrets_provider_enabled = true

  automatic_upgrade_channel = "stable"
  node_os_upgrade_channel   = "NodeImage"
  sku_tier                  = "Standard"

  log_analytics_workspace_id = dependency.monitoring.outputs.log_analytics_workspace_id

  system_node_pool = {
    name            = "system"
    vm_size         = "Standard_D2s_v5"
    zones           = ["1", "2", "3"]
    min_count       = 3
    max_count       = 6
    node_count      = 3
    max_pods        = 30
    os_disk_size_gb = 128
  }

  user_node_pools = {
    apps = {
      vm_size         = "Standard_D4s_v5"
      zones           = ["1", "2", "3"]
      min_count       = 3
      max_count       = 12
      node_count      = 3
      max_pods        = 50
      os_disk_size_gb = 128
      node_labels = {
        workload = "apps"
      }
    }
  }

  maintenance_window_auto_upgrade = {
    frequency   = "Weekly"
    interval    = 1
    duration    = 4
    day_of_week = "Sunday"
    utc_offset  = "-05:00"
    start_time  = "02:00"
  }

  maintenance_window_node_os = {
    frequency   = "Weekly"
    interval    = 1
    duration    = 4
    day_of_week = "Sunday"
    utc_offset  = "-05:00"
    start_time  = "04:00"
  }
}

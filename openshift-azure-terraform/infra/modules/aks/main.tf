resource "azurerm_user_assigned_identity" "aks_identity" {
  name                = "uai-aks-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name
}

resource "azurerm_role_assignment" "aks_acr_pull" {
  scope                = var.acr_id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_user_assigned_identity.aks_identity.principal_id
}

resource "azurerm_role_assignment" "aks_private_dns_zone_contributor" {
  count = var.private_cluster_enabled && var.private_dns_zone_id != null && var.private_dns_zone_id != "System" ? 1 : 0

  scope                = var.private_dns_zone_id
  role_definition_name = "Private DNS Zone Contributor"
  principal_id         = azurerm_user_assigned_identity.aks_identity.principal_id
}

resource "azurerm_kubernetes_cluster" "this" {
  name                = var.kubernetes_cluster_name
  location            = var.location
  resource_group_name = var.resource_group_name
  dns_prefix          = var.dns_prefix
  sku_tier            = var.sku_tier

  private_cluster_enabled = var.private_cluster_enabled
  private_dns_zone_id     = var.private_cluster_enabled ? var.private_dns_zone_id : null

  azure_policy_enabled      = var.azure_policy_enabled
  oidc_issuer_enabled       = var.oidc_issuer_enabled
  workload_identity_enabled = var.workload_identity_enabled

  automatic_upgrade_channel = var.automatic_upgrade_channel
  node_os_upgrade_channel   = var.node_os_upgrade_channel

  default_node_pool {
    name                         = var.system_node_pool.name
    vm_size                      = var.system_node_pool.vm_size
    vnet_subnet_id               = var.aks_subnet_id
    zones                        = var.system_node_pool.zones
    auto_scaling_enabled         = true
    min_count                    = var.system_node_pool.min_count
    max_count                    = var.system_node_pool.max_count
    node_count                   = var.system_node_pool.node_count
    max_pods                     = var.system_node_pool.max_pods
    os_disk_size_gb              = var.system_node_pool.os_disk_size_gb
    only_critical_addons_enabled = true
    orchestrator_version         = try(var.system_node_pool.orchestrator_version, null)

    upgrade_settings {
      max_surge = "33%"
    }
  }

  dynamic "oms_agent" {
    for_each = var.log_analytics_workspace_id == null ? [] : [1]
    content {
      log_analytics_workspace_id = var.log_analytics_workspace_id
    }
  }

  dynamic "key_vault_secrets_provider" {
    for_each = var.key_vault_secrets_provider_enabled ? [1] : []
    content {
      secret_rotation_enabled  = true
      secret_rotation_interval = "2m"
    }
  }

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.aks_identity.id]
  }

  network_profile {
    network_plugin = var.network_plugin
    network_policy = var.network_policy
    outbound_type  = var.outbound_type
  }

  maintenance_window_auto_upgrade {
    frequency   = var.maintenance_window_auto_upgrade.frequency
    interval    = var.maintenance_window_auto_upgrade.interval
    duration    = var.maintenance_window_auto_upgrade.duration
    day_of_week = try(var.maintenance_window_auto_upgrade.day_of_week, null)
    utc_offset  = try(var.maintenance_window_auto_upgrade.utc_offset, null)
    start_time  = try(var.maintenance_window_auto_upgrade.start_time, null)
  }

  maintenance_window_node_os {
    frequency   = var.maintenance_window_node_os.frequency
    interval    = var.maintenance_window_node_os.interval
    duration    = var.maintenance_window_node_os.duration
    day_of_week = try(var.maintenance_window_node_os.day_of_week, null)
    utc_offset  = try(var.maintenance_window_node_os.utc_offset, null)
    start_time  = try(var.maintenance_window_node_os.start_time, null)
  }

  tags = {
    Environment = var.environment
  }
}

resource "azurerm_kubernetes_cluster_node_pool" "user" {
  for_each = var.user_node_pools

  name                  = each.key
  kubernetes_cluster_id = azurerm_kubernetes_cluster.this.id
  vm_size               = each.value.vm_size
  mode                  = try(each.value.mode, "User")
  vnet_subnet_id        = var.aks_subnet_id
  zones                 = each.value.zones
  auto_scaling_enabled  = true
  min_count             = each.value.min_count
  max_count             = each.value.max_count
  node_count            = each.value.node_count
  max_pods              = each.value.max_pods
  os_disk_size_gb       = each.value.os_disk_size_gb
  node_labels           = try(each.value.node_labels, {})
  node_taints           = try(each.value.node_taints, [])
  orchestrator_version  = try(each.value.orchestrator_version, null)

  upgrade_settings {
    max_surge = "33%"
  }

  tags = {
    Environment = var.environment
  }
}

resource "azurerm_monitor_diagnostic_setting" "aks" {
  count = var.log_analytics_workspace_id == null ? 0 : 1

  name                       = "diag-aks-to-law"
  target_resource_id         = azurerm_kubernetes_cluster.this.id
  log_analytics_workspace_id = var.log_analytics_workspace_id

  enabled_log {
    category_group = "allLogs"
  }

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}

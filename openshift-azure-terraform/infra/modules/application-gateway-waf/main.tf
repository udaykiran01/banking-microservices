resource "azurerm_public_ip" "this" {
  name                = "pip-agw-banking-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name
  allocation_method   = "Static"
  sku                 = "Standard"
  zones               = ["1", "2", "3"]
  domain_name_label   = var.public_ip_domain_name_label
}

resource "azurerm_application_gateway" "this" {
  name                = "agw-banking-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name
  firewall_policy_id  = var.waf_policy_id

  sku {
    name = "WAF_v2"
    tier = "WAF_v2"
  }

  autoscale_configuration {
    min_capacity = 2
    max_capacity = 10
  }

  gateway_ip_configuration {
    name      = "gateway-ip-configuration"
    subnet_id = var.subnet_id
  }

  frontend_ip_configuration {
    name                 = "public-frontend"
    public_ip_address_id = azurerm_public_ip.this.id
  }

  frontend_port {
    name = "http"
    port = var.frontend_port
  }

  backend_address_pool {
    name         = "aks-ingress"
    fqdns        = var.backend_fqdns
    ip_addresses = var.backend_ip_addresses
  }

  backend_http_settings {
    name                  = "http"
    cookie_based_affinity = "Disabled"
    path                  = "/"
    port                  = var.backend_port
    protocol              = "Http"
    request_timeout       = 30
    probe_name            = "aks-ingress"
  }

  probe {
    name                                      = "aks-ingress"
    protocol                                  = "Http"
    path                                      = var.health_probe_path
    interval                                  = 30
    timeout                                   = 30
    unhealthy_threshold                       = 3
    pick_host_name_from_backend_http_settings = true
  }

  http_listener {
    name                           = "http"
    frontend_ip_configuration_name = "public-frontend"
    frontend_port_name             = "http"
    protocol                       = "Http"
  }

  request_routing_rule {
    name                       = "aks-ingress"
    rule_type                  = "Basic"
    http_listener_name         = "http"
    backend_address_pool_name  = "aks-ingress"
    backend_http_settings_name = "http"
    priority                   = 100
  }
}

resource "azurerm_monitor_diagnostic_setting" "application_gateway" {
  count = var.log_analytics_workspace_id == null ? 0 : 1

  name                       = "diag-appgw-to-law"
  target_resource_id         = azurerm_application_gateway.this.id
  log_analytics_workspace_id = var.log_analytics_workspace_id

  enabled_log {
    category_group = "allLogs"
  }

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}

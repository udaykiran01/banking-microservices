resource "azurerm_container_registry" "this" {
  name                = var.container_registry_name
  resource_group_name = var.resource_group_name
  #pipeline testing
  location      = var.location
  sku           = "Basic"
  admin_enabled = false
}
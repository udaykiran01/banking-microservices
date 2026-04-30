terraform {
  backend "azurerm" {
    resource_group_name  = "rg-terraform-state"
    storage_account_name = "bankingtfstate31065"
    container_name       = "tfstate"
    key                  = "banking/02-cluster-addons.tfstate"
  }
}
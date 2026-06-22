generate "backend" {
  path      = "backend.tf"
  if_exists = "overwrite"

  contents = <<EOF
terraform {
  backend "azurerm" {}
}
EOF
}

remote_state {
  backend = "azurerm"

  config = {
    resource_group_name  = "Banking-app"
    storage_account_name = "bankingopenshift"
    container_name       = "tfstate"

    key = "${path_relative_to_include()}/terraform.tfstate"
  }
} 

generate "provider" {
  path      = "provider.tf"
  if_exists = "overwrite"

  contents = <<EOF
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
}
EOF
}


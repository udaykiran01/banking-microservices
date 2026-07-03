variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "vnet_name" {
  type = string
}

variable "address_space" {
  type = list(string)
}

variable "aks_subnet_name" {
  type = string
}

variable "aks_subnet_prefixes" {
  type = list(string)
}

variable "postgres_subnet_name" {
  type = string
}

variable "postgres_subnet_prefixes" {
  type = list(string)
}

variable "app_gateway_subnet_name" {
  type    = string
  default = null
}

variable "app_gateway_subnet_prefixes" {
  type    = list(string)
  default = []
}

variable "firewall_enabled" {
  type    = bool
  default = false
}

variable "firewall_subnet_prefixes" {
  type    = list(string)
  default = []
}

variable "firewall_sku_tier" {
  type    = string
  default = "Standard"
}

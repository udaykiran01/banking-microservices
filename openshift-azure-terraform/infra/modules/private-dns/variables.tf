variable "resource_group_name" {
  type = string
}

variable "vnet_id" {
  type = string
}

variable "location" {
  type = string
}

variable "enable_aks_private_dns" {
  type    = bool
  default = false
}

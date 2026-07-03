variable "postgres_server_name" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "postgres_version" {
  type    = string
  default = "16"
}

variable "postgres_subnet_id" {
  type = string
}

variable "private_dns_zone_id" {
  type = string
}

variable "admin_username" {
  type = string
}

variable "admin_password" {
  type      = string
  sensitive = true
}

variable "sku_name" {
  type    = string
  default = "B_Standard_B1ms"
}

variable "storage_mb" {
  type    = number
  default = 32768
}

variable "zone" {
  type    = string
  default = "1"
}

variable "backup_retention_days" {
  type    = number
  default = 7
}

variable "high_availability_mode" {
  type    = string
  default = "Disabled"
}

variable "database_name" {
  type = string
}

variable "log_analytics_workspace_id" {
  type    = string
  default = null
}

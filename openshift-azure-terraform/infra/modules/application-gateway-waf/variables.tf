variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "environment" {
  type = string
}

variable "subnet_id" {
  type = string
}

variable "waf_policy_id" {
  type = string
}

variable "backend_fqdns" {
  type    = list(string)
  default = []
}

variable "backend_ip_addresses" {
  type    = list(string)
  default = []
}

variable "frontend_port" {
  type    = number
  default = 80
}

variable "backend_port" {
  type    = number
  default = 80
}

variable "public_ip_domain_name_label" {
  type    = string
  default = null
}

variable "health_probe_path" {
  type    = string
  default = "/health"
}

variable "log_analytics_workspace_id" {
  type    = string
  default = null
}

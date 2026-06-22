variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "profile_name" {
  type = string
}

variable "endpoint_name" {
  type = string
}

variable "origin_group_name" {
  type = string
}

variable "origin_name" {
  type = string
}

variable "origin_host_name" {
  type = string
}

variable "origin_host_header" {
  type = string
}

variable "custom_domain_name" {
  type    = string
  default = "banking-custom-domain"
}

variable "custom_domain_host_name" {
  type    = string
  default = null
}

variable "health_probe_path" {
  type = string
}

variable "route_name" {
  type = string
}

variable "waf_policy_name" {
  type = string
}

variable "security_policy_name" {
  type = string
}

variable "log_analytics_workspace_id" {
  type    = string
  default = null
}

variable "tags" {
  type = map(string)

  default = {
    environment = "dev"
    project     = "banking-app"
  }
}

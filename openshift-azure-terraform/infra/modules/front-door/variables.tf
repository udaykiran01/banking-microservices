variable "resource_group_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "origin_host_name" {
  description = "Application Gateway public DNS name or custom hostname used as Front Door origin."
  type        = string
}

variable "origin_host_header" {
  description = "Host header sent from Front Door to the origin."
  type        = string
  default     = null
}

variable "waf_policy_id" {
  type = string
}

variable "patterns_to_match" {
  type    = list(string)
  default = ["/*"]
}

variable "log_analytics_workspace_id" {
  type    = string
  default = null
}

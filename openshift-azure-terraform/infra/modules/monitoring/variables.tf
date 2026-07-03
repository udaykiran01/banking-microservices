variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "environment" {
  type = string
}

variable "retention_in_days" {
  type    = number
  default = 90
}

variable "diagnostic_target_resource_ids" {
  description = "Resource IDs that should send diagnostics to Log Analytics."
  type        = map(string)
  default     = {}
}

variable "enable_defender_for_containers" {
  type    = bool
  default = true
}

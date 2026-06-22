variable "name" {
  description = "Name of the IAM Identity Center permission set."
  type        = string
}

variable "description" {
  description = "Description for the IAM Identity Center permission set."
  type        = string
  default     = null
}

variable "session_duration" {
  description = "ISO-8601 session duration for the permission set."
  type        = string
  default     = "PT8H"
}

variable "relay_state" {
  description = "Relay state URL for federated sign-in."
  type        = string
  default     = null
}

variable "managed_policy_arns" {
  description = "AWS managed policy ARNs to attach to the permission set."
  type        = set(string)
  default     = []
}

variable "inline_policy_json" {
  description = "Optional inline IAM policy JSON for the permission set."
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags to apply to the permission set."
  type        = map(string)
  default     = {}
}

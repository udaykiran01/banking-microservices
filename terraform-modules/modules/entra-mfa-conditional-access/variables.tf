variable "policy_name" {
  description = "Name of the Entra Conditional Access policy."
  type        = string
}

variable "target_application_display_name" {
  description = "Display name of the enterprise application service principal to protect."
  type        = string
  default     = "AWS IAM Identity Center"
}

variable "state" {
  description = "Conditional Access policy state."
  type        = string
  default     = "enabledForReportingButNotEnforced"

  validation {
    condition     = contains(["enabled", "disabled", "enabledForReportingButNotEnforced"], var.state)
    error_message = "state must be enabled, disabled, or enabledForReportingButNotEnforced."
  }
}

variable "included_users" {
  description = "User object IDs included in the policy. Use [\"All\"] to include all users."
  type        = set(string)
  default     = ["All"]
}

variable "excluded_users" {
  description = "User object IDs excluded from the policy, such as break-glass accounts."
  type        = set(string)
  default     = []
}

variable "included_groups" {
  description = "Group object IDs included in the policy."
  type        = set(string)
  default     = []
}

variable "excluded_groups" {
  description = "Group object IDs excluded from the policy."
  type        = set(string)
  default     = []
}

variable "client_app_types" {
  description = "Client app types covered by the policy."
  type        = set(string)
  default     = ["all"]
}

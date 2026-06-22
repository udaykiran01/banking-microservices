variable "instance_arn" {
  description = "ARN of the IAM Identity Center instance."
  type        = string
}

variable "permission_set_arn" {
  description = "ARN of the permission set to assign."
  type        = string
}

variable "principal_id" {
  description = "Identity Store principal ID for the synced Entra group or user."
  type        = string
}

variable "principal_type" {
  description = "Principal type for the assignment."
  type        = string
  default     = "GROUP"

  validation {
    condition     = contains(["GROUP", "USER"], var.principal_type)
    error_message = "principal_type must be GROUP or USER."
  }
}

variable "target_account_id" {
  description = "AWS account ID that receives the permission set assignment."
  type        = string
}

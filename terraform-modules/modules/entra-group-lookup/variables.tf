variable "display_name" {
  description = "Display name of the Entra group synced into AWS IAM Identity Center."
  type        = string
}

variable "identity_store_id" {
  description = "Optional IAM Identity Center Identity Store ID. If omitted, the current instance is used."
  type        = string
  default     = null
}

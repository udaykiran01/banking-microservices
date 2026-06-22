variable "environment" {
  type        = string
  description = "Environment name"
}


variable "tags" {
  description = "Common tags for VPC resources"
  type        = map(string)
}

variable "repository_names" {
  type = list(string)
}
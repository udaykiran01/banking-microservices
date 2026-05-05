variable "vpc_name" {
description = "Name of the VPC"
type = string
}

variable "vpc_cidr" {
description = "CIDR block for the VPC"
type = string
}

variable "azs" {
description = "List of Availability Zones"
type = list(string)
}

variable "private_subnets" {
description = "List of private subnet CIDRs"
type = list(string)
}

variable "public_subnets" {
description = "List of public subnet CIDRs"
type = list(string)
}

variable "enable_nat_gateway" {
description = "Enable NAT Gateway for private subnets"
type = bool
default = true
}

variable "single_nat_gateway" {
description = "Use single NAT Gateway (true for dev, false for prod)"
type = bool
default = true
}

variable "environment" {
  description = "Environment name"
  type        = string
}
variable "tags" {
  description = "Common tags for VPC resources"
  type        = map(string)
}
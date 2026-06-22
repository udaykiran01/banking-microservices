variable "vpc_name" {
  type        = string
  description = "name of the vpc"
}

variable "vpc_cidr" {
  type    = string
  default = "10.0.0.0/16"
}

variable "private_subnets" {
  type = list(string)
}

variable "public_subnets" {
  type = list(string)
}

variable "azs" {
  type = list(string)
}

variable "enable_nat_gateway" {
  type = bool
}

variable "single_nat_gateway" {
  type = bool
}

variable "environment" {
  type        = string
  description = "Environment name"
}

variable "region" {
  type = string
}

variable "tags" {
  description = "Common tags for VPC resources"
  type        = map(string)
}

variable "repository_names" {
  type = list(string)
}

variable "cluster_name" {
  description = "Name of the EKS cluster"
  type        = string
}

variable "kubernetes_version" {
  description = "Kubernetes version for EKS"
  type        = string
}
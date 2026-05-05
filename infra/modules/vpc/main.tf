
module "vpc" {
  source = "terraform-aws-modules/vpc/aws"


name            = var.vpc_name
cidr            = var.vpc_cidr
azs             = var.azs
private_subnets = var.private_subnets
public_subnets  = var.public_subnets
enable_nat_gateway = var.enable_nat_gateway
single_nat_gateway = var.single_nat_gateway
tags = {
    
    Environment = var.environment
}

}


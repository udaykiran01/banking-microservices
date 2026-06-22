module "vpc" {
  source = "../../modules/vpc"

  vpc_name           = var.vpc_name
  vpc_cidr           = var.vpc_cidr
  azs                = var.azs
  private_subnets    = var.private_subnets
  public_subnets     = var.public_subnets
  enable_nat_gateway = var.enable_nat_gateway
  single_nat_gateway = var.single_nat_gateway
  environment        = var.environment
  tags               = var.tags
}

module "ecr" {
  source = "../../modules/ecr"

  repository_names = var.repository_names
  environment      = var.environment
  tags             = var.tags

}

module "eks" {
  source = "../../modules/eks"

  cluster_name       = var.cluster_name
  kubernetes_version = var.kubernetes_version
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
  environment        = var.environment
  tags               = var.tags
}

module "eks_addons" {
  source = "../../modules/eks-addons"

  cluster_name = module.eks.cluster_name
  vpc_id       = module.vpc.vpc_id
}

module "helm_banking_app" {
  source = "../../modules/helm-banking-app"

  depends_on = [
    module.eks_addons
  ]
}
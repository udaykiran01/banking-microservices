module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.0"

  name              =  var.cluster_name
  kubernetes_version = var.kubernetes_version

  endpoint_public_access = true
  enable_cluster_creator_admin_permissions = true
  authentication_mode = "API_AND_CONFIG_MAP"

  

  vpc_id     = var.vpc_id
  subnet_ids = var.private_subnet_ids

  eks_managed_node_groups = {
    general = {
      instance_types = ["t3.small"]
      ami_type       = "AL2023_x86_64_STANDARD"

      min_size     = 1
      max_size     = 2
      desired_size = 1
    }
  }

addons = {
  coredns = {
    most_recent = true
  }

  kube-proxy = {
    most_recent = true
  }

  vpc-cni = {
    most_recent = true
  }

  eks-pod-identity-agent = {
    most_recent = true
  }
}
  

  tags = merge(var.tags, {
    Environment = var.environment
  })
}
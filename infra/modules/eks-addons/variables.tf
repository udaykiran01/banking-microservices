variable "cluster_name" {
  type = string
}

variable "aws_load_balancer_controller" {
  type = object({
    name               = string
    helm_chart_version = string
    helm_repo_url      = string
  })

  default = {
    name               = "aws-load-balancer-controller"
    helm_chart_version = "1.12.0"
    helm_repo_url      = "https://aws.github.io/eks-charts"
  }
}

variable "vpc_id" {
  type = string
}
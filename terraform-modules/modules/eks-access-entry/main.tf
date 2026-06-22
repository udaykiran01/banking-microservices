resource "aws_eks_access_entry" "this" {
  cluster_name      = var.cluster_name
  principal_arn     = var.principal_arn
  type              = "STANDARD"
  kubernetes_groups = var.kubernetes_groups
  tags              = var.tags
}

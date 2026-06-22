data "aws_iam_policy_document" "devops_deployment_access" {
  statement {
    sid    = "DeploymentReadAccess"
    effect = "Allow"

    actions = [
      "cloudformation:Describe*",
      "cloudformation:Get*",
      "cloudformation:List*",
      "cloudwatch:Describe*",
      "cloudwatch:Get*",
      "cloudwatch:List*",
      "ecr:Describe*",
      "ecr:GetAuthorizationToken",
      "ecr:List*",
      "ecs:Describe*",
      "ecs:List*",
      "eks:Describe*",
      "eks:List*",
      "iam:GetRole",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "logs:Describe*",
      "logs:FilterLogEvents",
      "logs:Get*",
      "s3:GetBucketLocation",
      "s3:ListAllMyBuckets"
    ]

    resources = ["*"]
  }

  statement {
    sid    = "DeploymentWriteAccess"
    effect = "Allow"

    actions = [
      "cloudformation:CreateChangeSet",
      "cloudformation:DeleteChangeSet",
      "cloudformation:ExecuteChangeSet",
      "cloudformation:UpdateStack",
      "ecr:BatchCheckLayerAvailability",
      "ecr:CompleteLayerUpload",
      "ecr:InitiateLayerUpload",
      "ecr:PutImage",
      "ecr:UploadLayerPart",
      "ecs:UpdateService",
      "eks:UpdateClusterConfig",
      "eks:UpdateNodegroupConfig",
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]

    resources = ["*"]
  }

  statement {
    sid    = "PassDeploymentRoles"
    effect = "Allow"

    actions   = ["iam:PassRole"]
    resources = ["arn:aws:iam::${var.aws_account_id}:role/*"]
  }
}

module "devops_team" {
  source = "../../../modules/entra-group-lookup"

  display_name = var.entra_synced_group_display_name
}

module "devops_deployment_access" {
  source = "../../../modules/identity-center-permission-set"

  name               = "DevOpsDeploymentAccess"
  description        = "Deployment access for the banking app DevOps team in dev."
  session_duration   = "PT8H"
  inline_policy_json = data.aws_iam_policy_document.devops_deployment_access.json
  tags               = var.tags
}

module "devops_account_assignment" {
  source = "../../../modules/account-assignment"

  instance_arn       = module.devops_deployment_access.instance_arn
  permission_set_arn = module.devops_deployment_access.arn
  principal_id       = module.devops_team.group_id
  principal_type     = "GROUP"
  target_account_id  = var.aws_account_id
}

data "aws_iam_roles" "devops_identity_center_role" {
  name_regex  = "AWSReservedSSO_${module.devops_deployment_access.name}_.*"
  path_prefix = "/aws-reserved/sso.amazonaws.com/"

  depends_on = [module.devops_account_assignment]
}

module "devops_eks_access" {
  source = "../../../modules/eks-access-entry"

  cluster_name      = var.eks_cluster_name
  principal_arn     = one(data.aws_iam_roles.devops_identity_center_role.arns)
  kubernetes_groups = [var.devops_kubernetes_group]
  tags              = var.tags
}

module "devops_pod_rbac" {
  source = "../../../modules/kubernetes-pod-rbac"

  namespace        = var.kubernetes_namespace
  kubernetes_group = var.devops_kubernetes_group

  depends_on = [module.devops_eks_access]
}

module "aws_identity_center_mfa" {
  source = "../../../modules/entra-mfa-conditional-access"

  policy_name                     = "Require MFA for AWS IAM Identity Center - Dev"
  target_application_display_name = var.aws_identity_center_enterprise_app_display_name
  state                           = var.conditional_access_state
  included_users                  = ["All"]
  excluded_users                  = var.conditional_access_excluded_users
}

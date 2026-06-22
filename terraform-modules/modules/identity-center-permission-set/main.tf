data "aws_ssoadmin_instances" "this" {}

locals {
  instance_arn = one(data.aws_ssoadmin_instances.this.arns)
}

resource "aws_ssoadmin_permission_set" "this" {
  name             = var.name
  description      = var.description
  instance_arn     = local.instance_arn
  relay_state      = var.relay_state
  session_duration = var.session_duration
  tags             = var.tags
}

resource "aws_ssoadmin_managed_policy_attachment" "this" {
  for_each = var.managed_policy_arns

  instance_arn       = local.instance_arn
  managed_policy_arn = each.value
  permission_set_arn = aws_ssoadmin_permission_set.this.arn
}

resource "aws_ssoadmin_permission_set_inline_policy" "this" {
  count = var.inline_policy_json == null ? 0 : 1

  inline_policy      = var.inline_policy_json
  instance_arn       = local.instance_arn
  permission_set_arn = aws_ssoadmin_permission_set.this.arn
}

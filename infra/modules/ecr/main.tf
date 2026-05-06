module "ecr" {
  for_each = toset(var.repository_names)
  source = "terraform-aws-modules/ecr/aws"
  repository_name = each.value

  repository_image_tag_mutability = "IMMUTABLE"
  repository_image_scan_on_push   = true

  #repository_read_write_access_arns = ["arn:aws:iam::012345678901:role/terraform"]
  repository_lifecycle_policy = jsonencode({
    rules = [
      {
        rulePriority = 1,
        description  = "Keep last 10 images",
        selection = {
          tagStatus     = "tagged",
          tagPrefixList = ["v"],
          countType     = "imageCountMoreThan",
          countNumber   = 10
        },
        action = {
          type = "expire"
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
  }
}
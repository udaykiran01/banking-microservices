output "arn" {
  description = "ARN of the IAM Identity Center permission set."
  value       = aws_ssoadmin_permission_set.this.arn
}

output "id" {
  description = "ID of the IAM Identity Center permission set."
  value       = aws_ssoadmin_permission_set.this.id
}

output "instance_arn" {
  description = "ARN of the IAM Identity Center instance."
  value       = local.instance_arn
}

output "name" {
  description = "Permission set name."
  value       = aws_ssoadmin_permission_set.this.name
}

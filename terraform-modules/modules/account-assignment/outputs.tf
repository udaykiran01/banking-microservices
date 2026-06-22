output "id" {
  description = "Account assignment resource ID."
  value       = aws_ssoadmin_account_assignment.this.id
}

output "target_account_id" {
  description = "AWS account ID that received the assignment."
  value       = aws_ssoadmin_account_assignment.this.target_id
}

output "principal_id" {
  description = "Identity Store principal ID assigned to the account."
  value       = aws_ssoadmin_account_assignment.this.principal_id
}

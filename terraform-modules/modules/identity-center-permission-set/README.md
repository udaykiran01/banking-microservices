# identity-center-permission-set

Reusable module for an AWS IAM Identity Center permission set.

It creates:

- `aws_ssoadmin_permission_set`
- optional AWS managed policy attachments
- optional inline policy

The module discovers the IAM Identity Center instance dynamically and does not require hardcoded account IDs.

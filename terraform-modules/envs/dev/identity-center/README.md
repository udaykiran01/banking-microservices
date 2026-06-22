# Dev IAM Identity Center and Entra MFA

This example creates the dev banking-app identity integration:

- Looks up the Entra-synced `AWS-DevOps-Team` group in AWS IAM Identity Center Identity Store.
- Creates the `DevOpsDeploymentAccess` permission set.
- Assigns that permission set to one AWS account.
- Maps the IAM Identity Center reserved role into EKS with an access entry.
- Grants the mapped Kubernetes group namespaced pod operation permissions.
- Creates an Entra Conditional Access policy requiring MFA for the AWS IAM Identity Center enterprise app.

## Prerequisites

- AWS IAM Identity Center is enabled in the AWS organization.
- Microsoft Entra ID is configured as the external identity provider for IAM Identity Center.
- The `AWS-DevOps-Team` Entra group is provisioned into IAM Identity Center through SCIM.
- The Entra enterprise app display name matches `AWS IAM Identity Center`, or you override `aws_identity_center_enterprise_app_display_name`.
- The target EKS cluster has access entries enabled with authentication mode `API` or `API_AND_CONFIG_MAP`.
- The Terraform runner can connect to the EKS Kubernetes API endpoint.
- Terraform credentials can manage AWS SSO Admin, AWS Identity Store, IAM policy documents, and Entra Conditional Access.

## Configure

```bash
cd terraform-modules/envs/dev/identity-center
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` and set:

- `aws_account_id`
- `entra_tenant_id`
- `eks_cluster_name`
- optional `conditional_access_excluded_users` for break-glass accounts

No AWS account IDs are hardcoded in the modules or example.

## Human Pod Access Flow

This example keeps IRSA separate from human access:

```text
AWS-DevOps-Team in Entra
  -> IAM Identity Center account assignment
  -> AWSReservedSSO_DevOpsDeploymentAccess_* IAM role
  -> EKS access entry
  -> Kubernetes group banking-devops
  -> RoleBinding in banking-app namespace
```

The Kubernetes RBAC allows DevOps users to list, watch, get, and delete pods, read pod logs, and start `pods/exec` sessions in the configured namespace.

## Run

```bash
terraform init
terraform plan
terraform apply
```

The Conditional Access policy defaults to `enabledForReportingButNotEnforced`. After validating sign-in logs, set:

```hcl
conditional_access_state = "enabled"
```

Then run:

```bash
terraform plan
terraform apply
```

# Banking Microservices Terraform

This directory contains reusable Terraform modules and environment examples for the banking application.

## Entra ID and AWS IAM Identity Center

The identity-center structure integrates Microsoft Entra ID with AWS IAM Identity Center:

```text
modules/
  account-assignment/
  entra-group-lookup/
  entra-mfa-conditional-access/
  eks-access-entry/
  identity-center-permission-set/
  kubernetes-pod-rbac/
envs/
  dev/
    identity-center/
```

The dev example creates:

- `AWS-DevOps-Team` access through an Entra group synced into IAM Identity Center.
- `DevOpsDeploymentAccess` IAM Identity Center permission set.
- One AWS account assignment, with the account ID supplied by variable.
- EKS access for the IAM Identity Center reserved role.
- Kubernetes RBAC for pod operations in the banking namespace.
- Entra Conditional Access MFA for the AWS IAM Identity Center enterprise app.

## Run the Dev Identity Example

```bash
cd terraform-modules/envs/dev/identity-center
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your AWS account ID, Entra tenant ID, and EKS cluster name.

```bash
terraform init
terraform plan
terraform apply
```

The MFA policy starts in report-only mode by default:

```hcl
conditional_access_state = "enabledForReportingButNotEnforced"
```

After validating Entra sign-in logs, switch it to:

```hcl
conditional_access_state = "enabled"
```

Then run `terraform plan` and `terraform apply` again.

# Banking AKS Terraform

This Terraform creates the Azure infrastructure needed by the repo:

- Resource group
- Azure Container Registry, importing the existing `bankingappdev` registry when present
- AKS cluster with Key Vault CSI enabled
- PostgreSQL Flexible Server and application database
- Key Vault secrets matching `postgres-spc.yaml`
- AcrPull permission from AKS to ACR
- GitHub Actions OIDC app/service principal for `.github/workflows/deploy-aks.yml`

## Usage

```bash
cd terraform
terraform init \
  -backend-config="resource_group_name=<tfstate-resource-group>" \
  -backend-config="storage_account_name=<tfstate-storage-account>" \
  -backend-config="container_name=<tfstate-container>" \
  -backend-config="key=banking-dev.tfstate" \
  -backend-config="use_azuread_auth=true"
terraform plan
terraform apply
```

Before applying, set `postgres_admin_password` in your local `terraform.tfvars` or as an environment variable:

```bash
export TF_VAR_postgres_admin_password='your-strong-password'
```

After apply, set these GitHub repository secrets from Terraform outputs:

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_SUBSCRIPTION_ID`

The Terraform GitHub workflow also needs:

- Repository secret `POSTGRES_ADMIN_PASSWORD`
- Optional repository secrets `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_KEY`, and `AZURE_OPENAI_DEPLOYMENT`
- Repository or environment variables `TF_STATE_RESOURCE_GROUP`, `TF_STATE_STORAGE_ACCOUNT`, and `TF_STATE_CONTAINER`

The Azure Storage account/container for Terraform state must exist before the first workflow run.

## Kubernetes Notes

The output `key_vault_csi_identity_client_id` should be used as `userAssignedIdentityID` in `postgres-spc.yaml`.

The application still expects these cluster add-ons/manifests to be installed separately:

- Strimzi Kafka operator and a Kafka cluster named `my-cluster` in namespace `kafka`
- NGINX ingress controller if you apply `api-ingress.yaml`
- Prometheus Operator CRDs if you apply `ServiceMonitor` or `PrometheusRule` manifests

Your GitHub workflow deploys the Helm chart and pushes images to the ACR created here.

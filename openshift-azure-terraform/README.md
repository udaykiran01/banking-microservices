# Production AKS Platform Notes

This folder contains the Terraform/Terragrunt and Helm configuration for the Azure banking AKS platform.

## Production Architecture

Production is designed as:

Internet -> Azure Front Door Premium + WAF -> Application Gateway WAF_v2 -> private AKS ingress -> services

The AKS module stays focused on the cluster. Edge, WAF, monitoring, private DNS, Key Vault, and database concerns are separate modules.

## Terragrunt Apply Order

Recommended prod order:

```bash
cd openshift-azure-terraform/infra/live/prod
terragrunt run-all apply --terragrunt-include-dir resource-group --non-interactive
terragrunt run-all apply --terragrunt-include-dir vnet --non-interactive
terragrunt run-all apply --terragrunt-include-dir private-dns --non-interactive
terragrunt run-all apply --terragrunt-include-dir acr --non-interactive
terragrunt run-all apply --terragrunt-include-dir monitoring --non-interactive
terragrunt run-all apply --terragrunt-include-dir aks --non-interactive
terragrunt run-all apply --terragrunt-include-dir keyvault --non-interactive
terragrunt run-all apply --terragrunt-include-dir postgres --non-interactive
terragrunt run-all apply --terragrunt-include-dir waf-policies --non-interactive
terragrunt run-all apply --terragrunt-include-dir application-gateway-waf --non-interactive
terragrunt run-all apply --terragrunt-include-dir front-door --non-interactive
```

For private AKS access, run `az aks get-credentials` from a machine on the VNet path: VPN, ExpressRoute, peered admin VNet, or Azure Bastion/jump host. The prod cluster does not expose a public API server.

## AKS Hardening

Prod AKS enables private API, Azure CNI, Azure Network Policy, Azure Policy, OIDC issuer, Workload Identity, Key Vault CSI driver, system/user node pools, autoscaling, zone spread, UDR egress, and maintenance windows.

Egress is controlled by Azure Firewall through the VNet module route table. Add required Azure Firewall application/network rules for ACR, Azure Monitor, Key Vault, Microsoft Entra ID, package registries, and any external service dependencies.

## Monitoring and Diagnostics

The monitoring module creates Log Analytics and enables Defender for Containers. Resource modules accept `log_analytics_workspace_id` and attach diagnostics for AKS, Key Vault, PostgreSQL, Application Gateway, and Front Door.

## Key Vault and Workload Identity

The Key Vault module can create user-assigned managed identities and federated credentials for Kubernetes service accounts. Use the output `workload_identity_client_ids.banking_app` as:

```yaml
serviceAccount:
  annotations:
    azure.workload.identity/client-id: "<client-id>"
keyVault:
  clientId: "<client-id>"
```

## Blue-Green

Enable blue-green by setting:

```yaml
deploymentStrategy:
  blueGreen:
    enabled: true
    activeColor: blue
```

Deploy the preview color by changing `activeColor` after validation. Services select the active color label, so traffic moves when the value changes.

## Canary

API canary is optional and supports NGINX canary annotations:

```bash
helm upgrade --install banking-app ./helm-charts/banking-app \
  -n banking-prod \
  -f helm-charts/banking-app/values-prod.yaml \
  --set deploymentStrategy.canary.enabled=true \
  --set deploymentStrategy.canary.useNginxCanary=true \
  --set deploymentStrategy.canary.imageTag=<sha> \
  --set deploymentStrategy.canary.weight=10
```

Argo Rollouts is not installed by this chart. If you choose Argo later, keep it as a separate platform add-on and replace the API canary Deployment with a Rollout resource.

## NetworkPolicy Tests

Render policies:

```bash
helm template banking-app openshift-azure-terraform/helm-charts/banking-app \
  -n banking-prod \
  -f openshift-azure-terraform/helm-charts/banking-app/values-prod.yaml
```

Test allowed frontend/dashboard to API:

```bash
kubectl exec -n banking-prod deploy/frontend-service -- curl -sS http://api-service:3000/health
kubectl exec -n banking-prod deploy/fraud-dashboard -- curl -sS http://api-service:3000/health
```

Test worker to fraud-service:

```bash
kubectl exec -n banking-prod deploy/worker-service -- curl -sS http://fraud-service:80/health
```

Test DNS:

```bash
kubectl exec -n banking-prod deploy/frontend-service -- nslookup api-service.banking-prod.svc.cluster.local
```

Test blocked pod-to-pod traffic by trying a path that is not explicitly allowed:

```bash
kubectl exec -n banking-prod deploy/frontend-service -- curl -m 5 http://fraud-service:80/health
```

That request should time out or be denied when NetworkPolicy is enforced.

## CI/CD

Production deployment is manual in `.github/workflows/deploy-aks-prod.yml` and should be protected by the GitHub `prod` environment. It uses immutable SHA tags, scans images, generates SBOMs, signs images with keyless Cosign, and deploys Helm with `--atomic --wait`.

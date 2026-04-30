output "resource_group_name" {
  description = "Resource group containing the deployment."
  value       = azurerm_resource_group.main.name
}

output "aks_cluster_name" {
  description = "AKS cluster name."
  value       = azurerm_kubernetes_cluster.main.name
}

output "aks_get_credentials_command" {
  description = "Command to configure kubectl for this AKS cluster."
  value       = "az aks get-credentials --resource-group ${azurerm_resource_group.main.name} --name ${azurerm_kubernetes_cluster.main.name} --overwrite-existing"
}

output "acr_login_server" {
  description = "ACR login server for image tags."
  value       = azurerm_container_registry.main.login_server
}

output "key_vault_name" {
  description = "Key Vault containing application secrets."
  value       = azurerm_key_vault.main.name
}

output "key_vault_csi_identity_client_id" {
  description = "Client ID to use in SecretProviderClass userAssignedIdentityID."
  value       = azurerm_kubernetes_cluster.main.key_vault_secrets_provider[0].secret_identity[0].client_id
}

output "postgres_fqdn" {
  description = "PostgreSQL Flexible Server FQDN."
  value       = azurerm_postgresql_flexible_server.main.fqdn
}

output "github_actions_client_id" {
  description = "Use this value for the AZURE_CLIENT_ID GitHub Actions secret."
  value       = azuread_application.github_actions.client_id
}

output "tenant_id" {
  description = "Use this value for the AZURE_TENANT_ID GitHub Actions secret."
  value       = data.azurerm_client_config.current.tenant_id
}

output "subscription_id" {
  description = "Use this value for the AZURE_SUBSCRIPTION_ID GitHub Actions secret."
  value       = data.azurerm_client_config.current.subscription_id
}

output "resource_group_name" {
  value = var.resource_group_name
}

output "key_vault_id" {
  value = azurerm_key_vault.kv.id
}

output "key_vault_name" {
  value = azurerm_key_vault.kv.name
}

output "key_vault_uri" {
  value = azurerm_key_vault.kv.vault_uri
}

output "workload_identity_client_ids" {
  value = {
    for key, identity in azurerm_user_assigned_identity.workload : key => identity.client_id
  }
}

output "workload_identity_principal_ids" {
  value = {
    for key, identity in azurerm_user_assigned_identity.workload : key => identity.principal_id
  }
}

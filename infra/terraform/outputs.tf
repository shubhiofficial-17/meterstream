output "dashboard_url" {
  value = "https://${azurerm_container_app.api.ingress[0].fqdn}"
}

output "postgres_fqdn" {
  value = azurerm_postgresql_flexible_server.db.fqdn
}

output "resource_group" {
  value = azurerm_resource_group.rg.name
}

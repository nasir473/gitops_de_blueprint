output "catalog_name" {
  description = "The name of the provisioned Unity Catalog"
  value       = databricks_catalog.env_catalog.name
}

output "external_location_name" {
  description = "The name of the Databricks External Location"
  value       = databricks_external_location.silver_delta.name
}

output "external_location_url" {
  description = "The URL of the Databricks External Location"
  value       = databricks_external_location.silver_delta.url
}

output "schema_name" {
  description = "The name of the provisioned silver schema"
  value       = databricks_schema.silver.name
}

output "gold_schema_name" {
  description = "The name of the provisioned gold schema"
  value       = databricks_schema.gold.name
}


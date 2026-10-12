terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.40.0"
    }
  }
}

# ------------------------------------------------------------------------------
# Storage Credential for Unity Catalog S3 Access
# ------------------------------------------------------------------------------
resource "databricks_storage_credential" "external" {
  name = var.storage_credential_name
  aws_iam_role {
    role_arn = var.iam_role_arn
  }
  comment = "Storage credential for Unity Catalog S3 access"
}

# ------------------------------------------------------------------------------
# External Location for Silver Delta Folder
# Path: s3://<silver-bucket>/delta/
# ------------------------------------------------------------------------------
resource "databricks_external_location" "silver_delta" {
  name            = "${var.project_name}_silver_delta_${var.environment}"
  url             = "s3://${var.silver_bucket_name}/delta"
  credential_name = databricks_storage_credential.external.name
  comment         = "External location pointing to curated Silver Delta Lake folder in S3"

  depends_on = [databricks_storage_credential.external]
}

# ------------------------------------------------------------------------------
# Unity Catalog: gitops_dev_catalog
# Managed location backed by the Silver Delta external location
# ------------------------------------------------------------------------------
resource "databricks_catalog" "env_catalog" {
  name          = "${var.project_name}_${var.environment}_catalog"
  comment       = "Platform Unity Catalog managed via Terraform for ${var.environment}"
  storage_root  = databricks_external_location.silver_delta.url
  force_destroy = false

  properties = {
    purpose     = "gitops-data-platform"
    environment = var.environment
  }

  depends_on = [databricks_external_location.silver_delta]
}

# ------------------------------------------------------------------------------
# Unity Catalog Schema: silver
# Schema inside gitops_dev_catalog for curated Delta tables
# ------------------------------------------------------------------------------
resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.env_catalog.name
  name         = "silver"
  comment      = "Silver curated schema managed via Terraform for ${var.environment}"

  properties = {
    purpose     = "curated-silver-delta"
    environment = var.environment
  }
}

# ------------------------------------------------------------------------------
# Unity Catalog Schema: gold
# Schema inside gitops_dev_catalog for analytical/business-level Delta tables
# ------------------------------------------------------------------------------
resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.env_catalog.name
  name         = "gold"
  comment      = "Gold analytical schema managed via Terraform for ${var.environment}"

  properties = {
    purpose     = "analytical-gold-delta"
    environment = var.environment
  }
}


variable "aws_region" {
  description = "The AWS region to deploy resources into"
  type        = string
  default     = "ap-south-1"
}

variable "environment" {
  description = "Target environment stage (dev, qa, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name prefix used for resource identification"
  type        = string
  default     = "gitops"
}

variable "aws_account_id" {
  description = "AWS account ID (defaults to active session caller identity if blank)"
  type        = string
  default     = ""
}

variable "force_destroy" {
  description = "Allow destroying S3 buckets with objects inside (safe for dev)"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default = {
    Owner      = "DataEngineering"
    ManagedBy  = "Terraform"
    Repository = "gitops_de_blueprint"
  }
}

# ==============================================================================
# Databricks Workspace Configuration
# ==============================================================================
variable "enable_databricks" {
  description = "Toggle provisioning of Databricks resources"
  type        = bool
  default     = false
}

variable "databricks_host" {
  description = "Databricks Workspace URL (e.g., https://dbc-xxxx.cloud.databricks.com)"
  type        = string
  default     = ""
}

variable "databricks_token" {
  description = "Databricks Personal Access Token (PAT) or OAuth token"
  type        = string
  default     = ""
  sensitive   = true
}

# ==============================================================================
# Snowflake Iceberg Integration Configuration
# ==============================================================================
variable "enable_snowflake" {
  description = "Toggle provisioning of Snowflake IAM integration resources"
  type        = bool
  default     = true
}

variable "snowflake_storage_aws_iam_user_arn" {
  description = "Snowflake IAM user ARN obtained from DESC EXTERNAL VOLUME / DESC CATALOG INTEGRATION"
  type        = string
  default     = "arn:aws:iam::648423931155:user/ff7c2000-s"
}

variable "snowflake_storage_aws_external_id" {
  description = "Snowflake External ID obtained from DESC EXTERNAL VOLUME"
  type        = string
  default     = "VW16761_SFCRole=6_6mxxGpIEcUlW/XT2uc6Da+S6+ho="
}

variable "snowflake_catalog_aws_external_id" {
  description = "Snowflake External ID obtained from DESC CATALOG INTEGRATION"
  type        = string
  default     = "VW16761_SFCRole=6_mmjOTF+pBLVxyRnViGg1cTFnP5U="
}

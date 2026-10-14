variable "project_name" {
  description = "Project name prefix used for resource identification"
  type        = string
  default     = "gitops"
}

variable "environment" {
  description = "Target environment stage (dev, qa, prod)"
  type        = string
}

variable "aws_account_id" {
  description = "The AWS Account ID where resources are deployed"
  type        = string
}

variable "aws_region" {
  description = "The AWS Region where resources are deployed"
  type        = string
  default     = "us-east-1"
}

variable "silver_bucket_arn" {
  description = "ARN of the Silver S3 bucket containing the Iceberg warehouse"
  type        = string
}

variable "silver_bucket_name" {
  description = "Name of the Silver S3 bucket"
  type        = string
}

variable "glue_database_name" {
  description = "Name of the AWS Glue Data Catalog database containing the Iceberg metadata"
  type        = string
  default     = "gitops_dev_db"
}

variable "snowflake_storage_aws_iam_user_arn" {
  description = "Snowflake IAM user ARN obtained from DESC EXTERNAL VOLUME / DESC CATALOG INTEGRATION"
  type        = string
  default     = ""
}

variable "snowflake_storage_aws_external_id" {
  description = "Snowflake External ID obtained from DESC EXTERNAL VOLUME"
  type        = string
  default     = ""
}

variable "snowflake_catalog_aws_external_id" {
  description = "Snowflake External ID obtained from DESC CATALOG INTEGRATION"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Common tags applied to Snowflake IAM resources"
  type        = map(string)
  default     = {}
}

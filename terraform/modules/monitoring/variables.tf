variable "project_name" {
  description = "Project name prefix used for resource identification"
  type        = string
}

variable "environment" {
  description = "Deployment environment (e.g., dev, qa, prod)"
  type        = string
}

variable "aws_region" {
  description = "The AWS region where resources are deployed"
  type        = string
  default     = "ap-south-1"
}

variable "dashboard_name" {
  description = "Custom dashboard name (defaults to project-environment standard if empty)"
  type        = string
  default     = ""
}

# Lambda Variables
variable "lambda_function_name" {
  description = "Name of the Manifest Validator Lambda function"
  type        = string
}

# DynamoDB Variables
variable "dynamodb_table_name" {
  description = "Name of the DynamoDB batch barrier coordination table"
  type        = string
}

# SQS Variables
variable "sqs_primary_queue_name" {
  description = "Name of the primary SQS batch ready queue"
  type        = string
}

variable "sqs_dlq_name" {
  description = "Name of the SQS Dead-Letter Queue (DLQ)"
  type        = string
}

variable "sqs_dlq_alarm_arn" {
  description = "ARN of the existing SQS DLQ CloudWatch alarm"
  type        = string
}

# SNS Variables
variable "sns_batch_topic_name" {
  description = "Name of the SNS batch events topic"
  type        = string
}

variable "sns_alerts_topic_name" {
  description = "Name of the SNS operational alerts topic"
  type        = string
}

variable "sns_alerts_topic_arn" {
  description = "ARN of the SNS operational alerts topic for alarm actions"
  type        = string
}

# Glue Variables
variable "glue_parquet_job_name" {
  description = "Name of the Glue Bronze-to-Silver Parquet transformation job"
  type        = string
}

variable "glue_delta_job_name" {
  description = "Name of the Glue Bronze-to-Silver Delta Lake transformation job"
  type        = string
}

variable "glue_iceberg_job_name" {
  description = "Name of the Glue Bronze-to-Silver Apache Iceberg transformation job"
  type        = string
}

variable "glue_crawler_name" {
  description = "Name of the Glue Crawler for Silver Parquet partitions"
  type        = string
  default     = ""
}

# Athena Variables
variable "athena_workgroup_name" {
  description = "Name of the Athena workgroup"
  type        = string
}

# S3 Variables
variable "bronze_bucket_name" {
  description = "Name of the S3 Bronze bucket"
  type        = string
}

variable "silver_bucket_name" {
  description = "Name of the S3 Silver bucket"
  type        = string
}

variable "scripts_bucket_name" {
  description = "Name of the S3 Scripts bucket"
  type        = string
}

variable "athena_results_bucket_name" {
  description = "Name of the S3 Athena Results bucket"
  type        = string
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}

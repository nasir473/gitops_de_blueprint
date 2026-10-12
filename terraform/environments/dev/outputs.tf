output "bronze_bucket_id" {
  description = "The ID/Name of the Bronze S3 bucket"
  value       = module.s3_bronze.bucket_id
}

output "bronze_bucket_name" {
  description = "The globally unique name of the Bronze S3 bucket"
  value       = module.s3_bronze.bucket_name
}

output "bronze_bucket_arn" {
  description = "The ARN of the Bronze S3 bucket"
  value       = module.s3_bronze.bucket_arn
}

output "bronze_bucket_regional_domain_name" {
  description = "The regional domain name of the Bronze S3 bucket"
  value       = module.s3_bronze.bucket_regional_domain_name
}

output "silver_bucket_id" {
  description = "The ID/Name of the Silver S3 bucket"
  value       = module.s3_silver.bucket_id
}

output "silver_bucket_name" {
  description = "The globally unique name of the Silver S3 bucket"
  value       = module.s3_silver.bucket_name
}

output "silver_bucket_arn" {
  description = "The ARN of the Silver S3 bucket"
  value       = module.s3_silver.bucket_arn
}

output "silver_bucket_regional_domain_name" {
  description = "The regional domain name of the Silver S3 bucket"
  value       = module.s3_silver.bucket_regional_domain_name
}

output "scripts_bucket_id" {
  description = "The ID/Name of the Scripts S3 bucket"
  value       = module.s3_scripts.bucket_id
}

output "scripts_bucket_name" {
  description = "The globally unique name of the Scripts S3 bucket"
  value       = module.s3_scripts.bucket_name
}

output "scripts_bucket_arn" {
  description = "The ARN of the Scripts S3 bucket"
  value       = module.s3_scripts.bucket_arn
}

output "scripts_bucket_regional_domain_name" {
  description = "The regional domain name of the Scripts S3 bucket"
  value       = module.s3_scripts.bucket_regional_domain_name
}

output "glue_job_name" {
  description = "The name of the Bronze to Silver Glue ETL job"
  value       = module.glue_bronze_to_silver.job_name
}

output "glue_job_arn" {
  description = "The ARN of the Bronze to Silver Glue ETL job"
  value       = module.glue_bronze_to_silver.job_arn
}

output "glue_job_role_arn" {
  description = "The ARN of the IAM role used by the Glue ETL job"
  value       = module.glue_bronze_to_silver.role_arn
}

output "glue_delta_job_name" {
  description = "The name of the Bronze to Silver Delta Glue ETL job"
  value       = module.glue_bronze_to_silver_delta.job_name
}

output "glue_delta_job_arn" {
  description = "The ARN of the Bronze to Silver Delta Glue ETL job"
  value       = module.glue_bronze_to_silver_delta.job_arn
}

output "glue_delta_job_role_arn" {
  description = "The ARN of the IAM role used by the Delta Glue ETL job"
  value       = module.glue_bronze_to_silver_delta.role_arn
}

output "glue_iceberg_job_name" {
  description = "The name of the Bronze to Silver Iceberg Glue ETL job"
  value       = module.glue_bronze_to_silver_iceberg.job_name
}

output "glue_iceberg_job_arn" {
  description = "The ARN of the Bronze to Silver Iceberg Glue ETL job"
  value       = module.glue_bronze_to_silver_iceberg.job_arn
}

output "glue_iceberg_job_role_arn" {
  description = "The ARN of the IAM role used by the Iceberg Glue ETL job"
  value       = module.glue_bronze_to_silver_iceberg.role_arn
}

output "athena_results_bucket_name" {
  description = "The name of the S3 bucket used for Athena query results"
  value       = module.s3_athena_results.bucket_name
}

output "athena_results_bucket_arn" {
  description = "The ARN of the S3 bucket used for Athena query results"
  value       = module.s3_athena_results.bucket_arn
}

output "athena_workgroup_name" {
  description = "The name of the dedicated Athena workgroup"
  value       = aws_athena_workgroup.dev.name
}

output "glue_database_name" {
  description = "The name of the Glue Data Catalog database"
  value       = aws_glue_catalog_database.this.name
}

output "glue_silver_parquet_crawler_name" {
  description = "The name of the Glue crawler strictly for Silver Parquet data"
  value       = aws_glue_crawler.silver_parquet_crawler.name
}

# ==============================================================================
# Databricks Resource Outputs
# ==============================================================================
output "databricks_catalog_name" {
  description = "The Databricks Unity Catalog name"
  value       = var.enable_databricks ? module.databricks[0].catalog_name : null
}

output "databricks_external_location_name" {
  description = "The Databricks External Location name"
  value       = var.enable_databricks ? module.databricks[0].external_location_name : null
}

output "databricks_external_location_url" {
  description = "The Databricks External Location S3 URL"
  value       = var.enable_databricks ? module.databricks[0].external_location_url : null
}

output "databricks_schema_name" {
  description = "The Databricks Unity Catalog schema name"
  value       = var.enable_databricks ? module.databricks[0].schema_name : null
}

output "databricks_gold_schema_name" {
  description = "The Databricks Unity Catalog gold schema name"
  value       = var.enable_databricks ? module.databricks[0].gold_schema_name : null
}

output "sqs_batch_ready_queue_url" {
  description = "The URL of the SQS Batch Ready Queue"
  value       = module.sqs_batch_ready.queue_url
}

output "sqs_batch_ready_queue_arn" {
  description = "The ARN of the SQS Batch Ready Queue"
  value       = module.sqs_batch_ready.queue_arn
}

output "sqs_batch_ready_dlq_url" {
  description = "The URL of the SQS Batch Ready Dead Letter Queue"
  value       = module.sqs_batch_ready.dlq_url
}

output "dynamodb_batch_coordination_table_name" {
  description = "The name of the DynamoDB batch coordination table"
  value       = module.dynamodb_batch_coordination.table_name
}

output "dynamodb_batch_coordination_table_arn" {
  description = "The ARN of the DynamoDB batch coordination table"
  value       = module.dynamodb_batch_coordination.table_arn
}

output "lambda_manifest_validator_function_name" {
  description = "The name of the Manifest Validator Lambda function"
  value       = module.lambda_manifest_validator.function_name
}

output "lambda_manifest_validator_function_arn" {
  description = "The ARN of the Manifest Validator Lambda function"
  value       = module.lambda_manifest_validator.function_arn
}

output "sns_batch_events_topic_arn" {
  description = "The ARN of the SNS Batch Events Fan-Out topic"
  value       = module.sns_batch_events.topic_arn
}

output "sns_batch_events_topic_name" {
  description = "The name of the SNS Batch Events Fan-Out topic"
  value       = module.sns_batch_events.topic_name
}

output "sns_pipeline_alerts_topic_arn" {
  description = "The ARN of the SNS Pipeline Alerts topic"
  value       = module.sns_pipeline_alerts.topic_arn
}

output "sns_pipeline_alerts_topic_name" {
  description = "The name of the SNS Pipeline Alerts topic"
  value       = module.sns_pipeline_alerts.topic_name
}

output "cloudwatch_dashboard_name" {
  description = "Name of the Unified Platform CloudWatch Dashboard"
  value       = module.monitoring.dashboard_name
}

output "cloudwatch_dashboard_arn" {
  description = "ARN of the Unified Platform CloudWatch Dashboard"
  value       = module.monitoring.dashboard_arn
}

output "cloudwatch_lambda_errors_alarm_arn" {
  description = "ARN of the Lambda execution errors CloudWatch alarm"
  value       = module.monitoring.lambda_errors_alarm_arn
}




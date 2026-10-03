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




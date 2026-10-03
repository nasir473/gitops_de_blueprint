data "aws_caller_identity" "current" {}

locals {
  account_id = var.aws_account_id != "" ? var.aws_account_id : data.aws_caller_identity.current.account_id
}

# ==============================================================================
# Bronze Layer S3 Storage (Raw Ingestion Landing Zone)
# ==============================================================================
module "s3_bronze" {
  source = "../../modules/s3"

  bucket_name   = "${var.project_name}-bronze-bkt-${var.environment}-${local.account_id}"
  environment   = var.environment
  force_destroy = var.force_destroy

  transition_ia_days                 = 30
  transition_glacier_days            = 90
  noncurrent_version_expiration_days = 90

  layer = "Bronze"
  tags  = var.tags
}

# ==============================================================================
# Silver Layer S3 Storage (Curated & Validated Parquet Zone)
# ==============================================================================
module "s3_silver" {
  source = "../../modules/s3"

  bucket_name   = "${var.project_name}-silver-bkt-${var.environment}-${local.account_id}"
  environment   = var.environment
  layer         = "Silver"
  force_destroy = var.force_destroy

  transition_ia_days                 = 60
  transition_glacier_days            = 180
  noncurrent_version_expiration_days = 90

  tags = var.tags
}

# ==============================================================================
# Scripts & Artifacts S3 Storage (Lambda, Glue, Utilities)
# ==============================================================================
module "s3_scripts" {
  source = "../../modules/s3"

  bucket_name   = "${var.project_name}-scripts-bkt-${var.environment}-${local.account_id}"
  environment   = var.environment
  layer         = "Scripts"
  force_destroy = var.force_destroy

  # Code/scripts do not transition to Glacier/IA to preserve low-latency execution
  enable_tiering                     = false
  noncurrent_version_expiration_days = 90

  tags = var.tags
}

# ==============================================================================
# S3 Script Deployment: Bronze-to-Silver PySpark Script
# ==============================================================================
resource "aws_s3_object" "glue_script_bronze_to_silver" {
  bucket = module.s3_scripts.bucket_id
  key    = "glue/jobs/bronze_to_silver.py"
  source = "${path.module}/../../../src/glue/jobs/bronze_to_silver.py"
  etag   = filemd5("${path.module}/../../../src/glue/jobs/bronze_to_silver.py")

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

# ==============================================================================
# AWS Glue ETL Job: Bronze to Silver Employees Transformation
# ==============================================================================
module "glue_bronze_to_silver" {
  source = "../../modules/glue"

  job_name        = "${var.project_name}-bronze-to-silver-employees-${var.environment}"
  job_description = "ETL pipeline transforming raw employee CSV files from Bronze to curated Parquet in Silver"
  script_location = "s3://${module.s3_scripts.bucket_name}/${aws_s3_object.glue_script_bronze_to_silver.key}"

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  source_bucket_arn  = module.s3_bronze.bucket_arn
  target_bucket_arn  = module.s3_silver.bucket_arn
  scripts_bucket_arn = module.s3_scripts.bucket_arn
  environment        = var.environment

  default_arguments = {
    "--source_bucket" = module.s3_bronze.bucket_name
    "--target_bucket" = module.s3_silver.bucket_name
    "--source_prefix" = "raw/employees/"
    "--target_prefix" = "curated/employees/"
    "--environment"   = var.environment
  }

  tags = var.tags

  depends_on = [aws_s3_object.glue_script_bronze_to_silver]
}



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


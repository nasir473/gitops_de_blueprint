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

# ==============================================================================
# S3 Script Deployment: Bronze-to-Silver Delta PySpark Script
# ==============================================================================
resource "aws_s3_object" "glue_script_bronze_to_silver_delta" {
  bucket = module.s3_scripts.bucket_id
  key    = "glue/jobs/bronze_to_silver_delta.py"
  source = "${path.module}/../../../src/glue/jobs/bronze_to_silver_delta.py"
  etag   = filemd5("${path.module}/../../../src/glue/jobs/bronze_to_silver_delta.py")

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

# ==============================================================================
# AWS Glue ETL Job: Bronze to Silver Delta Lake Transformation
# ==============================================================================
module "glue_bronze_to_silver_delta" {
  source = "../../modules/glue"

  job_name        = "${var.project_name}-bronze-to-silver-delta-employees-${var.environment}"
  job_description = "ETL pipeline transforming raw employee CSV files from Bronze to curated Delta Lake in Silver"
  script_location = "s3://${module.s3_scripts.bucket_name}/${aws_s3_object.glue_script_bronze_to_silver_delta.key}"

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  source_bucket_arn  = module.s3_bronze.bucket_arn
  target_bucket_arn  = module.s3_silver.bucket_arn
  scripts_bucket_arn = module.s3_scripts.bucket_arn
  environment        = var.environment

  default_arguments = {
    "--datalake-formats" = "delta"
    "--conf"             = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    "--source_bucket"    = module.s3_bronze.bucket_name
    "--target_bucket"    = module.s3_silver.bucket_name
    "--source_prefix"    = "raw/employees/"
    "--target_prefix"    = "delta/employees/"
    "--environment"      = var.environment
  }

  tags = var.tags

  depends_on = [aws_s3_object.glue_script_bronze_to_silver_delta]
}

# ==============================================================================
# Athena Query Results S3 Storage
# ==============================================================================
module "s3_athena_results" {
  source = "../../modules/s3"

  bucket_name   = "${var.project_name}-athena-results-bkt-${var.environment}-${local.account_id}"
  environment   = var.environment
  layer         = "Athena"
  force_destroy = var.force_destroy

  # Athena query output spooling does not require Glacier archiving
  enable_tiering                     = false
  noncurrent_version_expiration_days = 7

  tags = var.tags
}

# ==============================================================================
# Amazon Athena Workgroup Configuration
# ==============================================================================
resource "aws_athena_workgroup" "dev" {
  name        = "${var.project_name}-${var.environment}-workgroup"
  description = "Dedicated Athena workgroup for ${var.project_name} modern data platform (${var.environment})"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${module.s3_athena_results.bucket_name}/query-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
      Name        = "${var.project_name}-${var.environment}-workgroup"
    }
  )
}

# ==============================================================================
# AWS Glue Data Catalog Database
# ==============================================================================
resource "aws_glue_catalog_database" "this" {
  name        = "${var.project_name}_${var.environment}_db"
  description = "Glue Data Catalog database for ${var.project_name} ${var.environment} data lake"

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

# ==============================================================================
# IAM Role & Policy for Glue Crawler (Strictly Restricted to Curated Parquet)
# ==============================================================================
data "aws_iam_policy_document" "crawler_assume_role" {
  statement {
    sid     = "GlueCrawlerAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_crawler_role" {
  name               = "${var.project_name}-silver-parquet-crawler-${var.environment}-role"
  description        = "Execution role for Glue crawler restricted strictly to curated Parquet folder"
  assume_role_policy = data.aws_iam_policy_document.crawler_assume_role.json

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

resource "aws_iam_role_policy_attachment" "crawler_glue_service" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "crawler_s3_restricted" {
  statement {
    sid    = "RestrictedSilverParquetRead"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
    # STRICT CONSTRAINT: Limited only to curated/employees/ folder and bucket listing
    resources = [
      module.s3_silver.bucket_arn,
      "${module.s3_silver.bucket_arn}/curated/employees/*",
      "${module.s3_silver.bucket_arn}/curated/employees"
    ]
  }
}

resource "aws_iam_policy" "crawler_s3_restricted" {
  name        = "${var.project_name}-silver-parquet-crawler-s3-${var.environment}"
  description = "Least privilege S3 access policy strictly limited to curated Parquet folder"
  policy      = data.aws_iam_policy_document.crawler_s3_restricted.json

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

resource "aws_iam_role_policy_attachment" "crawler_s3_attach" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = aws_iam_policy.crawler_s3_restricted.arn
}

# ==============================================================================
# AWS Glue Crawler: Strictly Crawls ONLY curated/employees/ (Parquet)
# ==============================================================================
resource "aws_glue_crawler" "silver_parquet_crawler" {
  name          = "${var.project_name}-silver-parquet-crawler-${var.environment}"
  description   = "Crawls only curated Parquet files under s3://${module.s3_silver.bucket_name}/curated/employees/"
  database_name = aws_glue_catalog_database.this.name
  role          = aws_iam_role.glue_crawler_role.arn

  # STRICT TARGET: Only curated/employees/, strictly excluding delta/
  s3_target {
    path = "s3://${module.s3_silver.bucket_name}/curated/employees/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
      Layer       = "Silver"
      Format      = "Parquet"
    }
  )

  depends_on = [
    aws_iam_role_policy_attachment.crawler_glue_service,
    aws_iam_role_policy_attachment.crawler_s3_attach,
    module.s3_silver
  ]
}





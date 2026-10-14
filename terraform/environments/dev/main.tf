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
# S3 Script Deployment: Bronze-to-Silver Iceberg PySpark Script
# ==============================================================================
resource "aws_s3_object" "glue_script_bronze_to_silver_iceberg" {
  bucket = module.s3_scripts.bucket_id
  key    = "glue/jobs/bronze_to_silver_iceberg.py"
  source = "${path.module}/../../../src/glue/jobs/bronze_to_silver_iceberg.py"
  etag   = filemd5("${path.module}/../../../src/glue/jobs/bronze_to_silver_iceberg.py")

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

# ==============================================================================
# AWS Glue ETL Job: Bronze to Silver Apache Iceberg Transformation
# ==============================================================================
module "glue_bronze_to_silver_iceberg" {
  source = "../../modules/glue"

  job_name        = "${var.project_name}-bronze-to-silver-iceberg-employees-${var.environment}"
  job_description = "ETL pipeline transforming raw employee CSV files from Bronze to curated Apache Iceberg in Silver"
  script_location = "s3://${module.s3_scripts.bucket_name}/${aws_s3_object.glue_script_bronze_to_silver_iceberg.key}"

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  source_bucket_arn  = module.s3_bronze.bucket_arn
  target_bucket_arn  = module.s3_silver.bucket_arn
  scripts_bucket_arn = module.s3_scripts.bucket_arn
  environment        = var.environment

  default_arguments = {
    "--datalake-formats" = "iceberg"
    "--conf"             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${module.s3_silver.bucket_name}/iceberg/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    "--source_bucket"    = module.s3_bronze.bucket_name
    "--target_bucket"    = module.s3_silver.bucket_name
    "--source_prefix"    = "raw/employees/"
    "--target_prefix"    = "iceberg/employees/"
    "--database_name"    = "${var.project_name}_${var.environment}_db"
    "--table_name"       = "employees_iceberg"
    "--catalog_name"     = "glue_catalog"
    "--environment"      = var.environment
  }

  tags = var.tags

  depends_on = [aws_s3_object.glue_script_bronze_to_silver_iceberg]
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

# ==============================================================================
# AWS IAM Role for Databricks Unity Catalog Storage Credential
# Role: arn:aws:iam::790347819012:role/gitops-databricks-unity-catalog-silver-role
# ==============================================================================
data "databricks_aws_unity_catalog_assume_role_policy" "databricks_uc_assume_role" {
  count          = var.enable_databricks ? 1 : 0
  aws_account_id = local.account_id
  role_name      = "gitops-databricks-unity-catalog-silver-role"
  external_id    = "36cf4c4b-d05b-4464-9925-93fe70a09621"
}

resource "aws_iam_role" "databricks_uc_role" {
  count              = var.enable_databricks ? 1 : 0
  name               = "gitops-databricks-unity-catalog-silver-role"
  description        = "IAM role assumed by Databricks Unity Catalog to access curated Silver Delta files in S3"
  assume_role_policy = data.databricks_aws_unity_catalog_assume_role_policy.databricks_uc_assume_role[0].json

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
      Service     = "Databricks"
    }
  )
}

data "aws_iam_policy_document" "databricks_uc_s3_policy" {
  count = var.enable_databricks ? 1 : 0

  statement {
    sid    = "DatabricksSilverBucketAccess"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      module.s3_silver.bucket_arn
    ]
  }

  statement {
    sid    = "DatabricksSilverObjectAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    resources = [
      "${module.s3_silver.bucket_arn}/*"
    ]
  }

  statement {
    sid       = "DatabricksSelfAssumeRole"
    effect    = "Allow"
    actions   = ["sts:AssumeRole"]
    resources = ["arn:aws:iam::${local.account_id}:role/gitops-databricks-unity-catalog-silver-role"]
  }
}

resource "aws_iam_policy" "databricks_uc_s3" {
  count       = var.enable_databricks ? 1 : 0
  name        = "${var.project_name}-databricks-uc-silver-s3-${var.environment}"
  description = "Allows Databricks Unity Catalog to access delta folder in Silver S3 bucket"
  policy      = data.aws_iam_policy_document.databricks_uc_s3_policy[0].json

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

resource "aws_iam_role_policy_attachment" "databricks_uc_s3_attach" {
  count      = var.enable_databricks ? 1 : 0
  role       = aws_iam_role.databricks_uc_role[0].name
  policy_arn = aws_iam_policy.databricks_uc_s3[0].arn
}

# ==============================================================================
# Databricks Resource Management: External Location & Unity Catalog
# ==============================================================================
module "databricks" {
  count  = var.enable_databricks ? 1 : 0
  source = "../../modules/databricks"

  project_name            = var.project_name
  environment             = var.environment
  silver_bucket_name      = module.s3_silver.bucket_name
  storage_credential_name = "gitops-silver-delta-credential"
  iam_role_arn            = aws_iam_role.databricks_uc_role[0].arn
  tags                    = var.tags

  depends_on = [
    aws_iam_role_policy_attachment.databricks_uc_s3_attach,
    module.s3_silver
  ]
}

# ==============================================================================
# Snowflake Iceberg & Glue Integration IAM Resources
# ==============================================================================
module "snowflake_iam" {
  count  = var.enable_snowflake ? 1 : 0
  source = "../../modules/snowflake_iam"

  project_name                       = var.project_name
  environment                        = var.environment
  aws_account_id                     = local.account_id
  aws_region                         = var.aws_region
  silver_bucket_arn                  = module.s3_silver.bucket_arn
  silver_bucket_name                 = module.s3_silver.bucket_name
  glue_database_name                 = aws_glue_catalog_database.this.name
  snowflake_storage_aws_iam_user_arn = var.snowflake_storage_aws_iam_user_arn
  snowflake_storage_aws_external_id  = var.snowflake_storage_aws_external_id
  snowflake_catalog_aws_external_id  = var.snowflake_catalog_aws_external_id
  tags                               = var.tags

  depends_on = [
    module.s3_silver,
    aws_glue_catalog_database.this
  ]
}

# ==============================================================================
# SQS Queue: Batch Ready Queue & DLQ
# ==============================================================================
module "sqs_batch_ready" {
  source = "../../modules/sqs"

  queue_name                 = "${var.project_name}-batch-ready-queue-${var.environment}"
  environment                = var.environment
  visibility_timeout_seconds = 300
  message_retention_seconds  = 345600
  enable_dlq                 = true
  max_receive_count          = 3
  tags                       = var.tags
}

# ==============================================================================
# Amazon SNS Topic: Batch Events Fan-Out Pattern
# ==============================================================================
module "sns_batch_events" {
  source = "../../modules/sns"

  topic_name      = "${var.project_name}-batch-events-topic-${var.environment}"
  environment     = var.environment
  display_name    = "GitOps DE Batch Events (${var.environment})"
  sqs_subscribers = [module.sqs_batch_ready.queue_arn]
  tags            = var.tags
}

# SQS Queue Policy: Allow SNS Topic to Send Messages to SQS (Fan-Out)
resource "aws_sqs_queue_policy" "batch_ready_allow_sns" {
  queue_url = module.sqs_batch_ready.queue_url

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowSNSToSendMessages"
        Effect = "Allow"
        Principal = {
          Service = "sns.amazonaws.com"
        }
        Action   = "sqs:SendMessage"
        Resource = module.sqs_batch_ready.queue_arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = module.sns_batch_events.topic_arn
          }
        }
      }
    ]
  })
}

# ==============================================================================
# Amazon SNS Topic: Operational Alerts & Incident Management
# ==============================================================================
module "sns_pipeline_alerts" {
  source = "../../modules/sns"

  topic_name   = "${var.project_name}-pipeline-alerts-topic-${var.environment}"
  environment  = var.environment
  display_name = "GitOps DE Pipeline Alerts (${var.environment})"
  tags         = var.tags
}

# ==============================================================================
# CloudWatch Metric Alarm: SQS Dead Letter Queue (DLQ) Monitoring
# ==============================================================================
resource "aws_cloudwatch_metric_alarm" "sqs_dlq_alarm" {
  alarm_name          = "${var.project_name}-sqs-dlq-messages-alarm-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Maximum"
  threshold           = 0
  alarm_description   = "Triggers when any failed message lands in the SQS DLQ"
  alarm_actions       = [module.sns_pipeline_alerts.topic_arn]

  dimensions = {
    QueueName = "${var.project_name}-batch-ready-queue-${var.environment}-dlq"
  }

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

# ==============================================================================
# DynamoDB Table: Multi-Source Batch Coordination State
# ==============================================================================
module "dynamodb_batch_coordination" {
  source = "../../modules/dynamodb"

  table_name  = "${var.project_name}-batch-coordination-${var.environment}"
  environment = var.environment
  hash_key    = "batch_date"
  enable_ttl  = true
  tags        = var.tags
}

# ==============================================================================
# Lambda Function: Manifest Validator & Batch Barrier
# ==============================================================================
module "lambda_manifest_validator" {
  source = "../../modules/lambda"

  function_name = "${var.project_name}-manifest-validator-${var.environment}"
  description   = "Validates multi-source manifests from S3, coordinates batch state in DynamoDB, and notifies SNS/SQS"
  environment   = var.environment
  source_dir    = "${path.module}/../../../src/lambda/manifest_validator"
  handler       = "index.lambda_handler"
  runtime       = "python3.11"
  memory_size   = 256
  timeout       = 30

  s3_bucket_arn      = module.s3_bronze.bucket_arn
  dynamodb_table_arn = module.dynamodb_batch_coordination.table_arn
  sqs_queue_arn      = module.sqs_batch_ready.queue_arn
  sns_topic_arn      = module.sns_batch_events.topic_arn

  environment_variables = {
    DYNAMODB_TABLE   = module.dynamodb_batch_coordination.table_name
    SQS_QUEUE_URL    = module.sqs_batch_ready.queue_url
    SNS_TOPIC_ARN    = module.sns_batch_events.topic_arn
    REQUIRED_SOURCES = "api,file_share,oracle"
  }

  tags = var.tags
}

# ==============================================================================
# S3 Event Notification: Trigger Lambda on Manifest Upload
# ==============================================================================
resource "aws_lambda_permission" "allow_s3_bronze_manifest" {
  statement_id  = "AllowExecutionFromS3Bronze"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_manifest_validator.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = module.s3_bronze.bucket_arn
}

resource "aws_s3_bucket_notification" "bronze_manifest_notification" {
  bucket = module.s3_bronze.bucket_id

  lambda_function {
    lambda_function_arn = module.lambda_manifest_validator.function_arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = ".manifest"
  }

  depends_on = [
    aws_lambda_permission.allow_s3_bronze_manifest
  ]
}

# ==============================================================================
# Production-Grade CloudWatch Unified Platform Dashboard & Alarms
# ==============================================================================
module "monitoring" {
  source = "../../modules/monitoring"

  project_name   = var.project_name
  environment    = var.environment
  aws_region     = var.aws_region
  dashboard_name = "${var.project_name}-data-platform-dashboard-${var.environment}"

  # Serverless Barrier & Event Coordination
  lambda_function_name = module.lambda_manifest_validator.function_name
  dynamodb_table_name  = module.dynamodb_batch_coordination.table_name

  # Messaging & Decoupling
  sqs_primary_queue_name = module.sqs_batch_ready.queue_name
  sqs_dlq_name           = "${module.sqs_batch_ready.queue_name}-dlq"
  sqs_dlq_alarm_arn      = aws_cloudwatch_metric_alarm.sqs_dlq_alarm.arn

  # SNS Topics
  sns_batch_topic_name  = module.sns_batch_events.topic_name
  sns_alerts_topic_name = module.sns_pipeline_alerts.topic_name
  sns_alerts_topic_arn  = module.sns_pipeline_alerts.topic_arn

  # Distributed PySpark ETL Jobs
  glue_parquet_job_name = module.glue_bronze_to_silver.job_name
  glue_delta_job_name   = module.glue_bronze_to_silver_delta.job_name
  glue_iceberg_job_name = module.glue_bronze_to_silver_iceberg.job_name
  glue_crawler_name     = aws_glue_crawler.silver_parquet_crawler.name

  # Analytics & Storage
  athena_workgroup_name      = aws_athena_workgroup.dev.name
  bronze_bucket_name         = module.s3_bronze.bucket_name
  silver_bucket_name         = module.s3_silver.bucket_name
  scripts_bucket_name        = module.s3_scripts.bucket_name
  athena_results_bucket_name = module.s3_athena_results.bucket_name

  tags = var.tags
}





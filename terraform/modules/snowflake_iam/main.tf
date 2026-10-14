locals {
  role_name                 = "${var.project_name}-snowflake-iceberg-role-${var.environment}"
  external_ids              = compact([var.snowflake_storage_aws_external_id, var.snowflake_catalog_aws_external_id])
  has_snowflake_credentials = var.snowflake_storage_aws_iam_user_arn != "" && length(local.external_ids) > 0
}

# ==============================================================================
# IAM Trust Policy (Conditional Handshake)
# ==============================================================================
data "aws_iam_policy_document" "snowflake_trust_policy" {
  statement {
    sid     = "SnowflakeAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type = "AWS"
      identifiers = [
        local.has_snowflake_credentials ? var.snowflake_storage_aws_iam_user_arn : "arn:aws:iam::${var.aws_account_id}:root"
      ]
    }

    dynamic "condition" {
      for_each = local.has_snowflake_credentials ? [1] : []
      content {
        test     = "StringEquals"
        variable = "sts:ExternalId"
        values   = local.external_ids
      }
    }
  }
}

# ==============================================================================
# IAM Role for Snowflake Iceberg & Glue Integration
# ==============================================================================
resource "aws_iam_role" "snowflake_iceberg_role" {
  name               = local.role_name
  description        = "Cross-account IAM role assumed by Snowflake to query Apache Iceberg tables in S3 Silver via AWS Glue Catalog"
  assume_role_policy = data.aws_iam_policy_document.snowflake_trust_policy.json

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      Service     = "Snowflake"
      Component   = "ApacheIceberg"
      ManagedBy   = "Terraform"
    }
  )
}

# ==============================================================================
# IAM Policy: S3 Silver Layer & Glue Catalog Read Permissions
# ==============================================================================
data "aws_iam_policy_document" "snowflake_iceberg_policy" {
  # 1. S3 Silver Bucket Location
  statement {
    sid    = "SnowflakeS3BucketLocation"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation"
    ]
    resources = [
      var.silver_bucket_arn
    ]
  }

  # 2. S3 Silver Bucket List
  statement {
    sid    = "SnowflakeS3BucketList"
    effect = "Allow"
    actions = [
      "s3:ListBucket"
    ]
    resources = [
      var.silver_bucket_arn
    ]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["iceberg/*", "iceberg*", ""]
    }
  }

  # 3. S3 Silver Object Level Permissions
  statement {
    sid    = "SnowflakeS3ObjectAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    resources = [
      "${var.silver_bucket_arn}/iceberg/*"
    ]
  }

  # 3. AWS Glue Data Catalog Read Permissions
  statement {
    sid    = "SnowflakeGlueCatalogRead"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartitions",
      "glue:GetPartition"
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:catalog",
      "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:database/${var.glue_database_name}",
      "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:table/${var.glue_database_name}/*"
    ]
  }
}

resource "aws_iam_policy" "snowflake_iceberg_access" {
  name        = "${var.project_name}-snowflake-iceberg-access-${var.environment}"
  description = "Grants Snowflake read permissions to Iceberg Parquet files in Silver S3 and Glue Catalog metadata"
  policy      = data.aws_iam_policy_document.snowflake_iceberg_policy.json

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      Service     = "Snowflake"
      ManagedBy   = "Terraform"
    }
  )
}

resource "aws_iam_role_policy_attachment" "snowflake_iceberg_attach" {
  role       = aws_iam_role.snowflake_iceberg_role.name
  policy_arn = aws_iam_policy.snowflake_iceberg_access.arn
}

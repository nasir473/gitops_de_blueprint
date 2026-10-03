# ==============================================================================
# IAM Role for AWS Glue Job Execution (Created if role_arn is not provided)
# ==============================================================================
locals {
  create_role = var.role_arn == ""
}

data "aws_iam_policy_document" "glue_assume_role" {
  count = local.create_role ? 1 : 0

  statement {
    sid     = "GlueAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "this" {
  count = local.create_role ? 1 : 0

  name               = "${var.job_name}-role"
  description        = "Execution role for AWS Glue job ${var.job_name}"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role[0].json

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
      Name        = "${var.job_name}-role"
    }
  )
}

# Attach AWS managed Glue service role
resource "aws_iam_role_policy_attachment" "glue_service" {
  count = local.create_role ? 1 : 0

  role       = aws_iam_role.this[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# S3 Access Policy for Bronze (Read), Silver (Read/Write), and Scripts (Read)
data "aws_iam_policy_document" "s3_access" {
  count = local.create_role ? 1 : 0

  statement {
    sid    = "S3SourceAndScriptsRead"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = compact([
      var.source_bucket_arn,
      "${var.source_bucket_arn}/*",
      var.scripts_bucket_arn,
      "${var.scripts_bucket_arn}/*"
    ])
  }

  statement {
    sid    = "S3TargetReadWrite"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket"
    ]
    resources = compact([
      var.target_bucket_arn,
      "${var.target_bucket_arn}/*"
    ])
  }
}

resource "aws_iam_policy" "s3_access" {
  count = local.create_role ? 1 : 0

  name        = "${var.job_name}-s3-policy"
  description = "Least privilege S3 access policy for Glue job ${var.job_name}"
  policy      = data.aws_iam_policy_document.s3_access[0].json

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

resource "aws_iam_role_policy_attachment" "s3_access" {
  count = local.create_role ? 1 : 0

  role       = aws_iam_role.this[0].name
  policy_arn = aws_iam_policy.s3_access[0].arn
}

# ==============================================================================
# AWS Glue ETL Job Resource
# ==============================================================================
resource "aws_glue_job" "this" {
  name        = var.job_name
  description = var.job_description
  role_arn    = local.create_role ? aws_iam_role.this[0].arn : var.role_arn

  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  max_retries       = var.max_retries
  timeout           = var.timeout

  command {
    name            = "glueetl"
    script_location = var.script_location
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = var.max_concurrent_runs
  }

  default_arguments = merge(
    {
      "--job-language"                     = "python"
      "--continuous-log-logGroup"          = "/aws-glue/jobs/${var.job_name}"
      "--enable-continuous-cloudwatch-log" = "true"
      "--enable-continuous-log-filter"     = "true"
      "--enable-metrics"                   = "true"
      "--enable-observability-metrics"     = "true"
      "--enable-auto-scaling"              = "true"
    },
    var.default_arguments
  )

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
      Name        = var.job_name
    }
  )

  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy_attachment.s3_access
  ]
}

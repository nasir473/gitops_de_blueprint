# ==============================================================================
# Archive Lambda Source Code
# ==============================================================================
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = var.source_dir
  output_path = "${path.module}/../../.terraform/${var.function_name}.zip"
}

# ==============================================================================
# IAM Role & Assume Role Policy
# ==============================================================================
data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "this" {
  name               = "${var.function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json

  tags = merge(
    var.tags,
    {
      Name        = "${var.function_name}-role"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

# Standard CloudWatch Logging
resource "aws_iam_role_policy_attachment" "basic_execution" {
  role       = aws_iam_role.this.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# CloudWatch Log Group with retention
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = 14

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

# ==============================================================================
# Custom IAM Policy: S3 Bronze + DynamoDB + SQS Access
# ==============================================================================
data "aws_iam_policy_document" "lambda_permissions" {
  statement {
    sid    = "S3BronzeManifestRead"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion"
    ]
    resources = [
      "${var.s3_bucket_arn}/*"
    ]
  }

  statement {
    sid    = "DynamoDBBatchStateCoordination"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:Query"
    ]
    resources = [
      var.dynamodb_table_arn
    ]
  }

  statement {
    sid    = "SQSBatchReadyPublish"
    effect = "Allow"
    actions = [
      "sqs:SendMessage",
      "sqs:GetQueueAttributes"
    ]
    resources = [
      var.sqs_queue_arn
    ]
  }

  dynamic "statement" {
    for_each = var.sns_topic_arn != "" ? [var.sns_topic_arn] : []
    content {
      sid    = "SNSTopicPublish"
      effect = "Allow"
      actions = [
        "sns:Publish"
      ]
      resources = [
        statement.value
      ]
    }
  }
}

resource "aws_iam_policy" "lambda_permissions" {
  name        = "${var.function_name}-policy"
  description = "Allows Lambda to read manifests from S3, coordinate state in DynamoDB, and notify SQS"
  policy      = data.aws_iam_policy_document.lambda_permissions.json

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

resource "aws_iam_role_policy_attachment" "lambda_permissions_attach" {
  role       = aws_iam_role.this.name
  policy_arn = aws_iam_policy.lambda_permissions.arn
}

# ==============================================================================
# AWS Lambda Function
# ==============================================================================
resource "aws_lambda_function" "this" {
  function_name    = var.function_name
  description      = var.description
  role             = aws_iam_role.this.arn
  handler          = var.handler
  runtime          = var.runtime
  memory_size      = var.memory_size
  timeout          = var.timeout
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  dynamic "environment" {
    for_each = length(var.environment_variables) > 0 ? [1] : []
    content {
      variables = var.environment_variables
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.lambda_logs,
    aws_iam_role_policy_attachment.basic_execution,
    aws_iam_role_policy_attachment.lambda_permissions_attach
  ]

  tags = merge(
    var.tags,
    {
      Name        = var.function_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

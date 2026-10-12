# ==============================================================================
# Amazon SNS Topic
# ==============================================================================
resource "aws_sns_topic" "this" {
  name              = var.topic_name
  display_name      = var.display_name != "" ? var.display_name : var.topic_name
  kms_master_key_id = var.kms_master_key_id

  tags = merge(
    var.tags,
    {
      Name        = var.topic_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

# ==============================================================================
# SQS Subscriptions (Fan-out Pattern with Raw Message Delivery)
# ==============================================================================
resource "aws_sns_topic_subscription" "sqs" {
  for_each = toset(var.sqs_subscribers)

  topic_arn            = aws_sns_topic.this.arn
  protocol             = "sqs"
  endpoint             = each.value
  raw_message_delivery = true
}

# ==============================================================================
# Email Subscriptions (Alerting / Incident Management)
# ==============================================================================
resource "aws_sns_topic_subscription" "email" {
  for_each = toset(var.email_subscribers)

  topic_arn = aws_sns_topic.this.arn
  protocol  = "email"
  endpoint  = each.value
}

# ==============================================================================
# SNS Topic Policy (Permit Account, CloudWatch Alarms, and Lambda to Publish)
# ==============================================================================
data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "topic_policy" {
  statement {
    sid    = "DefaultTopicOwnerAccess"
    effect = "Allow"
    actions = [
      "SNS:GetTopicAttributes",
      "SNS:SetTopicAttributes",
      "SNS:AddPermission",
      "SNS:RemovePermission",
      "SNS:DeleteTopic",
      "SNS:Subscribe",
      "SNS:ListSubscriptionsByTopic",
      "SNS:Publish"
    ]
    principals {
      type        = "AWS"
      identifiers = ["*"]
    }
    resources = [aws_sns_topic.this.arn]
    condition {
      test     = "StringEquals"
      variable = "AWS:SourceOwner"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }

  statement {
    sid    = "AllowCloudWatchAlarmsToPublish"
    effect = "Allow"
    actions = [
      "SNS:Publish"
    ]
    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }
    resources = [aws_sns_topic.this.arn]
  }

  statement {
    sid    = "AllowEventBridgeToPublish"
    effect = "Allow"
    actions = [
      "SNS:Publish"
    ]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    resources = [aws_sns_topic.this.arn]
  }
}

resource "aws_sns_topic_policy" "this" {
  arn    = aws_sns_topic.this.arn
  policy = data.aws_iam_policy_document.topic_policy.json
}

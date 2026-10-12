# ==============================================================================
# Dead Letter Queue (DLQ)
# ==============================================================================
resource "aws_sqs_queue" "dlq" {
  count = var.enable_dlq ? 1 : 0

  name                      = "${var.queue_name}-dlq"
  message_retention_seconds = 1209600 # 14 days retention for debugging failures
  sqs_managed_sse_enabled   = true

  tags = merge(
    var.tags,
    {
      Name        = "${var.queue_name}-dlq"
      Environment = var.environment
      Type        = "DeadLetterQueue"
      ManagedBy   = "Terraform"
    }
  )
}

# ==============================================================================
# Primary SQS Queue with Redrive Policy
# ==============================================================================
resource "aws_sqs_queue" "this" {
  name                       = var.queue_name
  visibility_timeout_seconds = var.visibility_timeout_seconds
  message_retention_seconds  = var.message_retention_seconds
  delay_seconds              = var.delay_seconds
  max_message_size           = var.max_message_size
  receive_wait_time_seconds  = var.receive_wait_time_seconds
  sqs_managed_sse_enabled    = true

  redrive_policy = var.enable_dlq ? jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq[0].arn
    maxReceiveCount     = var.max_receive_count
  }) : null

  tags = merge(
    var.tags,
    {
      Name        = var.queue_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

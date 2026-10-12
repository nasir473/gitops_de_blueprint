output "queue_id" {
  description = "The URL of the primary SQS queue"
  value       = aws_sqs_queue.this.id
}

output "queue_arn" {
  description = "The ARN of the primary SQS queue"
  value       = aws_sqs_queue.this.arn
}

output "queue_url" {
  description = "The URL of the primary SQS queue"
  value       = aws_sqs_queue.this.url
}

output "queue_name" {
  description = "The name of the primary SQS queue"
  value       = aws_sqs_queue.this.name
}

output "dlq_arn" {
  description = "The ARN of the Dead Letter Queue (if enabled)"
  value       = var.enable_dlq ? aws_sqs_queue.dlq[0].arn : null
}

output "dlq_url" {
  description = "The URL of the Dead Letter Queue (if enabled)"
  value       = var.enable_dlq ? aws_sqs_queue.dlq[0].url : null
}

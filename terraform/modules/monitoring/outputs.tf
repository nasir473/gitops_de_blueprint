output "dashboard_name" {
  description = "The name of the CloudWatch dashboard"
  value       = aws_cloudwatch_dashboard.platform_dashboard.dashboard_name
}

output "dashboard_arn" {
  description = "The ARN of the CloudWatch dashboard"
  value       = aws_cloudwatch_dashboard.platform_dashboard.dashboard_arn
}

output "lambda_errors_alarm_name" {
  description = "Name of the Lambda errors CloudWatch metric alarm"
  value       = aws_cloudwatch_metric_alarm.lambda_errors.alarm_name
}

output "lambda_errors_alarm_arn" {
  description = "ARN of the Lambda errors CloudWatch metric alarm"
  value       = aws_cloudwatch_metric_alarm.lambda_errors.arn
}

output "role_arn" {
  description = "The ARN of the IAM role for Snowflake to assume"
  value       = aws_iam_role.snowflake_iceberg_role.arn
}

output "role_name" {
  description = "The name of the IAM role for Snowflake"
  value       = aws_iam_role.snowflake_iceberg_role.name
}

output "policy_arn" {
  description = "The ARN of the IAM policy granting Iceberg and Glue catalog read permissions"
  value       = aws_iam_policy.snowflake_iceberg_access.arn
}

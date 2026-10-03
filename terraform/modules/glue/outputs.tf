output "job_name" {
  description = "The name of the Glue job"
  value       = aws_glue_job.this.name
}

output "job_arn" {
  description = "The ARN of the Glue job"
  value       = aws_glue_job.this.arn
}

output "job_id" {
  description = "The ID of the Glue job"
  value       = aws_glue_job.this.id
}

output "role_arn" {
  description = "The ARN of the IAM role used by the Glue job"
  value       = local.create_role ? aws_iam_role.this[0].arn : var.role_arn
}

output "role_name" {
  description = "The name of the IAM role used by the Glue job"
  value       = local.create_role ? aws_iam_role.this[0].name : ""
}

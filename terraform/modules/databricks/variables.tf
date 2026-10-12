variable "environment" {
  description = "Target deployment environment (dev, qa, prod)"
  type        = string
}

variable "project_name" {
  description = "Project name prefix"
  type        = string
}

variable "silver_bucket_name" {
  description = "Name of the Silver S3 bucket"
  type        = string
}

variable "storage_credential_name" {
  description = "Name of the Databricks storage credential"
  type        = string
  default     = "gitops-silver-delta-credential"
}

variable "iam_role_arn" {
  description = "ARN of the AWS IAM role for Unity Catalog storage credential"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}

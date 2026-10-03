variable "job_name" {
  description = "The name of the AWS Glue job"
  type        = string
}

variable "job_description" {
  description = "Description of the AWS Glue job"
  type        = string
  default     = "AWS Glue PySpark ETL job managed by Terraform"
}

variable "role_arn" {
  description = "IAM role ARN to use for the Glue job. If empty, a least-privilege role is created automatically."
  type        = string
  default     = ""
}

variable "script_location" {
  description = "S3 URI to the Glue PySpark script (e.g. s3://bucket/glue/jobs/script.py)"
  type        = string
}

variable "glue_version" {
  description = "The version of AWS Glue to use (e.g. 4.0)"
  type        = string
  default     = "4.0"
}

variable "worker_type" {
  description = "The type of predefined worker that is allocated to the job (G.1X, G.2X, Standard)"
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  description = "The number of workers of a defined workerType that are allocated when a job runs"
  type        = number
  default     = 2
}

variable "max_retries" {
  description = "The maximum number of times to retry this job if it fails"
  type        = number
  default     = 0
}

variable "timeout" {
  description = "The job timeout in minutes"
  type        = number
  default     = 30
}

variable "max_concurrent_runs" {
  description = "Maximum number of concurrent runs allowed for this Glue job"
  type        = number
  default     = 1
}

variable "default_arguments" {
  description = "A map of default arguments for this job"
  type        = map(string)
  default     = {}
}

variable "source_bucket_arn" {
  description = "ARN of source S3 bucket for least-privilege IAM policy generation"
  type        = string
  default     = ""
}

variable "target_bucket_arn" {
  description = "ARN of target S3 bucket for least-privilege IAM policy generation"
  type        = string
  default     = ""
}

variable "scripts_bucket_arn" {
  description = "ARN of scripts S3 bucket for least-privilege IAM policy generation"
  type        = string
  default     = ""
}

variable "environment" {
  description = "Environment identifier (dev, qa, prod)"
  type        = string
  default     = "dev"
}

variable "tags" {
  description = "Tags to assign to the Glue resources"
  type        = map(string)
  default     = {}
}

variable "aws_region" {
  description = "The AWS region to deploy resources into"
  type        = string
  default     = "ap-south-1"
}

variable "environment" {
  description = "Target environment stage (dev, qa, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name prefix used for resource identification"
  type        = string
  default     = "gitops"
}

variable "aws_account_id" {
  description = "AWS account ID (defaults to active session caller identity if blank)"
  type        = string
  default     = ""
}

variable "force_destroy" {
  description = "Allow destroying S3 buckets with objects inside (safe for dev)"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default = {
    Owner      = "DataEngineering"
    ManagedBy  = "Terraform"
    Repository = "gitops_de_blueprint"
  }
}

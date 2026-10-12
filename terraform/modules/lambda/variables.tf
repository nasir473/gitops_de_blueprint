variable "function_name" {
  description = "Name of the Lambda function"
  type        = string
}

variable "description" {
  description = "Description of the Lambda function"
  type        = string
  default     = "Lambda function deployed via Terraform"
}

variable "environment" {
  description = "Target deployment environment (dev, qa, prod)"
  type        = string
}

variable "handler" {
  description = "Function entrypoint in the code"
  type        = string
  default     = "index.lambda_handler"
}

variable "runtime" {
  description = "Lambda runtime language"
  type        = string
  default     = "python3.11"
}

variable "memory_size" {
  description = "Amount of memory in MB allocated to the Lambda function"
  type        = number
  default     = 256
}

variable "timeout" {
  description = "Amount of time function has to run in seconds"
  type        = number
  default     = 30
}

variable "source_dir" {
  description = "Path to the local directory containing Lambda source code"
  type        = string
}

variable "environment_variables" {
  description = "Key-value pairs of environment variables for the function"
  type        = map(string)
  default     = {}
}

variable "s3_bucket_arn" {
  description = "ARN of the S3 bucket the Lambda needs to read from"
  type        = string
}

variable "dynamodb_table_arn" {
  description = "ARN of the DynamoDB table the Lambda needs access to"
  type        = string
}

variable "sqs_queue_arn" {
  description = "ARN of the SQS queue the Lambda needs to publish to"
  type        = string
}

variable "sns_topic_arn" {
  description = "Optional ARN of the SNS topic the Lambda publishes batch ready events to"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}

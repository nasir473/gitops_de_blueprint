variable "topic_name" {
  description = "The name of the SNS topic"
  type        = string
}

variable "environment" {
  description = "Deployment environment (e.g., dev, qa, prod)"
  type        = string
}

variable "display_name" {
  description = "The display name for SNS topic (used in SMS/Email headers)"
  type        = string
  default     = ""
}

variable "kms_master_key_id" {
  description = "The ID of an AWS-managed customer master key (CMK) for Amazon SNS or custom CMK"
  type        = string
  default     = "alias/aws/sns"
}

variable "sqs_subscribers" {
  description = "List of SQS Queue ARNs to subscribe to this SNS topic"
  type        = list(string)
  default     = []
}

variable "email_subscribers" {
  description = "List of email addresses to subscribe to this SNS topic"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Common tags applied to resources"
  type        = map(string)
  default     = {}
}

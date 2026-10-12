variable "queue_name" {
  description = "Name of the primary SQS queue"
  type        = string
}

variable "environment" {
  description = "Target deployment environment (dev, qa, prod)"
  type        = string
}

variable "visibility_timeout_seconds" {
  description = "The visibility timeout for the queue in seconds"
  type        = number
  default     = 300
}

variable "message_retention_seconds" {
  description = "The number of seconds Amazon SQS retains a message"
  type        = number
  default     = 345600 # 4 days
}

variable "delay_seconds" {
  description = "The time in seconds that the delivery of all messages in the queue will be delayed"
  type        = number
  default     = 0
}

variable "max_message_size" {
  description = "The limit of how many bytes a message can contain"
  type        = number
  default     = 262144 # 256 KB
}

variable "receive_wait_time_seconds" {
  description = "The time for which a ReceiveMessage call will wait for a message to arrive (long polling)"
  type        = number
  default     = 20
}

variable "enable_dlq" {
  description = "Whether to create and attach a Dead Letter Queue"
  type        = bool
  default     = true
}

variable "max_receive_count" {
  description = "Maximum number of times a message can be delivered before sending to DLQ"
  type        = number
  default     = 3
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}

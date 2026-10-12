variable "table_name" {
  description = "Name of the DynamoDB table"
  type        = string
}

variable "environment" {
  description = "Target deployment environment (dev, qa, prod)"
  type        = string
}

variable "hash_key" {
  description = "The attribute to use as the hash (partition) key"
  type        = string
  default     = "batch_date"
}

variable "billing_mode" {
  description = "Controls how you are charged for read and write throughput"
  type        = string
  default     = "PAY_PER_REQUEST"
}

variable "enable_ttl" {
  description = "Indicates whether TTL is enabled on the table"
  type        = bool
  default     = true
}

variable "ttl_attribute_name" {
  description = "The name of the table attribute to store the TTL timestamp in"
  type        = string
  default     = "ttl_timestamp"
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}

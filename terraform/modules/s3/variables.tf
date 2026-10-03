variable "bucket_name" {
  description = "The globally unique name of the S3 bucket"
  type        = string
}

variable "environment" {
  description = "Environment identifier (e.g. dev, qa, prod)"
  type        = string
}

variable "layer" {
  description = "Medallion architecture layer identifier (Bronze, Silver, Gold, Quarantine, Athena)"
  type        = string
  default     = "Raw"
}

variable "force_destroy" {
  description = "Whether to delete all objects from the bucket so that the bucket can be destroyed without error"
  type        = bool
  default     = false
}

variable "versioning_enabled" {
  description = "Enable versioning for S3 bucket objects"
  type        = bool
  default     = true
}

variable "transition_ia_days" {
  description = "Number of days before transitioning current objects to Standard-IA"
  type        = number
  default     = 30
}

variable "transition_glacier_days" {
  description = "Number of days before transitioning current objects to Glacier Flexible Retrieval"
  type        = number
  default     = 90
}

variable "enable_tiering" {
  description = "Enable lifecycle transitions to Standard-IA and Glacier for current objects"
  type        = bool
  default     = true
}

variable "noncurrent_version_expiration_days" {
  description = "Number of days before permanently expiring noncurrent object versions"
  type        = number
  default     = 90
}

variable "tags" {
  description = "Additional tags to merge with bucket resources"
  type        = map(string)
  default     = {}
}

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.40.0"
    }
  }

  # Backend configuration will be added in Phase 2
  # backend "s3" {}
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "aws-data-platform"
      Environment = "dev"
      ManagedBy   = "Terraform"
    }
  }
}

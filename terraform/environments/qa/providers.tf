terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Backend configuration will be added in Phase 2
  # backend "s3" {}
}

provider "aws" {
  region = "us-east-1"

  default_tags {
    tags = {
      Project     = "aws-data-platform"
      Environment = "qa"
      ManagedBy   = "Terraform"
    }
  }
}

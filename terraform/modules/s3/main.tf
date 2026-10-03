# ==============================================================================
# S3 Storage Bucket
# ==============================================================================
resource "aws_s3_bucket" "this" {
  bucket        = var.bucket_name
  force_destroy = var.force_destroy

  tags = merge(
    {
      Layer       = var.layer
      Environment = var.environment
      ManagedBy   = "Terraform"
    },
    var.tags,
    {
      Name = var.bucket_name
    }
  )
}

# ==============================================================================
# Bucket Ownership Controls (Disable ACLs, Enforce Bucket Owner)
# ==============================================================================
resource "aws_s3_bucket_ownership_controls" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# ==============================================================================
# Block Public Access (100% Private Bucket)
# ==============================================================================
resource "aws_s3_bucket_public_access_block" "this" {
  bucket = aws_s3_bucket.this.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ==============================================================================
# Bucket Versioning
# ==============================================================================
resource "aws_s3_bucket_versioning" "this" {
  bucket = aws_s3_bucket.this.id

  versioning_configuration {
    status = var.versioning_enabled ? "Enabled" : "Suspended"
  }
}

# ==============================================================================
# Server-Side Encryption (SSE-S3 AES256 with S3 Bucket Keys)
# ==============================================================================
resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# ==============================================================================
# Lifecycle Rules (Multipart Abort & Medallion Storage Transitions)
# ==============================================================================
resource "aws_s3_bucket_lifecycle_configuration" "this" {
  depends_on = [aws_s3_bucket_versioning.this]
  bucket     = aws_s3_bucket.this.id

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }

    filter {}
  }

  dynamic "rule" {
    for_each = var.enable_tiering ? [1] : []
    content {
      id     = "raw-data-tiering-and-cleanup"
      status = "Enabled"

      filter {}

      transition {
        days          = var.transition_ia_days
        storage_class = "STANDARD_IA"
      }

      transition {
        days          = var.transition_glacier_days
        storage_class = "GLACIER"
      }

      noncurrent_version_expiration {
        noncurrent_days = var.noncurrent_version_expiration_days
      }
    }
  }

  dynamic "rule" {
    for_each = !var.enable_tiering && var.noncurrent_version_expiration_days > 0 ? [1] : []
    content {
      id     = "noncurrent-version-cleanup"
      status = "Enabled"

      filter {}

      noncurrent_version_expiration {
        noncurrent_days = var.noncurrent_version_expiration_days
      }
    }
  }
}

# ==============================================================================
# TLS Enforcement Policy (Deny Insecure HTTP Requests)
# ==============================================================================
data "aws_iam_policy_document" "enforce_tls" {
  statement {
    sid     = "EnforceTLSRequestsOnly"
    effect  = "Deny"
    actions = ["s3:*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    resources = [
      aws_s3_bucket.this.arn,
      "${aws_s3_bucket.this.arn}/*"
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "enforce_tls" {
  bucket = aws_s3_bucket.this.id
  policy = data.aws_iam_policy_document.enforce_tls.json

  depends_on = [aws_s3_bucket_public_access_block.this]
}

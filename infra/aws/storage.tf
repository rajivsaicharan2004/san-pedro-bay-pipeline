# AWS's S3 Free Tier is 5 GB standard storage (12 months from account
# creation) -- noticeably tighter than OCI's 20 GB always-free. The
# aggressive 14-day bronze expiration below is what keeps sustained AIS
# ingestion from quietly walking past that ceiling.
resource "aws_s3_bucket" "lakehouse" {
  bucket = var.lakehouse_bucket_name
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "expire-bronze"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    expiration {
      days = var.lakehouse_bronze_retention_days
    }

    # Versioning is on, so without this, every deleted/overwritten
    # bronze/* object leaves a noncurrent version behind forever --
    # storage quietly grows even though the "current" listing looks
    # clean. Expire those too, on the same schedule.
    noncurrent_version_expiration {
      noncurrent_days = var.lakehouse_bronze_retention_days
    }
  }
}

# Chicken-and-egg: created manually first (backend.hcl.example has the
# aws s3api commands), then adopted here:
#   terraform import aws_s3_bucket.tfstate <bucket-name>
# Don't destroy this one without a plan for where state goes next.
resource "aws_s3_bucket" "tfstate" {
  bucket = var.tfstate_bucket_name
}

resource "aws_s3_bucket_versioning" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  versioning_configuration {
    status = "Enabled"
  }
}

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Empty on purpose, same reasoning as infra/oci/providers.tf: backend
  # blocks can't reference variables, and the bucket name is account-
  # specific. Real values via `terraform init -backend-config=backend.hcl`
  # (backend.hcl is gitignored; backend.hcl.example is the template).
  backend "s3" {}
}

provider "aws" {
  region = var.aws_region
}

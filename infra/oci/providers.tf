terraform {
  required_version = ">= 1.5.0"

  required_providers {
    oci = {
      source  = "oracle/oci"
      version = "~> 6.0"
    }
  }

  # Remote (S3-compat) backend is on hold -- the Customer Secret Key pair
  # needed to authenticate to it wasn't validating, and that's orthogonal
  # to actually provisioning the A1 instance before capacity runs out.
  # Local backend (the default when no `backend` block is present) is a
  # legitimate choice for a solo account with no one else's state to
  # share, not just a stopgap. To switch back to remote state later:
  # uncomment the block below and run `terraform init
  # -backend-config=backend.hcl -migrate-state` (backend.hcl is
  # gitignored; see backend.hcl.example). Terraform's `backend` block
  # can't reference variables, which is why the namespace lives in that
  # gitignored file rather than here.
  # backend "s3" {}
}

provider "oci" {
  tenancy_ocid     = var.tenancy_ocid
  user_ocid        = var.user_ocid
  fingerprint      = var.fingerprint
  private_key_path = var.private_key_path
  region           = var.region
}

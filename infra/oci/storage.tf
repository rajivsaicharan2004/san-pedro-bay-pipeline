# Bronze/raw data lands here from the pipeline; ~20 GB free total across
# the whole tenancy means an aggressive expiry, not a generous one.
resource "oci_objectstorage_bucket" "lakehouse" {
  compartment_id = var.compartment_ocid
  namespace      = var.object_storage_namespace
  name           = "san-pedro-bay-lakehouse"
  access_type    = "NoPublicAccess"
  versioning     = "Enabled"
}

resource "oci_objectstorage_object_lifecycle_policy" "lakehouse" {
  namespace = var.object_storage_namespace
  bucket    = oci_objectstorage_bucket.lakehouse.name

  rules {
    name        = "expire-bronze"
    action      = "DELETE"
    is_enabled  = true
    time_amount = var.lakehouse_bronze_retention_days
    time_unit   = "DAYS"

    object_name_filter {
      inclusion_prefixes = ["bronze/"]
    }
  }
}

# Chicken-and-egg: Terraform's own state needs a bucket to live in before
# Terraform can create anything, so this one is created manually in the
# console first (Storage -> Object Storage -> Buckets -> Create Bucket,
# name "tf-state"). This resource block lets it be brought under
# management afterward rather than left as an unmanaged manual artifact:
#
#   terraform import oci_objectstorage_bucket.tf_state <namespace>/tf-state
#
# Don't `terraform destroy` this one without a plan for where state goes
# next -- destroying it out from under an initialized backend orphans
# every other resource's state.
resource "oci_objectstorage_bucket" "tf_state" {
  compartment_id = var.compartment_ocid
  namespace      = var.object_storage_namespace
  name           = "tf-state"
  access_type    = "NoPublicAccess"
  versioning     = "Enabled"
}

# Dynamic group: lets the pipeline instance itself authenticate to OCI
# APIs via instance principal auth, so the lakehouse credentials Spark
# needs aren't a static API key sitting in a config file on the box.
resource "oci_identity_dynamic_group" "pipeline_instance" {
  compartment_id = var.tenancy_ocid # dynamic groups always live in the tenancy (root) compartment
  name           = "spb-pipeline-instance-dg"
  description    = "Matches the pipeline compute instance."
  matching_rule  = "ANY {instance.id = '${oci_core_instance.pipeline.id}'}"
}

resource "oci_identity_policy" "pipeline_instance_policy" {
  compartment_id = var.compartment_ocid
  name           = "spb-pipeline-instance-policy"
  description    = "Scoped to exactly the lakehouse bucket -- not object-storage-wide, not compartment-wide manage."

  statements = [
    "Allow dynamic-group ${oci_identity_dynamic_group.pipeline_instance.name} to manage objects in compartment id ${var.compartment_ocid} where target.bucket.name = '${oci_objectstorage_bucket.lakehouse.name}'",
    "Allow dynamic-group ${oci_identity_dynamic_group.pipeline_instance.name} to read buckets in compartment id ${var.compartment_ocid} where target.bucket.name = '${oci_objectstorage_bucket.lakehouse.name}'",
  ]
}

# Object Storage's own lifecycle-policy enforcement runs as a service
# principal, not as you or the pipeline instance -- without this, applying
# an object_lifecycle_policy resource fails with InsufficientServicePermissions
# even though the human/admin creating it has full rights, because the
# service itself was never granted access to act on the bucket it's
# enforcing rules against.
resource "oci_identity_policy" "objectstorage_service_policy" {
  compartment_id = var.compartment_ocid
  name           = "spb-objectstorage-service-policy"
  description    = "Lets the Object Storage service principal enforce lifecycle rules on the lakehouse bucket."

  # The service principal name is region-specific ("objectstorage-<region>"),
  # not the generic "objectstorage" -- using the generic name 400s with
  # "Service {objectstorage} does not exist" instead of creating the policy.
  statements = [
    "Allow service objectstorage-${var.region} to manage object-family in compartment id ${var.compartment_ocid} where target.bucket.name = '${oci_objectstorage_bucket.lakehouse.name}'",
  ]
}

# For a solo/personal tenancy the signup account is already tenancy admin,
# so this group is somewhat redundant in practice -- included because it
# was asked for, and because "the human operator's access is also an
# explicit, auditable IAM policy, not just implicit root" is the right
# habit even when it's not strictly load-bearing yet (e.g. before adding
# a second team member later).
resource "oci_identity_group" "pipeline_admins" {
  compartment_id = var.tenancy_ocid
  name           = "spb-pipeline-admins"
  description    = "Humans allowed to manage spb-pipeline resources."
}

resource "oci_identity_policy" "pipeline_admins_policy" {
  compartment_id = var.compartment_ocid
  name           = "spb-pipeline-admins-policy"
  description    = "Full manage rights over this project's resources for the admins group."

  statements = [
    "Allow group ${oci_identity_group.pipeline_admins.name} to manage instance-family in compartment id ${var.compartment_ocid}",
    "Allow group ${oci_identity_group.pipeline_admins.name} to manage virtual-network-family in compartment id ${var.compartment_ocid}",
    "Allow group ${oci_identity_group.pipeline_admins.name} to manage object-family in compartment id ${var.compartment_ocid}",
  ]
}

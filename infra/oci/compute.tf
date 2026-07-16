data "oci_identity_availability_domains" "ads" {
  compartment_id = var.tenancy_ocid
}

# Image OCIDs are region-specific and rotate as Canonical ships updates --
# a data source lookup instead of a hardcoded OCID is what keeps this
# working after the image you provisioned with goes stale.
data "oci_core_images" "ubuntu" {
  compartment_id           = var.compartment_ocid
  operating_system         = "Canonical Ubuntu"
  operating_system_version = "24.04"
  shape                    = "VM.Standard.A1.Flex"
  sort_by                  = "TIMECREATED"
  sort_order               = "DESC"
}

# The contested resource. Exactly 2 OCPU / 12 GB is the current Always
# Free ceiling for A1.Flex -- go over and this stops being free, not just
# harder to provision. "Out of host capacity" on apply is common and not
# a config error; see scripts/retry_apply.sh for the standard workaround.
resource "oci_core_instance" "pipeline" {
  compartment_id      = var.compartment_ocid
  availability_domain = data.oci_identity_availability_domains.ads.availability_domains[0].name
  shape               = "VM.Standard.A1.Flex"
  display_name        = "spb-pipeline"

  shape_config {
    ocpus         = var.instance_ocpus
    memory_in_gbs = var.instance_memory_gbs
  }

  source_details {
    source_type = "image"
    source_id   = data.oci_core_images.ubuntu.images[0].id
  }

  create_vnic_details {
    subnet_id        = oci_core_subnet.public.id
    assign_public_ip = true
  }

  metadata = {
    ssh_authorized_keys = file(var.ssh_public_key_path)
  }
}

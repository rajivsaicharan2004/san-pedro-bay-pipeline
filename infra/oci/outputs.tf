output "instance_public_ip" {
  description = "SSH here: ssh ubuntu@<this>"
  value       = oci_core_instance.pipeline.public_ip
}

output "object_storage_namespace" {
  description = "Same namespace used in backend.hcl -- also needed for the app-side S3A endpoint config once this box runs Spark against real OCI Object Storage instead of MinIO."
  value       = var.object_storage_namespace
}

output "lakehouse_bucket_name" {
  value = oci_objectstorage_bucket.lakehouse.name
}

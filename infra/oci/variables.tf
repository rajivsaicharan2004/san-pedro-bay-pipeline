variable "tenancy_ocid" {
  description = "OCID of the tenancy (Profile -> Tenancy)."
  type        = string
}

variable "user_ocid" {
  description = "OCID of the user Terraform authenticates as (Profile -> My Profile)."
  type        = string
}

variable "fingerprint" {
  description = "Fingerprint of the API signing key (Profile -> API Keys)."
  type        = string
}

variable "private_key_path" {
  description = "Path to the API signing private key downloaded to ~/.oci/."
  type        = string
  default     = "~/.oci/oci_api_key.pem"
}

variable "region" {
  description = "Home region. Always Free A1 compute only provisions in your home region."
  type        = string
  default     = "us-sanjose-1"
}

variable "compartment_ocid" {
  description = "Compartment to create resources in. Defaults to the tenancy (root compartment) if unset -- fine for a single-project account, not for a shared one."
  type        = string
}

variable "object_storage_namespace" {
  description = "Object Storage namespace for this tenancy (Storage -> Object Storage -> Buckets shows it, or `oci os ns get`). Needed to construct the tf-state backend endpoint and app-side S3A config."
  type        = string
}

variable "my_ip_cidr" {
  description = "Your public IP as a /32 CIDR (e.g. 203.0.113.4/32). The security list opens SSH/8080/9092/3000 to this and nothing else -- get it wrong and you lock yourself out, not the world in."
  type        = string
}

variable "ssh_public_key_path" {
  description = "Path to the SSH public key to inject into the instance via cloud-init metadata."
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "instance_ocpus" {
  description = "A1.Flex OCPU count. 2/12 is the current Always Free ceiling per instance -- see compute.tf."
  type        = number
  default     = 2
}

variable "instance_memory_gbs" {
  type    = number
  default = 12
}

variable "lakehouse_bronze_retention_days" {
  description = "Lifecycle rule: delete objects under bronze/ after this many days. ~20 GB free total means being aggressive here, not generous."
  type        = number
  default     = 14
}

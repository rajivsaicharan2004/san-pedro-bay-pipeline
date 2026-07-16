resource "oci_core_vcn" "main" {
  compartment_id = var.compartment_ocid
  cidr_blocks    = ["10.0.0.0/16"]
  display_name   = "spb-pipeline-vcn"
  dns_label      = "spbpipeline"
}

resource "oci_core_internet_gateway" "main" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.main.id
  display_name   = "spb-pipeline-igw"
  enabled        = true
}

resource "oci_core_route_table" "public" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.main.id
  display_name   = "spb-pipeline-public-rt"

  route_rules {
    destination       = "0.0.0.0/0"
    network_entity_id = oci_core_internet_gateway.main.id
  }
}

# Least-privilege line item: SSH and the three app ports open only to
# my_ip_cidr, nothing open to 0.0.0.0/0 on ingress. Egress is left open --
# restricting it buys nothing here (apt/docker pulls, AISStream websocket)
# and wasn't part of the ask.
resource "oci_core_security_list" "public" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.main.id
  display_name   = "spb-pipeline-public-sl"

  egress_security_rules {
    destination = "0.0.0.0/0"
    protocol    = "all"
  }

  ingress_security_rules {
    source   = var.my_ip_cidr
    protocol = "6" # TCP
    tcp_options {
      min = 22
      max = 22
    }
    description = "SSH"
  }

  ingress_security_rules {
    source   = var.my_ip_cidr
    protocol = "6"
    tcp_options {
      min = 8080
      max = 8080
    }
    description = "Redpanda Console"
  }

  ingress_security_rules {
    source   = var.my_ip_cidr
    protocol = "6"
    tcp_options {
      min = 9092
      max = 9092
    }
    description = "Kafka"
  }

  ingress_security_rules {
    source   = var.my_ip_cidr
    protocol = "6"
    tcp_options {
      min = 3000
      max = 3000
    }
    description = "Dagit"
  }
}

resource "oci_core_subnet" "public" {
  compartment_id             = var.compartment_ocid
  vcn_id                     = oci_core_vcn.main.id
  cidr_block                 = "10.0.1.0/24"
  display_name               = "spb-pipeline-public-subnet"
  dns_label                  = "public"
  route_table_id             = oci_core_route_table.public.id
  security_list_ids          = [oci_core_security_list.public.id]
  prohibit_public_ip_on_vnic = false
}

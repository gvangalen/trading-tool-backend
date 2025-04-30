# ✅ main.tf — Oracle Cloud Infrastructure (OCI) Terraform setup

provider "oci" {
  tenancy_ocid     = var.tenancy_ocid
  user_ocid        = var.user_ocid
  fingerprint      = var.fingerprint
  private_key_path = var.private_key_path
  region           = var.region
}

# ✅ VCN (Virtual Cloud Network)
resource "oci_core_vcn" "trading_vcn" {
  cidr_block = "10.0.0.0/16"
  compartment_id = var.compartment_ocid
  display_name = "trading_vcn"
}

# ✅ Public subnet
resource "oci_core_subnet" "public_subnet" {
  cidr_block = "10.0.1.0/24"
  compartment_id = var.compartment_ocid
  vcn_id = oci_core_vcn.trading_vcn.id
  display_name = "public_subnet"
  prohibit_public_ip_on_vnic = false
}

# ✅ Internet Gateway
resource "oci_core_internet_gateway" "igw" {
  compartment_id = var.compartment_ocid
  vcn_id = oci_core_vcn.trading_vcn.id
  display_name = "trading_igw"
  enabled = true
}

# ✅ Route table for internet access
resource "oci_core_route_table" "public_rt" {
  compartment_id = var.compartment_ocid
  vcn_id = oci_core_vcn.trading_vcn.id
  display_name = "public_rt"

  route_rules {
    destination = "0.0.0.0/0"
    destination_type = "CIDR_BLOCK"
    network_entity_id = oci_core_internet_gateway.igw.id
  }
}

# ✅ NSG (Network Security Group)
resource "oci_core_network_security_group" "nsg" {
  compartment_id = var.compartment_ocid
  vcn_id = oci_core_vcn.trading_vcn.id
  display_name = "trading_nsg"
}

# ✅ NSG Rules (Allow HTTP, HTTPS, PostgreSQL)
resource "oci_core_network_security_group_security_rule" "allow_http" {
  network_security_group_id = oci_core_network_security_group.nsg.id
  direction = "INGRESS"
  protocol = "6" # TCP
  source = "0.0.0.0/0"
  source_type = "CIDR_BLOCK"
  tcp_options {
    destination_port_range {
      min = 80
      max = 80
    }
  }
}

resource "oci_core_network_security_group_security_rule" "allow_https" {
  network_security_group_id = oci_core_network_security_group.nsg.id
  direction = "INGRESS"
  protocol = "6"
  source = "0.0.0.0/0"
  source_type = "CIDR_BLOCK"
  tcp_options {
    destination_port_range {
      min = 443
      max = 443
    }
  }
}

resource "oci_core_network_security_group_security_rule" "allow_postgres" {
  network_security_group_id = oci_core_network_security_group.nsg.id
  direction = "INGRESS"
  protocol = "6"
  source = "0.0.0.0/0"
  source_type = "CIDR_BLOCK"
  tcp_options {
    destination_port_range {
      min = 5432
      max = 5432
    }
  }
}

# ✅ VM Instance (Ubuntu voor app + database)
resource "oci_core_instance" "trading_vm" {
  availability_domain = data.oci_identity_availability_domains.ads.availability_domains[0].name
  compartment_id = var.compartment_ocid
  shape = "VM.Standard.E2.1.Micro"
  display_name = "trading-vm"

  source_details {
    source_type = "image"
    source_id   = data.oci_core_images.ubuntu_image.images[0].id
  }

  create_vnic_details {
    subnet_id              = oci_core_subnet.public_subnet.id
    assign_public_ip       = true
    nsg_ids                = [oci_core_network_security_group.nsg.id]
  }

  metadata = {
    ssh_authorized_keys = file(var.ssh_public_key_path)
  }
}

# ✅ Data sources

data "oci_identity_availability_domains" "ads" {
  compartment_id = var.compartment_ocid
}

data "oci_core_images" "ubuntu_image" {
  compartment_id = var.compartment_ocid
  operating_system = "Canonical Ubuntu"
  operating_system_version = "22.04"
  shape = "VM.Standard.E2.1.Micro"
  sort_by = "TIMECREATED"
  sort_order = "DESC"
}

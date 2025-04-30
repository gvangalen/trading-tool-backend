variable "tenancy_ocid" {}
variable "compartment_ocid" {}
variable "user_ocid" {}
variable "fingerprint" {}
variable "private_key_path" {}
variable "region" {
  default = "eu-frankfurt-1"  # Pas dit aan indien je een andere regio gebruikt
}

variable "ssh_public_key_path" {
  default = "~/.ssh/id_rsa.pub"
}

variable "postgres_password" {
  default = "SecurePassword123"  # ✳️ Pas dit aan en maak het veilig
}

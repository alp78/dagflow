variable "project_id" {
  type = string
}

variable "zone" {
  type = string
}

variable "instance_name" {
  type = string
}

variable "machine_type" {
  type    = string
  default = "e2-standard-4"
}

variable "boot_image" {
  type    = string
  default = "projects/debian-cloud/global/images/family/debian-12"
}

variable "service_account_email" {
  type = string
}

variable "startup_script" {
  type    = string
  default = <<-EOT
    #!/usr/bin/env bash
    set -euo pipefail
    apt-get update
    apt-get install -y docker.io
  EOT
}

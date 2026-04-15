variable "project_id" {
  type = string
}

variable "location" {
  type = string
}

variable "service_name" {
  type = string
}

variable "image" {
  type = string
}

variable "service_account_email" {
  type = string
}

variable "env" {
  type    = map(string)
  default = {}
}

variable "allow_public_access" {
  type    = bool
  default = true
}

variable "ingress" {
  type    = string
  default = "INGRESS_TRAFFIC_ALL"
}

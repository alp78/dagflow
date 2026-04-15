variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "instance_name" {
  type = string
}

variable "database_name" {
  type = string
}

variable "db_user_name" {
  type = string
}

variable "db_user_password" {
  type      = string
  sensitive = true
}

variable "database_version" {
  type    = string
  default = "POSTGRES_16"
}

variable "tier" {
  type    = string
  default = "db-custom-2-7680"
}

variable "availability_type" {
  type    = string
  default = "ZONAL"
}

variable "deletion_protection" {
  type    = bool
  default = true
}

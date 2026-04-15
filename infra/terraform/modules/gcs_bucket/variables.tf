variable "project_id" {
  type = string
}

variable "location" {
  type = string
}

variable "bucket_name" {
  type = string
}

variable "storage_class" {
  type    = string
  default = "STANDARD"
}

variable "force_destroy" {
  type    = bool
  default = false
}

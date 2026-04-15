variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "zone" {
  type = string
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "api_image" {
  type = string
}

variable "ui_image" {
  type = string
}

variable "db_user_password" {
  type      = string
  sensitive = true
}

variable "github_repository" {
  type = string
}

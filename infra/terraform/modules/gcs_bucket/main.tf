resource "google_storage_bucket" "this" {
  name                        = var.bucket_name
  location                    = var.location
  project                     = var.project_id
  storage_class               = var.storage_class
  uniform_bucket_level_access = true
  force_destroy               = var.force_destroy
}

resource "google_sql_database_instance" "this" {
  name                = var.instance_name
  database_version    = var.database_version
  project             = var.project_id
  region              = var.region
  deletion_protection = var.deletion_protection

  settings {
    tier              = var.tier
    availability_type = var.availability_type
    disk_type         = "PD_SSD"
    disk_size         = 50

    ip_configuration {
      ipv4_enabled = true
    }
  }
}

resource "google_sql_database" "this" {
  name     = var.database_name
  instance = google_sql_database_instance.this.name
  project  = var.project_id
}

resource "google_sql_user" "this" {
  name     = var.db_user_name
  instance = google_sql_database_instance.this.name
  project  = var.project_id
  password = var.db_user_password
}

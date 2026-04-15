resource "google_compute_instance" "this" {
  name         = var.instance_name
  project      = var.project_id
  zone         = var.zone
  machine_type = var.machine_type
  tags         = ["dagflow", "dagster"]

  boot_disk {
    initialize_params {
      image = var.boot_image
      size  = 50
      type  = "pd-ssd"
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  metadata = {
    enable-oslogin = "TRUE"
    startup-script = var.startup_script
  }

  service_account {
    email  = var.service_account_email
    scopes = ["cloud-platform"]
  }
}

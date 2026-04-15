locals {
  prefix = "${var.environment}-dagflow"
}

module "artifact_registry" {
  source        = "../../modules/artifact_registry"
  project_id    = var.project_id
  location      = var.region
  repository_id = "${local.prefix}-images"
}

module "app_service_account" {
  source       = "../../modules/service_account"
  project_id   = var.project_id
  account_id   = "${var.environment}-dagflow-app"
  display_name = "Dagflow application service account"
}

module "dagster_service_account" {
  source       = "../../modules/service_account"
  project_id   = var.project_id
  account_id   = "${var.environment}-dagflow-dagster"
  display_name = "Dagflow Dagster service account"
}

module "raw_bucket" {
  source      = "../../modules/gcs_bucket"
  project_id  = var.project_id
  location    = var.region
  bucket_name = "${local.prefix}-raw"
}

module "export_bucket" {
  source      = "../../modules/gcs_bucket"
  project_id  = var.project_id
  location    = var.region
  bucket_name = "${local.prefix}-export"
}

module "cloud_sql" {
  source           = "../../modules/cloud_sql_postgres"
  project_id       = var.project_id
  region           = var.region
  instance_name    = "${local.prefix}-sql"
  database_name    = "dagflow"
  db_user_name     = "dagflow"
  db_user_password = var.db_user_password
}

module "api_service" {
  source                = "../../modules/cloud_run_service"
  project_id            = var.project_id
  location              = var.region
  service_name          = "${local.prefix}-api"
  image                 = var.api_image
  service_account_email = module.app_service_account.email
  env = {
    APP_ENV = var.environment
  }
}

module "ui_service" {
  source                = "../../modules/cloud_run_service"
  project_id            = var.project_id
  location              = var.region
  service_name          = "${local.prefix}-ui"
  image                 = var.ui_image
  service_account_email = module.app_service_account.email
  env = {
    VITE_API_BASE_URL = module.api_service.uri
  }
}

module "dagster_vm" {
  source                = "../../modules/dagster_vm"
  project_id            = var.project_id
  zone                  = var.zone
  instance_name         = "${local.prefix}-dagster"
  service_account_email = module.dagster_service_account.email
}

module "wif" {
  source            = "../../modules/workload_identity_federation"
  project_id        = var.project_id
  pool_id           = "${var.environment}-dagflow-gh-pool"
  provider_id       = "${var.environment}-dagflow-gh"
  display_name      = "Dagflow GitHub OIDC"
  github_repository = var.github_repository
}

module "app_artifact_registry_reader" {
  source     = "../../modules/iam"
  project_id = var.project_id
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${module.app_service_account.email}"
}

module "dagster_artifact_registry_reader" {
  source     = "../../modules/iam"
  project_id = var.project_id
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${module.dagster_service_account.email}"
}

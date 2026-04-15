module "stack" {
  source = "../dev"

  project_id        = var.project_id
  region            = var.region
  zone              = var.zone
  environment       = var.environment
  api_image         = var.api_image
  ui_image          = var.ui_image
  db_user_password  = var.db_user_password
  github_repository = var.github_repository
}

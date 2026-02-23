# Service Account for Cloud Run job
resource "google_service_account" "fuel_ingestor_sa" {
  account_id   = "${var.APP_NAME}-fuel-ingestor"
  description  = "Fuel Prices Ingestor Service Account created by terraform"
  display_name = "Cloud Run Job Service Account for Fuel Prices Ingestor"
}

# Grant necessary permissions
resource "google_project_iam_member" "cloud_run_job_ingestor_storage_permissions" {
  for_each = toset([
    "roles/storage.objectCreator",
    "roles/storage.objectViewer",
  ])
  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.fuel_ingestor_sa.email}"
  role    = each.value
}
# Job Definition
resource "google_cloud_run_v2_job" "fuel_ingestor_job" {
  name     = "${var.APP_NAME}-fuel-ingestor-job"
  location = var.REGION

  template {
    template {
      timeout         = "1200s"
      max_retries     = 0
      service_account = google_service_account.fuel_ingestor_sa.email

      containers {
        image = "docker.io/${var.DOCKER_HUB_USERNAME}/${var.APP_NAME}-fuel-ingestor:${var.DOCKER_IMAGE_TAG}"
        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }
      }
    }
  }
}

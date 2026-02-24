# Service Account for Cloud Run job
resource "google_service_account" "fuel_ingestor_sa" {
  account_id   = "${var.APP_NAME}-fuel-ingestor"
  description  = "Fuel Prices Ingestor Service Account created by terraform"
  display_name = "Cloud Run Job Service Account for Fuel Prices Ingestor"
}

# Grant bucket-level permissions
resource "google_storage_bucket_iam_member" "fuel_ingestor_bucket_creator" {
  bucket = google_storage_bucket.fuel_prices_bucket.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.fuel_ingestor_sa.email}"
}

resource "google_storage_bucket_iam_member" "fuel_ingestor_bucket_viewer" {
  bucket = google_storage_bucket.fuel_prices_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.fuel_ingestor_sa.email}"
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

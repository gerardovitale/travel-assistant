# Service Account for Dashboard
resource "google_service_account" "fuel_dashboard_sa" {
  account_id   = "${var.APP_NAME}-fuel-dashboard"
  description  = "Fuel Dashboard Service Account created by terraform"
  display_name = "Cloud Run Service Account for Fuel Dashboard"
}

# Grant storage read access to fuel prices bucket
resource "google_storage_bucket_iam_member" "fuel_dashboard_bucket_viewer" {
  bucket = google_storage_bucket.fuel_prices_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.fuel_dashboard_sa.email}"
}

# Cloud Run Service
resource "google_cloud_run_v2_service" "fuel_dashboard" {
  name     = "${var.APP_NAME}-fuel-dashboard"
  location = var.REGION

  template {
    service_account = google_service_account.fuel_dashboard_sa.email

    scaling {
      min_instance_count = 0
      max_instance_count = 2
    }

    containers {
      image = "docker.io/${var.DOCKER_HUB_USERNAME}/${var.APP_NAME}-fuel-dashboard:${var.DOCKER_IMAGE_TAG}"

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "2Gi"
        }
      }
    }
  }
}

# Allow public access
resource "google_cloud_run_v2_service_iam_member" "fuel_dashboard_public" {
  project  = var.PROJECT
  location = var.REGION
  name     = google_cloud_run_v2_service.fuel_dashboard.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

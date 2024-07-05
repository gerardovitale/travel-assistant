resource "google_service_account" "ingest_fuel_price_service_account" {
  account_id   = "${var.PREFIX}-ingest-fuel-price"
  display_name = "Ingest fuel price Service Account"
}

resource "google_project_iam_member" "ingest_fuel_price_service_account_roles" {
  for_each = toset([
    "roles/run.invoker",
    "roles/secretmanager.secretAccessor",
    "roles/storage.objectAdmin",
    "roles/resourcemanager.tagAdmin",
  ])

  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.ingest_fuel_price_service_account.email}"
  role    = each.value
}

resource "google_cloud_run_service" "ingest_fuel_prices" {
  name     = "ingest-fuel-prices"
  location = var.REGION

  template {
    spec {
      containers {
        image = "docker.io/${var.DOCKER_HUB_USERNAME}/${var.REPO_NAME}-${var.G_CLOUD_RUN_INSTANCE_NAME}:latest"

        env {
          name  = "PROD"
          value = "true"
        }

        env {
          name  = "DATA_SOURCE_URL"
          value = var.DATA_SOURCE_URL
        }

        env {
          name  = "DATA_DESTINATION_BUCKET"
          value = var.DATA_DESTINATION_BUCKET
        }

        env {
          name  = "G_CLOUD_PROJECT_ID"
          value = var.PROJECT
        }

        env {
          name  = "G_CLOUD_RUN_INSTANCE_NAME"
          value = var.G_CLOUD_RUN_INSTANCE_NAME
        }

        env {
          name  = "G_CLOUD_RUN_APPLICATION_CREDENTIALS_PATH"
          value = var.G_CLOUD_RUN_APPLICATION_CREDENTIALS_PATH
        }

        env {
          name  = "G_CLOUD_RUN_SECRET_NAME"
          value = var.G_CLOUD_RUN_SECRET_NAME
        }
      }

      service_account_name = var.gcloud_funct_service_account
    }

    metadata {
      annotations = {
        "run.googleapis.com/launch-stage" = "GA"
      }

      labels = local.tags
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  autogenerate_revision_name = true
}

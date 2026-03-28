resource "google_storage_bucket" "fuel_prices_bucket" {
  name          = "travel-assistant-spain-fuel-prices"
  location      = var.REGION
  force_destroy = false
  labels        = local.tags

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      num_newer_versions = 5
    }
    action {
      type = "Delete"
    }
  }
}

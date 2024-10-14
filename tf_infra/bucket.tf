resource "google_storage_bucket" "fuel_prices_bucket" {
  name          = "${var.PROJECT}-spain-fuel-prices"
  location      = var.REGION
  force_destroy = true
  labels        = local.tags
}

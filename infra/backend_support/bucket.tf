resource "google_storage_bucket" "tf_state_bucket" {
  name          = "${var.PREFIX}-bucket-tf-state"
  force_destroy = true
  location      = var.REGION
  storage_class = "STANDARD"

  versioning {
    enabled = true
  }

  labels = local.labels
}

terraform {
  backend "gcs" {
    bucket  = "travass-bucket-tf-state"
    prefix  = "terraform/state"
  }
}

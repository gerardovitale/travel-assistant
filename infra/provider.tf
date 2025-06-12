terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.34.0"
    }
  }
}

provider "google" {
  project = var.PROJECT
  region  = var.REGION
  default_labels = {
    "environment" = "production",
    "project"     = var.PROJECT,
    "app"         = var.APP_NAME,
    "manage_by"   = "${var.APP_NAME}-remote-tf",
  }
}

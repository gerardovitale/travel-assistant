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
}

locals {
  tags = {
    "environment" = "production",
    "project"     = "travel-assistant",
    "manage_by"   = "cicd-terraform"
  }
}

resource "google_tags_tag_key" "keys" {
  for_each   = local.tags
  short_name = each.key
  parent     = "projects/${var.PROJECT}"
}

resource "google_tags_tag_value" "values" {
  for_each   = local.tags
  short_name = each.value
  parent     = "tagKeys/${google_tags_tag_key.keys[each.key].name}"
}

resource "google_tags_tag_binding" "bindings" {
  for_each  = local.tags
  tag_value = "tagValues/${google_tags_tag_value.values[each.key].id}"
  parent = "//cloudresourcemanager.googleapis.com/projects/${var.PROJECT}"
}

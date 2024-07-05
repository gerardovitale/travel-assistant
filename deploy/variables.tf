variable "PREFIX" {
  type        = string
  description = "Prefix to set tags help identify resources in AWS"
}

variable "PROJECT" {
  type        = string
  description = "Project ID"
}

variable "REGION" {
  type        = string
  description = "GCP region to deploy tf state related resources"
}

variable "REPO_NAME" {
  type        = string
  description = "Name of the target repository"
}

variable "DOCKER_HUB_USERNAME" {
  type        = string
  description = "Docker Hub username"
}

variable "DATA_SOURCE_URL" {
  type        = string
  description = "Data source URL"
}

variable "DATA_DESTINATION_BUCKET" {
  type        = string
  description = "Data destination bucket URI"
}

variable "G_CLOUD_RUN_INSTANCE_NAME" {
  type        = string
  description = "Cloud run instance name"
}

variable "G_CLOUD_RUN_APPLICATION_CREDENTIALS_PATH" {
  type        = string
  description = "Cloud storage connector keyfile path"
}

variable "G_CLOUD_RUN_SECRET_NAME" {
  type        = string
  description = "Cloud storage connector secret name"
}
